use std::convert::Infallible;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;

use warp::Filter;
use warp::http::HeaderValue;

use processor::RapidSyncProcessor;

#[tokio::main]
async fn main() {
	let processor = Arc::new(RapidSyncProcessor::new());
	let gossip_server = RapidSyncServer {
		processor: Some(processor.clone())
	};
	tokio::spawn(async move {
		processor.start_sync().await;
	});
	gossip_server.start_gossip_server().await;
}

struct RapidSyncServer {
	processor: Option<Arc<RapidSyncProcessor>>,
}

impl RapidSyncServer {
	pub(crate) async fn start_gossip_server(self) {
		// let network_graph_clone = self.network_graph.clone();
		let arc_processor = self.processor.unwrap();
		let dynamic_gossip_route = warp::path!("dynamic" / u32).and_then(move |timestamp| {
			let processor = Arc::clone(&arc_processor);
			serve_dynamic(processor, timestamp)
		});

		// let network_graph_clone = self.network_graph.clone();
		let snapshotted_gossip_route = warp::path!("snapshot" / u32).and_then(move |timestamp| {
			// let network_graph = Arc::clone(&network_graph_clone);
			serve_snapshot(timestamp)
		});

		let routes = warp::get().and(snapshotted_gossip_route.or(dynamic_gossip_route));
		// let routes = warp::get().and(snapshotted_gossip_route);
		warp::serve(routes).run(([127, 0, 0, 1], 3030)).await;
	}
}

async fn serve_snapshot(last_sync_timestamp: u32) -> Result<impl warp::Reply, Infallible> {
	// retrieve snapshots
	let snapshot_directory = "./res/snapshots";
	struct SnapshotDescriptor {
		after: u64,
		days: u64,
		calculated: u64,
		path: PathBuf,
	}

	let files = fs::read_dir(snapshot_directory).unwrap();
	let mut relevant_snapshots = Vec::new();
	for current_file in files {
		if current_file.is_err() {
			continue;
		};
		let entry = current_file.as_ref().unwrap();
		if entry.file_type().is_err() {
			continue;
		};
		let file_type = entry.file_type().unwrap();
		if !file_type.is_file() {
			continue;
		}
		let file_name_result = entry.file_name().into_string();
		if file_name_result.is_err() {
			continue;
		}
		let file_name = file_name_result.unwrap();
		if !file_name.starts_with("snapshot-after") || !file_name.ends_with(".lngossip") {
			continue;
		}
		let substring_start_index = "snapshot-".len();
		let substring_end_index = file_name.len() - ".lngossip".len();
		let snapshot_descriptor = &file_name[substring_start_index..substring_end_index];
		let snapshot_components = snapshot_descriptor.split("-");

		let components: Vec<u64> = snapshot_components.map(|current_component| {
			let subcomponents: Vec<&str> = current_component.split("_").collect();
			if subcomponents.len() != 2 { return 0; }
			let component_value = subcomponents[1];
			let numeric_value = component_value.parse::<u64>().unwrap_or(0);
			numeric_value
		}).collect();

		if components.len() != 3 {
			// something went wrong here
			continue;
		}

		let descriptor = SnapshotDescriptor {
			after: components[0],
			days: components[1],
			calculated: components[2],
			path: entry.path(),
		};
		if descriptor.after > last_sync_timestamp as u64 {
			continue;
		}

		relevant_snapshots.push(descriptor);
	}

	// get the snapshot with the latest possible calculation timestamp
	let latest_relevant_snapshot = relevant_snapshots.iter().max_by(|a, b| {
		let mut ordering = a.after.cmp(&b.after);
		if a.after == b.after {
			// if two snapshots have the same "after", take the one that is calculated later
			ordering = a.calculated.cmp(&b.calculated);
		}
		ordering
	});

	if let Some(snapshot) = latest_relevant_snapshot {
		println!("Serving snapshot: {:?}", &snapshot.path);
		let file_contents = fs::read(&snapshot.path).unwrap();
		let compressed_length = file_contents.len();
		let response = warp::http::Response::builder()
			.header("X-LDK-Compressed-Output-Length", HeaderValue::from(compressed_length))
			.header("Content-Encoding", HeaderValue::from_static("gzip"))
			.header("Content-Length", HeaderValue::from(compressed_length))
			.body(file_contents);
		Ok(response)
	} else {
		let response = warp::http::Response::builder()
			.status(503)
			.header("X-LDK-Error", HeaderValue::from_static("Initial sync incomplete"))
			.body(vec![]);
		Ok(response)
	}
}

/// Server route for returning compressed gossip data
///
/// `block`: Starting block
///
/// `timestamp`: Starting timestamp
///
/// When `timestamp` is set to 0, the server returns a dump of all the latest channel updates.
/// Otherwise, the server compares the latest update prior to a given timestamp with the latest
/// overall update and, in the event of a difference, returns a partial update of only the affected
/// fields.
async fn serve_dynamic(processor: Arc<RapidSyncProcessor>, last_sync_timestamp: u32) -> Result<impl warp::Reply, Infallible> {
	let is_initial_sync_complete = processor.initial_sync_complete.load(Ordering::Acquire);
	if !is_initial_sync_complete {
		let response = warp::http::Response::builder()
			.status(503)
			.header("X-LDK-Error", HeaderValue::from_static("Initial sync incomplete"))
			.body(vec![]);
		return Ok(response);
	}

	let start = Instant::now();

	let response = processor.serialize_delta(last_sync_timestamp, false, true).await;

	let response_length = response.uncompressed.len();

	let mut response_builder = warp::http::Response::builder()
		.header("X-LDK-Gossip-Message-Count", HeaderValue::from(response.message_count))
		.header("X-LDK-Gossip-Message-Count-Announcements", HeaderValue::from(response.announcement_count))
		.header("X-LDK-Gossip-Message-Count-Updates", HeaderValue::from(response.update_count))
		.header("X-LDK-Gossip-Original-Update-Count", HeaderValue::from(response.update_count_full))
		.header("X-LDK-Gossip-Modified-Update-Count", HeaderValue::from(response.update_count_incremental))
		.header("X-LDK-Raw-Output-Length", HeaderValue::from(response_length));

	println!("message count: {}\nannouncement count: {}\nupdate count: {}\nraw output length: {}", response.message_count, response.announcement_count, response.update_count, response_length);

	let response_binary = if let Some(compressed_response) = response.compressed {
		let compressed_length = compressed_response.len();
		let compression_efficacy = 1.0 - (compressed_length as f64) / (response_length as f64);

		let efficacy_header = format!("{}%", (compression_efficacy * 1000.0).round() / 10.0);
		response_builder = response_builder
			.header("X-LDK-Compressed-Output-Length", HeaderValue::from(compressed_length))
			.header("X-LDK-Compression-Efficacy", HeaderValue::from_str(efficacy_header.as_str()).unwrap())
			.header("Content-Encoding", HeaderValue::from_static("gzip"))
			.header("Content-Length", HeaderValue::from(compressed_length));

		println!("compressed output length: {}\ncompression efficacy: {}", compressed_length, efficacy_header);
		compressed_response
	} else {
		response.uncompressed
	};

	let duration = start.elapsed();
	let elapsed_time = format!("{:?}", duration);

	let response = response_builder
		.header("X-LDK-Elapsed-Time", HeaderValue::from_str(elapsed_time.as_str()).unwrap())
		.body(response_binary);

	println!("elapsed time: {}", elapsed_time);
	Ok(response)
}
