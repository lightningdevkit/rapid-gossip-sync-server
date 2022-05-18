use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use bitcoin::secp256k1::PublicKey;
use lightning::routing::network_graph::NetworkGraph;
use lightning::util::ser::Writeable;
use tokio::sync::mpsc;
use tokio_postgres::NoTls;
use warp::Filter;
use warp::http::HeaderValue;

use crate::config;
use crate::server::lookup::DeltaSet;
use crate::server::serialization::UpdateSerializationMechanism;

mod lookup;
mod serialization;
mod snapshot;

struct SerializedResponse {
	uncompressed: Vec<u8>,
	compressed: Option<Vec<u8>>,
	message_count: u32,
	announcement_count: u32,
	update_count: u32,
	update_count_full: u32,
	update_count_incremental: u32,
}

pub(crate) struct GossipServer {
	pub(crate) sync_completion_sender: mpsc::Sender<()>,
	sync_completion_receiver: mpsc::Receiver<()>,
	network_graph: Arc<NetworkGraph>,
	initial_sync_complete: Arc<AtomicBool>
}

impl GossipServer {
	pub(crate) fn new(network_graph: Arc<NetworkGraph>) -> Self {
		let (sync_completion_sender, sync_completion_receiver) = mpsc::channel::<()>(1);
		// let service_unavailable_response = warp::http::Response::builder().status(503).body(vec![]).into_response();
		Self {
			sync_completion_sender,
			sync_completion_receiver,
			network_graph,
			initial_sync_complete: Arc::new(AtomicBool::new(false))
		}
	}

	pub(crate) async fn start_gossip_server(&mut self) {
		let network_graph_clone = self.network_graph.clone();
		let initial_sync_complete_clone = self.initial_sync_complete.clone();
		let dynamic_gossip_route = warp::path!("dynamic" / u32).and_then(move |timestamp| {
			let network_graph = Arc::clone(&network_graph_clone);
			serve_dynamic(network_graph, Arc::clone(&initial_sync_complete_clone), timestamp)
		});

		let network_graph_clone = self.network_graph.clone();
		let snapshotted_gossip_route = warp::path!("snapshot" / u32).and_then(move |timestamp| {
			let network_graph = Arc::clone(&network_graph_clone);
			serve_snapshot(network_graph, timestamp)
		});

		tokio::spawn(async move {
			let routes = warp::get().and(snapshotted_gossip_route.or(dynamic_gossip_route));
			warp::serve(routes).run(([127, 0, 0, 1], 3030)).await;
		});

		let snapshotter = self::snapshot::Snapshotter::new(self.network_graph.clone());
		if let Some(_) = self.sync_completion_receiver.recv().await {
			self.initial_sync_complete.store(true, Ordering::Release);
			println!("Initial sync complete!");
		}
		snapshotter.snapshot_gossip().await;
	}
}

async fn serve_snapshot(network_graph: Arc<NetworkGraph>, last_sync_timestamp: u32) -> Result<impl warp::Reply, Infallible> {
	Ok("everything is awesome!")
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
async fn serve_dynamic(network_graph: Arc<NetworkGraph>, initial_sync_complete: Arc<AtomicBool>, last_sync_timestamp: u32) -> Result<impl warp::Reply, Infallible> {
	let is_initial_sync_complete = initial_sync_complete.load(Ordering::Acquire);
	if !is_initial_sync_complete {
		let response = warp::http::Response::builder()
			.status(503)
			.header("X-LDK-Error", HeaderValue::from_static("Initial sync incomplete"))
			.body(vec![]);
		return Ok(response)
	}

	let start = Instant::now();

	let response = serialize_delta(network_graph, last_sync_timestamp, false, false).await;

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

async fn serialize_delta(network_graph: Arc<NetworkGraph>, last_sync_timestamp: u32, consider_intermediate_updates: bool, gzip_response: bool) -> SerializedResponse {
	let (client, connection) =
		tokio_postgres::connect(config::db_connection_string().as_str(), NoTls).await.unwrap();

	tokio::spawn(async move {
		if let Err(e) = connection.await {
			panic!("connection error: {}", e);
		}
	});

	let mut output: Vec<u8> = vec![];

	// set a flag if the chain hash is prepended
	// chain hash only necessary if either channel announcements or non-incremental updates are present
	// for announcement-free incremental-only updates, chain hash can be skipped

	let delta_set = DeltaSet::new();

	let mut node_id_set: HashSet<[u8; 33]> = HashSet::new();
	let mut node_id_indices: HashMap<[u8; 33], usize> = HashMap::new();
	let mut node_ids: Vec<PublicKey> = Vec::new();
	let mut duplicate_node_ids = 0;

	let mut get_node_id_index = |node_id: PublicKey| {
		let serialized_node_id = node_id.serialize();
		if node_id_set.insert(serialized_node_id) {
			node_ids.push(node_id);
			let index = node_ids.len() - 1;
			node_id_indices.insert(serialized_node_id, index);
			return index;
		}
		duplicate_node_ids += 1;
		node_id_indices[&serialized_node_id]
	};

	let delta_set = lookup::fetch_channel_announcements(delta_set, network_graph, &client, last_sync_timestamp).await;
	let delta_set = lookup::fetch_channel_updates(delta_set, &client, last_sync_timestamp).await;
	let delta_set = lookup::filter_delta_set(delta_set);
	let serialization_details = serialization::serialize_delta_set(delta_set, last_sync_timestamp, consider_intermediate_updates);

	// process announcements
	// write the number of channel announcements to the output
	let announcement_count = serialization_details.announcements.len() as u32;
	announcement_count.write(&mut output);
	let mut previous_announcement_scid = 0;
	for current_announcement in serialization_details.announcements {
		let id_index_1 = get_node_id_index(current_announcement.node_id_1);
		let id_index_2 = get_node_id_index(current_announcement.node_id_2);
		let mut stripped_announcement = serialization::serialize_stripped_channel_announcement(&current_announcement, id_index_1, id_index_2, previous_announcement_scid);
		output.append(&mut stripped_announcement);

		previous_announcement_scid = current_announcement.short_channel_id;
	}

	// process updates
	let mut previous_update_scid = 0;
	let update_count = serialization_details.updates.len() as u32;
	update_count.write(&mut output);

	let default_update_values = serialization_details.full_update_defaults;
	if update_count > 0 {
		default_update_values.cltv_expiry_delta.write(&mut output);
		default_update_values.htlc_minimum_msat.write(&mut output);
		default_update_values.fee_base_msat.write(&mut output);
		default_update_values.fee_proportional_millionths.write(&mut output);
		default_update_values.htlc_maximum_msat.write(&mut output);
	}

	let mut update_count_full = 0;
	let mut update_count_incremental = 0;
	for current_update in serialization_details.updates {
		match &current_update.mechanism {
			UpdateSerializationMechanism::Full => {
				update_count_full += 1;
			}
			UpdateSerializationMechanism::Incremental(_) => {
				update_count_incremental += 1;
			}
		};

		let mut stripped_update = serialization::serialize_stripped_channel_update(&current_update, &default_update_values, previous_update_scid);
		output.append(&mut stripped_update);

		previous_update_scid = current_update.update.short_channel_id;
	}

	// some statis
	let message_count = announcement_count + update_count;

	let mut prefixed_output = vec![76, 68, 75, 1];
	// always write the chain hash
	serialization_details.chain_hash.write(&mut prefixed_output);
	// always write the latest seen timestamp
	serialization_details.latest_seen.write(&mut prefixed_output);

	let node_id_count = node_ids.len() as u32;
	node_id_count.write(&mut prefixed_output);

	for current_node_id in node_ids {
		current_node_id.write(&mut prefixed_output);
	}

	prefixed_output.append(&mut output);

	let mut compressed_output = None;
	if gzip_response {
		let mut compressor = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
		compressor.write_all(&prefixed_output);
		let compressed_response = compressor.finish().unwrap();
		compressed_output = Some(compressed_response);
	}

	println!("duplicated node ids: {}", duplicate_node_ids);
	println!("latest seen timestamp: {:?}", serialization_details.latest_seen);

	SerializedResponse {
		uncompressed: prefixed_output,
		compressed: compressed_output,
		message_count,
		announcement_count,
		update_count,
		update_count_full,
		update_count_incremental,
	}
}
