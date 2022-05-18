use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::io::Write;
use std::sync::Arc;
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
use crate::server::serialization::{UpdateSerializationMechanism};

mod lookup;
mod serialization;

pub(crate) struct GossipServer {
	pub(crate) gossip_refresh_sender: mpsc::Sender<()>,
	gossip_refresh_receiver: Option<mpsc::Receiver<()>>,
	network_graph: Arc<NetworkGraph>,
}

impl GossipServer {
	pub(crate) fn new(network_graph: Arc<NetworkGraph>) -> Self {
		let (gossip_refresh_sender, gossip_refresh_receiver) = mpsc::channel::<()>(2);
		// let service_unavailable_response = warp::http::Response::builder().status(503).body(vec![]).into_response();
		Self {
			gossip_refresh_sender,
			gossip_refresh_receiver: Some(gossip_refresh_receiver),
			network_graph,
		}
	}

	pub(crate) async fn start_gossip_server(&mut self) {
		let network_graph_clone = self.network_graph.clone();
		let dynamic_gossip_route = warp::path!("dynamic" / u32).and_then(move |timestamp| {
			let network_graph = Arc::clone(&network_graph_clone);
			serve_dynamic(network_graph, timestamp)
		});

		let network_graph_clone = self.network_graph.clone();
		let snapshotted_gossip_route = warp::path!("snapshot" / u32).and_then(move |timestamp| {
			let network_graph = Arc::clone(&network_graph_clone);
			serve_snapshot(network_graph, timestamp)
		});

		let routes = warp::get().and(snapshotted_gossip_route.or(dynamic_gossip_route));
		warp::serve(routes).run(([127, 0, 0, 1], 3030)).await;
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
async fn serve_dynamic(network_graph: Arc<NetworkGraph>, last_sync_timestamp: u32) -> Result<impl warp::Reply, Infallible> {
	let start = Instant::now();

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

	let mut scid_deltas = vec![]; // all deltas, across both announcements and updates
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
	let serialization_details = serialization::serialize_delta_set(delta_set, last_sync_timestamp, true);

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

		scid_deltas.push(current_announcement.short_channel_id - previous_announcement_scid);
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

	let mut updates_full = 0;
	let mut updates_incremental = 0;
	for current_update in serialization_details.updates {
		match &current_update.mechanism {
			UpdateSerializationMechanism::Full => {
				updates_full += 1;
			}
			UpdateSerializationMechanism::Incremental(_) => {
				updates_incremental += 1;
			}
		};

		let mut stripped_update = serialization::serialize_stripped_channel_update(&current_update, &default_update_values, previous_update_scid);
		output.append(&mut stripped_update);

		scid_deltas.push(current_update.update.short_channel_id - previous_update_scid);
		previous_update_scid = current_update.update.short_channel_id;
	}

	// some statis
	let gossip_message_count = announcement_count + update_count;

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

	let response_length = prefixed_output.len();

	println!("packaging raw response: {:?}", prefixed_output);
	let should_compress = true;

	let mut response_builder = warp::http::Response::builder()
		.header("X-LDK-Gossip-Message-Count", HeaderValue::from(gossip_message_count))
		.header("X-LDK-Gossip-Message-Count-Announcements", HeaderValue::from(announcement_count))
		.header("X-LDK-Gossip-Message-Count-Updates", HeaderValue::from(update_count))
		.header("X-LDK-Gossip-Original-Update-Count", HeaderValue::from(updates_full))
		.header("X-LDK-Gossip-Modified-Update-Count", HeaderValue::from(updates_incremental))
		.header("X-LDK-Raw-Output-Length", HeaderValue::from(response_length));

	println!("message count: {}\nannouncement count: {}\nupdate count: {}\nraw output length: {}", gossip_message_count, announcement_count, update_count, response_length);
	println!("duplicated node ids: {}", duplicate_node_ids);
	println!("latest seen timestamp: {:?}", serialization_details.latest_seen);

	if should_compress {
		println!("compressing gossip data");
		let mut compressor = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
		compressor.write_all(&prefixed_output);
		let compressed_response = compressor.finish().unwrap();

		let compressed_length = compressed_response.len();
		let compression_efficacy = 1.0 - (compressed_length as f64) / (response_length as f64);

		let efficacy_header = format!("{}%", (compression_efficacy * 1000.0).round() / 10.0);
		prefixed_output = compressed_response;

		response_builder = response_builder
			.header("X-LDK-Compressed-Output-Length", HeaderValue::from(compressed_length))
			.header("X-LDK-Compression-Efficacy", HeaderValue::from_str(efficacy_header.as_str()).unwrap())
			.header("Content-Encoding", HeaderValue::from_static("gzip"))
			.header("Content-Length", HeaderValue::from(compressed_length));

		println!("compressed output length: {}\ncompression efficacy: {}", compressed_length, efficacy_header);
	}

	let duration = start.elapsed();
	let elapsed_time = format!("{:?}", duration);

	let response = response_builder
		.header("X-LDK-Elapsed-Time", HeaderValue::from_str(elapsed_time.as_str()).unwrap())
		.body(prefixed_output);

	println!("elapsed time: {}", elapsed_time);
	println!("max scid delta: {}", scid_deltas.iter().max().unwrap());
	println!("min scid delta: {}", scid_deltas.iter().min().unwrap());
	// println!("SCID deltas: {:?}", scid_deltas);

	// let response = format!("block: {}<br/>\ntimestamp: {}<br/>\nlength: {}<br/>\nelapsed: {:?}", block, timestamp, response_length, duration);
	Ok(response)
}
