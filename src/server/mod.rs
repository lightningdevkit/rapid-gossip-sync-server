use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::io::{Cursor, Write};
use std::sync::{Arc, RwLock};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use bitcoin::BlockHash;
use bitcoin::secp256k1::PublicKey;
use chrono::{DateTime, Utc};
use lightning::ln::msgs::{UnsignedChannelAnnouncement, UnsignedChannelUpdate};
use lightning::util::ser::{Readable, Writeable};
use tokio::sync::mpsc;
use tokio_postgres::NoTls;
use warp::Filter;
use warp::http::HeaderValue;
// use warp::http::{HeaderValue, Response};
use warp::Reply;

use crate::config;
use crate::hex_utils;
use crate::server::serialization::DefaultUpdateValues;

mod serialization;

pub(crate) struct GossipServer {
	pub(crate) gossip_refresh_sender: mpsc::Sender<()>,
	gossip_refresh_receiver: Option<mpsc::Receiver<()>>,
	// full_history_gossip: Arc<RwLock<warp::reply::Response>>
	full_history_gossip: Arc<RwLock<Vec<u8>>>
}

impl GossipServer {
	pub(crate) fn new() -> Self {
		let (gossip_refresh_sender, gossip_refresh_receiver) = mpsc::channel::<()>(2);
		// let service_unavailable_response = warp::http::Response::builder().status(503).body(vec![]).into_response();
		Self {
			gossip_refresh_sender,
			gossip_refresh_receiver: Some(gossip_refresh_receiver),
			// full_history_gossip: Arc::new(RwLock::new(service_unavailable_response))
			full_history_gossip: Arc::new(RwLock::new(Vec::new()))
		}
	}

	pub(crate) async fn start_gossip_server(&mut self) {
		let full_gossip_data = self.full_history_gossip.clone();
		let full_gossip_route = warp::path("full").map(move || {
			let arc_gossip_data = Arc::clone(&full_gossip_data);
			let gossip_data = arc_gossip_data.read().unwrap();

			let compressed_length = gossip_data.len();
			if compressed_length < 1 {
				let service_unavailable_response = warp::http::Response::builder().status(503).body(vec![]);
				return service_unavailable_response;
			}

			warp::http::Response::builder()
				.header("Content-Encoding", HeaderValue::from_static("gzip"))
				.header("Content-Length", HeaderValue::from(compressed_length))
				.body(gossip_data.clone())
		});

		let dynamic_gossip_route = warp::path!("composite" / "block" / u32 / "timestamp" / u64).and_then(serve_composite);

		if let Some(mut gossip_refresh_receiver) = self.gossip_refresh_receiver.take() {
			println!("background gossip refresher active!");
			let gossip_cache = self.full_history_gossip.clone();
			tokio::spawn(async move {
				while let Some(gossip_update) = gossip_refresh_receiver.recv().await {
					println!("refreshing background gossip");
					let warp_reply = serve_composite(0, 0).await.unwrap();
					let warp_response = warp_reply.into_response();

					let hyper_body = warp_response.into_body();
					let retrieved_output: Vec<u8> = warp::hyper::body::to_bytes(hyper_body).await.unwrap().to_vec();

					let mut response_writer = gossip_cache.write().unwrap();
					// *response_writer = warp_response;
					*response_writer = retrieved_output;
					println!("refreshed background gossip!");
				}
			});
		}

		let routes = warp::get().and(full_gossip_route.or(dynamic_gossip_route));
		warp::serve(routes).run(([127, 0, 0, 1], 3030)).await;
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
async fn serve_composite(block: u32, timestamp: u64) -> Result<impl warp::Reply, Infallible> {
	let start = Instant::now();
	let current_timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();

	let (client, connection) =
		tokio_postgres::connect(config::db_connection_string().as_str(), NoTls).await.unwrap();

	tokio::spawn(async move {
		if let Err(e) = connection.await {
			panic!("connection error: {}", e);
		}
	});

	let mut is_incremental = false;
	let mut output: Vec<u8> = vec![];

	if timestamp > 0 {
		is_incremental = true;
	}

	let mut gossip_message_announcement_count = 0u32;
	let mut gossip_message_update_count = 0u32;

	// set a flag if the chain hash is prepended
	// chain hash only necessary if either channel announcements or non-incremental updates are present
	// for announcement-free incremental-only updates, chain hash can be skipped
	let mut chain_hash: Option<BlockHash> = None;


	enum ExperimentalUpdateMode {
		Default,
		OldestDataFull,
		OldestDataDirection0,
		OldestDataDirection1,
		IncrementalOnlyBidirectional,
		IncrementalOnlyBidirectionalWithAnnouncements,
		IncrementalOnlyDirection0,
		IncrementalOnlyDirection1,
		AnnouncementsOnly,
	}
	let experimental_update_mode = Some(ExperimentalUpdateMode::OldestDataFull);
	// let experimental_update_mode = None;

	let mut scid_deltas = vec![]; // all deltas, across both announcements and updates
	let mut node_id_set: HashSet<[u8; 33]> = HashSet::new();
	let mut node_id_indices: HashMap<[u8; 33], usize> = HashMap::new();
	let mut node_ids: Vec<PublicKey> = Vec::new();
	let mut duplicate_node_ids = 0;
	let mut previous_announcement_scid = None;
	let mut latest_seen_timestamp = None;

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

	{
		println!("fetching channels…");

		let block_height_minimum = block as i32;
		// let block_height_minimum = 0i32;

		// the following commented line is purely for experiments where we want to return a limited
		// number of initial channel updates to test incremental updates against
		// TODO: remove
		let announcement_rows = if let Some(experimental_update_mode) = &experimental_update_mode {
			match experimental_update_mode {
				ExperimentalUpdateMode::Default | ExperimentalUpdateMode::OldestDataFull | ExperimentalUpdateMode::OldestDataDirection0 | ExperimentalUpdateMode::OldestDataDirection1 | ExperimentalUpdateMode::AnnouncementsOnly | ExperimentalUpdateMode::IncrementalOnlyBidirectionalWithAnnouncements  => {
					client.query("SELECT * FROM channels WHERE block_height >= $1 AND short_channel_id IN ('0899c000021b0000', '0adea20008260001') ORDER BY short_channel_id ASC", &[&block_height_minimum]).await.unwrap()
				}
				_ => {
					vec![]
				}
			}
		} else {
			client.query("SELECT * FROM channels WHERE block_height >= $1 ORDER BY short_channel_id ASC", &[&block_height_minimum]).await.unwrap()
		};

		// let announcement_rows = client.query("SELECT * FROM channels WHERE block_height >= $1 ORDER BY short_channel_id ASC", &[&block_height_minimum]).await.unwrap();
		gossip_message_announcement_count = announcement_rows.len() as u32;
		gossip_message_announcement_count.write(&mut output);

		for current_announcement_row in announcement_rows {
			let blob: String = current_announcement_row.get("announcement_unsigned");
			let data = hex_utils::to_vec(&blob).unwrap();
			let mut readable = Cursor::new(data);
			let unsigned_announcement = UnsignedChannelAnnouncement::read(&mut readable).unwrap();

			if chain_hash.is_none() {
				chain_hash = Some(unsigned_announcement.chain_hash);
			}

			let seen_timestamp: DateTime<Utc> = current_announcement_row.get("createdAt");
			latest_seen_timestamp = if let Some(latest_seen_timestamp) = latest_seen_timestamp {
				Some(DateTime::max(seen_timestamp, latest_seen_timestamp))
			} else {
				Some(seen_timestamp)
			};

			let id_index_1 = get_node_id_index(unsigned_announcement.node_id_1);
			let id_index_2 = get_node_id_index(unsigned_announcement.node_id_2);

			let mut stripped_announcement = serialization::serialize_stripped_channel_announcement(&unsigned_announcement, id_index_1, id_index_2, previous_announcement_scid);
			output.append(&mut stripped_announcement);

			if let Some(previous_scid) = previous_announcement_scid {
				let scid_delta = unsigned_announcement.short_channel_id - previous_scid;
				scid_deltas.push(scid_delta);
			}
			previous_announcement_scid.replace(unsigned_announcement.short_channel_id);
		}
	}

	// let mut sorted_node_ids: Vec<[u8; 33]> = node_ids.into_iter().collect();
	// sorted_node_ids.sort_by(|a, b| {
	// 	let pubkey_a = PublicKey::from_slice(&a[..]).unwrap();
	// 	let pubkey_b = PublicKey::from_slice(&b[..]).unwrap();
	// 	return pubkey_a.cmp(&pubkey_b)
	// });

	let enable_update_reference_comparisons = is_incremental; // true;
	let mut updates_with_prior_reference = 0;
	let mut updates_without_prior_reference = 0;
	let mut omitted_updates = 0;
	let mut modified_updates = 0;



	{
		println!("fetching updates…");

		let mut cltv_expiry_delta_histogram: HashMap<u16, usize> = HashMap::new();
		let mut htlc_minimum_msat_histogram: HashMap<u64, usize> = HashMap::new();
		let mut fee_base_msat_histogram: HashMap<u32, usize> = HashMap::new();
		let mut fee_proportional_millionths_histogram: HashMap<u32, usize> = HashMap::new();
		let mut htlc_maximum_msat_histogram: HashMap<u64, usize> = HashMap::new();

		let mut updates_with_reference_keys: Vec<(UnsignedChannelUpdate, String)> = Vec::new();

		let timestamp_minimum = timestamp as i64;
		// let timestamp_minimum = 0i64;
		// let rows = client.query("SELECT * FROM channel_updates", &[]).await.unwrap();
		let mut update_data = Vec::new();

		let mut reference: HashMap<String, UnsignedChannelUpdate> = HashMap::new();
		if enable_update_reference_comparisons {
			// the following commented line is purely for experiments where we want to return a limited
			// number of initial channel updates to test incremental updates against
			// TODO: remove
			let reference_rows = if let Some(experimental_update_mode) = &experimental_update_mode {
				match experimental_update_mode {
					ExperimentalUpdateMode::OldestDataFull | ExperimentalUpdateMode::OldestDataDirection0 | ExperimentalUpdateMode::OldestDataDirection1 => {
						vec![]
					}
					_ => {
						client.query("SELECT DISTINCT ON (short_channel_id, direction) * FROM channel_updates WHERE timestamp < $1 AND short_channel_id IN (SELECT short_channel_id FROM channel_updates WHERE timestamp >= $1 AND short_channel_id IN ('0899c000021b0000', '0adea20008260001') GROUP BY short_channel_id) ORDER BY short_channel_id ASC, direction ASC, timestamp DESC", &[&timestamp_minimum]).await.unwrap()
					}
				}
			} else {
				client.query("SELECT DISTINCT ON (short_channel_id, direction) * FROM channel_updates WHERE timestamp < $1 AND short_channel_id IN (SELECT short_channel_id FROM channel_updates WHERE timestamp >= $1 GROUP BY short_channel_id) ORDER BY short_channel_id ASC, direction ASC, timestamp DESC", &[&timestamp_minimum]).await.unwrap()
			};

			// let reference_rows = client.query("SELECT DISTINCT ON (short_channel_id, direction) * FROM channel_updates WHERE timestamp < $1 AND short_channel_id IN (SELECT short_channel_id FROM channel_updates WHERE timestamp >= $1 GROUP BY short_channel_id) ORDER BY short_channel_id ASC, direction ASC, timestamp DESC", &[&timestamp_minimum]).await.unwrap();
			for current_reference in reference_rows {
				let scid_hex: String = current_reference.get("short_channel_id");
				let direction: i32 = current_reference.get("direction");
				let reference_key = format!("{}:{}", scid_hex, direction);
				let blob: String = current_reference.get("blob_unsigned");
				let data = hex_utils::to_vec(&blob).unwrap();
				let mut readable = Cursor::new(data);
				// readable.set_position(2); // the first two bytes are the type, which in this case we already know
				let unsigned_channel_update = UnsignedChannelUpdate::read(&mut readable).unwrap();
				reference.insert(reference_key, unsigned_channel_update);
			}
		}

		// the following commented line is purely for experiments where we want to return a limited
		// number of initial channel updates to test incremental updates against
		// TODO: remove
		// oldest
		let update_rows = client.query("SELECT DISTINCT ON (short_channel_id, direction) * FROM channel_updates WHERE timestamp >= $1 AND short_channel_id IN ('0899c000021b0000', '0adea20008260001') ORDER BY short_channel_id ASC, direction ASC, timestamp ASC", &[&timestamp_minimum]).await.unwrap();
		let update_rows = if let Some(experimental_update_mode) = experimental_update_mode {
			match experimental_update_mode {
				ExperimentalUpdateMode::Default => {
					client.query("SELECT DISTINCT ON (short_channel_id, direction) * FROM channel_updates WHERE timestamp >= $1 AND short_channel_id IN ('0899c000021b0000', '0adea20008260001') ORDER BY short_channel_id ASC, direction ASC, timestamp DESC", &[&timestamp_minimum]).await.unwrap()
				}
				ExperimentalUpdateMode::OldestDataFull => {
					client.query("SELECT DISTINCT ON (short_channel_id, direction) * FROM channel_updates WHERE short_channel_id IN ('0899c000021b0000', '0adea20008260001') ORDER BY short_channel_id ASC, direction ASC, timestamp ASC", &[]).await.unwrap()
				}
				ExperimentalUpdateMode::OldestDataDirection0 => {
					client.query("SELECT DISTINCT ON (short_channel_id, direction) * FROM channel_updates WHERE direction = 0 AND short_channel_id IN ('0899c000021b0000', '0adea20008260001') ORDER BY short_channel_id ASC, direction ASC, timestamp ASC", &[]).await.unwrap()
				}
				ExperimentalUpdateMode::OldestDataDirection1 => {
					client.query("SELECT DISTINCT ON (short_channel_id, direction) * FROM channel_updates WHERE direction = 1 AND short_channel_id IN ('0899c000021b0000', '0adea20008260001') ORDER BY short_channel_id ASC, direction ASC, timestamp ASC", &[]).await.unwrap()
				}
				ExperimentalUpdateMode::IncrementalOnlyBidirectional | ExperimentalUpdateMode::IncrementalOnlyBidirectionalWithAnnouncements => {
					client.query("SELECT DISTINCT ON (short_channel_id, direction) * FROM channel_updates WHERE timestamp >= $1 AND short_channel_id IN ('0899c000021b0000', '0adea20008260001') ORDER BY short_channel_id ASC, direction ASC, timestamp DESC", &[&timestamp_minimum]).await.unwrap()
				}
				ExperimentalUpdateMode::IncrementalOnlyDirection0 => {
					client.query("SELECT DISTINCT ON (short_channel_id, direction) * FROM channel_updates WHERE timestamp >= $1 AND direction = 0 AND short_channel_id IN ('0899c000021b0000', '0adea20008260001') ORDER BY short_channel_id ASC, direction ASC, timestamp DESC", &[&timestamp_minimum]).await.unwrap()
				}
				ExperimentalUpdateMode::IncrementalOnlyDirection1 => {
					client.query("SELECT DISTINCT ON (short_channel_id, direction) * FROM channel_updates WHERE timestamp >= $1 AND direction = 1 AND short_channel_id IN ('0899c000021b0000', '0adea20008260001') ORDER BY short_channel_id ASC, direction ASC, timestamp DESC", &[&timestamp_minimum]).await.unwrap()
				}
				ExperimentalUpdateMode::AnnouncementsOnly => {
					vec![]
				}
			}
		} else {
			client.query("SELECT DISTINCT ON (short_channel_id, direction) * FROM channel_updates WHERE timestamp >= $1 ORDER BY short_channel_id ASC, direction ASC, timestamp DESC", &[&timestamp_minimum]).await.unwrap()
		};

		// let update_rows = client.query("SELECT DISTINCT ON (short_channel_id, direction) * FROM channel_updates WHERE timestamp >= $1 ORDER BY short_channel_id ASC, direction ASC, timestamp DESC", &[&timestamp_minimum]).await.unwrap();

		let mut modification_tally_by_field_count: HashMap<u8, u32> = HashMap::new();
		let mut modification_tally_by_affected_field_combination: HashMap<String, u32> = HashMap::new();
		for current_update_row in update_rows {
			let blob: String = current_update_row.get("blob_unsigned");
			let mut data = hex_utils::to_vec(&blob).unwrap();

			let mut readable = Cursor::new(&data);
			// readable.set_position(2); // the first two bytes are the type, which in this case we already know
			let mut unsigned_channel_update = UnsignedChannelUpdate::read(&mut readable).unwrap();

			if chain_hash.is_none() {
				chain_hash = Some(unsigned_channel_update.chain_hash);
			}

			let (has_reference, reference_key) = if enable_update_reference_comparisons {
				let scid_hex: String = current_update_row.get("short_channel_id");
				let direction: i32 = current_update_row.get("direction");
				let reference_key = format!("{}:{}", scid_hex, direction);
				(reference.contains_key(&reference_key), reference_key)
			} else {
				(false, "".to_owned())
			};

			if !has_reference {
				// we only care about updating the histogram for full updates
				*cltv_expiry_delta_histogram.entry(unsigned_channel_update.cltv_expiry_delta).or_insert(0) += 1;
				*htlc_minimum_msat_histogram.entry(unsigned_channel_update.htlc_minimum_msat).or_insert(0) += 1;
				*fee_base_msat_histogram.entry(unsigned_channel_update.fee_base_msat).or_insert(0) += 1;
				*fee_proportional_millionths_histogram.entry(unsigned_channel_update.fee_proportional_millionths).or_insert(0) += 1;
				let htlc_maximum_msat_key = serialization::optional_htlc_maximum_to_u64(&unsigned_channel_update.htlc_maximum_msat);
				*htlc_maximum_msat_histogram.entry(htlc_maximum_msat_key).or_insert(0) += 1;
			}

			let seen_timestamp: DateTime<Utc> = current_update_row.get("createdAt");
			latest_seen_timestamp = if let Some(latest_seen_timestamp) = latest_seen_timestamp {
				Some(DateTime::max(seen_timestamp, latest_seen_timestamp))
			} else {
				Some(seen_timestamp)
			};

			updates_with_reference_keys.push((unsigned_channel_update, reference_key));

		}

		// evaluate the histograms
		let default_update_values = if updates_with_reference_keys.len() > 0 {
			DefaultUpdateValues {
				cltv_expiry_delta: serialization::find_most_common_histogram_entry(cltv_expiry_delta_histogram),
				htlc_minimum_msat: serialization::find_most_common_histogram_entry(htlc_minimum_msat_histogram),
				fee_base_msat: serialization::find_most_common_histogram_entry(fee_base_msat_histogram),
				fee_proportional_millionths: serialization::find_most_common_histogram_entry(fee_proportional_millionths_histogram),
				htlc_maximum_msat: serialization::find_most_common_histogram_entry(htlc_maximum_msat_histogram)
			}
		} else {
			// we can't calculate the defaults if we have 0 entries
			DefaultUpdateValues {
				cltv_expiry_delta: 0,
				htlc_minimum_msat: 0,
				fee_base_msat: 0,
				fee_proportional_millionths: 0,
				htlc_maximum_msat: 0,
			}
		};

		let mut previous_update_scid = None;
		for (unsigned_channel_update, reference_key) in updates_with_reference_keys {

			let mut reference_update = if enable_update_reference_comparisons {
				let reference_channel_update = reference.get(&reference_key);
				if let Some(reference_update) = reference_channel_update {
					updates_with_prior_reference += 1;
					Some(reference_update)
				} else {
					None
				}
			} else {
				None
			};

			let delta = serialization::compare_update_with_reference(&unsigned_channel_update, &default_update_values, reference_update, previous_update_scid);

			if reference_update.is_some() {
				*modification_tally_by_field_count.entry(delta.affected_field_count).or_insert(0) += 1;
				if delta.affected_field_count > 0 {
					let modified_field_set_key = delta.affected_fields.join(", ");
					*modification_tally_by_affected_field_combination.entry(modified_field_set_key).or_insert(0) += 1;
					modified_updates += 1;
				} else {
					// there is no difference compared to the reference
					omitted_updates += 1;
					continue;
				}
			}

			gossip_message_update_count += 1;

			// unsigned_channel_update.timestamp = current_timestamp as u32;
			update_data.extend_from_slice(&delta.serialization);

			if let Some(previous_scid) = previous_update_scid {
				let scid_delta = unsigned_channel_update.short_channel_id - previous_scid;
				scid_deltas.push(scid_delta);
			}
			previous_update_scid.replace(unsigned_channel_update.short_channel_id);
		}

		gossip_message_update_count.write(&mut output);
		if gossip_message_update_count > 0 {
			// we don't care about the defaults if we have 0 updates
			default_update_values.cltv_expiry_delta.write(&mut output);
			default_update_values.htlc_minimum_msat.write(&mut output);
			default_update_values.fee_base_msat.write(&mut output);
			default_update_values.fee_proportional_millionths.write(&mut output);
			default_update_values.htlc_maximum_msat.write(&mut output);
			println!("default cltv_expiry_delta: {}", default_update_values.cltv_expiry_delta);
			println!("default htlc_minimum_msat: {}", default_update_values.htlc_minimum_msat);
			println!("default fee_base_msat: {}", default_update_values.fee_base_msat);
			println!("default fee_proportional_millionths: {}", default_update_values.fee_proportional_millionths);
			println!("default htlc_maximum_msat: {}", default_update_values.htlc_maximum_msat);
		}
		output.append(&mut update_data);

		println!("modification tally by count: {:#?}", modification_tally_by_field_count);
		println!("modification tally by combination: {:#?}", modification_tally_by_affected_field_combination);
	}

	let gossip_message_count = gossip_message_announcement_count + gossip_message_update_count;

	let mut prefixed_output = vec![76, 68, 75, 1];
	// always write the chain hash
	chain_hash.unwrap().write(&mut prefixed_output);
	// always write the latest seen timestamp
	(latest_seen_timestamp.unwrap().timestamp() as u32).write(&mut prefixed_output);

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
		.header("X-LDK-Gossip-Message-Count-Announcements", HeaderValue::from(gossip_message_announcement_count))
		.header("X-LDK-Gossip-Message-Count-Updates", HeaderValue::from(gossip_message_update_count))
		.header("X-LDK-Gossip-Omitted-Update-Count", HeaderValue::from(omitted_updates))
		.header("X-LDK-Gossip-Original-Update-Count", HeaderValue::from(updates_without_prior_reference))
		.header("X-LDK-Gossip-Modified-Update-Count", HeaderValue::from(modified_updates))
		.header("X-LDK-Raw-Output-Length", HeaderValue::from(response_length));

	println!("message count: {}\nannouncement count: {}\nupdate count: {}\nraw output length: {}", gossip_message_count, gossip_message_announcement_count, gossip_message_update_count, response_length);
	println!("duplicated node ids: {}", duplicate_node_ids);
	println!("latest seen timestamp: {:?}", latest_seen_timestamp);

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
