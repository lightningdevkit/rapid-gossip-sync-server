use std::collections::HashMap;
use std::convert::Infallible;
use std::io::{Cursor, Write};
use std::sync::{Arc, RwLock};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use lightning::ln::msgs::{OptionalField, UnsignedChannelUpdate};
use lightning::util::ser::{BigSize, Readable, Writeable};
use tokio::sync::mpsc;
use tokio_postgres::NoTls;
use warp::Filter;
use warp::http::{HeaderValue, Response};
use warp::Reply;

use crate::config;
use crate::sample::hex_utils;

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
	let mut output: Vec<u8> = vec![76, 68, 75, 2];
	if timestamp > 0 {
		is_incremental = true;
		output = vec![76, 68, 75, 3]; // indicate an incremental update type
	}

	let mut gossip_message_announcement_count = 0u32;
	let mut gossip_message_update_count = 0u32;

	{
		println!("fetching channels…");

		let block_height_minimum = block as i32;
		// let block_height_minimum = 0i32;

		let rows = client.query("SELECT * FROM channels WHERE block_height >= $1", &[&block_height_minimum]).await.unwrap();
		gossip_message_announcement_count = rows.len() as u32;

		// include the announcement count in lieu of per-announcement type markers
		let count = BigSize(gossip_message_announcement_count as u64);
		count.write(&mut output);

		for current_row in rows {
			let blob: String = current_row.get("announcement_unsigned");
			let mut data = hex_utils::to_vec(&blob).unwrap();
			let length = BigSize(data.len() as u64);
			length.write(&mut output);
			output.append(&mut data);
		}
	}

	let enable_update_reference_comparisons = is_incremental; // true;
	let mut updates_with_prior_reference = 0;
	let mut updates_without_prior_reference = 0;
	let mut omitted_updates = 0;
	let mut modified_updates = 0;

	{
		println!("fetching updates…");
		let timestamp_minimum = timestamp as i64;
		// let timestamp_minimum = 0i64;
		// let rows = client.query("SELECT * FROM channel_updates", &[]).await.unwrap();
		let mut update_data = Vec::new();

		let mut reference: HashMap<String, UnsignedChannelUpdate> = HashMap::new();
		if enable_update_reference_comparisons {
			let reference_rows = client.query("SELECT DISTINCT ON (short_channel_id, direction) * FROM channel_updates WHERE timestamp < $1 AND short_channel_id IN (SELECT short_channel_id FROM channel_updates WHERE timestamp >= $1 GROUP BY short_channel_id) ORDER BY short_channel_id ASC, direction ASC, timestamp DESC", &[&timestamp_minimum]).await.unwrap();
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

		let rows = client.query("SELECT DISTINCT ON (short_channel_id, direction) * FROM channel_updates WHERE timestamp >= $1 ORDER BY short_channel_id ASC, direction ASC, timestamp DESC", &[&timestamp_minimum]).await.unwrap();
		if !enable_update_reference_comparisons {
			// we're not omitting any updates, so might as well include them all here
			gossip_message_update_count = rows.len() as u32;
		}

		let mut modification_tally_by_field_count: HashMap<u8, u32> = HashMap::new();
		let mut modification_tally_by_affected_field_combination: HashMap<String, u32> = HashMap::new();
		for current_row in rows {
			let blob: String = current_row.get("blob_unsigned");
			let mut data = hex_utils::to_vec(&blob).unwrap();

			let mut readable = Cursor::new(&data);
			// readable.set_position(2); // the first two bytes are the type, which in this case we already know
			let mut unsigned_channel_update = UnsignedChannelUpdate::read(&mut readable).unwrap();

			if enable_update_reference_comparisons {
				// if we care about the comparison
				let scid_hex: String = current_row.get("short_channel_id");
				let direction: i32 = current_row.get("direction");
				let reference_key = format!("{}:{}", scid_hex, direction);
				let reference_channel_update = reference.get(&reference_key);

				if let Some(reference_update) = reference_channel_update {
					updates_with_prior_reference += 1;

					let delta = compare_update_with_reference(&unsigned_channel_update, reference_update);

					*modification_tally_by_field_count.entry(delta.affected_field_count).or_insert(0) += 1;
					if delta.affected_field_count > 0 {
						let modified_field_key = delta.affected_fields.join(", ");
						*modification_tally_by_affected_field_combination.entry(modified_field_key).or_insert(0) += 1;

						modified_updates += 1;

						{
							// comment this block to force full updates to always get sent
							gossip_message_update_count += 1;
							let length = BigSize(0u64); // if the length is 0, the following update is incremental
							length.write(&mut update_data);
							update_data.append(&mut delta.serialization.clone());
							continue;
						}
					} else {
						// there is something new
						omitted_updates += 1;
						continue;
					}
				} else {
					updates_without_prior_reference += 1;
				}

			}

			gossip_message_update_count += 1;
			unsigned_channel_update.timestamp = 0;
			// unsigned_channel_update.timestamp = current_timestamp as u32;
			let mut zero_timestamp_serialization = Vec::new(); // vec![1, 2];
			unsigned_channel_update.write(&mut zero_timestamp_serialization).unwrap();

			let length = BigSize(zero_timestamp_serialization.len() as u64);
			length.write(&mut update_data);
			update_data.append(&mut zero_timestamp_serialization);
		}

		let count = BigSize(gossip_message_update_count as u64);
		count.write(&mut output);
		output.append(&mut update_data);

		println!("modification tally by count: {:#?}", modification_tally_by_field_count);
		println!("modification tally by combination: {:#?}", modification_tally_by_affected_field_combination);
	}

	let gossip_message_count = gossip_message_announcement_count + gossip_message_update_count;

	let response_length = output.len();

	println!("packaging response!");
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

	if should_compress {
		println!("compressing gossip data");
		let mut compressor = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
		compressor.write_all(&output);
		let compressed_response = compressor.finish().unwrap();

		let compressed_length = compressed_response.len();
		let compression_efficacy = 1.0 - (compressed_length as f64) / (response_length as f64);

		let efficacy_header = format!("{}%", (compression_efficacy * 1000.0).round() / 10.0);
		output = compressed_response;

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
		.body(output);

	println!("elapsed time: {}", elapsed_time);

	// let response = format!("block: {}<br/>\ntimestamp: {}<br/>\nlength: {}<br/>\nelapsed: {:?}", block, timestamp, response_length, duration);
	Ok(response)
}


struct UpdateChangeSet {
	affected_field_count: u8,
	affected_fields: Vec<String>,
	serialization: Vec<u8>
}

fn compare_update_with_reference(latest_update: &UnsignedChannelUpdate, reference_update: &UnsignedChannelUpdate) -> UpdateChangeSet {
	let mut updated_field_count = 0;
	let mut modified_field_keys = vec![];

	let mut delta_serialization = Vec::new();
	let mut are_flags_changed = false;

	if latest_update.flags != reference_update.flags {
		updated_field_count += 1;
		modified_field_keys.push("flags".to_string());
		are_flags_changed = true;
	}

	if latest_update.cltv_expiry_delta != reference_update.cltv_expiry_delta {
		updated_field_count += 1;
		modified_field_keys.push("cltv_expiry_delta".to_string());

		1u8.write(&mut delta_serialization);
		latest_update.cltv_expiry_delta.write(&mut delta_serialization);
	}

	if latest_update.htlc_minimum_msat != reference_update.htlc_minimum_msat {
		updated_field_count += 1;
		modified_field_keys.push("htlc_minimum_msat".to_string());

		2u8.write(&mut delta_serialization);
		latest_update.htlc_minimum_msat.write(&mut delta_serialization);
	}

	if latest_update.fee_base_msat != reference_update.fee_base_msat {
		updated_field_count += 1;
		modified_field_keys.push("fee_base_msat".to_string());

		3u8.write(&mut delta_serialization);
		latest_update.fee_base_msat.write(&mut delta_serialization);
	}

	if latest_update.fee_proportional_millionths != reference_update.fee_proportional_millionths {
		updated_field_count += 1;
		modified_field_keys.push("fee_proportional_millionths".to_string());

		4u8.write(&mut delta_serialization);
		latest_update.fee_proportional_millionths.write(&mut delta_serialization);
	}

	let mut is_htlc_maximum_identical = false;
	if let OptionalField::Present(new_htlc_maximum) = latest_update.htlc_maximum_msat {
		if let OptionalField::Present(old_htlc_maximum) = reference_update.htlc_maximum_msat {
			if new_htlc_maximum == old_htlc_maximum {
				is_htlc_maximum_identical = true;
			}
		}
	} else if let OptionalField::Absent = reference_update.htlc_maximum_msat {
		is_htlc_maximum_identical = true;
	}

	if !is_htlc_maximum_identical {
		updated_field_count += 1;
		modified_field_keys.push("htlc_maximum_msat".to_string());

		5u8.write(&mut delta_serialization);
		if let OptionalField::Present(new_htlc_maximum) = latest_update.htlc_maximum_msat {
			1u8.write(&mut delta_serialization);
			new_htlc_maximum.write(&mut delta_serialization);
		} else {
			0u8.write(&mut delta_serialization);
		}
	}

	let mut prefixed_serialization = Vec::new();
	if updated_field_count > 0 {
		// if no field was changed, there is no point serializing anything at all
		latest_update.flags.write(&mut prefixed_serialization);

		let mut serialized_change_count = updated_field_count;
		if are_flags_changed {
			// the flags are always included to identify the node, so we don't want to count them
			serialized_change_count -= 1;
		}
		serialized_change_count.write(&mut prefixed_serialization);
		prefixed_serialization.append(&mut delta_serialization);
	}

	UpdateChangeSet {
		affected_field_count: updated_field_count,
		affected_fields: modified_field_keys,
		serialization: prefixed_serialization
	}
}
