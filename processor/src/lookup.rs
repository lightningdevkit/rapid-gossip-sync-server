use std::collections::{BTreeMap, HashSet};
use std::io::Cursor;
use std::sync::Arc;
use std::time::Instant;

use lightning::ln::msgs::{ChannelAnnouncement, ChannelUpdate, UnsignedChannelAnnouncement, UnsignedChannelUpdate};
use lightning::routing::gossip::NetworkGraph;
use lightning::util::ser::Readable;
use tokio_postgres::{Client, Connection, NoTls, Socket};
use tokio_postgres::tls::NoTlsStream;

use crate::{config, hex_utils, TestLogger};
use crate::serialization::MutatedProperties;

/// The delta set needs to be a BTreeMap so the keys are sorted.
/// That way, the scids in the response automatically grow monotonically
pub(super) type DeltaSet = BTreeMap<String, ChannelDelta>;

pub(super) struct AnnouncementDelta {
	pub(super) seen: u32,
	pub(super) announcement: UnsignedChannelAnnouncement,
}

pub(super) struct UpdateDelta {
	pub(super) seen: u32,
	pub(super) update: UnsignedChannelUpdate,
}

pub(super) struct DirectedUpdateDelta {
	pub(super) last_update_before_seen: Option<UnsignedChannelUpdate>,
	pub(super) mutated_properties: MutatedProperties,
	pub(super) latest_update_after_seen: Option<UpdateDelta>,
}

pub(super) struct ChannelDelta {
	pub(super) announcement: Option<AnnouncementDelta>,
	pub(super) updates: (Option<DirectedUpdateDelta>, Option<DirectedUpdateDelta>),
	pub(super) first_update_seen: Option<u32>,
}

impl Default for ChannelDelta {
	fn default() -> Self {
		Self { announcement: None, updates: (None, None), first_update_seen: None }
	}
}

impl Default for DirectedUpdateDelta {
	fn default() -> Self {
		Self {
			last_update_before_seen: None,
			mutated_properties: MutatedProperties::default(),
			latest_update_after_seen: None,
		}
	}
}

pub(super) async fn connect_to_db() -> (Client, Connection<Socket, NoTlsStream>) {
	let connection_config = config::db_connection_config();
	connection_config.connect(NoTls).await.unwrap()
}

/*
/// Slower, but more memory-efficient alternative to doing the three-step method below
pub(super) async fn calculate_delta_set(network_graph: Arc<NetworkGraph<Arc<TestLogger>>>, client: &Client, last_sync_timestamp: u32, consider_intermediate_updates: bool) -> DeltaSet {
	let delta_set = DeltaSet::new();

	// step 1: get all the current channel IDs
	let active_channel_ids = {
		let read_only_graph = network_graph.read_only();
		let channel_iterator = read_only_graph.channels().into_iter();
		channel_iterator
			.filter(|c| c.1.announcement_message.is_some())
			.map(|c| hex_utils::hex_str(&c.1.announcement_message.clone().unwrap().contents.short_channel_id.to_be_bytes()))
			.collect::<Vec<String>>()
	};

	// step 2: iterate over all the channel IDs and do slow as fuck database operations
	for current_scid_hex in active_channel_ids {
		/// determine whether this channel has any updates yet. Unless it has at least one update
		/// per direction, this channel can be skipped

		// fetch the latest seen channel updates per direction
		let last_seen_updates = client.query("SELECT DISTINCT ON (direction) * FROM channel_updates WHERE seen < $1 AND short_channel_id = $2 ORDER BY direction ASC, seen DESC", &[&last_sync_timestamp, &current_scid_hex]).await.unwrap();
		// println!("here we are");
	}

	delta_set
}*/

/// Fetch all the channel announcements that are presently in the network graph, regardless of
/// whether they had been seen before.
/// Also include all announcements for which the first update was announced
/// after `last_syc_timestamp`
pub(super) async fn fetch_channel_announcements(mut delta_set: DeltaSet, network_graph: Arc<NetworkGraph<Arc<TestLogger>>>, client: &Client, last_sync_timestamp: u32) -> DeltaSet {
	println!("Obtaining channel ids from network graph");
	let channel_ids = {
		let read_only_graph = network_graph.read_only();
		println!("Retrieved read-only network graph copy");
		let channel_iterator = read_only_graph.channels().into_iter();
		channel_iterator
			.filter(|c| c.1.announcement_message.is_some())
			.map(|c| hex_utils::hex_str(&c.1.announcement_message.clone().unwrap().contents.short_channel_id.to_be_bytes()))
			.collect::<Vec<String>>()
	};

	println!("Obtaining corresponding database entries");
	// get all the channel announcements that are currently in the network graph
	let announcement_rows = client.query("SELECT short_channel_id, announcement_signed, seen FROM channel_announcements WHERE short_channel_id = any($1) ORDER BY short_channel_id ASC", &[&channel_ids]).await.unwrap();

	for current_announcement_row in announcement_rows {
		let blob: String = current_announcement_row.get("announcement_signed");
		let data = hex_utils::to_vec(&blob).unwrap();
		let mut readable = Cursor::new(data);
		let unsigned_announcement = ChannelAnnouncement::read(&mut readable).unwrap().contents;

		let scid: String = current_announcement_row.get("short_channel_id");
		let current_seen_timestamp: u32 = current_announcement_row.get("seen");

		let mut current_channel_delta = delta_set.entry(scid).or_insert(ChannelDelta::default());
		(*current_channel_delta).announcement = Some(AnnouncementDelta {
			announcement: unsigned_announcement,
			seen: current_seen_timestamp,
		});
	}

	println!("Obtaining channel annoouncements whose first channel updates had not been seen yet");

	/// here is where the channels whose first update in either direction occurred after
	/// `last_seen_timestamp` are added to the selection
	let unannounced_rows = client.query("SELECT short_channel_id, seen FROM (SELECT DISTINCT ON (short_channel_id) short_channel_id, seen FROM channel_updates ORDER BY short_channel_id ASC, seen ASC) AS first_seens WHERE first_seens.seen >= $1", &[&last_sync_timestamp]).await.unwrap();
	for current_row in unannounced_rows {
		let scid: String = current_row.get("short_channel_id");
		let current_seen_timestamp: u32 = current_row.get("seen");
		let mut current_channel_delta = delta_set.entry(scid).or_insert(ChannelDelta::default());
		(*current_channel_delta).first_update_seen = Some(current_seen_timestamp);
	}
	delta_set
}

pub(super) async fn fetch_channel_updates(mut delta_set: DeltaSet, client: &Client, last_sync_timestamp: u32, consider_intermediate_updates: bool) -> DeltaSet {
	let start = Instant::now();

	// get the latest channel update in each direction prior to last_sync_timestamp, provided
	// there was an update in either direction that happened after the last sync (to avoid
	// collecting too many reference updates)
	let reference_rows = client.query("SELECT DISTINCT ON (short_channel_id, direction) id, short_channel_id, direction, blob_signed FROM channel_updates WHERE seen < $1 AND short_channel_id IN (SELECT short_channel_id FROM channel_updates WHERE seen >= $1 GROUP BY short_channel_id) ORDER BY short_channel_id ASC, direction ASC, seen DESC", &[&last_sync_timestamp]).await.unwrap();

	println!("Fetched reference rows ({}): {:?}", reference_rows.len(), start.elapsed());

	let mut last_seen_update_ids: Vec<i32> = Vec::with_capacity(reference_rows.len());
	let mut non_intermediate_ids: HashSet<i32> = HashSet::new();

	for current_reference in reference_rows {
		let update_id: i32 = current_reference.get("id");
		last_seen_update_ids.push(update_id);
		non_intermediate_ids.insert(update_id);

		let scid_hex: String = current_reference.get("short_channel_id");
		let direction: i32 = current_reference.get("direction");
		let _reference_key = format!("{}:{}", scid_hex, direction);
		let blob: String = current_reference.get("blob_signed");
		let data = hex_utils::to_vec(&blob).unwrap();
		let mut readable = Cursor::new(data);
		let unsigned_channel_update = ChannelUpdate::read(&mut readable).unwrap().contents;

		let current_channel_delta = delta_set.entry(scid_hex).or_insert(ChannelDelta::default());
		let mut update_delta = if direction == 0 {
			(*current_channel_delta).updates.0.get_or_insert(DirectedUpdateDelta::default())
		} else if direction == 1 {
			(*current_channel_delta).updates.1.get_or_insert(DirectedUpdateDelta::default())
		} else {
			panic!("Channel direction must be binary!")
		};
		update_delta.last_update_before_seen = Some(unsigned_channel_update);


	}

	println!("Processed reference rows (delta size: {}): {:?}", delta_set.len(), start.elapsed());

	// get all the intermediate channel updates
	// (to calculate the set of mutated fields for snapshotting, where intermediate updates may
	// have been omitted)
	// let intermediate_updates = client.query("SELECT * FROM channel_updates WHERE seen >= $1 AND id != all($2) AND id != all($3) ORDER BY short_channel_id ASC, direction ASC, seen ASC", &[&last_sync_timestamp, &last_seen_update_ids, &latest_update_ids]).await.unwrap();
	// let non_intermediate_id_array = Vec::from_iter(non_intermediate_ids.iter());
	// let intermediate_updates = client.query("SELECT * FROM channel_updates WHERE seen >= $1 AND id != all($2) ORDER BY short_channel_id ASC, direction ASC, seen ASC", &[&last_sync_timestamp, &non_intermediate_id_array]).await.unwrap();

	let mut intermediate_update_prefix = "";
	if !consider_intermediate_updates {
		intermediate_update_prefix = "DISTINCT ON (short_channel_id, direction)";
	}

	let query_string = format!("SELECT {} id, short_channel_id, direction, blob_signed, seen FROM channel_updates WHERE seen >= $1 ORDER BY short_channel_id ASC, direction ASC, seen DESC", intermediate_update_prefix);
	let intermediate_updates = client.query(&query_string, &[&last_sync_timestamp]).await.unwrap();
	println!("Fetched intermediate rows ({}): {:?}", intermediate_updates.len(), start.elapsed());

	let mut previous_scid_hex = "".to_string();
	let mut previously_seen_directions = (false, false);

	// let mut previously_seen_directions = (false, false);
	let mut intermediate_update_count = 0;
	for intermediate_update in intermediate_updates {
		let update_id: i32 = intermediate_update.get("id");
		if non_intermediate_ids.contains(&update_id) {
			continue;
		}
		intermediate_update_count += 1;

		let scid_hex: String = intermediate_update.get("short_channel_id");

		if scid_hex != previous_scid_hex {
			previous_scid_hex = scid_hex.clone();
			previously_seen_directions = (false, false);
		}

		let direction: i32 = intermediate_update.get("direction");
		let current_seen_timestamp: u32 = intermediate_update.get("seen");
		let blob: String = intermediate_update.get("blob_signed");
		let data = hex_utils::to_vec(&blob).unwrap();
		let mut readable = Cursor::new(data);
		let unsigned_channel_update = ChannelUpdate::read(&mut readable).unwrap().contents;

		// get the write configuration for this particular channel's directional details
		let current_channel_delta = delta_set.entry(scid_hex.clone()).or_insert(ChannelDelta::default());
		let update_delta = if direction == 0 {
			(*current_channel_delta).updates.0.get_or_insert(DirectedUpdateDelta::default())
		} else if direction == 1 {
			(*current_channel_delta).updates.1.get_or_insert(DirectedUpdateDelta::default())
		} else {
			panic!("Channel direction must be binary!")
		};

		{
			// handle the latest deltas
			if direction == 0 && !previously_seen_directions.0 {
				previously_seen_directions.0 = true;
				update_delta.latest_update_after_seen = Some(UpdateDelta {
					seen: current_seen_timestamp,
					update: unsigned_channel_update.clone(),
				});
			} else if direction == 1 && !previously_seen_directions.1 {
				previously_seen_directions.1 = true;
				update_delta.latest_update_after_seen = Some(UpdateDelta {
					seen: current_seen_timestamp,
					update: unsigned_channel_update.clone(),
				});
			}
		}

		// determine mutations
		if let Some(last_seen_update) = update_delta.last_update_before_seen.as_ref(){
			if unsigned_channel_update.flags != last_seen_update.flags {
				update_delta.mutated_properties.flags = true;
			}
			if unsigned_channel_update.cltv_expiry_delta != last_seen_update.cltv_expiry_delta {
				update_delta.mutated_properties.cltv_expiry_delta = true;
			}
			if unsigned_channel_update.htlc_minimum_msat != last_seen_update.htlc_minimum_msat {
				update_delta.mutated_properties.htlc_minimum_msat = true;
			}
			if unsigned_channel_update.fee_base_msat != last_seen_update.fee_base_msat {
				update_delta.mutated_properties.fee_base_msat = true;
			}
			if unsigned_channel_update.fee_proportional_millionths != last_seen_update.fee_proportional_millionths {
				update_delta.mutated_properties.fee_proportional_millionths = true;
			}
			if unsigned_channel_update.htlc_maximum_msat != last_seen_update.htlc_maximum_msat {
				update_delta.mutated_properties.htlc_maximum_msat = true;
			}
		}

	}
	println!("Processed intermediate rows ({}) (delta size: {}): {:?}", intermediate_update_count, delta_set.len(), start.elapsed());

	delta_set
}

pub(super) fn filter_delta_set(delta_set: DeltaSet) -> DeltaSet {
	let filtered_delta: DeltaSet = delta_set.into_iter().filter(|(_k, v)| {
		if v.announcement.is_none() {
			// this channel is not currently in the network graph
			return false;
		}

		let update_meets_criteria = |update: &Option<DirectedUpdateDelta>| {
			if update.is_none() {
				return false;
			};
			let update_reference = update.as_ref().unwrap();
			// update_reference.latest_update_after_seen.is_some() && !update_reference.intermediate_updates.is_empty()
			// if there has been an update after the channel was first seen
			update_reference.latest_update_after_seen.is_some()
		};

		let direction_a_meets_criteria = update_meets_criteria(&v.updates.0);
		let direction_b_meets_criteria = update_meets_criteria(&v.updates.1);

		direction_a_meets_criteria || direction_b_meets_criteria
	}).collect();
	println!("filtered delta count: {}", filtered_delta.len());
	filtered_delta
}
