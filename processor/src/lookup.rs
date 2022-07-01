use std::collections::{BTreeMap, HashSet};
use std::io::Cursor;
use std::sync::Arc;
use std::time::Instant;

use lightning::ln::msgs::{UnsignedChannelAnnouncement, UnsignedChannelUpdate};
use lightning::routing::gossip::NetworkGraph;
use lightning::util::ser::Readable;
use lightning::util::test_utils::TestLogger;
use tokio_postgres::{Client, Connection, NoTls, Socket};
use tokio_postgres::tls::NoTlsStream;

use crate::{config, hex_utils};

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
	pub(super) intermediate_updates: Vec<UnsignedChannelUpdate>,
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
			intermediate_updates: vec![],
			latest_update_after_seen: None,
		}
	}
}

pub(super) async fn connect_to_db() -> (Client, Connection<Socket, NoTlsStream>) {
	let connection_config = config::db_connection_config();
	connection_config.connect(NoTls).await.unwrap()
}

/// Fetch all the channel announcements that are presently in the network graph, regardless of
/// whether they had been seen before
pub(super) async fn fetch_channel_announcements(mut delta_set: DeltaSet, network_graph: Arc<NetworkGraph<Arc<TestLogger>>>, client: &Client, last_sync_timestamp: u32) -> DeltaSet {
	// also include all announcements for which the first update was announced
	// after `last_syc_timestamp`
	// omit all announcements for which the latest update is older than 14 days
	let channel_ids = {
		let read_only_graph = network_graph.read_only();
		let channel_iterator = read_only_graph.channels().into_iter();
		channel_iterator
			.filter(|c| c.1.announcement_message.is_some())
			.map(|c| hex_utils::hex_str(&c.1.announcement_message.clone().unwrap().contents.short_channel_id.to_be_bytes()))
			.collect::<Vec<String>>()
	};

	// get all the channel announcements that are currently in the network graph
	let announcement_rows = client.query("SELECT * FROM channels WHERE short_channel_id = any($1) ORDER BY short_channel_id ASC", &[&channel_ids]).await.unwrap();

	for current_announcement_row in announcement_rows {
		let blob: String = current_announcement_row.get("announcement_unsigned");
		let data = hex_utils::to_vec(&blob).unwrap();
		let mut readable = Cursor::new(data);
		let unsigned_announcement = UnsignedChannelAnnouncement::read(&mut readable).unwrap();

		let scid: String = current_announcement_row.get("short_channel_id");
		let current_seen_timestamp: u32 = current_announcement_row.get("seen");

		let mut current_channel_delta = delta_set.entry(scid).or_insert(ChannelDelta::default());
		(*current_channel_delta).announcement = Some(AnnouncementDelta {
			announcement: unsigned_announcement,
			seen: current_seen_timestamp,
		});
	}

	let unannounced_rows = client.query("SELECT * FROM (SELECT DISTINCT ON (short_channel_id) * FROM channel_updates ORDER BY short_channel_id ASC, seen ASC) AS first_seens WHERE first_seens.seen >= $1", &[&last_sync_timestamp]).await.unwrap();
	for current_row in unannounced_rows {
		let scid: String = current_row.get("short_channel_id");
		let current_seen_timestamp: u32 = current_row.get("seen");
		let mut current_channel_delta = delta_set.entry(scid).or_insert(ChannelDelta::default());
		(*current_channel_delta).first_update_seen = Some(current_seen_timestamp);
	}
	delta_set
}

pub(super) async fn fetch_channel_updates(mut delta_set: DeltaSet, client: &Client, last_sync_timestamp: u32) -> DeltaSet {
	let start = Instant::now();

	// get the latest channel update in each direction prior to last_sync_timestamp, provided
	// there was an update in either direction that happened after the last sync (to avoid
	// collecting too many reference updates)
	let reference_rows = client.query("SELECT DISTINCT ON (short_channel_id, direction) * FROM channel_updates WHERE seen < $1 AND short_channel_id IN (SELECT short_channel_id FROM channel_updates WHERE seen >= $1 GROUP BY short_channel_id) ORDER BY short_channel_id ASC, direction ASC, seen DESC", &[&last_sync_timestamp]).await.unwrap();

	println!("Fetched reference rows ({}): {:?}", reference_rows.len(), start.elapsed());

	let mut last_seen_update_ids: Vec<i32> = Vec::with_capacity(reference_rows.len());
	let mut non_intermediate_ids: HashSet<i32> = HashSet::new();

	for current_reference in reference_rows {
		let update_id: i32 = current_reference.get("id");
		last_seen_update_ids.push(update_id);
		non_intermediate_ids.insert(update_id);

		let scid_hex: String = current_reference.get("short_channel_id");
		let direction: i32 = current_reference.get("direction");
		let reference_key = format!("{}:{}", scid_hex, direction);
		let blob: String = current_reference.get("blob_unsigned");
		let data = hex_utils::to_vec(&blob).unwrap();
		let mut readable = Cursor::new(data);
		let unsigned_channel_update = UnsignedChannelUpdate::read(&mut readable).unwrap();

		let mut current_channel_delta = delta_set.entry(scid_hex).or_insert(ChannelDelta::default());
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

	// get the latest channel update in each direction, provided it happened after last_sync_timestamp
	let update_rows = client.query("SELECT DISTINCT ON (short_channel_id, direction) * FROM channel_updates WHERE seen >= $1 ORDER BY short_channel_id ASC, direction ASC, seen DESC", &[&last_sync_timestamp]).await.unwrap();
	println!("Fetched update rows ({}): {:?}", update_rows.len(), start.elapsed());

	let mut latest_update_ids: Vec<i32> = Vec::with_capacity(update_rows.len());
	for current_update in update_rows {
		let update_id: i32 = current_update.get("id");
		latest_update_ids.push(update_id);
		non_intermediate_ids.insert(update_id);

		let scid_hex: String = current_update.get("short_channel_id");
		let direction: i32 = current_update.get("direction");
		let current_seen_timestamp: u32 = current_update.get("seen");
		let blob: String = current_update.get("blob_unsigned");
		let data = hex_utils::to_vec(&blob).unwrap();
		let mut readable = Cursor::new(data);
		let unsigned_channel_update = UnsignedChannelUpdate::read(&mut readable).unwrap();

		let mut current_channel_delta = delta_set.entry(scid_hex).or_insert(ChannelDelta::default());
		let mut update_delta = if direction == 0 {
			(*current_channel_delta).updates.0.get_or_insert(DirectedUpdateDelta::default())
		} else if direction == 1 {
			(*current_channel_delta).updates.1.get_or_insert(DirectedUpdateDelta::default())
		} else {
			panic!("Channel direction must be binary!")
		};
		update_delta.latest_update_after_seen = Some(UpdateDelta {
			seen: current_seen_timestamp,
			update: unsigned_channel_update,
		});
	}

	println!("Processed update rows (delta size: {}): {:?}", delta_set.len(), start.elapsed());

	// get all the intermediate channel updates
	// (to calculate the set of mutated fields for snapshotting, where intermediate updates may
	// have been omitted)
	// let intermediate_updates = client.query("SELECT * FROM channel_updates WHERE seen >= $1 AND id != all($2) AND id != all($3) ORDER BY short_channel_id ASC, direction ASC, seen ASC", &[&last_sync_timestamp, &last_seen_update_ids, &latest_update_ids]).await.unwrap();
	// let non_intermediate_id_array = Vec::from_iter(non_intermediate_ids.iter());
	// let intermediate_updates = client.query("SELECT * FROM channel_updates WHERE seen >= $1 AND id != all($2) ORDER BY short_channel_id ASC, direction ASC, seen ASC", &[&last_sync_timestamp, &non_intermediate_id_array]).await.unwrap();
	let intermediate_updates = client.query("SELECT * FROM channel_updates WHERE seen >= $1 ORDER BY short_channel_id ASC, direction ASC, seen ASC", &[&last_sync_timestamp]).await.unwrap();
	println!("Fetched intermediate rows ({}): {:?}", intermediate_updates.len(), start.elapsed());

	let mut intermediate_update_count = 0;
	for intermediate_update in intermediate_updates {
		let update_id: i32 = intermediate_update.get("id");
		if non_intermediate_ids.contains(&update_id) {
			continue;
		}
		intermediate_update_count += 1;

		let scid_hex: String = intermediate_update.get("short_channel_id");
		let direction: i32 = intermediate_update.get("direction");
		let blob: String = intermediate_update.get("blob_unsigned");
		let data = hex_utils::to_vec(&blob).unwrap();
		let mut readable = Cursor::new(data);
		let unsigned_channel_update = UnsignedChannelUpdate::read(&mut readable).unwrap();

		let mut current_channel_delta = delta_set.entry(scid_hex).or_insert(ChannelDelta::default());
		let mut update_delta = if direction == 0 {
			(*current_channel_delta).updates.0.get_or_insert(DirectedUpdateDelta::default())
		} else if direction == 1 {
			(*current_channel_delta).updates.1.get_or_insert(DirectedUpdateDelta::default())
		} else {
			panic!("Channel direction must be binary!")
		};
		update_delta.intermediate_updates.push(unsigned_channel_update);
	}
	println!("Processed intermediate rows ({}) (delta size: {}): {:?}", intermediate_update_count, delta_set.len(), start.elapsed());

	delta_set
}

pub(super) fn filter_delta_set(delta_set: DeltaSet) -> DeltaSet {
	let filtered_delta: DeltaSet = delta_set.into_iter().filter(|(k, v)| {
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

		let mut direction_a_meets_criteria = update_meets_criteria(&v.updates.0);
		let mut direction_b_meets_criteria = update_meets_criteria(&v.updates.1);

		direction_a_meets_criteria || direction_b_meets_criteria
	}).collect();
	println!("filtered delta count: {}", filtered_delta.len());
	filtered_delta
}
