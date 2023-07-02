use std::collections::{BTreeMap, HashSet};
use std::io::Cursor;
use std::ops::Add;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use lightning::ln::msgs::{ChannelAnnouncement, ChannelUpdate, UnsignedChannelAnnouncement, UnsignedChannelUpdate};
use lightning::routing::gossip::NetworkGraph;
use lightning::util::ser::Readable;
use tokio_postgres::{Client, Connection, NoTls, Socket};
use tokio_postgres::tls::NoTlsStream;

use crate::{config, TestLogger};
use crate::serialization::MutatedProperties;

/// The delta set needs to be a BTreeMap so the keys are sorted.
/// That way, the scids in the response automatically grow monotonically
pub(super) type DeltaSet = BTreeMap<u64, ChannelDelta>;

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
	pub(super) serialization_update_flags: Option<u8>,
}

pub(super) struct ChannelDelta {
	pub(super) announcement: Option<AnnouncementDelta>,
	pub(super) updates: (Option<DirectedUpdateDelta>, Option<DirectedUpdateDelta>),
	pub(super) first_bidirectional_updates_seen: Option<u32>,
	/// The seen timestamp of the older of the two latest directional updates
	pub(super) requires_reminder: bool,
}

impl Default for ChannelDelta {
	fn default() -> Self {
		Self {
			announcement: None,
			updates: (None, None),
			first_bidirectional_updates_seen: None,
			requires_reminder: false,
		}
	}
}

impl Default for DirectedUpdateDelta {
	fn default() -> Self {
		Self {
			last_update_before_seen: None,
			mutated_properties: MutatedProperties::default(),
			latest_update_after_seen: None,
			serialization_update_flags: None,
		}
	}
}

pub(super) async fn connect_to_db() -> (Client, Connection<Socket, NoTlsStream>) {
	let connection_config = config::db_connection_config();
	connection_config.connect(NoTls).await.unwrap()
}

/// Fetch all the channel announcements that are presently in the network graph, regardless of
/// whether they had been seen before.
/// Also include all announcements for which the first update was announced
/// after `last_sync_timestamp`
pub(super) async fn fetch_channel_announcements(delta_set: &mut DeltaSet, network_graph: Arc<NetworkGraph<TestLogger>>, client: &Client, last_sync_timestamp: u32) {
	println!("Obtaining channel ids from network graph");
	let channel_ids = {
		let read_only_graph = network_graph.read_only();
		println!("Retrieved read-only network graph copy");
		let channel_iterator = read_only_graph.channels().unordered_iter();
		channel_iterator
			.filter(|c| c.1.announcement_message.is_some())
			.map(|c| c.1.announcement_message.as_ref().unwrap().contents.short_channel_id as i64)
			.collect::<Vec<_>>()
	};

	println!("Obtaining corresponding database entries");
	// get all the channel announcements that are currently in the network graph
	let announcement_rows = client.query("SELECT announcement_signed, seen FROM channel_announcements WHERE short_channel_id = any($1) ORDER BY short_channel_id ASC", &[&channel_ids]).await.unwrap();

	for current_announcement_row in announcement_rows {
		let blob: Vec<u8> = current_announcement_row.get("announcement_signed");
		let mut readable = Cursor::new(blob);
		let unsigned_announcement = ChannelAnnouncement::read(&mut readable).unwrap().contents;

		let scid = unsigned_announcement.short_channel_id;
		let current_seen_timestamp_object: SystemTime = current_announcement_row.get("seen");
		let current_seen_timestamp: u32 = current_seen_timestamp_object.duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs() as u32;

		let current_channel_delta = delta_set.entry(scid).or_insert(ChannelDelta::default());
		(*current_channel_delta).announcement = Some(AnnouncementDelta {
			announcement: unsigned_announcement,
			seen: current_seen_timestamp,
		});
	}

	{
		// THIS STEP IS USED TO DETERMINE IF A CHANNEL SHOULD BE OMITTED FROM THE DELTA

		println!("Annotating channel announcements whose oldest channel update in a given direction occurred after the last sync");
		// Steps:
		// — Obtain all updates, distinct by (scid, direction), ordered by seen DESC // to find the oldest update in a given direction
		// — From those updates, select distinct by (scid), ordered by seen DESC (to obtain the newer one per direction)
		// This will allow us to mark the first time updates in both directions were seen

		// here is where the channels whose first update in either direction occurred after
		// `last_seen_timestamp` are added to the selection
		let newer_oldest_directional_updates = client.query("
            SELECT DISTINCT ON (short_channel_id) *
            FROM (
                SELECT DISTINCT ON (short_channel_id, direction) short_channel_id, seen
                FROM channel_updates
                WHERE short_channel_id = any($1)
                ORDER BY short_channel_id ASC, direction ASC, seen ASC
            ) AS directional_last_seens
            ORDER BY short_channel_id ASC, seen DESC
        ", &[&channel_ids]).await.unwrap();

		for current_row in newer_oldest_directional_updates {
			let scid: i64 = current_row.get("short_channel_id");
			let current_seen_timestamp_object: SystemTime = current_row.get("seen");
			let current_seen_timestamp: u32 = current_seen_timestamp_object.duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs() as u32;

			if current_seen_timestamp > last_sync_timestamp {
				// the newer of the two oldest seen directional updates came after last sync timestamp
				let current_channel_delta = delta_set.entry(scid as u64).or_insert(ChannelDelta::default());
				// first time a channel was seen in both directions
				(*current_channel_delta).first_bidirectional_updates_seen = Some(current_seen_timestamp);
			}
		}
	}

	{
		// THIS STEP IS USED TO DETERMINE IF A REMINDER UPDATE SHOULD BE SENT

		println!("Annotating channel announcements whose latest channel update in a given direction occurred more than six days ago");
		// Steps:
		// — Obtain all updates, distinct by (scid, direction), ordered by seen DESC
		// — From those updates, select distinct by (scid), ordered by seen ASC (to obtain the older one per direction)
		let current_timestamp = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs() as u32;
		let reminder_threshold_timestamp = current_timestamp.saturating_sub(config::CHANNEL_REMINDER_AGE);
		let read_only_graph = network_graph.read_only();

		let older_latest_directional_updates = client.query("
			SELECT DISTINCT ON (short_channel_id) *
			FROM (
				SELECT DISTINCT ON (short_channel_id, direction) short_channel_id, seen
				FROM channel_updates
				WHERE short_channel_id = any($1)
				ORDER BY short_channel_id ASC, direction ASC, seen DESC
			) AS directional_last_seens
			ORDER BY short_channel_id ASC, seen ASC
		", &[&channel_ids]).await.unwrap();

		for current_row in older_latest_directional_updates {
			let scid: i64 = current_row.get("short_channel_id");
			let current_seen_timestamp_object: SystemTime = current_row.get("seen");
			let current_seen_timestamp: u32 = current_seen_timestamp_object.duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs() as u32;

			if current_seen_timestamp <= reminder_threshold_timestamp {
				// annotate this channel as requiring that reminders be sent to the client
				let current_channel_delta = delta_set.entry(scid as u64).or_insert(ChannelDelta::default());

				// way might be able to get away with not using this
				(*current_channel_delta).requires_reminder = true;

				if let Some(current_channel_info) = read_only_graph.channel(scid as u64) {
					if current_channel_info.one_to_two.is_none() || current_channel_info.two_to_one.is_none() {
						// we don't send reminders if we don't have bidirectional update data
						continue;
					}

					if let Some(info) = current_channel_info.one_to_two.as_ref() {
						let flags: u8 = if info.enabled { 0 } else { 2 };
						let current_update = (*current_channel_delta).updates.0.get_or_insert(DirectedUpdateDelta::default());
						current_update.serialization_update_flags = Some(flags);
					}

					if let Some(info) = current_channel_info.two_to_one.as_ref() {
						let flags: u8 = if info.enabled { 1 } else { 3 };
						let current_update = (*current_channel_delta).updates.1.get_or_insert(DirectedUpdateDelta::default());
						current_update.serialization_update_flags = Some(flags);
					}
				} else {
					// we don't send reminders if we don't have the channel
					continue;
				}
			}
		}
	}
}

pub(super) async fn fetch_channel_updates(delta_set: &mut DeltaSet, client: &Client, last_sync_timestamp: u32, consider_intermediate_updates: bool) {
	let start = Instant::now();
	let last_sync_timestamp_object = SystemTime::UNIX_EPOCH.add(Duration::from_secs(last_sync_timestamp as u64));

	// get the latest channel update in each direction prior to last_sync_timestamp, provided
	// there was an update in either direction that happened after the last sync (to avoid
	// collecting too many reference updates)
	let reference_rows = client.query("
		SELECT DISTINCT ON (short_channel_id, direction) id, direction, blob_signed
		FROM channel_updates
		WHERE seen < $1 AND short_channel_id IN (
			SELECT short_channel_id
			FROM channel_updates
			WHERE seen >= $1
			GROUP BY short_channel_id
		)
		ORDER BY short_channel_id ASC, direction ASC, seen DESC
		", &[&last_sync_timestamp_object]).await.unwrap();

	println!("Fetched reference rows ({}): {:?}", reference_rows.len(), start.elapsed());

	let mut last_seen_update_ids: Vec<i32> = Vec::with_capacity(reference_rows.len());
	let mut non_intermediate_ids: HashSet<i32> = HashSet::new();

	for current_reference in reference_rows {
		let update_id: i32 = current_reference.get("id");
		last_seen_update_ids.push(update_id);
		non_intermediate_ids.insert(update_id);

		let direction: bool = current_reference.get("direction");
		let blob: Vec<u8> = current_reference.get("blob_signed");
		let mut readable = Cursor::new(blob);
		let unsigned_channel_update = ChannelUpdate::read(&mut readable).unwrap().contents;
		let scid = unsigned_channel_update.short_channel_id;

		let current_channel_delta = delta_set.entry(scid).or_insert(ChannelDelta::default());
		let update_delta = if !direction {
			(*current_channel_delta).updates.0.get_or_insert(DirectedUpdateDelta::default())
		} else {
			(*current_channel_delta).updates.1.get_or_insert(DirectedUpdateDelta::default())
		};
		update_delta.last_update_before_seen = Some(unsigned_channel_update);
	}

	println!("Processed reference rows (delta size: {}): {:?}", delta_set.len(), start.elapsed());

	// get all the intermediate channel updates
	// (to calculate the set of mutated fields for snapshotting, where intermediate updates may
	// have been omitted)

	let mut intermediate_update_prefix = "";
	if !consider_intermediate_updates {
		intermediate_update_prefix = "DISTINCT ON (short_channel_id, direction)";
	}

	let query_string = format!("
		SELECT {} id, direction, blob_signed, seen
		FROM channel_updates
		WHERE seen >= $1
		ORDER BY short_channel_id ASC, direction ASC, seen DESC
		", intermediate_update_prefix);
	let intermediate_updates = client.query(&query_string, &[&last_sync_timestamp_object]).await.unwrap();
	println!("Fetched intermediate rows ({}): {:?}", intermediate_updates.len(), start.elapsed());

	let mut previous_scid = u64::MAX;
	let mut previously_seen_directions = (false, false);

	// let mut previously_seen_directions = (false, false);
	let mut intermediate_update_count = 0;
	for intermediate_update in intermediate_updates {
		let update_id: i32 = intermediate_update.get("id");
		if non_intermediate_ids.contains(&update_id) {
			continue;
		}
		intermediate_update_count += 1;

		let direction: bool = intermediate_update.get("direction");
		let current_seen_timestamp_object: SystemTime = intermediate_update.get("seen");
		let current_seen_timestamp: u32 = current_seen_timestamp_object.duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs() as u32;
		let blob: Vec<u8> = intermediate_update.get("blob_signed");
		let mut readable = Cursor::new(blob);
		let unsigned_channel_update = ChannelUpdate::read(&mut readable).unwrap().contents;

		let scid = unsigned_channel_update.short_channel_id;
		if scid != previous_scid {
			previous_scid = scid;
			previously_seen_directions = (false, false);
		}

		// get the write configuration for this particular channel's directional details
		let current_channel_delta = delta_set.entry(scid).or_insert(ChannelDelta::default());
		let update_delta = if !direction {
			(*current_channel_delta).updates.0.get_or_insert(DirectedUpdateDelta::default())
		} else {
			(*current_channel_delta).updates.1.get_or_insert(DirectedUpdateDelta::default())
		};

		{
			// handle the latest deltas
			if !direction && !previously_seen_directions.0 {
				previously_seen_directions.0 = true;
				update_delta.latest_update_after_seen = Some(UpdateDelta {
					seen: current_seen_timestamp,
					update: unsigned_channel_update.clone(),
				});
			} else if direction && !previously_seen_directions.1 {
				previously_seen_directions.1 = true;
				update_delta.latest_update_after_seen = Some(UpdateDelta {
					seen: current_seen_timestamp,
					update: unsigned_channel_update.clone(),
				});
			}
		}

		// determine mutations
		if let Some(last_seen_update) = update_delta.last_update_before_seen.as_ref() {
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
}

pub(super) fn filter_delta_set(delta_set: &mut DeltaSet) {
	let original_length = delta_set.len();
	let keys: Vec<u64> = delta_set.keys().cloned().collect();
	for k in keys {
		let v = delta_set.get(&k).unwrap();
		if v.announcement.is_none() {
			// this channel is not currently in the network graph
			delta_set.remove(&k);
			continue;
		}

		let update_meets_criteria = |update: &Option<DirectedUpdateDelta>| {
			if update.is_none() {
				return false;
			};
			let update_reference = update.as_ref().unwrap();
			// update_reference.latest_update_after_seen.is_some() && !update_reference.intermediate_updates.is_empty()
			// if there has been an update after the channel was first seen

			v.requires_reminder || update_reference.latest_update_after_seen.is_some()
		};

		let direction_a_meets_criteria = update_meets_criteria(&v.updates.0);
		let direction_b_meets_criteria = update_meets_criteria(&v.updates.1);

		if !v.requires_reminder && !direction_a_meets_criteria && !direction_b_meets_criteria {
			delta_set.remove(&k);
		}
	}

	let new_length = delta_set.len();
	if original_length != new_length {
		println!("length modified!");
	}
}
