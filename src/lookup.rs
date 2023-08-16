use std::collections::{BTreeMap, HashSet};
use std::io::Cursor;
use std::ops::Deref;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use lightning::ln::msgs::{ChannelAnnouncement, ChannelUpdate, UnsignedChannelAnnouncement, UnsignedChannelUpdate};
use lightning::routing::gossip::NetworkGraph;
use lightning::util::ser::Readable;
use tokio_postgres::{Client, Connection, NoTls, Socket};
use tokio_postgres::tls::NoTlsStream;

use futures::StreamExt;
use lightning::log_info;
use lightning::util::logger::Logger;

use crate::config;
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
pub(super) async fn fetch_channel_announcements<L: Deref>(delta_set: &mut DeltaSet, network_graph: Arc<NetworkGraph<L>>, client: &Client, last_sync_timestamp: u32, logger: L) where L::Target: Logger {
	log_info!(logger, "Obtaining channel ids from network graph");
	let channel_ids = {
		let read_only_graph = network_graph.read_only();
		log_info!(logger, "Retrieved read-only network graph copy");
		let channel_iterator = read_only_graph.channels().unordered_iter();
		channel_iterator
			.filter(|c| c.1.announcement_message.is_some())
			.map(|c| c.1.announcement_message.as_ref().unwrap().contents.short_channel_id as i64)
			.collect::<Vec<_>>()
	};
	#[cfg(test)]
	log_info!(logger, "Channel IDs: {:?}", channel_ids);
	log_info!(logger, "Last sync timestamp: {}", last_sync_timestamp);
	let last_sync_timestamp_float = last_sync_timestamp as f64;

	log_info!(logger, "Obtaining corresponding database entries");
	// get all the channel announcements that are currently in the network graph
	let announcement_rows = client.query_raw("SELECT announcement_signed, seen FROM channel_announcements WHERE short_channel_id = any($1) ORDER BY short_channel_id ASC", [&channel_ids]).await.unwrap();
	let mut pinned_rows = Box::pin(announcement_rows);

	let mut announcement_count = 0;
	while let Some(row_res) = pinned_rows.next().await {
		let current_announcement_row = row_res.unwrap();
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

		announcement_count += 1;
	}
	log_info!(logger, "Fetched {} announcement rows", announcement_count);

	{
		// THIS STEP IS USED TO DETERMINE IF A CHANNEL SHOULD BE OMITTED FROM THE DELTA

		log_info!(logger, "Annotating channel announcements whose oldest channel update in a given direction occurred after the last sync");
		// Steps:
		// — Obtain all updates, distinct by (scid, direction), ordered by seen DESC // to find the oldest update in a given direction
		// — From those updates, select distinct by (scid), ordered by seen DESC (to obtain the newer one per direction)
		// This will allow us to mark the first time updates in both directions were seen

		// here is where the channels whose first update in either direction occurred after
		// `last_seen_timestamp` are added to the selection
		let params: [&(dyn tokio_postgres::types::ToSql + Sync); 2] =
			[&channel_ids, &last_sync_timestamp_float];
		let newer_oldest_directional_updates = client.query_raw("
			SELECT * FROM (
				SELECT DISTINCT ON (short_channel_id) *
				FROM (
					SELECT DISTINCT ON (short_channel_id, direction) short_channel_id, seen
					FROM channel_updates
					WHERE short_channel_id = any($1)
					ORDER BY short_channel_id ASC, direction ASC, seen ASC
				) AS directional_last_seens
				ORDER BY short_channel_id ASC, seen DESC
			) AS distinct_chans
			WHERE distinct_chans.seen >= TO_TIMESTAMP($2)
			", params).await.unwrap();
		let mut pinned_updates = Box::pin(newer_oldest_directional_updates);

		let mut newer_oldest_directional_update_count = 0;
		while let Some(row_res) = pinned_updates.next().await {
			let current_row = row_res.unwrap();

			let scid: i64 = current_row.get("short_channel_id");
			let current_seen_timestamp_object: SystemTime = current_row.get("seen");
			let current_seen_timestamp: u32 = current_seen_timestamp_object.duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs() as u32;

			// the newer of the two oldest seen directional updates came after last sync timestamp
			let current_channel_delta = delta_set.entry(scid as u64).or_insert(ChannelDelta::default());
			// first time a channel was seen in both directions
			(*current_channel_delta).first_bidirectional_updates_seen = Some(current_seen_timestamp);

			newer_oldest_directional_update_count += 1;
		}
		log_info!(logger, "Fetched {} update rows of the first update in a new direction", newer_oldest_directional_update_count);
	}

	{
		// THIS STEP IS USED TO DETERMINE IF A REMINDER UPDATE SHOULD BE SENT

		log_info!(logger, "Annotating channel announcements whose latest channel update in a given direction occurred more than six days ago");
		// Steps:
		// — Obtain all updates, distinct by (scid, direction), ordered by seen DESC
		// — From those updates, select distinct by (scid), ordered by seen ASC (to obtain the older one per direction)
		let reminder_threshold_timestamp = SystemTime::now().checked_sub(config::CHANNEL_REMINDER_AGE).unwrap().duration_since(UNIX_EPOCH).unwrap().as_secs() as f64;

		let params: [&(dyn tokio_postgres::types::ToSql + Sync); 2] =
			[&channel_ids, &reminder_threshold_timestamp];
		let older_latest_directional_updates = client.query_raw("
			SELECT short_channel_id FROM (
				SELECT DISTINCT ON (short_channel_id) *
				FROM (
					SELECT DISTINCT ON (short_channel_id, direction) short_channel_id, seen
					FROM channel_updates
					WHERE short_channel_id = any($1)
					ORDER BY short_channel_id ASC, direction ASC, seen DESC
				) AS directional_last_seens
				ORDER BY short_channel_id ASC, seen ASC
			) AS distinct_chans
			WHERE distinct_chans.seen <= TO_TIMESTAMP($2)
			", params).await.unwrap();
		let mut pinned_updates = Box::pin(older_latest_directional_updates);

		let mut older_latest_directional_update_count = 0;
		while let Some(row_res) = pinned_updates.next().await {
			let current_row = row_res.unwrap();
			let scid: i64 = current_row.get("short_channel_id");

			// annotate this channel as requiring that reminders be sent to the client
			let current_channel_delta = delta_set.entry(scid as u64).or_insert(ChannelDelta::default());

			// way might be able to get away with not using this
			(*current_channel_delta).requires_reminder = true;

			if let Some(current_channel_info) = network_graph.read_only().channel(scid as u64) {
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
			older_latest_directional_update_count += 1;
		}
		log_info!(logger, "Fetched {} update rows of the latest update in the less recently updated direction", older_latest_directional_update_count);
	}
}

pub(super) async fn fetch_channel_updates<L: Deref>(delta_set: &mut DeltaSet, client: &Client, last_sync_timestamp: u32, logger: L) where L::Target: Logger {
	let start = Instant::now();
	let last_sync_timestamp_float = last_sync_timestamp as f64;

	// get the latest channel update in each direction prior to last_sync_timestamp, provided
	// there was an update in either direction that happened after the last sync (to avoid
	// collecting too many reference updates)
	let reference_rows = client.query_raw("
		SELECT id, direction, blob_signed FROM channel_updates
		WHERE id IN (
			SELECT DISTINCT ON (short_channel_id, direction) id
			FROM channel_updates
			WHERE seen < TO_TIMESTAMP($1) AND short_channel_id IN (
				SELECT DISTINCT ON (short_channel_id) short_channel_id
				FROM channel_updates
				WHERE seen >= TO_TIMESTAMP($1)
			)
			ORDER BY short_channel_id ASC, direction ASC, seen DESC
		)
		", [last_sync_timestamp_float]).await.unwrap();
	let mut pinned_rows = Box::pin(reference_rows);

	log_info!(logger, "Fetched reference rows in {:?}", start.elapsed());

	let mut last_seen_update_ids: Vec<i32> = Vec::new();
	let mut non_intermediate_ids: HashSet<i32> = HashSet::new();
	let mut reference_row_count = 0;

	while let Some(row_res) = pinned_rows.next().await {
		let current_reference = row_res.unwrap();
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
		reference_row_count += 1;
	}

	log_info!(logger, "Processed {} reference rows (delta size: {}) in {:?}",
		reference_row_count, delta_set.len(), start.elapsed());

	// get all the intermediate channel updates
	// (to calculate the set of mutated fields for snapshotting, where intermediate updates may
	// have been omitted)

	let intermediate_updates = client.query_raw("
		SELECT id, direction, blob_signed, seen
		FROM channel_updates
		WHERE seen >= TO_TIMESTAMP($1)
		", [last_sync_timestamp_float]).await.unwrap();
	let mut pinned_updates = Box::pin(intermediate_updates);
	log_info!(logger, "Fetched intermediate rows in {:?}", start.elapsed());

	let mut previous_scid = u64::MAX;
	let mut previously_seen_directions = (false, false);

	// let mut previously_seen_directions = (false, false);
	let mut intermediate_update_count = 0;
	while let Some(row_res) = pinned_updates.next().await {
		let intermediate_update = row_res.unwrap();
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
	log_info!(logger, "Processed intermediate rows ({}) (delta size: {}): {:?}", intermediate_update_count, delta_set.len(), start.elapsed());
}

pub(super) fn filter_delta_set<L: Deref>(delta_set: &mut DeltaSet, logger: L) where L::Target: Logger {
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
		log_info!(logger, "length modified!");
	}
}
