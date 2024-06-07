use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::Cursor;
use std::ops::Deref;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use lightning::ln::msgs::{ChannelAnnouncement, ChannelUpdate, NodeAnnouncement, SocketAddress, UnsignedChannelAnnouncement, UnsignedChannelUpdate};
use lightning::routing::gossip::{NetworkGraph, NodeId};
use lightning::util::ser::Readable;
use tokio_postgres::Client;

use futures::StreamExt;
use lightning::{log_debug, log_gossip, log_info};
use lightning::ln::features::NodeFeatures;
use lightning::util::logger::Logger;

use crate::config;
use crate::serialization::MutatedProperties;

/// The delta set needs to be a BTreeMap so the keys are sorted.
/// That way, the scids in the response automatically grow monotonically
pub(super) type DeltaSet = BTreeMap<u64, ChannelDelta>;
pub(super) type NodeDeltaSet = HashMap<NodeId, NodeDelta>;

pub(super) struct AnnouncementDelta {
	pub(super) seen: u32,
	pub(super) announcement: UnsignedChannelAnnouncement,
}

pub(super) struct UpdateDelta {
	pub(super) seen: u32,
	pub(super) update: UnsignedChannelUpdate,
}

pub(super) struct DirectedUpdateDelta {
	/// the last update we saw prior to the user-provided timestamp
	pub(super) last_update_before_seen: Option<UpdateDelta>,
	/// the latest update we saw overall
	pub(super) latest_update_after_seen: Option<UpdateDelta>,
	/// the set of all mutated properties across all updates between the last seen by the user and
	/// the latest one known to us
	pub(super) mutated_properties: MutatedProperties,
	/// Specifically for reminder updates, the flag-only value to send to the client
	pub(super) serialization_update_flags: Option<u8>
}

pub(super) struct ChannelDelta {
	pub(super) announcement: Option<AnnouncementDelta>,
	pub(super) updates: (Option<DirectedUpdateDelta>, Option<DirectedUpdateDelta>),
	pub(super) first_bidirectional_updates_seen: Option<u32>,
	/// The seen timestamp of the older of the two latest directional updates
	pub(super) requires_reminder: bool,
}

pub(super) struct NodeDelta {
	/// The most recently received, but new-to-the-client, node details
	pub(super) latest_details_after_seen: Option<NodeDetails>,

	/// Between last_details_before_seen and latest_details_after_seen, including any potential
	/// intermediate updates that are not kept track of here, has the set of features this node
	/// supports changed?
	pub(super) has_feature_set_changed: bool,

	/// Between last_details_before_seen and latest_details_after_seen, including any potential
	/// intermediate updates that are not kept track of here, has the set of socket addresses this
	/// node listens on changed?
	pub(super) has_address_set_changed: bool,

	/// The most recent node details that the client would have seen already
	pub(super) last_details_before_seen: Option<NodeDetails>
}

pub(super) struct NodeDetails {
	#[allow(unused)]
	pub(super) seen: u32,
	pub(super) features: NodeFeatures,
	pub(super) addresses: HashSet<SocketAddress>
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

impl Default for NodeDelta {
	fn default() -> Self {
		Self {
			latest_details_after_seen: None,
			has_feature_set_changed: false,
			has_address_set_changed: false,
			last_details_before_seen: None,
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

/// Fetch all the channel announcements that are presently in the network graph, regardless of
/// whether they had been seen before.
/// Also include all announcements for which the first update was announced
/// after `last_sync_timestamp`
pub(super) async fn fetch_channel_announcements<L: Deref>(delta_set: &mut DeltaSet, network_graph: Arc<NetworkGraph<L>>, client: &Client, last_sync_timestamp: u32, snapshot_reference_timestamp: Option<u64>, logger: L) where L::Target: Logger {
	log_info!(logger, "Obtaining channel ids from network graph");
	let channel_ids = {
		let read_only_graph = network_graph.read_only();
		log_info!(logger, "Retrieved read-only network graph copy");
		let channel_iterator = read_only_graph.channels().unordered_iter();
		channel_iterator
			.filter(|c| c.1.announcement_message.is_some() && c.1.one_to_two.is_some() && c.1.two_to_one.is_some())
			.map(|c| c.1.announcement_message.as_ref().unwrap().contents.short_channel_id as i64)
			.collect::<Vec<_>>()
	};
	#[cfg(test)]
	log_info!(logger, "Channel IDs: {:?}", channel_ids);
	log_info!(logger, "Last sync timestamp: {}", last_sync_timestamp);
	let last_sync_timestamp_float = last_sync_timestamp as f64;

	let current_timestamp = snapshot_reference_timestamp.unwrap_or(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs());
	log_info!(logger, "Current timestamp: {}", current_timestamp);

	let include_reminders = {
		let current_hour = current_timestamp / 3600;
		let current_day = current_timestamp / (24 * 3600);

		log_debug!(logger, "Current day index: {}", current_day);
		log_debug!(logger, "Current hour: {}", current_hour);

		// every 5th day at midnight
		let is_reminder_hour = (current_hour % 24) == 0;
		let is_reminder_day = (current_day % 5) == 0;

		let snapshot_scope = current_timestamp.saturating_sub(last_sync_timestamp as u64);
		let is_reminder_scope = snapshot_scope > (50 * 3600);
		log_debug!(logger, "Snapshot scope: {}s", snapshot_scope);

		(is_reminder_hour && is_reminder_day) || is_reminder_scope
	};

	log_info!(logger, "Obtaining corresponding database entries");
	// get all the channel announcements that are currently in the network graph
	let announcement_rows = client.query_raw("SELECT announcement_signed, CAST(EXTRACT('epoch' from seen) AS BIGINT) AS seen FROM channel_announcements WHERE short_channel_id = any($1) ORDER BY short_channel_id ASC", [&channel_ids]).await.unwrap();
	let mut pinned_rows = Box::pin(announcement_rows);

	let mut announcement_count = 0;
	while let Some(row_res) = pinned_rows.next().await {
		let current_announcement_row = row_res.unwrap();
		let blob: Vec<u8> = current_announcement_row.get("announcement_signed");
		let mut readable = Cursor::new(blob);
		let unsigned_announcement = ChannelAnnouncement::read(&mut readable).unwrap().contents;

		let scid = unsigned_announcement.short_channel_id;
		let current_seen_timestamp = current_announcement_row.get::<_, i64>("seen") as u32;

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
			SELECT short_channel_id, CAST(EXTRACT('epoch' from distinct_chans.seen) AS BIGINT) AS seen FROM (
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
			let current_seen_timestamp = current_row.get::<_, i64>("seen") as u32;

			// the newer of the two oldest seen directional updates came after last sync timestamp
			let current_channel_delta = delta_set.entry(scid as u64).or_insert(ChannelDelta::default());
			// first time a channel was seen in both directions
			(*current_channel_delta).first_bidirectional_updates_seen = Some(current_seen_timestamp);

			newer_oldest_directional_update_count += 1;
		}
		log_info!(logger, "Fetched {} update rows of the first update in a new direction", newer_oldest_directional_update_count);
	}

	if include_reminders {
		// THIS STEP IS USED TO DETERMINE IF A REMINDER UPDATE SHOULD BE SENT

		log_info!(logger, "Annotating channel announcements whose latest channel update in a given direction occurred more than six days ago");
		// Steps:
		// — Obtain all updates, distinct by (scid, direction), ordered by seen DESC
		// — From those updates, select distinct by (scid), ordered by seen ASC (to obtain the older one per direction)
		let reminder_threshold_timestamp = current_timestamp.checked_sub(config::CHANNEL_REMINDER_AGE.as_secs()).unwrap() as f64;

		log_info!(logger, "Fetch first time we saw the current value combination for each direction (prior mutations excepted)");
		let reminder_lookup_threshold_timestamp = current_timestamp.checked_sub(config::CHANNEL_REMINDER_AGE.as_secs() * 3).unwrap() as f64;
		let params: [&(dyn tokio_postgres::types::ToSql + Sync); 2] = [&channel_ids, &reminder_lookup_threshold_timestamp];

		/*
		What exactly is the below query doing?

		First, the inner query groups all channel updates by their scid/direction combination,
		and then sorts those in reverse chronological order by the "seen" column.

		Then, each row is annotated based on whether its subsequent row for the same scid/direction
		combination has a different value for any one of these six fields:
		disable, cltv_expiry_delta, htlc_minimum_msat, fee_base_msat, fee_proportional_millionths, htlc_maximum_msat
		Those are simply the properties we use to keep track of channel mutations.

		The outer query takes all of those results and selects the first value that has a distinct
		successor for each scid/direction combination. That yields the first instance at which
		a given channel configuration was received after any prior mutations.

		Knowing that, we can check whether or not there have been any mutations within the
		reminder requirement window. Because we only care about that window (and potentially the
		2-week-window), we pre-filter the scanned updates by only those that were received within
		3x the timeframe that we consider necessitates reminders.
		*/

		let mutated_updates = client.query_raw("
		SELECT DISTINCT ON (short_channel_id, direction) short_channel_id, direction, blob_signed, CAST(EXTRACT('epoch' from seen) AS BIGINT) AS seen FROM (
			SELECT short_channel_id, direction, timestamp, seen, blob_signed, COALESCE (
				disable<>lead(disable) OVER w1
					OR
				cltv_expiry_delta<>lead(cltv_expiry_delta) OVER w1
					OR
				htlc_minimum_msat<>lead(htlc_minimum_msat) OVER w1
					OR
				fee_base_msat<>lead(fee_base_msat) OVER w1
					OR
				fee_proportional_millionths<>lead(fee_proportional_millionths) OVER w1
					OR
				htlc_maximum_msat<>lead(htlc_maximum_msat) OVER w1,
				TRUE
			) has_distinct_successor
			FROM channel_updates
			WHERE short_channel_id = any($1) AND seen >= TO_TIMESTAMP($2)
			WINDOW w1 AS (PARTITION BY short_channel_id, direction ORDER BY seen DESC)
		) _
		WHERE has_distinct_successor
		ORDER BY short_channel_id ASC, direction ASC, timestamp DESC
		", params).await.unwrap();

		let mut pinned_updates = Box::pin(mutated_updates);
		let mut older_latest_directional_update_count = 0;
		while let Some(row_res) = pinned_updates.next().await {
			let current_row = row_res.unwrap();
			let seen = current_row.get::<_, i64>("seen") as u32;

			if seen < reminder_threshold_timestamp as u32 {
				let blob: Vec<u8> = current_row.get("blob_signed");
				let mut readable = Cursor::new(blob);
				let unsigned_channel_update = ChannelUpdate::read(&mut readable).unwrap().contents;

				let scid = unsigned_channel_update.short_channel_id;
				let direction: bool = current_row.get("direction");

				let current_channel_delta = delta_set.entry(scid).or_insert(ChannelDelta::default());

				// We might be able to get away with not using this
				(*current_channel_delta).requires_reminder = true;
				older_latest_directional_update_count += 1;

				if let Some(current_channel_info) = network_graph.read_only().channel(scid) {
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

				log_gossip!(logger, "Reminder requirement triggered by update for channel {} in direction {}", scid, direction);
			}
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
		SELECT id, direction, CAST(EXTRACT('epoch' from seen) AS BIGINT) AS seen, blob_signed FROM channel_updates
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
		let seen = current_reference.get::<_, i64>("seen") as u32;
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
		log_gossip!(logger, "Channel {} last update before seen: {}/{}/{}", scid, update_id, direction, unsigned_channel_update.timestamp);
		update_delta.last_update_before_seen = Some(UpdateDelta {
			seen,
			update: unsigned_channel_update,
		});

		reference_row_count += 1;
	}

	log_info!(logger, "Processed {} reference rows (delta size: {}) in {:?}",
		reference_row_count, delta_set.len(), start.elapsed());

	// get all the intermediate channel updates
	// (to calculate the set of mutated fields for snapshotting, where intermediate updates may
	// have been omitted)

	let intermediate_updates = client.query_raw("
		SELECT id, direction, blob_signed, CAST(EXTRACT('epoch' from seen) AS BIGINT) AS seen
		FROM channel_updates
		WHERE seen >= TO_TIMESTAMP($1)
		ORDER BY short_channel_id ASC, timestamp DESC
		", [last_sync_timestamp_float]).await.unwrap();
	let mut pinned_updates = Box::pin(intermediate_updates);
	log_info!(logger, "Fetched intermediate rows in {:?}", start.elapsed());

	let mut previous_scid = u64::MAX;
	let mut previously_seen_directions = (false, false);

	let mut intermediate_update_count = 0;
	while let Some(row_res) = pinned_updates.next().await {
		let intermediate_update = row_res.unwrap();
		let update_id: i32 = intermediate_update.get("id");
		if non_intermediate_ids.contains(&update_id) {
			continue;
		}
		intermediate_update_count += 1;

		let direction: bool = intermediate_update.get("direction");
		let current_seen_timestamp = intermediate_update.get::<_, i64>("seen") as u32;
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
			if unsigned_channel_update.flags != last_seen_update.update.flags {
				update_delta.mutated_properties.flags = true;
			}
			if unsigned_channel_update.cltv_expiry_delta != last_seen_update.update.cltv_expiry_delta {
				update_delta.mutated_properties.cltv_expiry_delta = true;
			}
			if unsigned_channel_update.htlc_minimum_msat != last_seen_update.update.htlc_minimum_msat {
				update_delta.mutated_properties.htlc_minimum_msat = true;
			}
			if unsigned_channel_update.fee_base_msat != last_seen_update.update.fee_base_msat {
				update_delta.mutated_properties.fee_base_msat = true;
			}
			if unsigned_channel_update.fee_proportional_millionths != last_seen_update.update.fee_proportional_millionths {
				update_delta.mutated_properties.fee_proportional_millionths = true;
			}
			if unsigned_channel_update.htlc_maximum_msat != last_seen_update.update.htlc_maximum_msat {
				update_delta.mutated_properties.htlc_maximum_msat = true;
			}
		}
	}
	log_info!(logger, "Processed intermediate rows ({}) (delta size: {}): {:?}", intermediate_update_count, delta_set.len(), start.elapsed());
}

pub(super) async fn fetch_node_updates<L: Deref>(client: &Client, last_sync_timestamp: u32, logger: L) -> NodeDeltaSet where L::Target: Logger {
	let start = Instant::now();
	let last_sync_timestamp_float = last_sync_timestamp as f64;

	let mut delta_set = NodeDeltaSet::new();

	// get the latest node updates prior to last_sync_timestamp
	let reference_rows = client.query_raw("
		SELECT DISTINCT ON (public_key) public_key, CAST(EXTRACT('epoch' from seen) AS BIGINT) AS seen, announcement_signed
		FROM node_announcements
		WHERE seen < TO_TIMESTAMP($1)
		ORDER BY public_key ASC, seen DESC
		", [last_sync_timestamp_float]).await.unwrap();
	let mut pinned_rows = Box::pin(reference_rows);

	log_info!(logger, "Fetched node announcement reference rows in {:?}", start.elapsed());

	let mut reference_row_count = 0;

	while let Some(row_res) = pinned_rows.next().await {
		let current_reference = row_res.unwrap();

		let seen = current_reference.get::<_, i64>("seen") as u32;
		let blob: Vec<u8> = current_reference.get("announcement_signed");
		let mut readable = Cursor::new(blob);
		let unsigned_node_announcement = NodeAnnouncement::read(&mut readable).unwrap().contents;
		let node_id = unsigned_node_announcement.node_id;

		let current_node_delta = delta_set.entry(node_id).or_insert(NodeDelta::default());
		(*current_node_delta).last_details_before_seen.get_or_insert_with(|| {
			let address_set: HashSet<SocketAddress> = unsigned_node_announcement.addresses.into_iter().collect();
			NodeDetails {
				seen,
				features: unsigned_node_announcement.features,
				addresses: address_set,
			}
		});
		log_gossip!(logger, "Node {} last update before seen: {} (seen at {})", node_id, unsigned_node_announcement.timestamp, seen);

		reference_row_count += 1;
	}


	log_info!(logger, "Processed {} node announcement reference rows (delta size: {}) in {:?}",
		reference_row_count, delta_set.len(), start.elapsed());

	// get all the intermediate node updates
	// (to calculate the set of mutated fields for snapshotting, where intermediate updates may
	// have been omitted)
	let intermediate_updates = client.query_raw("
		SELECT announcement_signed, CAST(EXTRACT('epoch' from seen) AS BIGINT) AS seen
		FROM node_announcements
		WHERE seen >= TO_TIMESTAMP($1)
		ORDER BY public_key ASC, timestamp DESC
		", [last_sync_timestamp_float]).await.unwrap();
	let mut pinned_updates = Box::pin(intermediate_updates);
	log_info!(logger, "Fetched intermediate node announcement rows in {:?}", start.elapsed());

	let mut previous_node_id: Option<NodeId> = None;

	let mut intermediate_update_count = 0;
	while let Some(row_res) = pinned_updates.next().await {
		let intermediate_update = row_res.unwrap();
		intermediate_update_count += 1;

		let current_seen_timestamp = intermediate_update.get::<_, i64>("seen") as u32;
		let blob: Vec<u8> = intermediate_update.get("announcement_signed");
		let mut readable = Cursor::new(blob);
		let unsigned_node_announcement = NodeAnnouncement::read(&mut readable).unwrap().contents;

		let node_id = unsigned_node_announcement.node_id;
		let is_previously_processed_node_id = Some(node_id) == previous_node_id;

		// get this node's address set
		let current_node_delta = delta_set.entry(node_id).or_insert(NodeDelta::default());
		let address_set: HashSet<SocketAddress> = unsigned_node_announcement.addresses.into_iter().collect();

		// determine mutations
		if let Some(last_seen_update) = current_node_delta.last_details_before_seen.as_ref() {
			if unsigned_node_announcement.features != last_seen_update.features {
				current_node_delta.has_feature_set_changed = true;
			}
			if address_set != last_seen_update.addresses {
				current_node_delta.has_address_set_changed = true;
			}
		} else if !is_previously_processed_node_id {
			if current_node_delta.last_details_before_seen.is_none() {
				if !address_set.is_empty() {
					current_node_delta.has_address_set_changed = true;
				}
				if unsigned_node_announcement.features != NodeFeatures::empty() {
					current_node_delta.has_feature_set_changed = true;
				}
			}
		}

		if !is_previously_processed_node_id {
			(*current_node_delta).latest_details_after_seen.get_or_insert(NodeDetails {
				seen: current_seen_timestamp,
				features: unsigned_node_announcement.features,
				addresses: address_set,
			});
		}

		previous_node_id = Some(node_id);
	}
	log_info!(logger, "Processed intermediate node announcement rows ({}) (delta size: {}): {:?}", intermediate_update_count, delta_set.len(), start.elapsed());

	delta_set
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
