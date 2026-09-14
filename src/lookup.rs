use std::collections::{BTreeMap, HashMap, HashSet};
use std::ops::Deref;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use bitcoin::io::Cursor;

use lightning::ln::msgs::{ChannelAnnouncement, ChannelUpdate, NodeAnnouncement, SocketAddress, UnsignedChannelAnnouncement, UnsignedChannelUpdate};
use lightning::routing::gossip::{NetworkGraph, NodeId};
use lightning::util::ser::Readable;
use tokio_postgres::Client;

use futures::StreamExt;
use hex_conservative::DisplayHex;
use lightning::{log_gossip, log_info};
use lightning::types::features::NodeFeatures;
use lightning::util::logger::Logger;

use crate::config;
use crate::serialization::{MutatedNodeProperties, MutatedProperties, NodeSerializationStrategy};

/// The delta set needs to be a BTreeMap so the keys are sorted.
/// That way, the scids in the response automatically grow monotonically
pub(super) type DeltaSet = BTreeMap<u64, ChannelDelta>;
pub(super) type NodeDeltaSet = HashMap<NodeId, NodeDelta>;

pub(super) struct AnnouncementDelta {
	pub(super) seen: u32,
	pub(super) announcement: UnsignedChannelAnnouncement,
	pub(super) funding_sats: u64,
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
	/// The `seen` timestamp at which this channel most recently (re)gained updates in a direction
	/// that had previously not had any for at least the prune interval (or ever), provided that
	/// happened after the client's last sync. Such a channel needs to be (re-)announced.
	pub(super) updates_resumed_seen: Option<u32>,
	/// Whether this channel's reminder bucket became due within the snapshot's window, in which
	/// case each direction without a real update to send gets a flags-only reminder update
	pub(super) requires_reminder: bool,
}

pub(super) struct NodeDelta {
	/// The most recently received, but new-to-the-client, node details
	pub(super) latest_details: Option<NodeDetails>,

	/// How should this delta be serialized?
	pub(super) strategy: Option<NodeSerializationStrategy>,

	/// The most recent node details that the client would have seen already
	pub(super) last_details_before_seen: Option<NodeDetails>
}

pub(super) struct NodeDetails {
	pub(super) seen: Option<u32>,
	pub(super) features: NodeFeatures,
	pub(super) addresses: HashSet<SocketAddress>
}

impl Default for ChannelDelta {
	fn default() -> Self {
		Self {
			announcement: None,
			updates: (None, None),
			updates_resumed_seen: None,
			requires_reminder: false,
		}
	}
}

impl Default for NodeDelta {
	fn default() -> Self {
		Self {
			latest_details: None,
			last_details_before_seen: None,
			strategy: None,
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

/// The reminder buckets that became due within the window `(last_sync_timestamp, current_timestamp]`,
/// as a bitmask.
pub(super) fn reminder_buckets_due(last_sync_timestamp: u32, current_timestamp: u64) -> u64 {
	assert!(config::REMINDER_BUCKET_COUNT < 64);

	// the first slot boundary strictly after the last sync, and the last one at or before now
	let first_due_slot = (last_sync_timestamp as u64) / config::REMINDER_SLOT_INTERVAL.as_secs() + 1;
	let last_due_slot = current_timestamp / config::REMINDER_SLOT_INTERVAL.as_secs();
	if last_due_slot < first_due_slot {
		return 0;
	}
	if last_due_slot - first_due_slot + 1 >= config::REMINDER_BUCKET_COUNT {
		return (1u64 << config::REMINDER_BUCKET_COUNT) - 1;
	}

	let mut due_buckets = 0u64;
	for slot in first_due_slot..=last_due_slot {
		due_buckets |= 1u64 << (slot % config::REMINDER_BUCKET_COUNT);
	}
	due_buckets
}

pub(super) fn channel_reminder_bucket_flag(short_channel_id: u64) -> u64 {
	// splitmix64 finalizer
	let mut mixed = short_channel_id;
	mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
	mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94d049bb133111eb);
	mixed ^= mixed >> 31;
	1u64 << (mixed % config::REMINDER_BUCKET_COUNT)
}

/// Node ids are compressed public keys, whose x coordinate is already uniformly distributed.
pub(super) fn node_reminder_bucket_flag(node_id: &NodeId) -> u64 {
	let x_coordinate_prefix: [u8; 8] = node_id.as_slice()[1..9].try_into().unwrap();
	1u64 << (u64::from_be_bytes(x_coordinate_prefix) % config::REMINDER_BUCKET_COUNT)
}

/// Fetch all the channel announcements that are presently in the network graph, regardless of
/// whether they had been seen before.
/// Also include all announcements for which updates in either direction (re)started after
/// `last_sync_timestamp`, be it because the channel is new or because it had been pruned
pub(super) async fn fetch_channel_announcements<L: Deref>(delta_set: &mut DeltaSet, network_graph: &NetworkGraph<L>, client: &Client, last_sync_timestamp: u32, snapshot_reference_timestamp: Option<u64>, logger: L) where L::Target: Logger {
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

	let due_reminder_buckets = reminder_buckets_due(last_sync_timestamp, current_timestamp);
	log_info!(logger, "Reminder buckets due in this snapshot: {:#b}", due_reminder_buckets);

	log_info!(logger, "Obtaining corresponding database entries");
	let start = Instant::now();
	// get all the channel announcements that are currently in the network graph
	let announcement_rows = client.query_raw("SELECT announcement_signed, funding_amount_sats, CAST(EXTRACT('epoch' from seen) AS BIGINT) AS seen FROM channel_announcements WHERE short_channel_id = any($1) ORDER BY short_channel_id ASC", [&channel_ids]).await.unwrap();
	let mut pinned_rows = Box::pin(announcement_rows);

	let mut announcement_count = 0;
	while let Some(row_res) = pinned_rows.next().await {
		let current_announcement_row = row_res.unwrap();
		let blob: Vec<u8> = current_announcement_row.get("announcement_signed");
		let mut readable = Cursor::new(&blob);
		let unsigned_announcement = ChannelAnnouncement::read(&mut readable).unwrap().contents;

		let scid = unsigned_announcement.short_channel_id;
		let funding_sats = current_announcement_row.get::<_, i64>("funding_amount_sats") as u64;
		let current_seen_timestamp = current_announcement_row.get::<_, i64>("seen") as u32;

		let current_channel_delta = delta_set.entry(scid).or_insert(ChannelDelta::default());
		(*current_channel_delta).announcement = Some(AnnouncementDelta {
			announcement: unsigned_announcement,
			funding_sats,
			seen: current_seen_timestamp,
		});

		announcement_count += 1;
	}
	log_info!(logger, "Fetched {} announcement rows in {:?}", announcement_count, start.elapsed());

	{
		log_info!(logger, "Annotating channels whose updates in a direction (re)started after the last sync");
		// Clients only receive a channel's announcement once, and prune the channel if our
		// snapshots stop covering it, which happens when one of its peers stops announcing
		// for two weeks. When that peer comes back we need to provide clients a fresh
		// update.
		//
		// We detect this from the update history alone: for each direction, take the first update
		// seen at or after the last sync and check whether it had a predecessor within the prune
		// interval. If not, updates in that direction (re)started after the client's last sync, be
		// it because the channel is brand new or because it was pruned and has come back. As we
		// only ever drop channels after a full prune interval without updates, and block them from
		// being re-added for another week, any such resumption implies a gap in the history.
		let prune_interval_seconds = (config::PRUNE_INTERVAL.as_secs() - 60 * 60 * 24) as f64;
		let start = Instant::now();
		let params: [&(dyn tokio_postgres::types::ToSql + Sync); 3] =
			[&channel_ids, &last_sync_timestamp_float, &prune_interval_seconds];
		let resumed_directional_updates = client.query_raw("
			SELECT scids.short_channel_id, CAST(EXTRACT('epoch' from GREATEST(dir0.seen, dir1.seen)) AS BIGINT) AS seen
			FROM unnest($1::bigint[]) AS scids(short_channel_id)
			LEFT JOIN LATERAL (
				SELECT first_recent.seen
				FROM (
					SELECT seen
					FROM channel_updates
					WHERE short_channel_id = scids.short_channel_id AND direction = false AND seen >= TO_TIMESTAMP($2)
					ORDER BY seen ASC
					LIMIT 1
				) first_recent
				WHERE NOT EXISTS (
					SELECT 1
					FROM channel_updates AS predecessor
					WHERE predecessor.short_channel_id = scids.short_channel_id AND predecessor.direction = false
						AND predecessor.seen < first_recent.seen
						AND predecessor.seen >= first_recent.seen - $3 * INTERVAL '1 second'
				)
			) dir0 ON TRUE
			LEFT JOIN LATERAL (
				SELECT first_recent.seen
				FROM (
					SELECT seen
					FROM channel_updates
					WHERE short_channel_id = scids.short_channel_id AND direction = true AND seen >= TO_TIMESTAMP($2)
					ORDER BY seen ASC
					LIMIT 1
				) first_recent
				WHERE NOT EXISTS (
					SELECT 1
					FROM channel_updates AS predecessor
					WHERE predecessor.short_channel_id = scids.short_channel_id AND predecessor.direction = true
						AND predecessor.seen < first_recent.seen
						AND predecessor.seen >= first_recent.seen - $3 * INTERVAL '1 second'
				)
			) dir1 ON TRUE
			WHERE dir0.seen IS NOT NULL OR dir1.seen IS NOT NULL
			", params).await.unwrap();
		let mut pinned_updates = Box::pin(resumed_directional_updates);

		let mut resumed_directional_update_count = 0;
		while let Some(row_res) = pinned_updates.next().await {
			let current_row = row_res.unwrap();

			let scid: i64 = current_row.get("short_channel_id");
			let current_seen_timestamp = current_row.get::<_, i64>("seen") as u32;

			let current_channel_delta = delta_set.entry(scid as u64).or_insert(ChannelDelta::default());
			(*current_channel_delta).updates_resumed_seen = Some(current_seen_timestamp);

			resumed_directional_update_count += 1;
		}
		log_info!(logger, "Fetched {} update rows of the first update in a (re)started direction in {:?}", resumed_directional_update_count, start.elapsed());
	}

	if due_reminder_buckets != 0 {
		let read_only_graph = network_graph.read_only();
		let mut reminder_channel_count = 0;
		for scid in channel_ids.iter().map(|scid| *scid as u64) {
			if due_reminder_buckets & channel_reminder_bucket_flag(scid) == 0 {
				continue;
			}

			match delta_set.get(&scid) {
				Some(current_channel_delta) => {
					let is_new_announcement = current_channel_delta.announcement.as_ref()
						.map(|announcement| announcement.seen >= last_sync_timestamp)
						.unwrap_or(false);
					if is_new_announcement || current_channel_delta.updates_resumed_seen.is_some() {
						// the client is about to receive the announcement alongside full updates
						// for this channel, so a reminder would be pointless
						continue;
					}
				},
				None => {
					// we don't have the announcement in the database (yet), so this channel is
					// going to be dropped from the delta anyway
					continue;
				}
			}

			// the graph may have changed since the channel ids were collected; we don't send
			// reminders if we don't have bidirectional update data
			let (one_to_two, two_to_one) = match read_only_graph.channel(scid) {
				Some(channel_info) => match (channel_info.one_to_two.as_ref(), channel_info.two_to_one.as_ref()) {
					(Some(one_to_two), Some(two_to_one)) => (one_to_two, two_to_one),
					_ => continue,
				},
				None => continue,
			};

			let current_channel_delta = delta_set.get_mut(&scid).unwrap();
			(*current_channel_delta).requires_reminder = true;

			let flags: u8 = if one_to_two.enabled { 0 } else { 2 };
			let current_update = (*current_channel_delta).updates.0.get_or_insert(DirectedUpdateDelta::default());
			current_update.serialization_update_flags = Some(flags);

			let flags: u8 = if two_to_one.enabled { 1 } else { 3 };
			let current_update = (*current_channel_delta).updates.1.get_or_insert(DirectedUpdateDelta::default());
			current_update.serialization_update_flags = Some(flags);

			log_gossip!(logger, "Reminder due for channel {}", scid);
			reminder_channel_count += 1;
		}
		log_info!(logger, "Annotated {} channels for reminders", reminder_channel_count);
	}
}

pub(super) async fn fetch_channel_updates<L: Deref>(delta_set: &mut DeltaSet, client: &Client, last_sync_timestamp: u32, logger: L) where L::Target: Logger {
	let start = Instant::now();
	let last_sync_timestamp_float = last_sync_timestamp as f64;

	// get the latest channel update in each direction prior to last_sync_timestamp, provided
	// there was an update in either direction that happened after the last sync (to avoid
	// collecting too many reference updates)
	let reference_rows = client.query_raw("
		SELECT cu.id, d.direction, CAST(EXTRACT('epoch' from cu.seen) AS BIGINT) AS seen, cu.blob_signed
		FROM (
			SELECT DISTINCT short_channel_id
			FROM channel_updates
			WHERE seen >= TO_TIMESTAMP($1)
		) AS recent_scids
		CROSS JOIN (VALUES (false), (true)) AS d(direction)
		JOIN LATERAL (
			SELECT id, seen, blob_signed
			FROM channel_updates
			WHERE short_channel_id = recent_scids.short_channel_id
				AND direction = d.direction
				AND seen < TO_TIMESTAMP($1)
			ORDER BY seen DESC
			LIMIT 1
		) cu ON true
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
		let mut readable = Cursor::new(&blob);
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

	log_info!(logger, "Fetched + processed {} reference rows (delta size: {}) in {:?}",
		reference_row_count, delta_set.len(), start.elapsed());

	// get all the intermediate channel updates
	// (to calculate the set of mutated fields for snapshotting, where intermediate updates may
	// have been omitted)

	let start = Instant::now();
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
		let mut readable = Cursor::new(&blob);
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
			if unsigned_channel_update.channel_flags != last_seen_update.update.channel_flags {
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
	log_info!(logger, "Fetched + processed intermediate rows ({}) (delta size: {}): {:?}", intermediate_update_count, delta_set.len(), start.elapsed());
}

pub(super) async fn fetch_node_updates<L: Deref + Clone>(network_graph: &NetworkGraph<L>, client: &Client, last_sync_timestamp: u32, snapshot_reference_timestamp: Option<u64>, logger: L) -> NodeDeltaSet where L::Target: Logger {
	let start = Instant::now();
	let last_sync_timestamp_float = last_sync_timestamp as f64;

	let mut delta_set: NodeDeltaSet = {
		let read_only_graph = network_graph.read_only();
		read_only_graph.nodes().unordered_iter().flat_map(|(node_id, node_info)| {
			let details: NodeDetails = if let Some(details) = node_info.announcement_info.as_ref() {
				NodeDetails {
					seen: None,
					features: details.features().clone(),
					addresses: details.addresses().into_iter().cloned().collect(),
				}
			} else {
				return None;
			};
			Some((node_id.clone(), NodeDelta {
				latest_details: Some(details),
				strategy: None,
				last_details_before_seen: None,
			}))
		}).collect()
	};

	let node_ids: Vec<String> = delta_set.keys().into_iter().map(|id| id.as_slice().to_lower_hex_string()).collect();
	#[cfg(test)]
	log_info!(logger, "Node IDs: {:?}", node_ids);

	// get the latest node updates prior to last_sync_timestamp
	let params: [&(dyn tokio_postgres::types::ToSql + Sync); 2] = [&node_ids, &last_sync_timestamp_float];
	let reference_rows = client.query_raw("
		SELECT pk.public_key, CAST(EXTRACT('epoch' from na.seen) AS BIGINT) AS seen, na.announcement_signed
		FROM unnest($1::varchar[]) AS pk(public_key)
		CROSS JOIN LATERAL (
			SELECT seen, announcement_signed
			FROM node_announcements
			WHERE public_key = pk.public_key
				AND seen < TO_TIMESTAMP($2)
			ORDER BY seen DESC
			LIMIT 1
		) na
		", params).await.unwrap();
	let mut pinned_rows = Box::pin(reference_rows);

	log_info!(logger, "Fetched node announcement reference rows in {:?}", start.elapsed());

	let mut reference_row_count = 0;

	while let Some(row_res) = pinned_rows.next().await {
		let current_reference = row_res.unwrap();

		let seen = current_reference.get::<_, i64>("seen") as u32;
		let blob: Vec<u8> = current_reference.get("announcement_signed");
		let mut readable = Cursor::new(&blob);
		let unsigned_node_announcement = NodeAnnouncement::read(&mut readable).unwrap().contents;
		let node_id = unsigned_node_announcement.node_id;

		let current_node_delta = delta_set.entry(node_id).or_insert(NodeDelta::default());
		(*current_node_delta).last_details_before_seen.get_or_insert_with(|| {
			let address_set: HashSet<SocketAddress> = unsigned_node_announcement.addresses.into_iter().collect();
			NodeDetails {
				seen: Some(seen),
				features: unsigned_node_announcement.features,
				addresses: address_set,
			}
		});
		log_gossip!(logger, "Node {} last update before seen: {} (seen at {})", node_id, unsigned_node_announcement.timestamp, seen);

		reference_row_count += 1;
	}


	log_info!(logger, "Fetched + processed {} node announcement reference rows (delta size: {}) in {:?}",
		reference_row_count, delta_set.len(), start.elapsed());

	let current_timestamp = snapshot_reference_timestamp.unwrap_or(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs());
	let reminder_inclusion_threshold_timestamp = current_timestamp.checked_sub(config::CHANNEL_REMINDER_AGE.as_secs()).unwrap() as u32;
	let reminder_lookup_threshold_timestamp = current_timestamp.checked_sub(config::PRUNE_INTERVAL.as_secs()).unwrap() as u32;

	// Nodes whose reminder bucket became due within this snapshot's window are candidates for a
	// reminder. For those, the decision logic is as follows:
	// If the pre-sync update was more than 6 days ago, serialize in full.
	// Otherwise:
	// If the last mutation occurred after the last sync, serialize the mutated properties.
	// Otherwise:
	// If the last mutation occurred more than 6 days ago, serialize as a reminder.
	// Otherwise, don't serialize at all.
	// Determining when the last mutation occurred requires looking at the announcements from the
	// whole prune interval, rather than just since the last sync, for those nodes only.
	let due_reminder_buckets = reminder_buckets_due(last_sync_timestamp, current_timestamp);
	log_info!(logger, "Reminder buckets due in this snapshot: {:#b}", due_reminder_buckets);
	let reminder_node_ids: Vec<String> = delta_set.keys()
		.filter(|node_id| due_reminder_buckets & node_reminder_bucket_flag(node_id) != 0)
		.map(|node_id| node_id.as_slice().to_lower_hex_string())
		.collect();
	let reminder_lookup_threshold_timestamp_float = std::cmp::min(last_sync_timestamp, reminder_lookup_threshold_timestamp) as f64;

	// get all the intermediate node updates
	// (to calculate the set of mutated fields for snapshotting, where intermediate updates may
	// have been omitted)
	let start = Instant::now();
	let params: [&(dyn tokio_postgres::types::ToSql + Sync); 4] = [&node_ids, &last_sync_timestamp_float, &reminder_node_ids, &reminder_lookup_threshold_timestamp_float];
	let intermediate_updates = client.query_raw("
		SELECT announcement_signed, seen FROM (
			SELECT public_key, timestamp, announcement_signed, CAST(EXTRACT('epoch' from seen) AS BIGINT) AS seen
			FROM node_announcements
			WHERE
				public_key = ANY($1) AND
				seen >= TO_TIMESTAMP($2)
			UNION ALL
			SELECT public_key, timestamp, announcement_signed, CAST(EXTRACT('epoch' from seen) AS BIGINT) AS seen
			FROM node_announcements
			WHERE
				public_key = ANY($3) AND
				seen >= TO_TIMESTAMP($4) AND
				seen < TO_TIMESTAMP($2)
		) _
		ORDER BY public_key ASC, timestamp DESC
		", params).await.unwrap();
	let mut pinned_updates = Box::pin(intermediate_updates);
	log_info!(logger, "Fetched intermediate node announcement rows in {:?}", start.elapsed());

	let mut previous_node_id: Option<NodeId> = None;

	let mut intermediate_update_count = 0;
	let mut has_address_set_changed = false;
	let mut has_feature_set_changed = false;
	let mut latest_mutation_timestamp = None;
	let mut is_reminder_due = false;
	while let Some(row_res) = pinned_updates.next().await {
		let intermediate_update = row_res.unwrap();
		intermediate_update_count += 1;

		let current_seen_timestamp = intermediate_update.get::<_, i64>("seen") as u32;
		let blob: Vec<u8> = intermediate_update.get("announcement_signed");
		let mut readable = Cursor::new(&blob);
		let unsigned_node_announcement = NodeAnnouncement::read(&mut readable).unwrap().contents;

		let node_id = unsigned_node_announcement.node_id;

		// get this node's address set
		let current_node_delta = delta_set.entry(node_id).or_insert(NodeDelta::default());
		let address_set: HashSet<SocketAddress> = unsigned_node_announcement.addresses.into_iter().collect();

		if previous_node_id != Some(node_id) {
			// we're traversing a new node id, initialize the values
			has_address_set_changed = false;
			has_feature_set_changed = false;
			latest_mutation_timestamp = None;
			is_reminder_due = node_reminder_bucket_flag(&node_id) & due_reminder_buckets != 0;

			// this is the highest timestamp value, so set the seen timestamp accordingly
			current_node_delta.latest_details.as_mut().map(|d| d.seen.replace(current_seen_timestamp));
		}

		if let Some(last_seen_update) = current_node_delta.last_details_before_seen.as_ref() {
			{ // determine the latest mutation timestamp
				if address_set != last_seen_update.addresses {
					has_address_set_changed = true;
					if latest_mutation_timestamp.is_none() {
						latest_mutation_timestamp = Some(current_seen_timestamp);
					}
				}
				if unsigned_node_announcement.features != last_seen_update.features {
					has_feature_set_changed = true;
					if latest_mutation_timestamp.is_none() {
						latest_mutation_timestamp = Some(current_seen_timestamp);
					}
				}
			}

			if current_seen_timestamp >= last_sync_timestamp {
				if has_address_set_changed || has_feature_set_changed {
					// if the last mutation occurred since the last sync, send the mutation variant
					current_node_delta.strategy = Some(NodeSerializationStrategy::Mutated(MutatedNodeProperties {
						addresses: has_address_set_changed,
						features: has_feature_set_changed,
					}));
				}
			} else if is_reminder_due && latest_mutation_timestamp.unwrap_or(u32::MAX) <= reminder_inclusion_threshold_timestamp {
				// only send a reminder if the latest mutation occurred at least 6 days ago
				current_node_delta.strategy = Some(NodeSerializationStrategy::Reminder);
			}

			// Note that we completely ignore the case when the last mutation occurred less than
			// 6 days ago, but prior to the last sync. In that scenario, we send nothing.

		} else {
			// absent any update that was seen prior to the last sync, send the full version
			current_node_delta.strategy = Some(NodeSerializationStrategy::Full);
		}

		previous_node_id = Some(node_id);
	}
	log_info!(logger, "Fetched + processed intermediate node announcement rows ({}) (delta size: {}): {:?}", intermediate_update_count, delta_set.len(), start.elapsed());

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
