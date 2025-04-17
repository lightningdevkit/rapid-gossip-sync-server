use std::cmp::max;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use bitcoin::blockdata::constants::ChainHash;
use lightning::ln::msgs::{UnsignedChannelAnnouncement, UnsignedChannelUpdate};
use lightning::types::features::NodeFeatures;
use lightning::util::ser::{BigSize, Writeable};
use crate::config;

use crate::lookup::{DeltaSet, DirectedUpdateDelta, NodeDeltaSet};

pub(super) struct SerializationSet {
	pub(super) announcements: Vec<UnsignedChannelAnnouncement>,
	pub(super) updates: Vec<UpdateSerialization>,
	pub(super) full_update_defaults: DefaultUpdateValues,
	pub(super) node_announcement_feature_defaults: Vec<NodeFeatures>,
	pub(super) node_mutations: NodeDeltaSet,
	pub(super) latest_seen: u32,
	pub(super) chain_hash: ChainHash,
}

pub(super) struct DefaultUpdateValues {
	pub(super) cltv_expiry_delta: u16,
	pub(super) htlc_minimum_msat: u64,
	pub(super) fee_base_msat: u32,
	pub(super) fee_proportional_millionths: u32,
	pub(super) htlc_maximum_msat: u64,
}

impl Default for DefaultUpdateValues {
	fn default() -> Self {
		Self {
			cltv_expiry_delta: 0,
			htlc_minimum_msat: 0,
			fee_base_msat: 0,
			fee_proportional_millionths: 0,
			htlc_maximum_msat: 0,
		}
	}
}

pub(super) struct MutatedProperties {
	pub(super) flags: bool,
	pub(super) cltv_expiry_delta: bool,
	pub(super) htlc_minimum_msat: bool,
	pub(super) fee_base_msat: bool,
	pub(super) fee_proportional_millionths: bool,
	pub(super) htlc_maximum_msat: bool,
}

impl Default for MutatedProperties {
	fn default() -> Self {
		Self {
			flags: false,
			cltv_expiry_delta: false,
			htlc_minimum_msat: false,
			fee_base_msat: false,
			fee_proportional_millionths: false,
			htlc_maximum_msat: false,
		}
	}
}

impl MutatedProperties {
	/// Does not include flags because the flag byte is always sent in full
	fn len(&self) -> u8 {
		let mut mutations = 0;
		if self.cltv_expiry_delta { mutations += 1; };
		if self.htlc_minimum_msat { mutations += 1; };
		if self.fee_base_msat { mutations += 1; };
		if self.fee_proportional_millionths { mutations += 1; };
		if self.htlc_maximum_msat { mutations += 1; };
		mutations
	}
}

pub(super) enum UpdateSerialization {
	Full(UnsignedChannelUpdate),
	Incremental(UnsignedChannelUpdate, MutatedProperties),
	Reminder(u64, u8),
}
impl UpdateSerialization {
	pub(super) fn scid(&self) -> u64 {
		match self {
			UpdateSerialization::Full(latest_update)|
			UpdateSerialization::Incremental(latest_update, _) => latest_update.short_channel_id,
			UpdateSerialization::Reminder(scid, _) => *scid,
		}
	}

	fn flags(&self) -> u8 {
		match self {
			UpdateSerialization::Full(latest_update)|
			UpdateSerialization::Incremental(latest_update, _) => latest_update.channel_flags,
			UpdateSerialization::Reminder(_, flags) => *flags,
		}
	}
}

pub(super) struct MutatedNodeProperties {
	pub(super) addresses: bool,
	pub(super) features: bool,
}

pub(super) enum NodeSerializationStrategy {
	/// Only serialize the aspects of the node ID that have been mutated. Skip if they haven't been
	Mutated(MutatedNodeProperties),
	/// Whether or not the addresses or features have been mutated, serialize this node in full. It
	/// may have been purged from the client.
	Full,
	/// This node ID has been seen recently enough to not have been pruned, and this update serves
	/// solely the purpose of delaying any pruning, without applying any mutations
	Reminder
}

struct FullUpdateValueHistograms {
	cltv_expiry_delta: HashMap<u16, usize>,
	htlc_minimum_msat: HashMap<u64, usize>,
	fee_base_msat: HashMap<u32, usize>,
	fee_proportional_millionths: HashMap<u32, usize>,
	htlc_maximum_msat: HashMap<u64, usize>,
}

pub(super) fn serialize_delta_set(channel_delta_set: DeltaSet, node_delta_set: NodeDeltaSet, last_sync_timestamp: u32) -> SerializationSet {
	let mut serialization_set = SerializationSet {
		announcements: vec![],
		updates: vec![],
		full_update_defaults: Default::default(),
		node_announcement_feature_defaults: vec![],
		node_mutations: Default::default(),
		chain_hash: ChainHash::using_genesis_block(config::network()),
		latest_seen: 0,
	};

	let mut full_update_histograms = FullUpdateValueHistograms {
		cltv_expiry_delta: Default::default(),
		htlc_minimum_msat: Default::default(),
		fee_base_msat: Default::default(),
		fee_proportional_millionths: Default::default(),
		htlc_maximum_msat: Default::default(),
	};

	let mut record_full_update_in_histograms = |full_update: &UnsignedChannelUpdate| {
		*full_update_histograms.cltv_expiry_delta.entry(full_update.cltv_expiry_delta).or_insert(0) += 1;
		*full_update_histograms.htlc_minimum_msat.entry(full_update.htlc_minimum_msat).or_insert(0) += 1;
		*full_update_histograms.fee_base_msat.entry(full_update.fee_base_msat).or_insert(0) += 1;
		*full_update_histograms.fee_proportional_millionths.entry(full_update.fee_proportional_millionths).or_insert(0) += 1;
		*full_update_histograms.htlc_maximum_msat.entry(full_update.htlc_maximum_msat).or_insert(0) += 1;
	};

	// if the previous seen update happened more than 6 days ago, the client may have pruned it, and an incremental update wouldn't work
	let non_incremental_previous_update_threshold_timestamp = SystemTime::now().checked_sub(config::CHANNEL_REMINDER_AGE).unwrap().duration_since(UNIX_EPOCH).unwrap().as_secs() as u32;

	for (scid, channel_delta) in channel_delta_set.into_iter() {
		// any announcement chain hash is gonna be the same value. Just set it from the first one.
		let channel_announcement_delta = channel_delta.announcement.as_ref().unwrap();

		let current_announcement_seen = channel_announcement_delta.seen;
		let is_new_announcement = current_announcement_seen >= last_sync_timestamp;
		let is_newly_included_announcement = if let Some(first_update_seen) = channel_delta.first_bidirectional_updates_seen {
			first_update_seen >= last_sync_timestamp
		} else {
			false
		};
		let send_announcement = is_new_announcement || is_newly_included_announcement;
		if send_announcement {
			serialization_set.latest_seen = max(serialization_set.latest_seen, current_announcement_seen);
			serialization_set.announcements.push(channel_delta.announcement.unwrap().announcement);
		}

		let direction_a_updates = channel_delta.updates.0;
		let direction_b_updates = channel_delta.updates.1;

		let mut categorize_directed_update_serialization = |directed_updates: Option<DirectedUpdateDelta>| {
			if let Some(updates) = directed_updates {
				if let Some(latest_update_delta) = updates.latest_update_after_seen {
					let latest_update = latest_update_delta.update;
					assert_eq!(latest_update.short_channel_id, scid, "Update in DB had wrong SCID column");

					// the returned seen timestamp should be the latest of all the returned
					// announcements and latest updates
					serialization_set.latest_seen = max(serialization_set.latest_seen, latest_update_delta.seen);

					if let Some(update_delta) = updates.last_update_before_seen {
						let mutated_properties = updates.mutated_properties;
						if send_announcement || mutated_properties.len() == 5 || update_delta.seen <= non_incremental_previous_update_threshold_timestamp {
							// all five values have changed, it makes more sense to just
							// serialize the update as a full update instead of as a change
							// this way, the default values can be computed more efficiently
							record_full_update_in_histograms(&latest_update);
							serialization_set.updates.push(UpdateSerialization::Full(latest_update));
						} else if mutated_properties.len() > 0 || mutated_properties.flags {
							// we don't count flags as mutated properties
							serialization_set.updates.push(
								UpdateSerialization::Incremental(latest_update, mutated_properties));
						} else if channel_delta.requires_reminder {
							if let Some(flags) = updates.serialization_update_flags {
								serialization_set.updates.push(UpdateSerialization::Reminder(scid, flags));
							}
						}
					} else {
						// serialize the full update
						record_full_update_in_histograms(&latest_update);
						serialization_set.updates.push(UpdateSerialization::Full(latest_update));
					}
				} else if is_newly_included_announcement {
					if let Some(unannounced_update) = updates.last_update_before_seen {
						serialization_set.updates.push(UpdateSerialization::Full(unannounced_update.update));
					}
				} else if let Some(flags) = updates.serialization_update_flags {
					serialization_set.updates.push(UpdateSerialization::Reminder(scid, flags));
				}
			}
		};

		categorize_directed_update_serialization(direction_a_updates);
		categorize_directed_update_serialization(direction_b_updates);
	}

	let default_update_values = DefaultUpdateValues {
		cltv_expiry_delta: find_most_common_histogram_entry_with_default(full_update_histograms.cltv_expiry_delta, 0),
		htlc_minimum_msat: find_most_common_histogram_entry_with_default(full_update_histograms.htlc_minimum_msat, 0),
		fee_base_msat: find_most_common_histogram_entry_with_default(full_update_histograms.fee_base_msat, 0),
		fee_proportional_millionths: find_most_common_histogram_entry_with_default(full_update_histograms.fee_proportional_millionths, 0),
		htlc_maximum_msat: find_most_common_histogram_entry_with_default(full_update_histograms.htlc_maximum_msat, 0),
	};

	serialization_set.full_update_defaults = default_update_values;

	serialization_set.node_mutations = node_delta_set.into_iter().filter_map(|(id, mut delta)| {
		if delta.strategy.is_none() {
			return None;
		}
		if let Some(last_details_before_seen) = delta.last_details_before_seen.as_ref() {
			if let Some(last_details_seen) = last_details_before_seen.seen {
				if last_details_seen <= non_incremental_previous_update_threshold_timestamp {
					delta.strategy = Some(NodeSerializationStrategy::Full)
				}
			}
			Some((id, delta))
		} else {
			None
		}
	}).collect();

	let mut node_feature_histogram: HashMap<&NodeFeatures, usize> = Default::default();
	for (_id, delta) in serialization_set.node_mutations.iter() {
		// consider either full or feature-mutating serializations for histogram
		let mut should_add_to_histogram = matches!(delta.strategy, Some(NodeSerializationStrategy::Full));
		if let Some(NodeSerializationStrategy::Mutated(mutation)) = delta.strategy.as_ref() {
			should_add_to_histogram = mutation.features;
		}

		if should_add_to_histogram {
			if let Some(latest_details) = delta.latest_details.as_ref() {
				*node_feature_histogram.entry(&latest_details.features).or_insert(0) += 1;
			};
		}
	}
	serialization_set.node_announcement_feature_defaults = find_leading_histogram_entries(node_feature_histogram, config::NODE_DEFAULT_FEATURE_COUNT as usize);

	serialization_set
}

pub fn serialize_stripped_channel_announcement(announcement: &UnsignedChannelAnnouncement, node_id_a_index: usize, node_id_b_index: usize, previous_scid: u64) -> Vec<u8> {
	let mut stripped_announcement = vec![];

	announcement.features.write(&mut stripped_announcement).unwrap();

	if previous_scid > announcement.short_channel_id {
		panic!("unsorted scids!");
	}
	let scid_delta = BigSize(announcement.short_channel_id - previous_scid);
	scid_delta.write(&mut stripped_announcement).unwrap();

	// write indices of node ids rather than the node IDs themselves
	BigSize(node_id_a_index as u64).write(&mut stripped_announcement).unwrap();
	BigSize(node_id_b_index as u64).write(&mut stripped_announcement).unwrap();

	// println!("serialized CA: {}, \n{:?}\n{:?}\n", announcement.short_channel_id, announcement.node_id_1, announcement.node_id_2);
	stripped_announcement
}

pub(super) fn serialize_stripped_channel_update(update: &UpdateSerialization, default_values: &DefaultUpdateValues, previous_scid: u64) -> Vec<u8> {
	let mut serialized_flags = update.flags();

	if previous_scid > update.scid() {
		panic!("unsorted scids!");
	}

	let mut delta_serialization = Vec::new();
	let mut prefixed_serialization = Vec::new();

	match update {
		UpdateSerialization::Full(latest_update) => {
			if latest_update.cltv_expiry_delta != default_values.cltv_expiry_delta {
				serialized_flags |= 0b_0100_0000;
				latest_update.cltv_expiry_delta.write(&mut delta_serialization).unwrap();
			}

			if latest_update.htlc_minimum_msat != default_values.htlc_minimum_msat {
				serialized_flags |= 0b_0010_0000;
				latest_update.htlc_minimum_msat.write(&mut delta_serialization).unwrap();
			}

			if latest_update.fee_base_msat != default_values.fee_base_msat {
				serialized_flags |= 0b_0001_0000;
				latest_update.fee_base_msat.write(&mut delta_serialization).unwrap();
			}

			if latest_update.fee_proportional_millionths != default_values.fee_proportional_millionths {
				serialized_flags |= 0b_0000_1000;
				latest_update.fee_proportional_millionths.write(&mut delta_serialization).unwrap();
			}

			if latest_update.htlc_maximum_msat != default_values.htlc_maximum_msat {
				serialized_flags |= 0b_0000_0100;
				latest_update.htlc_maximum_msat.write(&mut delta_serialization).unwrap();
			}
		}
		UpdateSerialization::Incremental(latest_update, mutated_properties) => {
			// indicate that this update is incremental
			serialized_flags |= 0b_1000_0000;

			if mutated_properties.cltv_expiry_delta {
				serialized_flags |= 0b_0100_0000;
				latest_update.cltv_expiry_delta.write(&mut delta_serialization).unwrap();
			}

			if mutated_properties.htlc_minimum_msat {
				serialized_flags |= 0b_0010_0000;
				latest_update.htlc_minimum_msat.write(&mut delta_serialization).unwrap();
			}

			if mutated_properties.fee_base_msat {
				serialized_flags |= 0b_0001_0000;
				latest_update.fee_base_msat.write(&mut delta_serialization).unwrap();
			}

			if mutated_properties.fee_proportional_millionths {
				serialized_flags |= 0b_0000_1000;
				latest_update.fee_proportional_millionths.write(&mut delta_serialization).unwrap();
			}

			if mutated_properties.htlc_maximum_msat {
				serialized_flags |= 0b_0000_0100;
				latest_update.htlc_maximum_msat.write(&mut delta_serialization).unwrap();
			}
		},
		UpdateSerialization::Reminder(_, _) => {
			// indicate that this update is incremental
			serialized_flags |= 0b_1000_0000;
		}
	}
	let scid_delta = BigSize(update.scid() - previous_scid);
	scid_delta.write(&mut prefixed_serialization).unwrap();

	serialized_flags.write(&mut prefixed_serialization).unwrap();
	prefixed_serialization.append(&mut delta_serialization);

	prefixed_serialization
}

pub(super) fn find_most_common_histogram_entry_with_default<T: Copy>(histogram: HashMap<T, usize>, default: T) -> T {
	let most_frequent_entry = histogram.iter().max_by(|a, b| a.1.cmp(&b.1));
	if let Some(entry_details) = most_frequent_entry {
		// .0 is the value
		// .1 is the frequency
		return entry_details.0.to_owned();
	}
	// the default should pretty much always be a 0 as T
	// though for htlc maximum msat it could be a u64::max
	default
}

pub(super) fn find_leading_histogram_entries(histogram: HashMap<&NodeFeatures, usize>, count: usize) -> Vec<NodeFeatures> {
	let mut entry_counts: Vec<_> = histogram.iter().filter(|&(_, &count)| count > 1).collect();
	entry_counts.sort_by(|a, b| b.1.cmp(&a.1));
	entry_counts.into_iter().take(count).map(|(&features, _count)| features.clone()).collect()
}
