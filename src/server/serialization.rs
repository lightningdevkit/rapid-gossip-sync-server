use std::collections::HashMap;
use lightning::ln::msgs::{OptionalField, UnsignedChannelAnnouncement, UnsignedChannelUpdate};
use lightning::util::ser::{BigSize, Writeable};

pub(super) struct DefaultUpdateValues {
	pub(super) cltv_expiry_delta: u16,
	pub(super) htlc_minimum_msat: u64,
	pub(super) fee_base_msat: u32,
	pub(super) fee_proportional_millionths: u32,
	pub(super) htlc_maximum_msat: u64,
}

pub(super) struct UpdateChangeSet {
	pub(super) affected_field_count: u8,
	pub(super) affected_fields: Vec<String>,
	pub(super) serialization: Vec<u8>,
}

pub fn serialize_stripped_channel_announcement(announcement: &UnsignedChannelAnnouncement, node_id_a_index: usize, node_id_b_index: usize, previous_scid: u64) -> Vec<u8> {
	let mut stripped_announcement = vec![];
	announcement.features.write(&mut stripped_announcement);

	if previous_scid > announcement.short_channel_id {
		panic!("unsorted scids!");
	}
	let scid_delta = BigSize(announcement.short_channel_id - previous_scid);
	scid_delta.write(&mut stripped_announcement);

	// announcement.node_id_1.write(&mut stripped_announcement);
	// announcement.node_id_2.write(&mut stripped_announcement);

	// write indices of node ids rather than the node IDs themselves
	BigSize(node_id_a_index as u64).write(&mut stripped_announcement);
	BigSize(node_id_b_index as u64).write(&mut stripped_announcement);

	// println!("serialized CA: {}, \n{:?}\n{:?}\n", announcement.short_channel_id, announcement.node_id_1, announcement.node_id_2);
	stripped_announcement
}

fn serialize_stripped_channel_update_old(update: &UnsignedChannelUpdate, previous_scid: u64) -> Vec<u8> {
	let mut stripped_update = vec![];
	// standard inclusions

	if previous_scid > update.short_channel_id {
		panic!("unsorted scids!");
	}
	let scid_delta = BigSize(update.short_channel_id - previous_scid);
	scid_delta.write(&mut stripped_update);

	println!("full update scid/flags: {}/{}", update.short_channel_id, update.flags);
	update.flags.write(&mut stripped_update);
	// skip and ignore CLTV expiry delta?
	update.cltv_expiry_delta.write(&mut stripped_update);
	update.htlc_minimum_msat.write(&mut stripped_update);
	update.fee_base_msat.write(&mut stripped_update);
	update.fee_proportional_millionths.write(&mut stripped_update);
	let htlc_maximum_msat = optional_htlc_maximum_to_u64(&update.htlc_maximum_msat);
	htlc_maximum_msat.write(&mut stripped_update);
	stripped_update
}

pub(super) fn serialize_stripped_channel_update(latest_update: &UnsignedChannelUpdate, default_values: &DefaultUpdateValues, previous_scid: u64) -> Vec<u8> {
	let mut aberrant_field_keys = vec![];

	let mut prefixed_serialization = Vec::new();


	if previous_scid > latest_update.short_channel_id {
		panic!("unsorted scids!");
	}
	let scid_delta = BigSize(latest_update.short_channel_id - previous_scid);
	scid_delta.write(&mut prefixed_serialization);


	let mut serialized_flags = latest_update.flags;
	let mut delta_serialization = Vec::new();

	if latest_update.cltv_expiry_delta != default_values.cltv_expiry_delta {
		aberrant_field_keys.push("cltv_expiry_delta".to_string());

		serialized_flags |= 0b_0100_0000;
		latest_update.cltv_expiry_delta.write(&mut delta_serialization);
	}

	if latest_update.htlc_minimum_msat != default_values.htlc_minimum_msat {
		aberrant_field_keys.push("htlc_minimum_msat".to_string());

		serialized_flags |= 0b_0010_0000;
		latest_update.htlc_minimum_msat.write(&mut delta_serialization);
	}

	if latest_update.fee_base_msat != default_values.fee_base_msat {
		aberrant_field_keys.push("fee_base_msat".to_string());

		serialized_flags |= 0b_0001_0000;
		latest_update.fee_base_msat.write(&mut delta_serialization);
	}

	if latest_update.fee_proportional_millionths != default_values.fee_proportional_millionths {
		aberrant_field_keys.push("fee_proportional_millionths".to_string());

		serialized_flags |= 0b_0000_1000;
		latest_update.fee_proportional_millionths.write(&mut delta_serialization);
	}

	let latest_update_htlc_maximum = optional_htlc_maximum_to_u64(&latest_update.htlc_maximum_msat);
	if latest_update_htlc_maximum != default_values.htlc_maximum_msat {
		aberrant_field_keys.push("htlc_maximum_msat".to_string());

		serialized_flags |= 0b_0000_0100;
		latest_update_htlc_maximum.write(&mut delta_serialization);
	}

	// standard inclusions
	serialized_flags.write(&mut prefixed_serialization);
	prefixed_serialization.append(&mut delta_serialization);

	prefixed_serialization
}

pub(super) fn compare_update_with_reference(latest_update: &UnsignedChannelUpdate, default_values: &DefaultUpdateValues, reference_update: Option<&UnsignedChannelUpdate>, previous_scid: u64) -> UpdateChangeSet {
	let mut updated_field_count = 0;
	let mut modified_field_keys = vec![];

	let mut serialized_flags = latest_update.flags;
	let mut delta_serialization = Vec::new();

	let mut prefixed_serialization = Vec::new();

	if let Some(reference_update) = reference_update {
		if latest_update.flags != reference_update.flags {
			updated_field_count += 1;
			modified_field_keys.push("flags".to_string());
		}

		// ignore CLTV expiry delta
		if latest_update.cltv_expiry_delta != reference_update.cltv_expiry_delta {
			updated_field_count += 1;
			modified_field_keys.push("cltv_expiry_delta".to_string());

			serialized_flags |= 0b_0100_0000;
			latest_update.cltv_expiry_delta.write(&mut delta_serialization);
		}

		if latest_update.htlc_minimum_msat != reference_update.htlc_minimum_msat {
			updated_field_count += 1;
			modified_field_keys.push("htlc_minimum_msat".to_string());

			serialized_flags |= 0b_0010_0000;
			latest_update.htlc_minimum_msat.write(&mut delta_serialization);
		}

		if latest_update.fee_base_msat != reference_update.fee_base_msat {
			updated_field_count += 1;
			modified_field_keys.push("fee_base_msat".to_string());

			serialized_flags |= 0b_0001_0000;
			latest_update.fee_base_msat.write(&mut delta_serialization);
		}

		if latest_update.fee_proportional_millionths != reference_update.fee_proportional_millionths {
			updated_field_count += 1;
			modified_field_keys.push("fee_proportional_millionths".to_string());

			serialized_flags |= 0b_0000_1000;
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
			serialized_flags |= 0b_0000_0100;

			let new_htlc_maximum = optional_htlc_maximum_to_u64(&latest_update.htlc_maximum_msat);
			new_htlc_maximum.write(&mut delta_serialization);
		}

		if updated_field_count > 0 {
			// if no field was changed, there is no point serializing anything at all

			// standard inclusions
			if previous_scid > latest_update.short_channel_id {
				panic!("unsorted scids!");
			}
			let scid_delta = BigSize(latest_update.short_channel_id - previous_scid);
			scid_delta.write(&mut prefixed_serialization);


			serialized_flags |= 0b_1000_0000; // signify with the most significant bit that this update is incremental
			serialized_flags.write(&mut prefixed_serialization);

			// debugging purposes only
			// let serialization_delta_string = format!("{:b}", serialized_flags);

			prefixed_serialization.append(&mut delta_serialization);
		}
	} else {
		prefixed_serialization = serialize_stripped_channel_update(latest_update, default_values, previous_scid);
	}

	UpdateChangeSet {
		affected_field_count: updated_field_count,
		affected_fields: modified_field_keys,
		serialization: prefixed_serialization,
	}
}

pub(super) fn find_most_common_histogram_entry<T: Copy>(histogram: HashMap<T, usize>) -> T {
	histogram.iter().max_by(|a, b| a.1.cmp(&b.1)).unwrap().0.to_owned()
}

pub(super) fn optional_htlc_maximum_to_u64(htlc_maximum_msat: &OptionalField<u64>) -> u64 {
	if let OptionalField::Present(maximum) = htlc_maximum_msat {
		maximum.clone()
	} else {
		u64::MAX
	}
}
