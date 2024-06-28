#![deny(unsafe_code)]
#![deny(broken_intra_doc_links)]
#![deny(private_intra_doc_links)]
#![deny(non_upper_case_globals)]
#![deny(non_camel_case_types)]
#![deny(non_snake_case)]
#![deny(unused_variables)]
#![deny(unused_imports)]

extern crate core;

use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::BufReader;
use std::ops::Deref;
use std::sync::Arc;
use bitcoin::blockdata::constants::ChainHash;
use lightning::log_info;

use lightning::routing::gossip::{NetworkGraph, NodeId};
use lightning::util::logger::Logger;
use lightning::util::ser::{ReadableArgs, Writeable};
use tokio::sync::mpsc;
use tokio_postgres::{Client, NoTls};
use crate::config::SYMLINK_GRANULARITY_INTERVAL;
use crate::lookup::DeltaSet;

use crate::persistence::GossipPersister;
use crate::serialization::{SerializationSet, UpdateSerialization};
use crate::snapshot::Snapshotter;
use crate::types::RGSSLogger;

mod downloader;
mod tracking;
mod lookup;
mod persistence;
mod serialization;
mod snapshot;
mod config;
mod hex_utils;
mod verifier;

pub mod types;

#[cfg(test)]
mod tests;

/// The purpose of this prefix is to identify the serialization format, should other rapid gossip
/// sync formats arise in the future.
///
/// The fourth byte is the protocol version in case our format gets updated.
const GOSSIP_PREFIX: [u8; 3] = [76, 68, 75];

pub struct RapidSyncProcessor<L: Deref> where L::Target: Logger {
	network_graph: Arc<NetworkGraph<L>>,
	logger: L
}

pub struct SerializedResponse {
	pub data: Vec<u8>,
	pub message_count: u32,
	pub node_announcement_count: u32,
	/// Despite the name, the count of node announcements that have associated updates, be those
	/// features, addresses, or both
	pub node_update_count: u32,
	pub node_feature_update_count: u32,
	pub node_address_update_count: u32,
	pub channel_announcement_count: u32,
	pub update_count: u32,
	pub update_count_full: u32,
	pub update_count_incremental: u32,
}

impl<L: Deref + Clone + Send + Sync + 'static> RapidSyncProcessor<L> where L::Target: Logger {
	pub fn new(logger: L) -> Self {
		let network = config::network();
		let network_graph = if let Ok(file) = File::open(&config::network_graph_cache_path()) {
			log_info!(logger, "Initializing from cached network graph…");
			let mut buffered_reader = BufReader::new(file);
			let network_graph_result = NetworkGraph::read(&mut buffered_reader, logger.clone());
			if let Ok(network_graph) = network_graph_result {
				log_info!(logger, "Initialized from cached network graph!");
				network_graph
			} else {
				log_info!(logger, "Initialization from cached network graph failed: {}", network_graph_result.err().unwrap());
				NetworkGraph::new(network, logger.clone())
			}
		} else {
			NetworkGraph::new(network, logger.clone())
		};
		let arc_network_graph = Arc::new(network_graph);
		Self {
			network_graph: arc_network_graph,
			logger
		}
	}

	pub async fn start_sync(&self) {
		log_info!(self.logger, "Starting Rapid Gossip Sync Server");
		log_info!(self.logger, "Snapshot interval: {} seconds", config::snapshot_generation_interval());

		// means to indicate sync completion status within this module
		let (sync_completion_sender, mut sync_completion_receiver) = mpsc::channel::<()>(1);

		if config::DOWNLOAD_NEW_GOSSIP {
			let (mut persister, persistence_sender) = GossipPersister::new(self.network_graph.clone(), self.logger.clone());

			log_info!(self.logger, "Starting gossip download");
			tokio::spawn(tracking::download_gossip(persistence_sender, sync_completion_sender,
				Arc::clone(&self.network_graph), self.logger.clone()));
			log_info!(self.logger, "Starting gossip db persistence listener");
			tokio::spawn(async move { persister.persist_gossip().await; });
		} else {
			sync_completion_sender.send(()).await.unwrap();
		}

		let sync_completion = sync_completion_receiver.recv().await;
		if sync_completion.is_none() {
			panic!("Sync failed!");
		}
		log_info!(self.logger, "Initial sync complete!");

		// start the gossip snapshotting service
		Snapshotter::new(Arc::clone(&self.network_graph), self.logger.clone()).snapshot_gossip().await;
	}
}

pub(crate) async fn connect_to_db() -> Client {
	let connection_config = config::db_connection_config();
	let (client, connection) = connection_config.connect(NoTls).await.unwrap();

	tokio::spawn(async move {
		if let Err(e) = connection.await {
			panic!("connection error: {}", e);
		}
	});

	#[cfg(test)]
	{
		let schema_name = tests::db_test_schema();
		let schema_creation_command = format!("CREATE SCHEMA IF NOT EXISTS {}", schema_name);
		client.execute(&schema_creation_command, &[]).await.unwrap();
		client.execute(&format!("SET search_path TO {}", schema_name), &[]).await.unwrap();
	}

	client.execute("set time zone UTC", &[]).await.unwrap();
	client
}

/// This method generates a no-op blob that can be used as a delta where none exists.
///
/// The primary purpose of this method is the scenario of a client retrieving and processing a
/// given snapshot, and then immediately retrieving the would-be next snapshot at the timestamp
/// indicated by the one that was just processed.
/// Previously, there would not be a new snapshot to be processed for that particular timestamp yet,
/// and the server would return a 404 error.
///
/// In principle, this method could also be used to address another unfortunately all too common
/// pitfall: requesting snapshots from intermediate timestamps, i. e. those that are not multiples
/// of our granularity constant. Note that for that purpose, this method could be very dangerous,
/// because if consumed, the `timestamp` value calculated here will overwrite the timestamp that
/// the client previously had, which could result in duplicated or omitted gossip down the line.
fn serialize_empty_blob(current_timestamp: u64) -> Vec<u8> {
	let mut blob = GOSSIP_PREFIX.to_vec();

	let network = config::network();
	let chain_hash = ChainHash::using_genesis_block(network);
	chain_hash.write(&mut blob).unwrap();

	let blob_timestamp = Snapshotter::<Arc<RGSSLogger>>::round_down_to_nearest_multiple(current_timestamp, SYMLINK_GRANULARITY_INTERVAL as u64) as u32;
	blob_timestamp.write(&mut blob).unwrap();

	0u32.write(&mut blob).unwrap(); // node count
	0u32.write(&mut blob).unwrap(); // announcement count
	0u32.write(&mut blob).unwrap(); // update count

	blob
}

async fn calculate_delta<L: Deref + Clone>(network_graph: Arc<NetworkGraph<L>>, last_sync_timestamp: u32, snapshot_reference_timestamp: Option<u64>, logger: L) -> SerializationSet where L::Target: Logger {
	let client = connect_to_db().await;

	network_graph.remove_stale_channels_and_tracking();

	// set a flag if the chain hash is prepended
	// chain hash only necessary if either channel announcements or non-incremental updates are present
	// for announcement-free incremental-only updates, chain hash can be skipped

	let mut delta_set = DeltaSet::new();
	lookup::fetch_channel_announcements(&mut delta_set, network_graph, &client, last_sync_timestamp, snapshot_reference_timestamp, logger.clone()).await;
	log_info!(logger, "announcement channel count: {}", delta_set.len());
	lookup::fetch_channel_updates(&mut delta_set, &client, last_sync_timestamp, logger.clone()).await;
	log_info!(logger, "update-fetched channel count: {}", delta_set.len());
	let node_delta_set = lookup::fetch_node_updates(&client, last_sync_timestamp, logger.clone()).await;
	log_info!(logger, "update-fetched node count: {}", node_delta_set.len());
	lookup::filter_delta_set(&mut delta_set, logger.clone());
	log_info!(logger, "update-filtered channel count: {}", delta_set.len());
	serialization::serialize_delta_set(delta_set, node_delta_set, last_sync_timestamp)
}

fn serialize_delta<L: Deref + Clone>(serialization_details: &SerializationSet, serialization_version: u8, logger: L) -> SerializedResponse where L::Target: Logger {
	let mut output: Vec<u8> = vec![];
	let snapshot_interval = config::snapshot_generation_interval();

	let mut node_id_set: HashSet<NodeId> = HashSet::new();
	let mut node_id_indices: HashMap<NodeId, usize> = HashMap::new();
	let mut node_ids: Vec<NodeId> = Vec::new();
	let mut duplicate_node_ids: i32 = 0;

	let mut get_node_id_index = |node_id: NodeId| {
		if node_id_set.insert(node_id) {
			node_ids.push(node_id);
			let index = node_ids.len() - 1;
			node_id_indices.insert(node_id, index);
			return index;
		}
		duplicate_node_ids += 1;
		node_id_indices[&node_id]
	};

	// process announcements
	// write the number of channel announcements to the output
	let announcement_count = serialization_details.announcements.len() as u32;
	announcement_count.write(&mut output).unwrap();
	let mut previous_announcement_scid = 0;
	for current_announcement in &serialization_details.announcements {
		let id_index_1 = get_node_id_index(current_announcement.node_id_1);
		let id_index_2 = get_node_id_index(current_announcement.node_id_2);
		let mut stripped_announcement = serialization::serialize_stripped_channel_announcement(&current_announcement, id_index_1, id_index_2, previous_announcement_scid);
		output.append(&mut stripped_announcement);

		previous_announcement_scid = current_announcement.short_channel_id;
	}

	// process updates
	let mut previous_update_scid = 0;
	let update_count = serialization_details.updates.len() as u32;
	update_count.write(&mut output).unwrap();

	let default_update_values = &serialization_details.full_update_defaults;
	if update_count > 0 {
		default_update_values.cltv_expiry_delta.write(&mut output).unwrap();
		default_update_values.htlc_minimum_msat.write(&mut output).unwrap();
		default_update_values.fee_base_msat.write(&mut output).unwrap();
		default_update_values.fee_proportional_millionths.write(&mut output).unwrap();
		default_update_values.htlc_maximum_msat.write(&mut output).unwrap();
	}

	let mut update_count_full = 0;
	let mut update_count_incremental = 0;
	for current_update in &serialization_details.updates {
		match &current_update {
			UpdateSerialization::Full(_) => {
				update_count_full += 1;
			}
			UpdateSerialization::Incremental(_, _) | UpdateSerialization::Reminder(_, _) => {
				update_count_incremental += 1;
			}
		};

		let mut stripped_update = serialization::serialize_stripped_channel_update(&current_update, &default_update_values, previous_update_scid);
		output.append(&mut stripped_update);

		previous_update_scid = current_update.scid();
	}

	// some stats
	let message_count = announcement_count + update_count;

	let mut prefixed_output = GOSSIP_PREFIX.to_vec();
	prefixed_output.push(serialization_version);

	// always write the chain hash
	serialization_details.chain_hash.write(&mut prefixed_output).unwrap();
	// always write the latest seen timestamp
	let latest_seen_timestamp = serialization_details.latest_seen;
	let overflow_seconds = latest_seen_timestamp % snapshot_interval;
	let serialized_seen_timestamp = latest_seen_timestamp.saturating_sub(overflow_seconds);
	serialized_seen_timestamp.write(&mut prefixed_output).unwrap();

	if serialization_version >= 2 { // serialize the most common node features
		for mutated_node_id in serialization_details.node_mutations.keys() {
			// consider mutated nodes outside channel announcements
			get_node_id_index(mutated_node_id.clone());
		}

		let default_feature_count = serialization_details.node_announcement_feature_defaults.len() as u8;
		debug_assert!(default_feature_count <= config::NODE_DEFAULT_FEATURE_COUNT, "Default feature count cannot exceed maximum");
		default_feature_count.write(&mut prefixed_output).unwrap();

		for current_feature in &serialization_details.node_announcement_feature_defaults {
			current_feature.write(&mut prefixed_output).unwrap();
		}
	}

	let node_id_count = node_ids.len() as u32;
	node_id_count.write(&mut prefixed_output).unwrap();

	let mut node_update_count = 0u32;
	let mut node_feature_update_count = 0u32;
	let mut node_address_update_count = 0u32;

	for current_node_id in node_ids {
		let mut current_node_delta_serialization: Vec<u8> = Vec::new();
		current_node_id.write(&mut current_node_delta_serialization).unwrap();

		if serialization_version >= 2 {
			if let Some(node_delta) = serialization_details.node_mutations.get(&current_node_id) {
				/*
				Bitmap:
				7: expect extra data after the pubkey (a u16 for the count, and then that number of bytes)
				5-3: index of new features among default (1-6). If index is 7 (all 3 bits are set, it's
				outside the present default range). 0 means no feature changes.
				2: addresses have changed

				1: used for all keys
				0: used for odd keys
				*/

				if node_delta.has_address_set_changed {
					node_address_update_count += 1;

					let address_set = &node_delta.latest_details.as_ref().unwrap().addresses;
					let mut address_serialization = Vec::new();

					// we don't know a priori how many are <= 255 bytes
					let mut total_address_count = 0u8;

					for address in address_set.iter() {
						if total_address_count == u8::MAX {
							// don't serialize more than 255 addresses
							break;
						}
						if let Ok(serialized_length) = u8::try_from(address.serialized_length()) {
							total_address_count += 1;
							serialized_length.write(&mut address_serialization).unwrap();
							address.write(&mut address_serialization).unwrap();
						};
					}

					// signal the presence of node addresses
					current_node_delta_serialization[0] |= 1 << 2;
					// serialize the actual addresses and count
					total_address_count.write(&mut current_node_delta_serialization).unwrap();
					current_node_delta_serialization.append(&mut address_serialization);
				}

				if node_delta.has_feature_set_changed {
					node_feature_update_count += 1;

					let latest_features = &node_delta.latest_details.as_ref().unwrap().features;

					// are these features among the most common ones?
					if let Some(index) = serialization_details.node_announcement_feature_defaults.iter().position(|f| f == latest_features) {
						// this feature set is among the 6 defaults
						current_node_delta_serialization[0] |= ((index + 1) as u8) << 3;
					} else {
						current_node_delta_serialization[0] |= 0b_0011_1000; // 7 << 3
						latest_features.write(&mut current_node_delta_serialization).unwrap();
					}
				}

				if node_delta.has_address_set_changed || node_delta.has_feature_set_changed {
					node_update_count += 1;
				}
			}
		}

		prefixed_output.append(&mut current_node_delta_serialization);
	}

	prefixed_output.append(&mut output);

	log_info!(logger, "duplicated node ids: {}", duplicate_node_ids);
	log_info!(logger, "latest seen timestamp: {:?}", serialization_details.latest_seen);

	SerializedResponse {
		data: prefixed_output,
		message_count,
		node_announcement_count: node_id_count,
		node_update_count,
		node_feature_update_count,
		node_address_update_count,
		channel_announcement_count: announcement_count,
		update_count,
		update_count_full,
		update_count_incremental,
	}
}
