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
use lightning::log_info;

use lightning::routing::gossip::{NetworkGraph, NodeId};
use lightning::util::logger::Logger;
use lightning::util::ser::{ReadableArgs, Writeable};
use tokio::sync::mpsc;
use crate::lookup::DeltaSet;

use crate::persistence::GossipPersister;
use crate::serialization::UpdateSerialization;
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

/// The purpose of this prefix is to identify the serialization format, should other rapid gossip
/// sync formats arise in the future.
///
/// The fourth byte is the protocol version in case our format gets updated.
const GOSSIP_PREFIX: [u8; 4] = [76, 68, 75, 1];

pub struct RapidSyncProcessor<L: Deref> where L::Target: Logger {
	network_graph: Arc<NetworkGraph<L>>,
	logger: L
}

pub struct SerializedResponse {
	pub data: Vec<u8>,
	pub message_count: u32,
	pub announcement_count: u32,
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
		// means to indicate sync completion status within this module
		let (sync_completion_sender, mut sync_completion_receiver) = mpsc::channel::<()>(1);

		if config::DOWNLOAD_NEW_GOSSIP {
			let (mut persister, persistence_sender) = GossipPersister::new(Arc::clone(&self.network_graph));

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
	let genesis_block = bitcoin::blockdata::constants::genesis_block(network);
	let chain_hash = genesis_block.block_hash();
	chain_hash.write(&mut blob).unwrap();

	let blob_timestamp = Snapshotter::<Arc<RGSSLogger>>::round_down_to_nearest_multiple(current_timestamp, config::SNAPSHOT_CALCULATION_INTERVAL as u64) as u32;
	blob_timestamp.write(&mut blob).unwrap();

	0u32.write(&mut blob).unwrap(); // node count
	0u32.write(&mut blob).unwrap(); // announcement count
	0u32.write(&mut blob).unwrap(); // update count

	blob
}

async fn serialize_delta<L: Deref>(network_graph: Arc<NetworkGraph<L>>, last_sync_timestamp: u32, logger: L) -> SerializedResponse where L::Target: Logger {
	let (client, connection) = lookup::connect_to_db().await;

	network_graph.remove_stale_channels_and_tracking();

	tokio::spawn(async move {
		if let Err(e) = connection.await {
			panic!("connection error: {}", e);
		}
	});

	let mut output: Vec<u8> = vec![];

	// set a flag if the chain hash is prepended
	// chain hash only necessary if either channel announcements or non-incremental updates are present
	// for announcement-free incremental-only updates, chain hash can be skipped

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

	let mut delta_set = DeltaSet::new();
	lookup::fetch_channel_announcements(&mut delta_set, network_graph, &client, last_sync_timestamp).await;
	log_info!(logger, "announcement channel count: {}", delta_set.len());
	lookup::fetch_channel_updates(&mut delta_set, &client, last_sync_timestamp).await;
	log_info!(logger, "update-fetched channel count: {}", delta_set.len());
	lookup::filter_delta_set(&mut delta_set);
	log_info!(logger, "update-filtered channel count: {}", delta_set.len());
	let serialization_details = serialization::serialize_delta_set(delta_set, last_sync_timestamp);

	// process announcements
	// write the number of channel announcements to the output
	let announcement_count = serialization_details.announcements.len() as u32;
	announcement_count.write(&mut output).unwrap();
	let mut previous_announcement_scid = 0;
	for current_announcement in serialization_details.announcements {
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

	let default_update_values = serialization_details.full_update_defaults;
	if update_count > 0 {
		default_update_values.cltv_expiry_delta.write(&mut output).unwrap();
		default_update_values.htlc_minimum_msat.write(&mut output).unwrap();
		default_update_values.fee_base_msat.write(&mut output).unwrap();
		default_update_values.fee_proportional_millionths.write(&mut output).unwrap();
		default_update_values.htlc_maximum_msat.write(&mut output).unwrap();
	}

	let mut update_count_full = 0;
	let mut update_count_incremental = 0;
	for current_update in serialization_details.updates {
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

	// always write the chain hash
	serialization_details.chain_hash.write(&mut prefixed_output).unwrap();
	// always write the latest seen timestamp
	let latest_seen_timestamp = serialization_details.latest_seen;
	let overflow_seconds = latest_seen_timestamp % config::SNAPSHOT_CALCULATION_INTERVAL;
	let serialized_seen_timestamp = latest_seen_timestamp.saturating_sub(overflow_seconds);
	serialized_seen_timestamp.write(&mut prefixed_output).unwrap();

	let node_id_count = node_ids.len() as u32;
	node_id_count.write(&mut prefixed_output).unwrap();

	for current_node_id in node_ids {
		current_node_id.write(&mut prefixed_output).unwrap();
	}

	prefixed_output.append(&mut output);

	log_info!(logger, "duplicated node ids: {}", duplicate_node_ids);
	log_info!(logger, "latest seen timestamp: {:?}", serialization_details.latest_seen);

	SerializedResponse {
		data: prefixed_output,
		message_count,
		announcement_count,
		update_count,
		update_count_full,
		update_count_incremental,
	}
}
