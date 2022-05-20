#![deny(unsafe_code)]
#![deny(broken_intra_doc_links)]
#![deny(non_upper_case_globals)]
#![deny(non_camel_case_types)]
#![deny(non_snake_case)]
// #![deny(unused_mut)]
// #![deny(unused_variables)]
#![deny(unused_imports)]

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use bitcoin::blockdata::constants::genesis_block;
use bitcoin::Network;
use bitcoin::secp256k1::PublicKey;
use lightning::routing::network_graph::NetworkGraph;
use lightning::util::ser::{Writeable, Writer};
use tokio::sync::mpsc;
use crate::lookup::DeltaSet;

use crate::persistence::GossipPersister;
use crate::serialization::UpdateSerializationMechanism;
use crate::snapshot::Snapshotter;
use crate::types::GossipChainAccess;

mod router;
mod types;
mod download;
mod lookup;
mod persistence;
mod serialization;
mod snapshot;
mod config;
mod hex_utils;

pub struct RapidSyncProcessor {
	network_graph: Arc<NetworkGraph>,
	pub initial_sync_complete: Arc<AtomicBool>,
}

pub struct SerializedResponse {
	pub uncompressed: Vec<u8>,
	pub compressed: Option<Vec<u8>>,
	pub message_count: u32,
	pub announcement_count: u32,
	pub update_count: u32,
	pub update_count_full: u32,
	pub update_count_incremental: u32,
}

impl RapidSyncProcessor {
	pub fn new() -> Self {
		let network_graph = NetworkGraph::new(genesis_block(Network::Bitcoin).header.block_hash());
		let arc_network_graph = Arc::new(network_graph);
		let (sync_termination_sender, sync_termination_receiver) = mpsc::channel::<()>(1);
		Self {
			network_graph: arc_network_graph,
			initial_sync_complete: Arc::new(AtomicBool::new(false)),
		}
	}

	pub async fn start_sync(&self) {
		// means to indicate sync completion status within this module
		let (sync_completion_sender, mut sync_completion_receiver) = mpsc::channel::<()>(1);
		let initial_sync_complete = self.initial_sync_complete.clone();

		let network_graph = self.network_graph.clone();

		let mut snapshotter = Snapshotter::new(network_graph.clone());
		let mut persister = GossipPersister::new(sync_completion_sender);

		let persistence_sender = persister.gossip_persistence_sender.clone();
		let download_future = download::download_gossip(persistence_sender, network_graph.clone());
		let _download_thread = tokio::spawn(async move {
			// initiate the whole download stuff in the background
			download_future.await;
		});
		let _persistence_thread = tokio::spawn(async move {
			// initiate persistence of the gossip data
			let persistence_future = persister.persist_gossip();
			persistence_future.await;
		});

		tokio::spawn(async move {
			sync_completion_receiver.recv().await;
			initial_sync_complete.store(true, Ordering::Release);
			println!("Initial sync complete!");
		});

		// let mut sync_termination_receiver = self.sync_termination_receiver.borrow_mut();
		// sync_termination_receiver.recv().await;
		// download_thread.abort();
		// persistence_thread.await.unwrap();
	}

	pub async fn serialize_delta(&self, last_sync_timestamp: u32, consider_intermediate_updates: bool, gzip_response: bool) -> SerializedResponse {
		crate::serialize_delta(self.network_graph.clone(), last_sync_timestamp, consider_intermediate_updates, gzip_response).await
	}

	// pub fn stop_sync(&self) {
	// 	self.sync_termination_sender.send(());
	// 	abort();
	// }
}

async fn serialize_delta(network_graph: Arc<NetworkGraph>, last_sync_timestamp: u32, consider_intermediate_updates: bool, gzip_response: bool) -> SerializedResponse {
	let (client, connection) = lookup::connect_to_db().await;

	tokio::spawn(async move {
		if let Err(e) = connection.await {
			panic!("connection error: {}", e);
		}
	});

	let mut output: Vec<u8> = vec![];

	// set a flag if the chain hash is prepended
	// chain hash only necessary if either channel announcements or non-incremental updates are present
	// for announcement-free incremental-only updates, chain hash can be skipped

	let delta_set = DeltaSet::new();

	let mut node_id_set: HashSet<[u8; 33]> = HashSet::new();
	let mut node_id_indices: HashMap<[u8; 33], usize> = HashMap::new();
	let mut node_ids: Vec<PublicKey> = Vec::new();
	let mut duplicate_node_ids = 0;

	let mut get_node_id_index = |node_id: PublicKey| {
		let serialized_node_id = node_id.serialize();
		if node_id_set.insert(serialized_node_id) {
			node_ids.push(node_id);
			let index = node_ids.len() - 1;
			node_id_indices.insert(serialized_node_id, index);
			return index;
		}
		duplicate_node_ids += 1;
		node_id_indices[&serialized_node_id]
	};

	let delta_set = lookup::fetch_channel_announcements(delta_set, network_graph, &client, last_sync_timestamp).await;
	let delta_set = lookup::fetch_channel_updates(delta_set, &client, last_sync_timestamp).await;
	let delta_set = lookup::filter_delta_set(delta_set);
	let serialization_details = serialization::serialize_delta_set(delta_set, last_sync_timestamp, consider_intermediate_updates);

	// process announcements
	// write the number of channel announcements to the output
	let announcement_count = serialization_details.announcements.len() as u32;
	announcement_count.write(&mut output);
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
	update_count.write(&mut output);

	let default_update_values = serialization_details.full_update_defaults;
	if update_count > 0 {
		default_update_values.cltv_expiry_delta.write(&mut output);
		default_update_values.htlc_minimum_msat.write(&mut output);
		default_update_values.fee_base_msat.write(&mut output);
		default_update_values.fee_proportional_millionths.write(&mut output);
		default_update_values.htlc_maximum_msat.write(&mut output);
	}

	let mut update_count_full = 0;
	let mut update_count_incremental = 0;
	for current_update in serialization_details.updates {
		match &current_update.mechanism {
			UpdateSerializationMechanism::Full => {
				update_count_full += 1;
			}
			UpdateSerializationMechanism::Incremental(_) => {
				update_count_incremental += 1;
			}
		};

		let mut stripped_update = serialization::serialize_stripped_channel_update(&current_update, &default_update_values, previous_update_scid);
		output.append(&mut stripped_update);

		previous_update_scid = current_update.update.short_channel_id;
	}

	// some statis
	let message_count = announcement_count + update_count;

	let mut prefixed_output = vec![76, 68, 75, 1];
	// always write the chain hash
	serialization_details.chain_hash.write(&mut prefixed_output);
	// always write the latest seen timestamp
	serialization_details.latest_seen.write(&mut prefixed_output);

	let node_id_count = node_ids.len() as u32;
	node_id_count.write(&mut prefixed_output);

	for current_node_id in node_ids {
		current_node_id.write(&mut prefixed_output);
	}

	prefixed_output.append(&mut output);

	let mut compressed_output = None;
	if gzip_response {
		let mut compressor = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
		compressor.write_all(&prefixed_output);
		let compressed_response = compressor.finish().unwrap();
		compressed_output = Some(compressed_response);
	}

	println!("duplicated node ids: {}", duplicate_node_ids);
	println!("latest seen timestamp: {:?}", serialization_details.latest_seen);

	SerializedResponse {
		uncompressed: prefixed_output,
		compressed: compressed_output,
		message_count,
		announcement_count,
		update_count,
		update_count_full,
		update_count_incremental,
	}
}
