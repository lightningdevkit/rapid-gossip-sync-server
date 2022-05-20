#![deny(unsafe_code)]
#![deny(broken_intra_doc_links)]
#![deny(non_upper_case_globals)]
#![deny(non_camel_case_types)]
#![deny(non_snake_case)]
// #![deny(unused_mut)]
// #![deny(unused_variables)]
#![deny(unused_imports)]

use std::sync::Arc;
use bitcoin::blockdata::constants::genesis_block;
use bitcoin::Network;
use lightning::routing::network_graph::NetworkGraph;
use crate::persistence::GossipPersister;
use crate::server::GossipServer;
use crate::types::GossipChainAccess;

mod router;
mod types;
mod download;
mod persistence;
mod server;
mod config;
mod hex_utils;

#[tokio::main]
async fn main() {
	let network_graph = NetworkGraph::new(genesis_block(Network::Bitcoin).header.block_hash());
	let arc_network_graph = Arc::new(network_graph);

	let mut server = GossipServer::new(arc_network_graph.clone());
	let mut persister = GossipPersister::new(server.sync_completion_sender.clone());

	{
		let persistence_sender = persister.gossip_persistence_sender.clone();
		let download_future = download::download_gossip(persistence_sender, arc_network_graph.clone());
		tokio::spawn(async move {
			// initiate the whole download stuff in the background
			download_future.await;
		});
		tokio::spawn(async move {
			// initiate persistence of the gossip data
			let persistence_future = persister.persist_gossip();
			persistence_future.await;
		});
	}


	server.start_gossip_server().await;
}
