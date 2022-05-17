#![deny(unsafe_code)]
#![deny(broken_intra_doc_links)]
#![deny(non_upper_case_globals)]
#![deny(non_camel_case_types)]
#![deny(non_snake_case)]
// #![deny(unused_mut)]
// #![deny(unused_variables)]
#![deny(unused_imports)]

use crate::persistence::GossipPersister;
use crate::server::GossipServer;
use crate::types::GossipChainAccess;

mod compression;
mod router;
mod types;
mod download;
mod persistence;
mod server;
mod config;
mod hex_utils;

#[tokio::main]
async fn main() {
	let mut persister = GossipPersister::new();

	{
		let persistence_sender = persister.gossip_persistence_sender.clone();
		let download_future = download::download_gossip(persistence_sender);
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

	let mut server = GossipServer::new();
	server.start_gossip_server().await;
}
