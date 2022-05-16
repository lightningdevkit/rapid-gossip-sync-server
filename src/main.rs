#![deny(unsafe_code)]
#![deny(broken_intra_doc_links)]
#![deny(non_upper_case_globals)]
#![deny(non_camel_case_types)]
#![deny(non_snake_case)]
// #![deny(unused_mut)]
// #![deny(unused_variables)]
#![deny(unused_imports)]

use crate::server::GossipServer;
use crate::types::GossipChainAccess;

mod compression;
mod router;
// mod sample;
mod types;
mod download;
mod server;
mod config;
mod hex_utils;

#[tokio::main]
async fn main() {
	let mut server = GossipServer::new();

	{
		let refresh_sender = server.gossip_refresh_sender.clone();
		let download_future = download::download_gossip(Some(refresh_sender));
		tokio::spawn(async move {
			// initiate the whole download stuff in the background
			download_future.await;
		});
	}

	server.start_gossip_server().await;
}
