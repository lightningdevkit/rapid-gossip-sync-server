use crate::server::GossipServer;
use crate::types::GossipChainAccess;

mod compression;
mod router;
mod sample;
mod types;
mod download;
mod server;
mod config;

#[tokio::main]
async fn main() {
	let mut server = GossipServer::new();

	let refresh_sender = server.gossip_refresh_sender.clone();
	let download_future = download::download_gossip(Some(refresh_sender));
	tokio::spawn(async move {
		// initiate the whole download stuff in the background
		download_future.await;
	});

	server.serve_gossip().await;
}
