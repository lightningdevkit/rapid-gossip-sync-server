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
	// download::download_gossip().await;
	server::serve_gossip().await;
}
