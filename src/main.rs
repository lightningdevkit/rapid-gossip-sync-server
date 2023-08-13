use std::sync::Arc;
use rapid_gossip_sync_server::RapidSyncProcessor;
use rapid_gossip_sync_server::types::RGSSLogger;

#[tokio::main]
async fn main() {
	let logger = Arc::new(RGSSLogger::new());
	RapidSyncProcessor::new(logger).start_sync().await;
}
