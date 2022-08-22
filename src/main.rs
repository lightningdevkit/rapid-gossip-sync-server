use rapid_gossip_sync_server::RapidSyncProcessor;

#[tokio::main]
async fn main() {
	RapidSyncProcessor::new().start_sync().await;
}
