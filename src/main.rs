use rapid_gossip_sync_server::RapidSyncProcessor;

#[tokio::main]
async fn main() {
    let processor = RapidSyncProcessor::new();
	processor.start_sync().await;
}
