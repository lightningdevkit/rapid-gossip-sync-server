use processor::RapidSyncProcessor;

#[tokio::main]
async fn main() {
    let processor = RapidSyncProcessor::new();
	processor.start_sync().await;
}
