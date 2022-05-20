use processor::RapidSyncProcessor;

#[tokio::main]
async fn main() {
    let mut processor = RapidSyncProcessor::new();
	processor.start_sync().await;
}
