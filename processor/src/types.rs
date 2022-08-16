use std::sync::Arc;
use lightning::chain;
use lightning::ln::msgs::{ChannelAnnouncement, ChannelUpdate};
use lightning::util::logger::{Logger, Record};


pub(crate) type GossipChainAccess = Arc<dyn chain::Access + Send + Sync>;

pub(crate) enum GossipMessage {
    ChannelAnnouncement(ChannelAnnouncement),
    ChannelUpdate(ChannelUpdate),
	InitialSyncComplete
}

pub(crate) struct DetectedGossipMessage {
	pub(crate) timestamp_seen: u32,
	pub(crate) message: GossipMessage
}

pub(crate) struct TestLogger{}
impl TestLogger {
	pub(crate) fn new() -> TestLogger {
		Self {}
	}
}
impl Logger for TestLogger {
	fn log(&self, _record: &Record) {
		// println!("{:<5} [{} : {}, {}] {}", record.level.to_string(), record.module_path, record.file, record.line, record.args);
	}
}
