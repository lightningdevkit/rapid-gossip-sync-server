use std::sync::Arc;
use lightning::ln::msgs::{ChannelAnnouncement, ChannelUpdate};
use lightning::util::logger::{Logger, Record};
use crate::verifier::ChainVerifier;

pub(crate) type GossipChainAccess = Arc<ChainVerifier>;

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
	fn log(&self, record: &Record) {
		// TODO: allow log level threshold to be set
		println!("{:<5} [{} : {}, {}] {}", record.level.to_string(), record.module_path, record.file, record.line, record.args);
	}
}
