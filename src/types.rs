use std::sync::Arc;
use std::ops::Deref;

use lightning::sign::KeysManager;
use lightning::ln::msgs::{ChannelAnnouncement, ChannelUpdate};
use lightning::ln::peer_handler::{ErroringMessageHandler, IgnoringMessageHandler, PeerManager};
use lightning::util::logger::{Logger, Record};

use crate::downloader::GossipRouter;
use crate::verifier::ChainVerifier;

pub(crate) type GossipChainAccess = Arc<ChainVerifier>;
pub(crate) type GossipPeerManager = Arc<PeerManager<lightning_net_tokio::SocketDescriptor, ErroringMessageHandler, Arc<GossipRouter>, IgnoringMessageHandler, TestLogger, IgnoringMessageHandler, Arc<KeysManager>>>;

#[derive(Debug)]
pub(crate) enum GossipMessage {
	ChannelAnnouncement(ChannelAnnouncement),
	ChannelUpdate(ChannelUpdate),
}

#[derive(Clone, Copy)]
pub(crate) struct TestLogger {}
impl Deref for TestLogger {
	type Target = Self;
	fn deref(&self) -> &Self { self }
}

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
