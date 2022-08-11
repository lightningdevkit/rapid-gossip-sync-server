use std::sync::Arc;
use lightning::chain;
use lightning::chain::Access;
use lightning::ln::msgs::{ChannelAnnouncement, ChannelUpdate};
use lightning::ln::peer_handler::{ErroringMessageHandler, IgnoringMessageHandler};
use lightning::routing::gossip::{P2PGossipSync, NetworkGraph};
use lightning::util::logger::{Logger, Record};
use lightning_net_tokio::SocketDescriptor;


pub(crate) type GossipChainAccess = Arc<dyn chain::Access + Send + Sync>;
pub(crate) type GossipPeerManager = lightning::ln::peer_handler::PeerManager<SocketDescriptor, ErroringMessageHandler, Arc<P2PGossipSync<Arc<NetworkGraph<Arc<TestLogger>>>, Arc<dyn Access + Sync + std::marker::Send>, Arc<TestLogger>>>, Arc<TestLogger>, IgnoringMessageHandler>;

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
