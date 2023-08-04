use std::sync::Arc;

use lightning::sign::KeysManager;
use lightning::ln::msgs::{ChannelAnnouncement, ChannelUpdate};
use lightning::ln::peer_handler::{ErroringMessageHandler, IgnoringMessageHandler, PeerManager};
use lightning::util::logger::{Logger, Record};
use crate::config;

use crate::downloader::GossipRouter;
use crate::verifier::ChainVerifier;

pub(crate) type GossipChainAccess<L> = Arc<ChainVerifier<L>>;
pub(crate) type GossipPeerManager<L> = Arc<PeerManager<lightning_net_tokio::SocketDescriptor, ErroringMessageHandler, Arc<GossipRouter<L>>, IgnoringMessageHandler, L, IgnoringMessageHandler, Arc<KeysManager>>>;

#[derive(Debug)]
pub(crate) enum GossipMessage {
	ChannelAnnouncement(ChannelAnnouncement),
	ChannelUpdate(ChannelUpdate),
}

#[derive(Clone, Copy)]
pub struct RGSSLogger {}

impl RGSSLogger {
	pub fn new() -> RGSSLogger {
		Self {}
	}
}

impl Logger for RGSSLogger {
	fn log(&self, record: &Record) {
		let threshold = config::log_level();
		if record.level < threshold {
			return;
		}
		println!("{:<5} [{} : {}, {}] {}", record.level.to_string(), record.module_path, record.file, record.line, record.args);
	}
}
