use std::sync::{Arc, RwLock};

use bitcoin::secp256k1::PublicKey;
use lightning::ln::msgs::{ChannelAnnouncement, ChannelUpdate, Init, LightningError, NodeAnnouncement, QueryChannelRange, QueryShortChannelIds, ReplyChannelRange, ReplyShortChannelIdsEnd, RoutingMessageHandler};
use lightning::routing::gossip::{NetworkGraph, P2PGossipSync};
use lightning::util::events::{MessageSendEvent, MessageSendEventsProvider};
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;

use crate::TestLogger;
use crate::types::{GossipMessage, GossipChainAccess};
use crate::verifier::ChainVerifier;

pub(crate) struct GossipCounter {
	pub(crate) channel_announcements: u64,
	pub(crate) channel_updates: u64,
	pub(crate) channel_updates_without_htlc_max_msats: u64,
	pub(crate) channel_announcements_with_mismatched_scripts: u64
}

impl GossipCounter {
	pub(crate) fn new() -> Self {
		Self {
			channel_announcements: 0,
			channel_updates: 0,
			channel_updates_without_htlc_max_msats: 0,
			channel_announcements_with_mismatched_scripts: 0,
		}
	}
}

pub(crate) struct GossipRouter {
	native_router: P2PGossipSync<Arc<NetworkGraph<TestLogger>>, GossipChainAccess, TestLogger>,
	pub(crate) counter: RwLock<GossipCounter>,
	sender: mpsc::Sender<GossipMessage>,
}

impl GossipRouter {
	pub(crate) fn new(network_graph: Arc<NetworkGraph<TestLogger>>, sender: mpsc::Sender<GossipMessage>) -> Self {
		Self {
			native_router: P2PGossipSync::new(network_graph, Some(Arc::new(ChainVerifier::new())), TestLogger::new()),
			counter: RwLock::new(GossipCounter::new()),
			sender
		}
	}
}

impl MessageSendEventsProvider for GossipRouter {
	fn get_and_clear_pending_msg_events(&self) -> Vec<MessageSendEvent> {
		self.native_router.get_and_clear_pending_msg_events()
	}
}

impl RoutingMessageHandler for GossipRouter {
	fn handle_node_announcement(&self, msg: &NodeAnnouncement) -> Result<bool, LightningError> {
		self.native_router.handle_node_announcement(msg)
	}

	fn handle_channel_announcement(&self, msg: &ChannelAnnouncement) -> Result<bool, LightningError> {
		let mut counter = self.counter.write().unwrap();

		let output_value = self.native_router.handle_channel_announcement(msg).map_err(|error| {
			if error.err.contains("didn't match on-chain script") {
				counter.channel_announcements_with_mismatched_scripts += 1;
			}
			error
		})?;

		counter.channel_announcements += 1;
		let gossip_message = GossipMessage::ChannelAnnouncement(msg.clone());
		if let Err(err) = self.sender.try_send(gossip_message) {
			let gossip_message = match err { TrySendError::Full(msg)|TrySendError::Closed(msg) => msg };
			tokio::task::block_in_place(move || { tokio::runtime::Handle::current().block_on(async move {
				self.sender.send(gossip_message).await.unwrap();
			})});
		}

		Ok(output_value)
	}

	fn handle_channel_update(&self, msg: &ChannelUpdate) -> Result<bool, LightningError> {
		let output_value = self.native_router.handle_channel_update(msg)?;

		let mut counter = self.counter.write().unwrap();
		counter.channel_updates += 1;
		let gossip_message = GossipMessage::ChannelUpdate(msg.clone());

		if let Err(err) = self.sender.try_send(gossip_message) {
			let gossip_message = match err { TrySendError::Full(msg)|TrySendError::Closed(msg) => msg };
			tokio::task::block_in_place(move || { tokio::runtime::Handle::current().block_on(async move {
				self.sender.send(gossip_message).await.unwrap();
			})});
		}

		Ok(output_value)
	}

	fn get_next_channel_announcements(&self, starting_point: u64, batch_amount: u8) -> Vec<(ChannelAnnouncement, Option<ChannelUpdate>, Option<ChannelUpdate>)> {
		self.native_router.get_next_channel_announcements(starting_point, batch_amount)
	}

	fn get_next_node_announcements(&self, starting_point: Option<&PublicKey>, batch_amount: u8) -> Vec<NodeAnnouncement> {
		self.native_router.get_next_node_announcements(starting_point, batch_amount)
	}

	fn peer_connected(&self, their_node_id: &PublicKey, init: &Init) {
		self.native_router.peer_connected(their_node_id, init)
	}

	fn handle_reply_channel_range(&self, their_node_id: &PublicKey, msg: ReplyChannelRange) -> Result<(), LightningError> {
		self.native_router.handle_reply_channel_range(their_node_id, msg)
	}

	fn handle_reply_short_channel_ids_end(&self, their_node_id: &PublicKey, msg: ReplyShortChannelIdsEnd) -> Result<(), LightningError> {
		self.native_router.handle_reply_short_channel_ids_end(their_node_id, msg)
	}

	fn handle_query_channel_range(&self, their_node_id: &PublicKey, msg: QueryChannelRange) -> Result<(), LightningError> {
		self.native_router.handle_query_channel_range(their_node_id, msg)
	}

	fn handle_query_short_channel_ids(&self, their_node_id: &PublicKey, msg: QueryShortChannelIds) -> Result<(), LightningError> {
		self.native_router.handle_query_short_channel_ids(their_node_id, msg)
	}
}
