use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use bitcoin::secp256k1::PublicKey;
use lightning::ln::msgs::{ChannelAnnouncement, ChannelUpdate, Init, LightningError, NodeAnnouncement, QueryChannelRange, QueryShortChannelIds, ReplyChannelRange, ReplyShortChannelIdsEnd, RoutingMessageHandler};
use lightning::routing::gossip::{P2PGossipSync, NetworkGraph};
use lightning::util::events::{MessageSendEvent, MessageSendEventsProvider};
use lightning::util::test_utils::TestLogger;
use tokio::sync::mpsc;

use crate::GossipChainAccess;
use crate::types::{DetectedGossipMessage, GossipMessage};

pub(crate) struct GossipCounter {
	pub(crate) channel_announcements: u64,
	pub(crate) channel_updates: u64,
}

impl GossipCounter {
	pub(crate) fn new() -> Self {
		Self {
			channel_announcements: 0,
			channel_updates: 0,
		}
	}
}

pub(crate) struct GossipRouter {
	pub(crate) native_router: Arc<P2PGossipSync<Arc<NetworkGraph<Arc<TestLogger>>>, GossipChainAccess, Arc<TestLogger>>>,
	pub(crate) counter: RwLock<GossipCounter>,
	pub(crate) sender: mpsc::Sender<DetectedGossipMessage>,
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
		let timestamp_seen = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
		let result = self.native_router.handle_channel_announcement(msg);
		if result.is_ok() {
			let mut counter = self.counter.write().unwrap();
			counter.channel_announcements += 1;
			let gossip_message = GossipMessage::ChannelAnnouncement(msg.clone());
			let detected_gossip_message = DetectedGossipMessage {
				message: gossip_message,
				timestamp_seen: timestamp_seen as u32,
			};
			let sender = self.sender.clone();
			tokio::spawn(async move {
				sender.send(detected_gossip_message).await;
			});
		}
		result
	}

	fn handle_channel_update(&self, msg: &ChannelUpdate) -> Result<bool, LightningError> {
		let timestamp_seen = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
		let result = self.native_router.handle_channel_update(msg);
		if result.is_ok() {
			let mut counter = self.counter.write().unwrap();
			counter.channel_updates += 1;
			let gossip_message = GossipMessage::ChannelUpdate(msg.clone());
			let detected_gossip_message = DetectedGossipMessage {
				message: gossip_message,
				timestamp_seen: timestamp_seen as u32,
			};
			let sender = self.sender.clone();
			tokio::spawn(async move {
				sender.send(detected_gossip_message).await;
			});
		}
		result
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
		self.handle_query_short_channel_ids(their_node_id, msg)
	}
}
