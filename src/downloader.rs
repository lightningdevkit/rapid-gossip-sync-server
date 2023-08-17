use std::ops::Deref;
use std::sync::{Arc, RwLock};

use bitcoin::secp256k1::PublicKey;
use lightning::events::{MessageSendEvent, MessageSendEventsProvider};
use lightning::ln::features::{InitFeatures, NodeFeatures};
use lightning::ln::msgs::{ChannelAnnouncement, ChannelUpdate, Init, LightningError, NodeAnnouncement, QueryChannelRange, QueryShortChannelIds, ReplyChannelRange, ReplyShortChannelIdsEnd, RoutingMessageHandler};
use lightning::routing::gossip::{NetworkGraph, NodeId, P2PGossipSync};
use lightning::util::logger::Logger;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;

use crate::types::{GossipMessage, GossipChainAccess, GossipPeerManager};
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

pub(crate) struct GossipRouter<L: Deref + Clone + Send + Sync + 'static> where L::Target: Logger {
	native_router: P2PGossipSync<Arc<NetworkGraph<L>>, GossipChainAccess<L>, L>,
	pub(crate) counter: RwLock<GossipCounter>,
	sender: mpsc::Sender<GossipMessage>,
	verifier: Arc<ChainVerifier<L>>,
	outbound_gossiper: Arc<P2PGossipSync<Arc<NetworkGraph<L>>, GossipChainAccess<L>, L>>,
}

impl<L: Deref + Clone + Send + Sync> GossipRouter<L> where L::Target: Logger {
	pub(crate) fn new(network_graph: Arc<NetworkGraph<L>>, sender: mpsc::Sender<GossipMessage>, logger: L) -> Self {
		let outbound_gossiper = Arc::new(P2PGossipSync::new(Arc::clone(&network_graph), None, logger.clone()));
		let verifier = Arc::new(ChainVerifier::new(Arc::clone(&network_graph), Arc::clone(&outbound_gossiper), logger.clone()));
		Self {
			native_router: P2PGossipSync::new(network_graph, Some(Arc::clone(&verifier)), logger.clone()),
			outbound_gossiper,
			counter: RwLock::new(GossipCounter::new()),
			sender,
			verifier
		}
	}

	pub(crate) fn set_pm(&self, peer_handler: GossipPeerManager<L>) {
		self.verifier.set_ph(peer_handler);
	}

	fn new_channel_announcement(&self, msg: ChannelAnnouncement) {
		{
			let mut counter = self.counter.write().unwrap();
			counter.channel_announcements += 1;
		}

		let gossip_message = GossipMessage::ChannelAnnouncement(msg, None);
		if let Err(err) = self.sender.try_send(gossip_message) {
			let gossip_message = match err { TrySendError::Full(msg)|TrySendError::Closed(msg) => msg };
			tokio::task::block_in_place(move || { tokio::runtime::Handle::current().block_on(async move {
				self.sender.send(gossip_message).await.unwrap();
			})});
		}
	}

	fn new_channel_update(&self, msg: ChannelUpdate) {
		self.counter.write().unwrap().channel_updates += 1;
		let gossip_message = GossipMessage::ChannelUpdate(msg);

		if let Err(err) = self.sender.try_send(gossip_message) {
			let gossip_message = match err { TrySendError::Full(msg)|TrySendError::Closed(msg) => msg };
			tokio::task::block_in_place(move || { tokio::runtime::Handle::current().block_on(async move {
				self.sender.send(gossip_message).await.unwrap();
			})});
		}
	}
}

impl<L: Deref + Clone + Send + Sync> MessageSendEventsProvider for GossipRouter<L> where L::Target: Logger {
	fn get_and_clear_pending_msg_events(&self) -> Vec<MessageSendEvent> {
		let gossip_evs = self.outbound_gossiper.get_and_clear_pending_msg_events();
		for ev in gossip_evs {
			match ev {
				MessageSendEvent::BroadcastChannelAnnouncement { msg, .. } => {
					self.new_channel_announcement(msg);
				},
				MessageSendEvent::BroadcastNodeAnnouncement { .. } => {},
				MessageSendEvent::BroadcastChannelUpdate { msg } => {
					self.new_channel_update(msg);
				},
				_ => { unreachable!() },
			}
		}
		self.native_router.get_and_clear_pending_msg_events()
	}
}

impl<L: Deref + Clone + Send + Sync> RoutingMessageHandler for GossipRouter<L> where L::Target: Logger {
	fn handle_node_announcement(&self, msg: &NodeAnnouncement) -> Result<bool, LightningError> {
		self.native_router.handle_node_announcement(msg)
	}

	fn handle_channel_announcement(&self, msg: &ChannelAnnouncement) -> Result<bool, LightningError> {
		let res = self.native_router.handle_channel_announcement(msg)?;
		self.new_channel_announcement(msg.clone());
		Ok(res)
	}

	fn handle_channel_update(&self, msg: &ChannelUpdate) -> Result<bool, LightningError> {
		let res = self.native_router.handle_channel_update(msg)?;
		self.new_channel_update(msg.clone());
		Ok(res)
	}

	fn processing_queue_high(&self) -> bool {
		self.native_router.processing_queue_high()
	}

	fn get_next_channel_announcement(&self, starting_point: u64) -> Option<(ChannelAnnouncement, Option<ChannelUpdate>, Option<ChannelUpdate>)> {
		self.native_router.get_next_channel_announcement(starting_point)
	}

	fn get_next_node_announcement(&self, starting_point: Option<&NodeId>) -> Option<NodeAnnouncement> {
		self.native_router.get_next_node_announcement(starting_point)
	}

	fn peer_connected(&self, their_node_id: &PublicKey, init: &Init, inbound: bool) -> Result<(), ()> {
		self.native_router.peer_connected(their_node_id, init, inbound)
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

	fn provided_init_features(&self, their_node_id: &PublicKey) -> InitFeatures {
		self.native_router.provided_init_features(their_node_id)
	}

	fn provided_node_features(&self) -> NodeFeatures {
		self.native_router.provided_node_features()
	}
}
