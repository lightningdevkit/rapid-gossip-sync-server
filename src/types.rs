use std::sync::Arc;
use lightning::chain;
use lightning::chain::Access;
use lightning::ln::msgs::{ChannelAnnouncement, ChannelUpdate};
use lightning::ln::peer_handler::{ErroringMessageHandler, IgnoringMessageHandler};
use lightning::routing::network_graph::{NetGraphMsgHandler, NetworkGraph};
use lightning::util::test_utils::TestLogger;
use lightning_net_tokio::SocketDescriptor;

pub(crate) type GossipChainAccess = Arc<dyn chain::Access + Send + Sync>;
pub(crate) type GossipPeerManager = lightning::ln::peer_handler::PeerManager<SocketDescriptor, ErroringMessageHandler, Arc<NetGraphMsgHandler<Arc<NetworkGraph>, Arc<dyn Access + Sync + std::marker::Send>, Arc<TestLogger>>>, Arc<TestLogger>, IgnoringMessageHandler>;

pub(crate) enum GossipMessage {
    ChannelAnnouncement(ChannelAnnouncement),
    ChannelUpdate(ChannelUpdate)
}