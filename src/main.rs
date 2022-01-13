use std::convert::Infallible;
use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use std::ops::Deref;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use bitcoin::blockdata::constants::genesis_block;
use bitcoin::Network;
use bitcoin::secp256k1::{PublicKey, SecretKey};
use lightning;
use lightning::chain;
use lightning::chain::Access;
use lightning::ln::msgs::{
    ChannelAnnouncement, ChannelUpdate, Init, LightningError, NodeAnnouncement, OptionalField,
    QueryChannelRange, QueryShortChannelIds, ReplyChannelRange, ReplyShortChannelIdsEnd,
    RoutingMessageHandler,
};
use lightning::ln::peer_handler::{
    ErroringMessageHandler, IgnoringMessageHandler, MessageHandler, PeerManager,
    SimpleArcPeerManager,
};
use lightning::ln::wire::Type;
use lightning::routing::network_graph::{NetGraphMsgHandler, NetworkGraph};
use lightning::util::events::{MessageSendEvent, MessageSendEventsProvider};
use lightning::util::logger::Level;
use lightning::util::ser::Writeable;
use lightning::util::test_utils::TestLogger;
use lightning_net_tokio::SocketDescriptor;
// use rand::{RngCore, thread_rng};
use rand::{Rng, thread_rng};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_postgres::{Error, NoTls};
use warp::Filter;
use warp::http::{HeaderMap, HeaderValue};
use warp::reply::Response;

use crate::router::{GossipCounter, GossipRouter};
use crate::sample::hex_utils;
use crate::types::{GossipChainAccess, GossipMessage};

mod compression;
mod router;
mod sample;
mod types;
mod download;
mod server;

#[tokio::main]
async fn main() {
    // download::download_gossip().await;
    server::serve_gossip().await;
}
