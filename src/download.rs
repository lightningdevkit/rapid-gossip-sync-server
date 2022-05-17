use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

use bitcoin::blockdata::constants::genesis_block;
use bitcoin::Network;
use bitcoin::secp256k1::SecretKey;
use lightning;
use lightning::ln::peer_handler::{
	ErroringMessageHandler, IgnoringMessageHandler, MessageHandler, PeerManager,
};
use lightning::routing::network_graph::{NetGraphMsgHandler, NetworkGraph};
use lightning::util::logger::Level;
use lightning::util::test_utils::TestLogger;
use rand::{Rng, thread_rng};
use tokio::sync::mpsc;

use crate::router::{GossipCounter, GossipRouter};
use crate::hex_utils;
use crate::types::{GossipChainAccess, DetectedGossipMessage};

pub(crate) async fn download_gossip(persistence_sender: mpsc::Sender<DetectedGossipMessage>) {
	let mut key = [0; 32];
	let mut random_data = [0; 32];
	thread_rng().fill_bytes(&mut key);
	thread_rng().fill_bytes(&mut random_data);
	let our_node_secret = SecretKey::from_slice(&key).unwrap();

	let network_graph = NetworkGraph::new(genesis_block(Network::Bitcoin).header.block_hash());
	let arc_network_graph = Arc::new(network_graph);

	let arc_chain_access = None::<GossipChainAccess>;
	let ignorer = IgnoringMessageHandler {};
	let arc_ignorer = Arc::new(ignorer);

	let errorer = ErroringMessageHandler::new();
	let arc_errorer = Arc::new(errorer);

	let mut logger = TestLogger::new();
	// logger.enable(Level::Debug);
	logger.enable(Level::Warn);
	let arc_logger = Arc::new(logger);

	let router = NetGraphMsgHandler::new(
		arc_network_graph.clone(),
		arc_chain_access,
		arc_logger.clone(),
	);
	let arc_router = Arc::new(router);
	let wrapped_router = GossipRouter {
		native_router: arc_router,
		counter: RwLock::new(GossipCounter::new()),
		sender: persistence_sender,
	};
	let arc_wrapped_router = Arc::new(wrapped_router);

	let message_handler = MessageHandler {
		chan_handler: arc_errorer,
		route_handler: arc_wrapped_router.clone(),
	};
	let peer_handler = PeerManager::new(
		message_handler,
		our_node_secret,
		&random_data,
		arc_logger.clone(),
		arc_ignorer,
	);
	let arc_peer_handler = Arc::new(peer_handler);

	let socket_address: SocketAddr = "34.65.85.39:9735".parse().unwrap();
	let peer_pubkey_hex = "033d8656219478701227199cbd6f670335c8d408a92ae88b962c49d4dc0e83e025";
	let peer_pubkey = hex_utils::to_compressed_pubkey(peer_pubkey_hex).unwrap();

	lightning_net_tokio::connect_outbound(
		Arc::clone(&arc_peer_handler),
		peer_pubkey,
		socket_address,
	).await;
}
