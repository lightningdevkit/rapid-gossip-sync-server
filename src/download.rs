use std::sync::{Arc, RwLock};

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

use crate::config;
use crate::router::{GossipCounter, GossipRouter};
use crate::types::{DetectedGossipMessage, GossipChainAccess};

pub(crate) async fn download_gossip(persistence_sender: mpsc::Sender<DetectedGossipMessage>, network_graph: Arc<NetworkGraph>) {
	let mut key = [0; 32];
	let mut random_data = [0; 32];
	thread_rng().fill_bytes(&mut key);
	thread_rng().fill_bytes(&mut random_data);
	let our_node_secret = SecretKey::from_slice(&key).unwrap();

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
		network_graph.clone(),
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

	let peers = config::ln_peers();
	for current_peer in peers {
		println!("connecting peer!");
		lightning_net_tokio::connect_outbound(
			Arc::clone(&arc_peer_handler),
			current_peer.0,
			current_peer.1,
		).await;
	}
}
