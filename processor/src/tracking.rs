use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bitcoin::secp256k1::SecretKey;
use lightning;
use lightning::ln::peer_handler::{
	ErroringMessageHandler, IgnoringMessageHandler, MessageHandler, PeerManager,
};
use lightning::routing::gossip::{NetworkGraph, P2PGossipSync};
use rand::{Rng, thread_rng};
use tokio::sync::mpsc;

use crate::{config, TestLogger};
use crate::downloader::{GossipCounter, GossipRouter};
use crate::types::{DetectedGossipMessage, GossipChainAccess, GossipMessage};

pub(crate) async fn download_gossip(persistence_sender: mpsc::Sender<DetectedGossipMessage>, network_graph: Arc<NetworkGraph<Arc<TestLogger>>>) {
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

	let logger = TestLogger::new();
	let arc_logger = Arc::new(logger);

	let router = P2PGossipSync::new(
		network_graph.clone(),
		arc_chain_access,
		Arc::clone(&arc_logger),
	);
	let arc_router = Arc::new(router);
	let wrapped_router = GossipRouter {
		native_router: arc_router,
		counter: RwLock::new(GossipCounter::new()),
		sender: persistence_sender.clone(),
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
		Arc::clone(&arc_logger),
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

	tokio::spawn(async move {
		let mut previous_announcement_count = 0u64;
		let mut previous_update_count = 0u64;
		let mut is_caught_up_with_gossip = false;

		let mut i = 0u32;
		let mut latest_new_gossip_time = Instant::now();
		let mut needs_to_notify_persister = false;

		loop {
			// println!("Reached point A");
			i += 1; // count the background activity
			let sleep = tokio::time::sleep(Duration::from_secs(5));
			sleep.await;
			// println!("Reached point B");

			let current_timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
			let router_clone = Arc::clone(&arc_wrapped_router);

			{
				let counter = router_clone.counter.read().unwrap();
				let total_message_count = counter.channel_announcements + counter.channel_updates;
				let new_message_count = total_message_count - previous_announcement_count - previous_update_count;

				let was_previously_caught_up_with_gossip = is_caught_up_with_gossip;
				// TODO: when connected to multiple peers, the message count never seems to stabilize
				is_caught_up_with_gossip = counter.channel_announcements == previous_announcement_count && counter.channel_updates == previous_update_count && previous_announcement_count > 0 && previous_update_count > 0;
				// is_caught_up_with_gossip = total_message_count > 0 && new_message_count < 150;
				// is_caught_up_with_gossip = total_message_count > 10000;
				if new_message_count > 0 {
					latest_new_gossip_time = Instant::now();
				}

				// println!("Reached point C");

				// if we either aren't caught up, or just stopped/started being caught up
				if !is_caught_up_with_gossip || (is_caught_up_with_gossip != was_previously_caught_up_with_gossip) {
					println!(
						"gossip count (iteration {}): {} (delta: {}):\n\tannouncements: {}\n\tupdates: {}\n\t\tno HTLC max: {}\n",
						i,
						total_message_count,
						new_message_count,
						counter.channel_announcements,
						counter.channel_updates,
						counter.channel_updates_without_htlc_max_msats
					);
				} else {
					println!("Monitoring for gossip…")
				}

				// println!("Reached point D");

				if is_caught_up_with_gossip && !was_previously_caught_up_with_gossip {
					println!("caught up with gossip!");
					needs_to_notify_persister = true;

				} else if !is_caught_up_with_gossip && was_previously_caught_up_with_gossip {
					println!("Received new messages since catching up with gossip!");
				}

				// println!("Reached point E");

				let continuous_caught_up_duration = latest_new_gossip_time.elapsed();
				if continuous_caught_up_duration.as_secs() > 600 {
					eprintln!("No new gossip messages in 10 minutes! Something's amiss!");
				}

				// println!("Reached point F");

				previous_announcement_count = counter.channel_announcements;
				previous_update_count = counter.channel_updates;
			}

			if needs_to_notify_persister {
				needs_to_notify_persister = false;
				let sender = persistence_sender.clone();
				tokio::spawn(async move {
					let _ = sender.send(DetectedGossipMessage {
						timestamp_seen: current_timestamp as u32,
						message: GossipMessage::InitialSyncComplete,
					}).await;
				});
			}
		}
	});
}
