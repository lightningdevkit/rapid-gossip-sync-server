use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use bitcoin::blockdata::constants::genesis_block;
use bitcoin::Network;
use bitcoin::secp256k1::SecretKey;
use lightning;
use lightning::ln::msgs::OptionalField;
use lightning::ln::peer_handler::{
	ErroringMessageHandler, IgnoringMessageHandler, MessageHandler, PeerManager,
};
use lightning::routing::network_graph::{NetGraphMsgHandler, NetworkGraph};
use lightning::util::logger::Level;
use lightning::util::ser::Writeable;
use lightning::util::test_utils::TestLogger;
use rand::{Rng, thread_rng};
use tokio::sync::mpsc;
use tokio_postgres::NoTls;

use crate::config;
use crate::router::{GossipCounter, GossipRouter};
use crate::hex_utils;
use crate::types::{GossipChainAccess, GossipMessage};

pub(crate) async fn download_gossip(gossip_sender: Option<mpsc::Sender<()>>) {
	let (sender, mut receiver) = mpsc::channel(10000);

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
		sender,
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

	tokio::spawn(async move {
		println!("connecting outbound in background");
		lightning_net_tokio::connect_outbound(
			Arc::clone(&arc_peer_handler),
			peer_pubkey,
			socket_address,
		)
			.await;
		println!("background outbound connected");

		let mut previous_announcement_count = 0u64;
		let mut previous_update_count = 0u64;
		let mut has_caught_up_with_gossip = false;

		let mut i = 0u32;
		loop {
			i += 1;
			println!("keeping background thread alive #{}", i);
			let sleep = tokio::time::sleep(Duration::from_secs(5));
			sleep.await;

			let router_clone = Arc::clone(&arc_wrapped_router);
			let mut needs_to_notify_gossip = false;

			{
				let counter = router_clone.counter.read().unwrap();

				if counter.channel_announcements == previous_announcement_count && counter.channel_updates == previous_update_count && previous_announcement_count > 0 && previous_update_count > 0 {
					if !has_caught_up_with_gossip {
						// upon the first catch-up, we need to refresh the gossip
						needs_to_notify_gossip = true;
						println!("caught up with gossip!");
					}
					has_caught_up_with_gossip = true;
				} else if has_caught_up_with_gossip {
					// some change is afoot, meaning we're no longer caught up
					has_caught_up_with_gossip = false;
					println!("no longer caught up with gossip!");
				}

				if !has_caught_up_with_gossip || needs_to_notify_gossip {
					println!(
						"gossip count: \n\tannouncements: {}\n\tupdates: {}\n",
						counter.channel_announcements, counter.channel_updates
					);
				}

				previous_announcement_count = counter.channel_announcements;
				previous_update_count = counter.channel_updates;
			}

			if needs_to_notify_gossip {
				needs_to_notify_gossip = false;
				if let Some(gossip_notifer) = gossip_sender.clone() {
					gossip_notifer.send(()).await;
				}
			}
		}
	});

	// let peer_handler: GossipPeerManager = lightning::ln::peer_handler::PeerManager::new_routing_only(arc_message_handler, our_node_secret, &random_data, arc_logger.clone());

	let (client, connection) =
		tokio_postgres::connect(config::db_connection_string().as_str(), NoTls)
			.await
			.unwrap();

	tokio::spawn(async move {
		if let Err(e) = connection.await {
			eprintln!("connection error: {}", e);
		}
	});

	{
		// initialize the database
		let initialization = client.execute(config::db_channel_table_creation_query().as_str(), &[]).await;
		if let Err(initialization_error) = initialization {
			eprintln!("db init error: {}", initialization_error);
		}

		let initialization = client.execute(config::db_channel_update_table_creation_query().as_str(), &[]).await;
		if let Err(initialization_error) = initialization {
			eprintln!("db init error: {}", initialization_error);
		}
	}

	let mut i = 0u32;
	while let Some(gossip_message) = receiver.recv().await {
		i += 1;

		match gossip_message {
			GossipMessage::ChannelAnnouncement(announcement) => {
				// println!("got message #{}: announcement", i);

				let scid = announcement.contents.short_channel_id;
				let scid_hex = hex_utils::hex_str(&scid.to_be_bytes());
				// scid is 8 bytes
				// block height is the first three bytes
				// to obtain block height, shift scid right by 5 bytes (40 bits)
				let block_height = (scid >> 5 * 8) as i32;
				let chain_hash = announcement.contents.chain_hash.as_ref();
				let chain_hash_hex = hex_utils::hex_str(chain_hash);

				// start with the type prefix, which is already known a priori
				let mut announcement_signed = Vec::new(); // vec![1, 0];
				let mut announcement_unsigned = Vec::new(); // vec![1, 0];

				// let type_id = announcement.type_id();
				// type_id.write(&mut announcement_signed);
				// type_id.write(&mut announcement_unsigned);

				announcement.write(&mut announcement_signed).unwrap();
				announcement.contents.write(&mut announcement_unsigned).unwrap();
				let announcement_hex = hex_utils::hex_str(&announcement_signed);
				let announcement_hex_unsigned = hex_utils::hex_str(&announcement_unsigned);

				let result = client
					.execute(
						"INSERT INTO channels (\
                    short_channel_id, \
                    block_height, \
                    chain_hash, \
                    announcement_signed, \
                    announcement_unsigned\
                ) VALUES ($1, $2, $3, $4, $5) ON CONFLICT (short_channel_id) DO NOTHING",
						&[
							&scid_hex,
							&block_height,
							&chain_hash_hex,
							&announcement_hex,
							&announcement_hex_unsigned,
						],
					)
					.await;
				if result.is_err() {
					panic!("error: {}", result.err().unwrap());
				}
			}
			GossipMessage::ChannelUpdate(update) => {
				// println!("got message #{}: update", i);

				let scid = update.contents.short_channel_id;
				let scid_hex = hex_utils::hex_str(&scid.to_be_bytes());

				let chain_hash = update.contents.chain_hash.as_ref();
				let chain_hash_hex = hex_utils::hex_str(chain_hash);

				let timestamp = update.contents.timestamp as i64;

				let channel_flags = update.contents.flags as i32;
				let direction = channel_flags & 1;
				let disable = (channel_flags & 2) > 0;

				let composite_index = format!("{}:{}:{}", scid_hex, timestamp, direction);

				let cltv_expiry_delta = update.contents.cltv_expiry_delta as i32;
				let htlc_minimum_msat = update.contents.htlc_minimum_msat as i64;
				let fee_base_msat = update.contents.fee_base_msat as i32;
				let fee_proportional_millionths =
					update.contents.fee_proportional_millionths as i32;
				let htlc_maximum_msat = match update.contents.htlc_maximum_msat {
					OptionalField::Present(maximum) => Some(maximum as i64),
					OptionalField::Absent => None,
				};

				// start with the type prefix, which is already known a priori
				let mut update_signed = Vec::new(); // vec![1, 2];
				let mut update_unsigned = Vec::new(); // vec![1, 2];
				update.write(&mut update_signed).unwrap();
				update.contents.write(&mut update_unsigned).unwrap();
				let update_hex = hex_utils::hex_str(&update_signed);
				let update_hex_unsigned = hex_utils::hex_str(&update_unsigned);

				let result = client
					.execute(
						"INSERT INTO channel_updates (\
                        composite_index, \
                        chain_hash, \
                        short_channel_id, \
                        timestamp, \
                        channel_flags, \
                        direction, \
                        disable, \
                        cltv_expiry_delta, \
                        htlc_minimum_msat, \
                        fee_base_msat, \
                        fee_proportional_millionths, \
                        htlc_maximum_msat, \
                        blob_signed, \
                        blob_unsigned\
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)  ON CONFLICT (composite_index) DO NOTHING",
						&[
							&composite_index,
							&chain_hash_hex,
							&scid_hex,
							&timestamp,
							&channel_flags,
							&direction,
							&disable,
							&cltv_expiry_delta,
							&htlc_minimum_msat,
							&fee_base_msat,
							&fee_proportional_millionths,
							&htlc_maximum_msat,
							&update_hex,
							&update_hex_unsigned,
						],
					)
					.await;
				if result.is_err() {
					panic!("error: {}", result.err().unwrap());
				}
			}
		}
	}
}
