//! Multi-module tests that use database fixtures

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use bitcoin::{BlockHash, Network};
use bitcoin::secp256k1::ecdsa::Signature;
use bitcoin::secp256k1::{Secp256k1, SecretKey};
use bitcoin::hashes::Hash;
use bitcoin::hashes::sha256d::Hash as Sha256dHash;
use lightning::ln::features::ChannelFeatures;
use lightning::ln::msgs::{ChannelAnnouncement, ChannelUpdate, UnsignedChannelAnnouncement, UnsignedChannelUpdate};
use lightning::routing::gossip::{NetworkGraph, NodeId};
use lightning::util::ser::Writeable;
use lightning_rapid_gossip_sync::RapidGossipSync;
use tokio_postgres::NoTls;
use crate::{config, serialize_delta};
use crate::persistence::GossipPersister;
use crate::types::{GossipMessage, tests::TestLogger};

const CLIENT_BACKDATE_INTERVAL: u32 = 3600 * 24 * 7; // client backdates RGS by a week

fn blank_signature() -> Signature {
	Signature::from_compact(&[0u8; 64]).unwrap()
}

fn genesis_hash() -> BlockHash {
	bitcoin::blockdata::constants::genesis_block(Network::Bitcoin).block_hash()
}

fn current_time() -> u32 {
	SystemTime::now().duration_since(UNIX_EPOCH).expect("Time must be > 1970").as_secs() as u32
}

fn generate_announcement(short_channel_id: u64) -> ChannelAnnouncement {
	let secp_context = Secp256k1::new();

	let random_private_key_1 = SecretKey::from_slice(&[1; 32]).unwrap();
	let random_public_key_1 = random_private_key_1.public_key(&secp_context);
	let node_id_1 = NodeId::from_pubkey(&random_public_key_1);

	let random_private_key_2 = SecretKey::from_slice(&[2; 32]).unwrap();
	let random_public_key_2 = random_private_key_2.public_key(&secp_context);
	let node_id_2 = NodeId::from_pubkey(&random_public_key_2);

	let announcement = UnsignedChannelAnnouncement {
		features: ChannelFeatures::empty(),
		chain_hash: genesis_hash(),
		short_channel_id,
		node_id_1,
		node_id_2,
		bitcoin_key_1: node_id_1,
		bitcoin_key_2: node_id_2,
		excess_data: vec![],
	};

	let msg_hash = bitcoin::secp256k1::Message::from_slice(&Sha256dHash::hash(&announcement.encode()[..])[..]).unwrap();
	let node_signature_1 = secp_context.sign_ecdsa(&msg_hash, &random_private_key_1);
	let node_signature_2 = secp_context.sign_ecdsa(&msg_hash, &random_private_key_2);

	ChannelAnnouncement {
		node_signature_1,
		node_signature_2,
		bitcoin_signature_1: node_signature_1,
		bitcoin_signature_2: node_signature_2,
		contents: announcement,
	}
}

fn generate_update(scid: u64, direction: bool, timestamp: u32, expiry_delta: u16, min_msat: u64, max_msat: u64, base_msat: u32, fee_rate: u32) -> ChannelUpdate {
	let flag_mask = if direction { 1 } else { 0 };
	ChannelUpdate {
		signature: blank_signature(),
		contents: UnsignedChannelUpdate {
			chain_hash: genesis_hash(),
			short_channel_id: scid,
			timestamp,
			flags: 0 | flag_mask,
			cltv_expiry_delta: expiry_delta,
			htlc_minimum_msat: min_msat,
			htlc_maximum_msat: max_msat,
			fee_base_msat: base_msat,
			fee_proportional_millionths: fee_rate,
			excess_data: vec![],
		},
	}
}

async fn clean_test_db() {
	let connection_config = config::db_connection_config();
	let (client, connection) = connection_config.connect(NoTls).await.unwrap();

	tokio::spawn(async move {
		if let Err(e) = connection.await {
			panic!("connection error: {}", e);
		}
	});

	client.query("TRUNCATE TABLE channel_announcements RESTART IDENTITY CASCADE", &[]).await.unwrap();
	client.query("TRUNCATE TABLE channel_updates RESTART IDENTITY CASCADE", &[]).await.unwrap();
	client.query("TRUNCATE TABLE config RESTART IDENTITY CASCADE", &[]).await.unwrap();
}

#[tokio::test]
async fn test_trivial_setup() {
	// start off with a clean slate
	clean_test_db().await;

	let logger = Arc::new(TestLogger::new());
	let network_graph = NetworkGraph::new(Network::Bitcoin, logger.clone());
	let network_graph_arc = Arc::new(network_graph);
	let (mut persister, receiver) = GossipPersister::new(network_graph_arc.clone(), logger.clone());

	let short_channel_id = 1;
	let timestamp = current_time() - 10;
	println!("timestamp: {}", timestamp);

	{ // seed the db
		let announcement = generate_announcement(short_channel_id);
		let update_1 = generate_update(short_channel_id, false, timestamp, 0, 0, 0, 5, 0);
		let update_2 = generate_update(short_channel_id, true, timestamp, 0, 0, 0, 10, 0);

		network_graph_arc.update_channel_from_announcement_no_lookup(&announcement).unwrap();
		network_graph_arc.update_channel_unsigned(&update_1.contents).unwrap();
		network_graph_arc.update_channel_unsigned(&update_2.contents).unwrap();

		receiver.send(GossipMessage::ChannelAnnouncement(announcement)).await.unwrap();
		receiver.send(GossipMessage::ChannelUpdate(update_1)).await.unwrap();
		receiver.send(GossipMessage::ChannelUpdate(update_2)).await.unwrap();
		drop(receiver);
		persister.persist_gossip().await;
	}

	let serialization = serialize_delta(network_graph_arc.clone(), 0, logger.clone()).await;
	logger.assert_log_contains("rapid_gossip_sync_server", "announcement channel count: 1", 1);
	clean_test_db().await;

	let channel_count = network_graph_arc.read_only().channels().len();

	assert_eq!(channel_count, 1);
	assert_eq!(serialization.message_count, 3);
	assert_eq!(serialization.announcement_count, 1);
	assert_eq!(serialization.update_count, 2);

	let client_graph = NetworkGraph::new(Network::Bitcoin, logger.clone());
	let client_graph_arc = Arc::new(client_graph);
	let rgs = RapidGossipSync::new(client_graph_arc.clone(), logger.clone());
	let update_result = rgs.update_network_graph(&serialization.data).unwrap();
	println!("update result: {}", update_result);
	// the update result must be a multiple of our snapshot granularity
	assert_eq!(update_result % config::SNAPSHOT_CALCULATION_INTERVAL, 0);
	assert!(update_result < timestamp);

	let timestamp_delta = timestamp - update_result;
	println!("timestamp delta: {}", timestamp_delta);
	assert!(timestamp_delta < config::SNAPSHOT_CALCULATION_INTERVAL);

	let readonly_graph = client_graph_arc.read_only();
	let channels = readonly_graph.channels();
	let client_channel_count = channels.len();
	assert_eq!(client_channel_count, 1);

	let first_channel = channels.get(&short_channel_id).unwrap();
	assert!(&first_channel.announcement_message.is_none());
	assert_eq!(first_channel.one_to_two.as_ref().unwrap().fees.base_msat, 5);
	assert_eq!(first_channel.two_to_one.as_ref().unwrap().fees.base_msat, 10);
	let last_update_seen_a = first_channel.one_to_two.as_ref().unwrap().last_update;
	let last_update_seen_b = first_channel.two_to_one.as_ref().unwrap().last_update;
	println!("last update a: {}", last_update_seen_a);
	println!("last update b: {}", last_update_seen_b);
	assert_eq!(last_update_seen_a, update_result - CLIENT_BACKDATE_INTERVAL);
	assert_eq!(last_update_seen_b, update_result - CLIENT_BACKDATE_INTERVAL);
}
