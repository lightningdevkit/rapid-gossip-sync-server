#![allow(unused_variables)]
#![allow(unused_imports)]
//! Multi-module tests that use database fixtures

use std::cell::{RefCell, RefMut};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use bitcoin::{BlockHash, Network};
use bitcoin::secp256k1::ecdsa::Signature;
use bitcoin::secp256k1::{Secp256k1, SecretKey};
use bitcoin::hashes::Hash;
use bitcoin::hashes::hex::ToHex;
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

thread_local! {
	static DB_TEST_SCHEMA:RefCell<Option<String>> = RefCell::new(None);
}

fn blank_signature() -> Signature {
	Signature::from_compact(&[0u8; 64]).unwrap()
}

fn genesis_hash() -> BlockHash {
	bitcoin::blockdata::constants::genesis_block(Network::Bitcoin).block_hash()
}

fn current_time() -> u32 {
	SystemTime::now().duration_since(UNIX_EPOCH).expect("Time must be > 1970").as_secs() as u32
}

pub(crate) fn db_test_schema() -> String {
	DB_TEST_SCHEMA.with(|suffix_reference| {
		let mut suffix_option = suffix_reference.borrow_mut();
		match suffix_option.as_ref() {
			None => {
				let current_time = SystemTime::now();
				let unix_time = current_time.duration_since(UNIX_EPOCH).expect("Time went backwards");
				let timestamp_seconds = unix_time.as_secs();
				let timestamp_nanos = unix_time.as_nanos();
				let preimage = format!("{}", timestamp_nanos);
				let suffix = Sha256dHash::hash(preimage.as_bytes()).into_inner().to_hex();
				// the schema must start with a letter
				let schema = format!("test_{}_{}", timestamp_seconds, suffix);
				suffix_option.replace(schema.clone());
				schema
			}
			Some(suffix) => {
				suffix.clone()
			}
		}
	})
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
	let client = crate::connect_to_db().await;
	let schema = db_test_schema();
	client.execute(&format!("DROP SCHEMA IF EXISTS {} CASCADE", schema), &[]).await.unwrap();
}

#[tokio::test]
async fn test_trivial_setup() {
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

		receiver.send(GossipMessage::ChannelAnnouncement(announcement, Some(timestamp))).await.unwrap();
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
	assert_eq!(update_result % config::snapshot_generation_interval(), 0);
	assert!(update_result < timestamp);

	let timestamp_delta = timestamp - update_result;
	println!("timestamp delta: {}", timestamp_delta);
	assert!(timestamp_delta < config::snapshot_generation_interval());

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

/// If a channel has only seen updates in one direction, it should not be announced
#[tokio::test]
async fn test_unidirectional_intermediate_update_consideration() {
	// start off with a clean slate
	clean_test_db().await;

	let logger = Arc::new(TestLogger::new());
	let network_graph = NetworkGraph::new(Network::Bitcoin, logger.clone());
	let network_graph_arc = Arc::new(network_graph);
	let (mut persister, receiver) = GossipPersister::new(network_graph_arc.clone(), logger.clone());

	let short_channel_id = 1;
	let current_timestamp = current_time() - 10;

	{ // seed the db
		let announcement = generate_announcement(short_channel_id);
		let update_1 = generate_update(short_channel_id, false, current_timestamp - 4, 0, 0, 0, 5, 0);
		let update_2 = generate_update(short_channel_id, false, current_timestamp - 3, 0, 0, 0, 4, 0);
		let update_3 = generate_update(short_channel_id, false, current_timestamp - 2, 0, 0, 0, 3, 0);
		let update_4 = generate_update(short_channel_id, true, current_timestamp, 0, 0, 0, 5, 0);

		network_graph_arc.update_channel_from_announcement_no_lookup(&announcement).unwrap();
		network_graph_arc.update_channel_unsigned(&update_1.contents).unwrap();
		network_graph_arc.update_channel_unsigned(&update_2.contents).unwrap();
		network_graph_arc.update_channel_unsigned(&update_3.contents).unwrap();
		network_graph_arc.update_channel_unsigned(&update_4.contents).unwrap();

		receiver.send(GossipMessage::ChannelAnnouncement(announcement, Some(current_timestamp))).await.unwrap();
		receiver.send(GossipMessage::ChannelUpdate(update_1)).await.unwrap();
		receiver.send(GossipMessage::ChannelUpdate(update_2)).await.unwrap();
		receiver.send(GossipMessage::ChannelUpdate(update_3)).await.unwrap();
		receiver.send(GossipMessage::ChannelUpdate(update_4)).await.unwrap();
		drop(receiver);
		persister.persist_gossip().await;
	}

	let channel_count = network_graph_arc.read_only().channels().len();
	assert_eq!(channel_count, 1);

	let client_graph = NetworkGraph::new(Network::Bitcoin, logger.clone());
	let client_graph_arc = Arc::new(client_graph);
	let rgs = RapidGossipSync::new(client_graph_arc.clone(), logger.clone());

	let last_sync_timestamp = current_timestamp - 3;
	let serialization = serialize_delta(network_graph_arc.clone(), last_sync_timestamp, logger.clone()).await;
	println!(
		"serialization data: \nmessages: {}\n\tannouncements: {}\n\tupdates: {}\n\t\tfull: {}\n\t\tincremental: {}",
		serialization.message_count,
		serialization.announcement_count,
		serialization.update_count,
		serialization.update_count_full,
		serialization.update_count_incremental
	);

	logger.assert_log_contains("rapid_gossip_sync_server::lookup", &format!("Channel IDs: [{}]", short_channel_id), 1);
	logger.assert_log_contains("rapid_gossip_sync_server::lookup", "Fetched 1 announcement rows", 1);
	logger.assert_log_contains("rapid_gossip_sync_server::lookup", "Processed 1 reference rows", 1);
	logger.assert_log_contains("rapid_gossip_sync_server::lookup", &format!("Channel {} last update before seen: 1/false/{}", short_channel_id, current_timestamp - 4), 1);
	logger.assert_log_contains("rapid_gossip_sync_server::lookup", "Processed intermediate rows (3)", 1);
	println!("current_timestamp: {}", current_timestamp);
	logger.assert_log_contains("rapid_gossip_sync_server::lookup", &format!("Channel {} latest update in direction 0: {}", short_channel_id, current_timestamp - 2), 1);
	logger.assert_log_contains("rapid_gossip_sync_server::lookup", &format!("Channel {} latest update in direction 1: {}", short_channel_id, current_timestamp), 1);
	logger.assert_log_contains("rapid_gossip_sync_server::lookup", &format!("Channel {} first update to complete bidirectional data seen at: {}", short_channel_id, current_timestamp), 1);

	assert_eq!(serialization.message_count, 3);
	assert_eq!(serialization.announcement_count, 1);
	assert_eq!(serialization.update_count, 2);
	assert_eq!(serialization.update_count_full, 2);
	assert_eq!(serialization.update_count_incremental, 0);

	let next_timestamp = rgs.update_network_graph(&serialization.data).unwrap();
	println!("last sync timestamp: {}", last_sync_timestamp);
	println!("next timestamp: {}", next_timestamp);
	// the update result must be a multiple of our snapshot granularity
	assert_eq!(next_timestamp % config::snapshot_generation_interval(), 0);

	let readonly_graph = client_graph_arc.read_only();
	let channels = readonly_graph.channels();
	let client_channel_count = channels.len();
	assert_eq!(client_channel_count, 1);
}
