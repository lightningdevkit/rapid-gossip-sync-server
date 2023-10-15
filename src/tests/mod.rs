//! Multi-module tests that use database fixtures

use std::cell::RefCell;
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
use crate::{config, serialize_delta};
use crate::persistence::GossipPersister;
use crate::types::{GossipMessage, tests::TestLogger};

const CLIENT_BACKDATE_INTERVAL: u32 = 3600 * 24 * 7; // client backdates RGS by a week

thread_local! {
	static DB_TEST_SCHEMA: RefCell<Option<String>> = RefCell::new(None);
	static IS_TEST_SCHEMA_CLEAN: RefCell<Option<bool>> = RefCell::new(None);
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
		let mut suffix_option = suffix_reference.borrow();
		suffix_option.as_ref().unwrap().clone()
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

struct SchemaSanitizer {}

impl SchemaSanitizer {
	fn new() -> Self {
		IS_TEST_SCHEMA_CLEAN.with(|cleanliness_reference| {
			let mut is_clean_option = cleanliness_reference.borrow_mut();
			assert!(is_clean_option.is_none());
			*is_clean_option = Some(false);
		});

		DB_TEST_SCHEMA.with(|suffix_reference| {
			let mut suffix_option = suffix_reference.borrow_mut();
			let current_time = SystemTime::now();
			let unix_time = current_time.duration_since(UNIX_EPOCH).expect("Time went backwards");
			let timestamp_seconds = unix_time.as_secs();
			let timestamp_nanos = unix_time.as_nanos();
			let preimage = format!("{}", timestamp_nanos);
			let suffix = Sha256dHash::hash(preimage.as_bytes()).into_inner().to_hex();
			// the schema must start with a letter
			let schema = format!("test_{}_{}", timestamp_seconds, suffix);
			*suffix_option = Some(schema);
		});

		return Self {};
	}
}

impl Drop for SchemaSanitizer {
	fn drop(&mut self) {
		IS_TEST_SCHEMA_CLEAN.with(|cleanliness_reference| {
			let is_clean_option = cleanliness_reference.borrow();
			if let Some(is_clean) = *is_clean_option {
				assert_eq!(is_clean, true);
			}
		});
	}
}


async fn clean_test_db() {
	let client = crate::connect_to_db().await;
	let schema = db_test_schema();
	client.execute(&format!("DROP SCHEMA IF EXISTS {} CASCADE", schema), &[]).await.unwrap();
	IS_TEST_SCHEMA_CLEAN.with(|cleanliness_reference| {
		let mut is_clean_option = cleanliness_reference.borrow_mut();
		*is_clean_option = Some(true);
	});
}

#[tokio::test]
async fn test_trivial_setup() {
	let _sanitizer = SchemaSanitizer::new();
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
