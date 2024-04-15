//! Multi-module tests that use database fixtures

use std::cell::RefCell;
use std::sync::Arc;
use std::{fs, thread};
use std::io::Cursor;
use std::time::{SystemTime, UNIX_EPOCH};
use bitcoin::blockdata::constants::ChainHash;
use bitcoin::Network;
use bitcoin::secp256k1::ecdsa::Signature;
use bitcoin::secp256k1::{Secp256k1, SecretKey};
use bitcoin::hashes::Hash;
use bitcoin::hashes::sha256d::Hash as Sha256dHash;
use hex_conservative::DisplayHex;
use lightning::ln::features::ChannelFeatures;
use lightning::ln::msgs::{ChannelAnnouncement, ChannelUpdate, UnsignedChannelAnnouncement, UnsignedChannelUpdate};
use lightning::routing::gossip::{NetworkGraph, NodeId};
use lightning::util::logger::Level;
use lightning::util::ser::{ReadableArgs, Writeable};
use lightning_rapid_gossip_sync::RapidGossipSync;
use crate::{config, serialize_delta};
use crate::persistence::GossipPersister;
use crate::snapshot::Snapshotter;
use crate::types::{GossipMessage, tests::TestLogger};

const CLIENT_BACKDATE_INTERVAL: u32 = 3600 * 24 * 7; // client backdates RGS by a week

thread_local! {
	static DB_TEST_SCHEMA: RefCell<Option<String>> = RefCell::new(None);
	static IS_TEST_SCHEMA_CLEAN: RefCell<Option<bool>> = RefCell::new(None);
}

fn blank_signature() -> Signature {
	Signature::from_compact(&[0u8; 64]).unwrap()
}

fn genesis_hash() -> ChainHash {
	ChainHash::using_genesis_block(Network::Bitcoin)
}

fn current_time() -> u32 {
	SystemTime::now().duration_since(UNIX_EPOCH).expect("Time must be > 1970").as_secs() as u32
}

pub(crate) fn db_test_schema() -> String {
	DB_TEST_SCHEMA.with(|suffix_reference| {
		let suffix_option = suffix_reference.borrow();
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
			// sometimes Rust thinks two tests start at the same nanosecond, causing a schema conflict
			let thread_id = thread::current().id();
			let preimage = format!("{:?}-{}", thread_id, timestamp_nanos);
			println!("test schema preimage: {}", preimage);
			let suffix = Sha256dHash::hash(preimage.as_bytes()).encode();
			// the schema must start with a letter
			let schema = format!("test_{}_{}", timestamp_seconds, suffix.as_hex());
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

struct CacheSanitizer {}

impl CacheSanitizer {
	/// The CacheSanitizer instantiation requires that there be a schema sanitizer
	fn new(_: &SchemaSanitizer) -> Self {
		Self {}
	}

	fn cache_path(&self) -> String {
		format!("./res/{}/", db_test_schema())
	}
}

impl Drop for CacheSanitizer {
	fn drop(&mut self) {
		let cache_path = self.cache_path();
		fs::remove_dir_all(cache_path).unwrap();
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
async fn test_persistence_runtime() {
	let _sanitizer = SchemaSanitizer::new();
	let logger = Arc::new(TestLogger::new());
	let network_graph = NetworkGraph::new(Network::Bitcoin, logger.clone());
	let network_graph_arc = Arc::new(network_graph);
	let (_persister, _receiver) = GossipPersister::new(network_graph_arc.clone(), logger.clone());

	tokio::task::spawn_blocking(move || {
		drop(_persister);
	}).await.unwrap();

	clean_test_db().await;
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

		receiver.send(GossipMessage::ChannelAnnouncement(announcement, None)).await.unwrap();
		receiver.send(GossipMessage::ChannelUpdate(update_1, None)).await.unwrap();
		receiver.send(GossipMessage::ChannelUpdate(update_2, None)).await.unwrap();
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

	tokio::task::spawn_blocking(move || {
		drop(persister);
	}).await.unwrap();
}

/// If a channel has only seen updates in one direction, it should not be announced
#[tokio::test]
async fn test_unidirectional_intermediate_update_consideration() {
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
		let update_1 = generate_update(short_channel_id, false, timestamp, 0, 0, 0, 6, 0);
		let update_2 = generate_update(short_channel_id, true, timestamp + 1, 0, 0, 0, 3, 0);
		let update_3 = generate_update(short_channel_id, true, timestamp + 2, 0, 0, 0, 4, 0);

		network_graph_arc.update_channel_from_announcement_no_lookup(&announcement).unwrap();
		network_graph_arc.update_channel_unsigned(&update_1.contents).unwrap();
		network_graph_arc.update_channel_unsigned(&update_2.contents).unwrap();
		network_graph_arc.update_channel_unsigned(&update_3.contents).unwrap();

		receiver.send(GossipMessage::ChannelAnnouncement(announcement, Some(timestamp))).await.unwrap();
		receiver.send(GossipMessage::ChannelUpdate(update_1, None)).await.unwrap();
		receiver.send(GossipMessage::ChannelUpdate(update_2, None)).await.unwrap();
		receiver.send(GossipMessage::ChannelUpdate(update_3, None)).await.unwrap();
		drop(receiver);
		persister.persist_gossip().await;
	}

	let channel_count = network_graph_arc.read_only().channels().len();
	assert_eq!(channel_count, 1);

	let client_graph = NetworkGraph::new(Network::Bitcoin, logger.clone());
	let client_graph_arc = Arc::new(client_graph);
	let rgs = RapidGossipSync::new(client_graph_arc.clone(), logger.clone());

	let serialization = serialize_delta(network_graph_arc.clone(), timestamp + 1, logger.clone()).await;

	logger.assert_log_contains("rapid_gossip_sync_server::lookup", "Fetched 1 update rows of the first update in a new direction", 1);
	logger.assert_log_contains("rapid_gossip_sync_server::lookup", "Processed 1 reference rows", 1);
	logger.assert_log_contains("rapid_gossip_sync_server::lookup", "Processed intermediate rows (2)", 1);

	assert_eq!(serialization.message_count, 3);
	assert_eq!(serialization.announcement_count, 1);
	assert_eq!(serialization.update_count, 2);
	assert_eq!(serialization.update_count_full, 2);
	assert_eq!(serialization.update_count_incremental, 0);

	let update_result = rgs.update_network_graph(&serialization.data).unwrap();
	println!("update result: {}", update_result);
	// the update result must be a multiple of our snapshot granularity

	let readonly_graph = client_graph_arc.read_only();
	let channels = readonly_graph.channels();
	let client_channel_count = channels.len();
	assert_eq!(client_channel_count, 1);

	tokio::task::spawn_blocking(move || {
		drop(persister);
	}).await.unwrap();

	clean_test_db().await;
}

/// If a channel has only seen updates in one direction, it should not be announced
#[tokio::test]
async fn test_bidirectional_intermediate_update_consideration() {
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
		let update_2 = generate_update(short_channel_id, false, timestamp + 1, 0, 0, 0, 4, 0);
		let update_3 = generate_update(short_channel_id, false, timestamp + 2, 0, 0, 0, 3, 0);
		let update_4 = generate_update(short_channel_id, true, timestamp, 0, 0, 0, 3, 0);

		network_graph_arc.update_channel_from_announcement_no_lookup(&announcement).unwrap();
		network_graph_arc.update_channel_unsigned(&update_1.contents).unwrap();
		network_graph_arc.update_channel_unsigned(&update_2.contents).unwrap();
		network_graph_arc.update_channel_unsigned(&update_3.contents).unwrap();
		network_graph_arc.update_channel_unsigned(&update_4.contents).unwrap();

		receiver.send(GossipMessage::ChannelAnnouncement(announcement, Some(timestamp))).await.unwrap();
		receiver.send(GossipMessage::ChannelUpdate(update_1, None)).await.unwrap();
		receiver.send(GossipMessage::ChannelUpdate(update_2, None)).await.unwrap();
		receiver.send(GossipMessage::ChannelUpdate(update_3, None)).await.unwrap();
		receiver.send(GossipMessage::ChannelUpdate(update_4, None)).await.unwrap();
		drop(receiver);
		persister.persist_gossip().await;
	}

	let channel_count = network_graph_arc.read_only().channels().len();
	assert_eq!(channel_count, 1);

	let serialization = serialize_delta(network_graph_arc.clone(), timestamp + 1, logger.clone()).await;

	logger.assert_log_contains("rapid_gossip_sync_server::lookup", "Fetched 0 update rows of the first update in a new direction", 1);
	logger.assert_log_contains("rapid_gossip_sync_server::lookup", "Processed 2 reference rows", 1);
	logger.assert_log_contains("rapid_gossip_sync_server::lookup", "Processed intermediate rows (2)", 1);

	assert_eq!(serialization.message_count, 1);
	assert_eq!(serialization.announcement_count, 0);
	assert_eq!(serialization.update_count, 1);
	assert_eq!(serialization.update_count_full, 0);
	assert_eq!(serialization.update_count_incremental, 1);

	tokio::task::spawn_blocking(move || {
		drop(persister);
	}).await.unwrap();

	clean_test_db().await;
}

#[tokio::test]
async fn test_full_snapshot_recency() {
	let _sanitizer = SchemaSanitizer::new();
	let logger = Arc::new(TestLogger::new());
	let network_graph = NetworkGraph::new(Network::Bitcoin, logger.clone());
	let network_graph_arc = Arc::new(network_graph);

	let short_channel_id = 1;
	let timestamp = current_time();
	println!("timestamp: {}", timestamp);

	{ // seed the db
		let (mut persister, receiver) = GossipPersister::new(network_graph_arc.clone(), logger.clone());
		let announcement = generate_announcement(short_channel_id);
		network_graph_arc.update_channel_from_announcement_no_lookup(&announcement).unwrap();
		receiver.send(GossipMessage::ChannelAnnouncement(announcement, None)).await.unwrap();

		{ // direction false
			{ // first update
				let update = generate_update(short_channel_id, false, timestamp - 1, 0, 0, 0, 0, 38);
				network_graph_arc.update_channel_unsigned(&update.contents).unwrap();
				receiver.send(GossipMessage::ChannelUpdate(update, None)).await.unwrap();
			}
			{ // second update
				let update = generate_update(short_channel_id, false, timestamp, 0, 0, 0, 0, 39);
				network_graph_arc.update_channel_unsigned(&update.contents).unwrap();
				receiver.send(GossipMessage::ChannelUpdate(update, None)).await.unwrap();
			}
		}
		{ // direction true
			{ // first and only update
				let update = generate_update(short_channel_id, true, timestamp, 0, 0, 0, 0, 10);
				network_graph_arc.update_channel_unsigned(&update.contents).unwrap();
				receiver.send(GossipMessage::ChannelUpdate(update, None)).await.unwrap();
			}
		}

		drop(receiver);
		persister.persist_gossip().await;

		tokio::task::spawn_blocking(move || {
			drop(persister);
		}).await.unwrap();
	}

	let client_graph = NetworkGraph::new(Network::Bitcoin, logger.clone());
	let client_graph_arc = Arc::new(client_graph);

	{ // sync after initial seed
		let serialization = serialize_delta(network_graph_arc.clone(), 0, logger.clone()).await;
		logger.assert_log_contains("rapid_gossip_sync_server", "announcement channel count: 1", 1);

		let channel_count = network_graph_arc.read_only().channels().len();

		assert_eq!(channel_count, 1);
		assert_eq!(serialization.message_count, 3);
		assert_eq!(serialization.announcement_count, 1);
		assert_eq!(serialization.update_count, 2);

		let rgs = RapidGossipSync::new(client_graph_arc.clone(), logger.clone());
		let update_result = rgs.update_network_graph(&serialization.data).unwrap();
		// the update result must be a multiple of our snapshot granularity
		assert_eq!(update_result % config::snapshot_generation_interval(), 0);
		assert!(update_result < timestamp);

		let readonly_graph = client_graph_arc.read_only();
		let channels = readonly_graph.channels();
		let client_channel_count = channels.len();
		assert_eq!(client_channel_count, 1);

		let first_channel = channels.get(&short_channel_id).unwrap();
		assert!(&first_channel.announcement_message.is_none());
		// ensure the update in one direction shows the latest fee
		assert_eq!(first_channel.one_to_two.as_ref().unwrap().fees.proportional_millionths, 39);
		assert_eq!(first_channel.two_to_one.as_ref().unwrap().fees.proportional_millionths, 10);
	}

	clean_test_db().await;
}

#[tokio::test]
async fn test_full_snapshot_recency_with_wrong_seen_order() {
	let _sanitizer = SchemaSanitizer::new();
	let logger = Arc::new(TestLogger::new());
	let network_graph = NetworkGraph::new(Network::Bitcoin, logger.clone());
	let network_graph_arc = Arc::new(network_graph);

	let short_channel_id = 1;
	let timestamp = current_time();
	println!("timestamp: {}", timestamp);

	{ // seed the db
		let (mut persister, receiver) = GossipPersister::new(network_graph_arc.clone(), logger.clone());
		let announcement = generate_announcement(short_channel_id);
		network_graph_arc.update_channel_from_announcement_no_lookup(&announcement).unwrap();
		receiver.send(GossipMessage::ChannelAnnouncement(announcement, None)).await.unwrap();

		{ // direction false
			{ // first update, seen latest
				let update = generate_update(short_channel_id, false, timestamp - 1, 0, 0, 0, 0, 38);
				network_graph_arc.update_channel_unsigned(&update.contents).unwrap();
				receiver.send(GossipMessage::ChannelUpdate(update, Some(timestamp))).await.unwrap();
			}
			{ // second update, seen first
				let update = generate_update(short_channel_id, false, timestamp, 0, 0, 0, 0, 39);
				network_graph_arc.update_channel_unsigned(&update.contents).unwrap();
				receiver.send(GossipMessage::ChannelUpdate(update, Some(timestamp - 1))).await.unwrap();
			}
		}
		{ // direction true
			{ // first and only update
				let update = generate_update(short_channel_id, true, timestamp, 0, 0, 0, 0, 10);
				network_graph_arc.update_channel_unsigned(&update.contents).unwrap();
				receiver.send(GossipMessage::ChannelUpdate(update, None)).await.unwrap();
			}
		}

		drop(receiver);
		persister.persist_gossip().await;

		tokio::task::spawn_blocking(move || {
			drop(persister);
		}).await.unwrap();
	}

	let client_graph = NetworkGraph::new(Network::Bitcoin, logger.clone());
	let client_graph_arc = Arc::new(client_graph);

	{ // sync after initial seed
		let serialization = serialize_delta(network_graph_arc.clone(), 0, logger.clone()).await;
		logger.assert_log_contains("rapid_gossip_sync_server", "announcement channel count: 1", 1);

		let channel_count = network_graph_arc.read_only().channels().len();

		assert_eq!(channel_count, 1);
		assert_eq!(serialization.message_count, 3);
		assert_eq!(serialization.announcement_count, 1);
		assert_eq!(serialization.update_count, 2);

		let rgs = RapidGossipSync::new(client_graph_arc.clone(), logger.clone());
		let update_result = rgs.update_network_graph(&serialization.data).unwrap();
		// the update result must be a multiple of our snapshot granularity
		assert_eq!(update_result % config::snapshot_generation_interval(), 0);
		assert!(update_result < timestamp);

		let readonly_graph = client_graph_arc.read_only();
		let channels = readonly_graph.channels();
		let client_channel_count = channels.len();
		assert_eq!(client_channel_count, 1);

		let first_channel = channels.get(&short_channel_id).unwrap();
		assert!(&first_channel.announcement_message.is_none());
		// ensure the update in one direction shows the latest fee
		assert_eq!(first_channel.one_to_two.as_ref().unwrap().fees.proportional_millionths, 39);
		assert_eq!(first_channel.two_to_one.as_ref().unwrap().fees.proportional_millionths, 10);
	}

	clean_test_db().await;
}

#[tokio::test]
async fn test_full_snapshot_recency_with_wrong_propagation_order() {
	let _sanitizer = SchemaSanitizer::new();
	let logger = Arc::new(TestLogger::new());
	let network_graph = NetworkGraph::new(Network::Bitcoin, logger.clone());
	let network_graph_arc = Arc::new(network_graph);

	let short_channel_id = 1;
	let timestamp = current_time();
	println!("timestamp: {}", timestamp);

	{ // seed the db
		let (mut persister, receiver) = GossipPersister::new(network_graph_arc.clone(), logger.clone());
		let announcement = generate_announcement(short_channel_id);
		network_graph_arc.update_channel_from_announcement_no_lookup(&announcement).unwrap();
		receiver.send(GossipMessage::ChannelAnnouncement(announcement, None)).await.unwrap();

		{ // direction false
			// apply updates in their timestamp order
			let update_1 = generate_update(short_channel_id, false, timestamp - 1, 0, 0, 0, 0, 38);
			let update_2 = generate_update(short_channel_id, false, timestamp, 0, 0, 0, 0, 39);
			network_graph_arc.update_channel_unsigned(&update_1.contents).unwrap();
			network_graph_arc.update_channel_unsigned(&update_2.contents).unwrap();

			// propagate updates in their seen order
			receiver.send(GossipMessage::ChannelUpdate(update_2, Some(timestamp - 1))).await.unwrap();
			receiver.send(GossipMessage::ChannelUpdate(update_1, Some(timestamp))).await.unwrap();
		}
		{ // direction true
			{ // first and only update
				let update = generate_update(short_channel_id, true, timestamp, 0, 0, 0, 0, 10);
				network_graph_arc.update_channel_unsigned(&update.contents).unwrap();
				receiver.send(GossipMessage::ChannelUpdate(update, None)).await.unwrap();
			}
		}

		drop(receiver);
		persister.persist_gossip().await;

		tokio::task::spawn_blocking(move || {
			drop(persister);
		}).await.unwrap();
	}

	let client_graph = NetworkGraph::new(Network::Bitcoin, logger.clone());
	let client_graph_arc = Arc::new(client_graph);

	{ // sync after initial seed
		let serialization = serialize_delta(network_graph_arc.clone(), 0, logger.clone()).await;
		logger.assert_log_contains("rapid_gossip_sync_server", "announcement channel count: 1", 1);

		let channel_count = network_graph_arc.read_only().channels().len();

		assert_eq!(channel_count, 1);
		assert_eq!(serialization.message_count, 3);
		assert_eq!(serialization.announcement_count, 1);
		assert_eq!(serialization.update_count, 2);

		let rgs = RapidGossipSync::new(client_graph_arc.clone(), logger.clone());
		let update_result = rgs.update_network_graph(&serialization.data).unwrap();
		// the update result must be a multiple of our snapshot granularity
		assert_eq!(update_result % config::snapshot_generation_interval(), 0);
		assert!(update_result < timestamp);

		let readonly_graph = client_graph_arc.read_only();
		let channels = readonly_graph.channels();
		let client_channel_count = channels.len();
		assert_eq!(client_channel_count, 1);

		let first_channel = channels.get(&short_channel_id).unwrap();
		assert!(&first_channel.announcement_message.is_none());
		// ensure the update in one direction shows the latest fee
		assert_eq!(first_channel.one_to_two.as_ref().unwrap().fees.proportional_millionths, 39);
		assert_eq!(first_channel.two_to_one.as_ref().unwrap().fees.proportional_millionths, 10);
	}

	clean_test_db().await;
}

#[tokio::test]
async fn test_full_snapshot_mutiny_scenario() {
	let _sanitizer = SchemaSanitizer::new();
	let logger = Arc::new(TestLogger::new());
	let network_graph = NetworkGraph::new(Network::Bitcoin, logger.clone());
	let network_graph_arc = Arc::new(network_graph);

	let short_channel_id = 873706024403271681;
	let timestamp = current_time();
	// let oldest_simulation_timestamp = 1693300588;
	let latest_simulation_timestamp = 1695909301;
	let timestamp_offset = timestamp - latest_simulation_timestamp;
	println!("timestamp: {}", timestamp);

	{ // seed the db
		let (mut persister, receiver) = GossipPersister::new(network_graph_arc.clone(), logger.clone());
		let announcement = generate_announcement(short_channel_id);
		network_graph_arc.update_channel_from_announcement_no_lookup(&announcement).unwrap();
		receiver.send(GossipMessage::ChannelAnnouncement(announcement, None)).await.unwrap();

		{ // direction false
			{
				let update = generate_update(short_channel_id, false, 1693507369 + timestamp_offset, 0, 0, 0, 0, 38);
				network_graph_arc.update_channel_unsigned(&update.contents).unwrap();
				receiver.send(GossipMessage::ChannelUpdate(update, None)).await.unwrap();
			}
			{
				let update = generate_update(short_channel_id, false, 1693680390 + timestamp_offset, 0, 0, 0, 0, 38);
				network_graph_arc.update_channel_unsigned(&update.contents).unwrap();
				receiver.send(GossipMessage::ChannelUpdate(update, None)).await.unwrap();
			}
			{
				let update = generate_update(short_channel_id, false, 1693749109 + timestamp_offset, 0, 0, 0, 0, 200);
				network_graph_arc.update_channel_unsigned(&update.contents).unwrap();
				receiver.send(GossipMessage::ChannelUpdate(update, None)).await.unwrap();
			}
			{
				let update = generate_update(short_channel_id, false, 1693925190 + timestamp_offset, 0, 0, 0, 0, 200);
				network_graph_arc.update_channel_unsigned(&update.contents).unwrap();
				receiver.send(GossipMessage::ChannelUpdate(update, None)).await.unwrap();
			}
			{
				let update = generate_update(short_channel_id, false, 1694008323 + timestamp_offset, 0, 0, 0, 0, 209);
				network_graph_arc.update_channel_unsigned(&update.contents).unwrap();
				receiver.send(GossipMessage::ChannelUpdate(update, None)).await.unwrap();
			}
			{
				let update = generate_update(short_channel_id, false, 1694219924 + timestamp_offset, 0, 0, 0, 0, 209);
				network_graph_arc.update_channel_unsigned(&update.contents).unwrap();
				receiver.send(GossipMessage::ChannelUpdate(update, None)).await.unwrap();
			}
			{
				let update = generate_update(short_channel_id, false, 1694267536 + timestamp_offset, 0, 0, 0, 0, 210);
				network_graph_arc.update_channel_unsigned(&update.contents).unwrap();
				receiver.send(GossipMessage::ChannelUpdate(update, None)).await.unwrap();
			}
			{
				let update = generate_update(short_channel_id, false, 1694458808 + timestamp_offset, 0, 0, 0, 0, 210);
				network_graph_arc.update_channel_unsigned(&update.contents).unwrap();
				receiver.send(GossipMessage::ChannelUpdate(update, None)).await.unwrap();
			}
			{
				let update = generate_update(short_channel_id, false, 1694526734 + timestamp_offset, 0, 0, 0, 0, 200);
				network_graph_arc.update_channel_unsigned(&update.contents).unwrap();
				receiver.send(GossipMessage::ChannelUpdate(update, None)).await.unwrap();
			}
			{
				let update = generate_update(short_channel_id, false, 1694794765 + timestamp_offset, 0, 0, 0, 0, 200);
				network_graph_arc.update_channel_unsigned(&update.contents).unwrap();
				receiver.send(GossipMessage::ChannelUpdate(update, Some(1695909301 + 2 * config::SYMLINK_GRANULARITY_INTERVAL + timestamp_offset))).await.unwrap();
			}
			{
				let update = generate_update(short_channel_id, false, 1695909301 + timestamp_offset, 0, 0, 0, 0, 130);
				// network_graph_arc.update_channel_unsigned(&update.contents).unwrap();
				receiver.send(GossipMessage::ChannelUpdate(update, None)).await.unwrap();
			}
		}
		{ // direction true
			{
				let update = generate_update(short_channel_id, true, 1693300588 + timestamp_offset, 0, 0, 0, 0, 10);
				network_graph_arc.update_channel_unsigned(&update.contents).unwrap();
				receiver.send(GossipMessage::ChannelUpdate(update, None)).await.unwrap();
			}
			{
				let update = generate_update(short_channel_id, true, 1695003621 + timestamp_offset, 0, 0, 0, 0, 10);
				network_graph_arc.update_channel_unsigned(&update.contents).unwrap();
				receiver.send(GossipMessage::ChannelUpdate(update, None)).await.unwrap();
			}
		}

		drop(receiver);
		persister.persist_gossip().await;

		tokio::task::spawn_blocking(move || {
			drop(persister);
		}).await.unwrap();
	}

	let client_graph = NetworkGraph::new(Network::Bitcoin, logger.clone());
	let client_graph_arc = Arc::new(client_graph);

	{ // sync after initial seed
		let serialization = serialize_delta(network_graph_arc.clone(), 0, logger.clone()).await;
		logger.assert_log_contains("rapid_gossip_sync_server", "announcement channel count: 1", 1);

		let channel_count = network_graph_arc.read_only().channels().len();

		assert_eq!(channel_count, 1);
		assert_eq!(serialization.message_count, 3);
		assert_eq!(serialization.announcement_count, 1);
		assert_eq!(serialization.update_count, 2);

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
		assert_eq!(first_channel.one_to_two.as_ref().unwrap().fees.proportional_millionths, 130);
		assert_eq!(first_channel.two_to_one.as_ref().unwrap().fees.proportional_millionths, 10);
	}

	clean_test_db().await;
}

#[tokio::test]
async fn test_full_snapshot_interlaced_channel_timestamps() {
	let _sanitizer = SchemaSanitizer::new();
	let logger = Arc::new(TestLogger::new());
	let network_graph = NetworkGraph::new(Network::Bitcoin, logger.clone());
	let network_graph_arc = Arc::new(network_graph);

	let main_channel_id = 1;
	let timestamp = current_time();
	println!("timestamp: {}", timestamp);

	{ // seed the db
		let (mut persister, receiver) = GossipPersister::new(network_graph_arc.clone(), logger.clone());
		let secondary_channel_id = main_channel_id + 1;

		{ // main channel
			let announcement = generate_announcement(main_channel_id);
			network_graph_arc.update_channel_from_announcement_no_lookup(&announcement).unwrap();
			receiver.send(GossipMessage::ChannelAnnouncement(announcement, None)).await.unwrap();
		}

		{ // secondary channel
			let announcement = generate_announcement(secondary_channel_id);
			network_graph_arc.update_channel_from_announcement_no_lookup(&announcement).unwrap();
			receiver.send(GossipMessage::ChannelAnnouncement(announcement, None)).await.unwrap();
		}

		{ // main channel
			{ // direction false
				let update = generate_update(main_channel_id, false, timestamp - 2, 0, 0, 0, 0, 10);
				network_graph_arc.update_channel_unsigned(&update.contents).unwrap();
				receiver.send(GossipMessage::ChannelUpdate(update, None)).await.unwrap();
			}
			{ // direction true
				let update = generate_update(main_channel_id, true, timestamp - 2, 0, 0, 0, 0, 5);
				network_graph_arc.update_channel_unsigned(&update.contents).unwrap();
				receiver.send(GossipMessage::ChannelUpdate(update, None)).await.unwrap();
			}
		}

		{ // in-between channel
			{ // direction false
				let update = generate_update(secondary_channel_id, false, timestamp - 1, 0, 0, 0, 0, 42);
				network_graph_arc.update_channel_unsigned(&update.contents).unwrap();
				receiver.send(GossipMessage::ChannelUpdate(update, None)).await.unwrap();
			}
			{ // direction true
				let update = generate_update(secondary_channel_id, true, timestamp - 1, 0, 0, 0, 0, 42);
				network_graph_arc.update_channel_unsigned(&update.contents).unwrap();
				receiver.send(GossipMessage::ChannelUpdate(update, None)).await.unwrap();
			}
		}

		{ // main channel
			{ // direction false
				let update = generate_update(main_channel_id, false, timestamp, 0, 0, 0, 0, 11);
				network_graph_arc.update_channel_unsigned(&update.contents).unwrap();
				receiver.send(GossipMessage::ChannelUpdate(update, None)).await.unwrap();
			}
			{ // direction true
				let update = generate_update(main_channel_id, true, timestamp, 0, 0, 0, 0, 6);
				network_graph_arc.update_channel_unsigned(&update.contents).unwrap();
				receiver.send(GossipMessage::ChannelUpdate(update, None)).await.unwrap();
			}
		}

		drop(receiver);
		persister.persist_gossip().await;

		tokio::task::spawn_blocking(move || {
			drop(persister);
		}).await.unwrap();
	}

	let client_graph = NetworkGraph::new(Network::Bitcoin, logger.clone());
	let client_graph_arc = Arc::new(client_graph);

	{ // sync after initial seed
		let serialization = serialize_delta(network_graph_arc.clone(), 0, logger.clone()).await;
		logger.assert_log_contains("rapid_gossip_sync_server", "announcement channel count: 2", 1);

		let channel_count = network_graph_arc.read_only().channels().len();

		assert_eq!(channel_count, 2);
		assert_eq!(serialization.message_count, 6);
		assert_eq!(serialization.announcement_count, 2);
		assert_eq!(serialization.update_count, 4);

		let rgs = RapidGossipSync::new(client_graph_arc.clone(), logger.clone());
		let update_result = rgs.update_network_graph(&serialization.data).unwrap();
		// the update result must be a multiple of our snapshot granularity
		assert_eq!(update_result % config::snapshot_generation_interval(), 0);
		assert!(update_result < timestamp);

		let readonly_graph = client_graph_arc.read_only();
		let channels = readonly_graph.channels();
		let client_channel_count = channels.len();
		assert_eq!(client_channel_count, 2);

		let first_channel = channels.get(&main_channel_id).unwrap();
		assert!(&first_channel.announcement_message.is_none());
		// ensure the update in one direction shows the latest fee
		assert_eq!(first_channel.one_to_two.as_ref().unwrap().fees.proportional_millionths, 11);
		assert_eq!(first_channel.two_to_one.as_ref().unwrap().fees.proportional_millionths, 6);
	}

	clean_test_db().await;
}

#[tokio::test]
async fn test_full_snapshot_synonym_deserialization() {
	let mut raw_logger = TestLogger::with_id("");
	raw_logger.enable(Level::Trace);
	let logger = Arc::new(raw_logger);

	let scid = 804725963415420929;

	{
		let snapshot_fixture_path = "./res/fixtures/snapshot-next-1697749200-requested-0.bin";
		let fixture_data = fs::read(&snapshot_fixture_path).unwrap();
		let client_graph = NetworkGraph::new(Network::Bitcoin, logger.clone());
		let client_graph_arc = Arc::new(client_graph);

		let rgs = RapidGossipSync::new(client_graph_arc.clone(), logger.clone());
		let update_result = rgs.update_network_graph(&fixture_data).unwrap();
		println!("UPDATE RESULT: {}", update_result);

		// the update result must be a multiple of our snapshot granularity
		assert_eq!(update_result % config::snapshot_generation_interval(), 0);

		let readonly_graph = client_graph_arc.read_only();
		let channels = readonly_graph.channels();
		let client_channel_count = channels.len();
		assert_eq!(client_channel_count, 69455);

		let first_channel = channels.get(&scid).unwrap();
		assert!(&first_channel.announcement_message.is_none());
		// ensure the update in one direction shows the latest fee

		// false direction
		assert_eq!(first_channel.one_to_two.as_ref().unwrap().fees.proportional_millionths, 1000);

		// true direction
		assert_eq!(first_channel.two_to_one.as_ref().unwrap().fees.proportional_millionths, 900);
	}

	{
		let graph_path = "./res/fixtures/synonym_graph.bin";
		let graph_data = fs::read(&graph_path).unwrap();
		let mut cursor = Cursor::new(graph_data);
		let network_graph = NetworkGraph::read(&mut cursor, logger.clone()).unwrap();
		let readonly_graph = network_graph.read_only();
		let channels = readonly_graph.channels();
		let relevant_channel = channels.get(&scid).unwrap();

		// false direction
		assert_eq!(relevant_channel.one_to_two.as_ref().unwrap().fees.proportional_millionths, 1000);

		// true direction
		assert_eq!(relevant_channel.two_to_one.as_ref().unwrap().fees.proportional_millionths, 900);
	}
}

#[tokio::test]
async fn test_full_snapshot_persistence() {
	let schema_sanitizer = SchemaSanitizer::new();
	let logger = Arc::new(TestLogger::new());
	let network_graph = NetworkGraph::new(Network::Bitcoin, logger.clone());
	let network_graph_arc = Arc::new(network_graph);
	let snapshotter = Snapshotter::new(network_graph_arc.clone(), logger.clone());
	let cache_sanitizer = CacheSanitizer::new(&schema_sanitizer);

	let short_channel_id = 1;
	let timestamp = current_time();
	println!("timestamp: {}", timestamp);

	{ // seed the db
		let (mut persister, receiver) = GossipPersister::new(network_graph_arc.clone(), logger.clone());
		let announcement = generate_announcement(short_channel_id);
		network_graph_arc.update_channel_from_announcement_no_lookup(&announcement).unwrap();
		receiver.send(GossipMessage::ChannelAnnouncement(announcement, None)).await.unwrap();

		{ // direction true
			let update = generate_update(short_channel_id, true, timestamp, 0, 0, 0, 0, 10);
			network_graph_arc.update_channel_unsigned(&update.contents).unwrap();
			receiver.send(GossipMessage::ChannelUpdate(update, None)).await.unwrap();
		}

		{ // direction false
			let update = generate_update(short_channel_id, false, timestamp - 1, 0, 0, 0, 0, 38);
			network_graph_arc.update_channel_unsigned(&update.contents).unwrap();
			receiver.send(GossipMessage::ChannelUpdate(update, None)).await.unwrap();
		}


		drop(receiver);
		persister.persist_gossip().await;

		tokio::task::spawn_blocking(move || {
			drop(persister);
		}).await.unwrap();
	}

	let cache_path = cache_sanitizer.cache_path();
	let symlink_path = format!("{}/symlinks/0.bin", cache_path);

	// generate snapshots
	{
		snapshotter.generate_snapshots(20, 5, &[5, u64::MAX], &cache_path, Some(10)).await;

		let symlinked_data = fs::read(&symlink_path).unwrap();
		let client_graph = NetworkGraph::new(Network::Bitcoin, logger.clone());
		let client_graph_arc = Arc::new(client_graph);

		let rgs = RapidGossipSync::new(client_graph_arc.clone(), logger.clone());
		let update_result = rgs.update_network_graph(&symlinked_data).unwrap();
		// the update result must be a multiple of our snapshot granularity
		assert_eq!(update_result % config::snapshot_generation_interval(), 0);

		let readonly_graph = client_graph_arc.read_only();
		let channels = readonly_graph.channels();
		let client_channel_count = channels.len();
		assert_eq!(client_channel_count, 1);

		let first_channel = channels.get(&short_channel_id).unwrap();
		assert!(&first_channel.announcement_message.is_none());
		// ensure the update in one direction shows the latest fee
		assert_eq!(first_channel.one_to_two.as_ref().unwrap().fees.proportional_millionths, 38);
		assert_eq!(first_channel.two_to_one.as_ref().unwrap().fees.proportional_millionths, 10);
	}

	{ // update the db
		let (mut persister, receiver) = GossipPersister::new(network_graph_arc.clone(), logger.clone());

		{ // second update
			let update = generate_update(short_channel_id, false, timestamp + 30, 0, 0, 0, 0, 39);
			network_graph_arc.update_channel_unsigned(&update.contents).unwrap();
			receiver.send(GossipMessage::ChannelUpdate(update, None)).await.unwrap();
		}

		drop(receiver);
		persister.persist_gossip().await;

		tokio::task::spawn_blocking(move || {
			drop(persister);
		}).await.unwrap();
	}

	// regenerate snapshots
	{
		snapshotter.generate_snapshots(20, 5, &[5, u64::MAX], &cache_path, Some(10)).await;

		let symlinked_data = fs::read(&symlink_path).unwrap();
		let client_graph = NetworkGraph::new(Network::Bitcoin, logger.clone());
		let client_graph_arc = Arc::new(client_graph);

		let rgs = RapidGossipSync::new(client_graph_arc.clone(), logger.clone());
		let update_result = rgs.update_network_graph(&symlinked_data).unwrap();
		// the update result must be a multiple of our snapshot granularity
		assert_eq!(update_result % config::snapshot_generation_interval(), 0);

		let readonly_graph = client_graph_arc.read_only();
		let channels = readonly_graph.channels();
		let client_channel_count = channels.len();
		assert_eq!(client_channel_count, 1);

		let first_channel = channels.get(&short_channel_id).unwrap();
		assert!(&first_channel.announcement_message.is_none());
		// ensure the update in one direction shows the latest fee
		assert_eq!(first_channel.one_to_two.as_ref().unwrap().fees.proportional_millionths, 39);
		assert_eq!(first_channel.two_to_one.as_ref().unwrap().fees.proportional_millionths, 10);
	}

	// clean up afterwards
	clean_test_db().await;
}
