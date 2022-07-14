use std::fs::OpenOptions;
use std::sync::Arc;
use lightning::ln::msgs::OptionalField;
use lightning::routing::gossip::NetworkGraph;
use lightning::util::ser::Writeable;
use lightning::util::test_utils::TestLogger;
use tokio::sync::mpsc;
use tokio_postgres::NoTls;

use crate::{config, hex_utils};
use crate::types::{DetectedGossipMessage, GossipMessage};

pub(crate) struct GossipPersister {
	pub(crate) gossip_persistence_sender: mpsc::Sender<DetectedGossipMessage>,
	gossip_persistence_receiver: mpsc::Receiver<DetectedGossipMessage>,
	server_sync_completion_sender: mpsc::Sender<()>,
	network_graph: Arc<NetworkGraph<Arc<TestLogger>>>,
}

impl GossipPersister {
	pub fn new(server_sync_completion_sender: mpsc::Sender<()>, network_graph: Arc<NetworkGraph<Arc<TestLogger>>>) -> Self {
		let (gossip_persistence_sender, gossip_persistence_receiver) =
			mpsc::channel::<DetectedGossipMessage>(10000);
		GossipPersister {
			gossip_persistence_sender,
			gossip_persistence_receiver,
			server_sync_completion_sender,
			network_graph
		}
	}

	pub(crate) async fn persist_gossip(&mut self) {
		println!("Reached point K");
		let connection_config = config::db_connection_config();
		let (client, connection) =
			connection_config.connect(NoTls).await.unwrap();

		println!("Reached point L");

		tokio::spawn(async move {
			println!("Reached point M");
			if let Err(e) = connection.await {
				eprintln!("connection error: {}", e);
			}
		});

		println!("Reached point N");

		{
			// initialize the database
			let initialization = client
				.execute(config::db_config_table_creation_query().as_str(), &[])
				.await;
			if let Err(initialization_error) = initialization {
				eprintln!("db init error: {}", initialization_error);
			}

			println!("Reached point O");

			let initialization = client
				.execute(config::db_announcement_table_creation_query().as_str(), &[])
				.await;
			if let Err(initialization_error) = initialization {
				eprintln!("db init error: {}", initialization_error);
			}

			println!("Reached point P");

			let initialization = client
				.execute(
					config::db_channel_update_table_creation_query().as_str(),
					&[],
				)
				.await;
			if let Err(initialization_error) = initialization {
				eprintln!("db init error: {}", initialization_error);
			}

			println!("Reached point Q");

			let initialization = client
				.batch_execute(config::db_index_creation_query().as_str())
				.await;
			if let Err(initialization_error) = initialization {
				eprintln!("db init error: {}", initialization_error);
			}

			println!("Reached point R");
		}

		// print log statement every 10,000 messages
		let mut persistence_log_threshold = 10000;
		let mut i = 0u32;
		let mut server_sync_completion_sent = false;
		// TODO: it would be nice to have some sort of timeout here so after 10 seconds of
		// inactivity, some sort of message could be broadcast signaling the activation of request
		// processing
		println!("Reached point S");
		while let Some(detected_gossip_message) = &self.gossip_persistence_receiver.recv().await {
			println!("Reached point T");
			i += 1; // count the persisted gossip messages

			if i == 1 || i % persistence_log_threshold == 0 {
				println!("Persisting gossip message #{}", i);
			}

			println!("Reached point U");

			let timestamp_seen = detected_gossip_message.timestamp_seen;
			match &detected_gossip_message.message {
				GossipMessage::InitialSyncComplete => {
					// signal to the server that it may now serve dynamic responses and calculate
					// snapshots
					// we take this detour through the persister to ensure that all previous
					// messages have already been persisted to the database
					println!("Persister caught up with gossip!");
					i -= 1; // this wasn't an actual gossip message that needed persisting
					persistence_log_threshold = 50;
					if !server_sync_completion_sent {
						server_sync_completion_sent = true;
						self.server_sync_completion_sender.send(()).await;
						println!("Server has been notified of persistence completion.");
					}

					// now, cache the persisted network graph
					// also persist the network graph here
					println!("Caching network graph…");
					let cache_path = config::network_graph_cache_path();
					let mut file = OpenOptions::new()
						.create(true)
						.write(true)
						.truncate(true)
						.open(&cache_path)
						.unwrap();
					self.network_graph.remove_stale_channels();
					println!("Reached point V");
					self.network_graph.write(&mut file).unwrap();
					println!("Cached network graph!");
				}
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
					announcement.write(&mut announcement_signed).unwrap();
					let announcement_hex = hex_utils::hex_str(&announcement_signed);

					let result = client
						.execute("INSERT INTO channel_announcements (\
							short_channel_id, \
							block_height, \
							chain_hash, \
							announcement_signed, \
							seen \
						) VALUES ($1, $2, $3, $4, $5) ON CONFLICT (short_channel_id) DO NOTHING", &[
							&scid_hex,
							&block_height,
							&chain_hash_hex,
							&announcement_hex,
							&timestamp_seen,
						]).await;
					if result.is_err() {
						panic!("error: {}", result.err().unwrap());
					}
					println!("Reached point W");
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
					update.write(&mut update_signed).unwrap();
					let update_hex = hex_utils::hex_str(&update_signed);

					let result = client
						.execute("INSERT INTO channel_updates (\
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
							seen \
						) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)  ON CONFLICT (composite_index) DO NOTHING", &[
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
							&timestamp_seen
						]).await;
					if result.is_err() {
						panic!("error: {}", result.err().unwrap());
					}
					println!("Reached point X");
				}
			}
			println!("Reached point Y");
		}
	}
}
