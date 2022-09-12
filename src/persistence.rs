use std::fs::OpenOptions;
use std::io::{BufWriter, Write};
use std::sync::Arc;
use std::time::{Duration, Instant};
use lightning::routing::gossip::NetworkGraph;
use lightning::util::ser::Writeable;
use tokio::sync::mpsc;
use tokio_postgres::NoTls;

use crate::{config, hex_utils, TestLogger};
use crate::types::GossipMessage;

pub(crate) struct GossipPersister {
	gossip_persistence_receiver: mpsc::Receiver<GossipMessage>,
	network_graph: Arc<NetworkGraph<TestLogger>>,
}

impl GossipPersister {
	pub fn new(network_graph: Arc<NetworkGraph<TestLogger>>) -> (Self, mpsc::Sender<GossipMessage>) {
		let (gossip_persistence_sender, gossip_persistence_receiver) =
			mpsc::channel::<GossipMessage>(100);
		(GossipPersister {
			gossip_persistence_receiver,
			network_graph
		}, gossip_persistence_sender)
	}

	pub(crate) async fn persist_gossip(&mut self) {
		let connection_config = config::db_connection_config();
		let (mut client, connection) =
			connection_config.connect(NoTls).await.unwrap();

		tokio::spawn(async move {
			if let Err(e) = connection.await {
				panic!("connection error: {}", e);
			}
		});

		{
			// initialize the database
			let initialization = client
				.execute(config::db_config_table_creation_query(), &[])
				.await;
			if let Err(initialization_error) = initialization {
				panic!("db init error: {}", initialization_error);
			}

			let cur_schema = client.query("SELECT db_schema FROM config WHERE id = $1", &[&1]).await.unwrap();
			if !cur_schema.is_empty() {
				config::upgrade_db(cur_schema[0].get(0), &mut client).await;
			}

			let initialization = client
				.execute(
					// TODO: figure out a way to fix the id value without Postgres complaining about
					// its value not being default
					"INSERT INTO config (id, db_schema) VALUES ($1, $2) ON CONFLICT (id) DO NOTHING",
					&[&1, &config::SCHEMA_VERSION]
				).await;
			if let Err(initialization_error) = initialization {
				panic!("db init error: {}", initialization_error);
			}

			let initialization = client
				.execute(config::db_announcement_table_creation_query(), &[])
				.await;
			if let Err(initialization_error) = initialization {
				panic!("db init error: {}", initialization_error);
			}

			let initialization = client
				.execute(
					config::db_channel_update_table_creation_query(),
					&[],
				)
				.await;
			if let Err(initialization_error) = initialization {
				panic!("db init error: {}", initialization_error);
			}

			let initialization = client
				.batch_execute(config::db_index_creation_query())
				.await;
			if let Err(initialization_error) = initialization {
				panic!("db init error: {}", initialization_error);
			}
		}

		// print log statement every minute
		let mut latest_persistence_log = Instant::now() - Duration::from_secs(60);
		let mut i = 0u32;
		let mut latest_graph_cache_time = Instant::now();
		// TODO: it would be nice to have some sort of timeout here so after 10 seconds of
		// inactivity, some sort of message could be broadcast signaling the activation of request
		// processing
		while let Some(gossip_message) = &self.gossip_persistence_receiver.recv().await {
			i += 1; // count the persisted gossip messages

			if latest_persistence_log.elapsed().as_secs() >= 60 {
				println!("Persisting gossip message #{}", i);
				latest_persistence_log = Instant::now();
			}

			// has it been ten minutes? Just cache it
			if latest_graph_cache_time.elapsed().as_secs() >= 600 {
				self.persist_network_graph();
				latest_graph_cache_time = Instant::now();
			}

			match &gossip_message {
				GossipMessage::ChannelAnnouncement(announcement) => {
					let scid = announcement.contents.short_channel_id as i64;
					// scid is 8 bytes
					// block height is the first three bytes
					// to obtain block height, shift scid right by 5 bytes (40 bits)
					let block_height = (scid >> 5 * 8) as i32;

					// start with the type prefix, which is already known a priori
					let mut announcement_signed = Vec::new();
					announcement.write(&mut announcement_signed).unwrap();

					let result = client
						.execute("INSERT INTO channel_announcements (\
							short_channel_id, \
							block_height, \
							announcement_signed \
						) VALUES ($1, $2, $3) ON CONFLICT (short_channel_id) DO NOTHING", &[
							&scid,
							&block_height,
							&announcement_signed
						]).await;
					if result.is_err() {
						panic!("error: {}", result.err().unwrap());
					}
				}
				GossipMessage::ChannelUpdate(update) => {
					let scid = update.contents.short_channel_id as i64;

					let timestamp = update.contents.timestamp as i64;

					let direction = (update.contents.flags & 1) == 1;
					let disable = (update.contents.flags & 2) > 0;

					let composite_index = hex_utils::to_composite_index(scid, timestamp, direction);

					let cltv_expiry_delta = update.contents.cltv_expiry_delta as i32;
					let htlc_minimum_msat = update.contents.htlc_minimum_msat as i64;
					let fee_base_msat = update.contents.fee_base_msat as i32;
					let fee_proportional_millionths =
						update.contents.fee_proportional_millionths as i32;
					let htlc_maximum_msat = update.contents.htlc_maximum_msat as i64;

					// start with the type prefix, which is already known a priori
					let mut update_signed = Vec::new();
					update.write(&mut update_signed).unwrap();

					let result = client
						.execute("INSERT INTO channel_updates (\
							composite_index, \
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
							blob_signed \
						) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)  ON CONFLICT (composite_index) DO NOTHING", &[
							&composite_index,
							&scid,
							&timestamp,
							&(update.contents.flags as i32),
							&direction,
							&disable,
							&cltv_expiry_delta,
							&htlc_minimum_msat,
							&fee_base_msat,
							&fee_proportional_millionths,
							&htlc_maximum_msat,
							&update_signed
						]).await;
					if result.is_err() {
						panic!("error: {}", result.err().unwrap());
					}
				}
			}
		}
	}

	fn persist_network_graph(&self) {
		println!("Caching network graph…");
		let cache_path = config::network_graph_cache_path();
		let file = OpenOptions::new()
			.create(true)
			.write(true)
			.truncate(true)
			.open(&cache_path)
			.unwrap();
		self.network_graph.remove_stale_channels();
		let mut writer = BufWriter::new(file);
		self.network_graph.write(&mut writer).unwrap();
		writer.flush().unwrap();
		println!("Cached network graph!");
	}
}
