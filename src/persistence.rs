use std::fs::OpenOptions;
use std::io::{BufWriter, Write};
use std::ops::Deref;
use std::sync::Arc;
use std::time::{Duration, Instant};
use lightning::log_info;
use lightning::routing::gossip::NetworkGraph;
use lightning::util::logger::Logger;
use lightning::util::ser::Writeable;
use tokio::runtime::Runtime;
use tokio::sync::{mpsc, Mutex, Semaphore};

use crate::config;
use crate::types::GossipMessage;

const POSTGRES_INSERT_TIMEOUT: Duration = Duration::from_secs(15);
const INSERT_PARALELLISM: usize = 16;

pub(crate) struct GossipPersister<L: Deref> where L::Target: Logger {
	gossip_persistence_receiver: mpsc::Receiver<GossipMessage>,
	network_graph: Arc<NetworkGraph<L>>,
	tokio_runtime: Runtime,
	logger: L
}

impl<L: Deref> GossipPersister<L> where L::Target: Logger {
	pub fn new(network_graph: Arc<NetworkGraph<L>>, logger: L) -> (Self, mpsc::Sender<GossipMessage>) {
		let (gossip_persistence_sender, gossip_persistence_receiver) =
			mpsc::channel::<GossipMessage>(100);
		let runtime = Runtime::new().unwrap();
		(GossipPersister {
			gossip_persistence_receiver,
			network_graph,
			tokio_runtime: runtime,
			logger
		}, gossip_persistence_sender)
	}

	pub(crate) async fn persist_gossip(&mut self) {
		{ // initialize the database
			// this client instance is only used once
			let mut client = crate::connect_to_db().await;

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

			let preparation = client.execute("set time zone UTC", &[]).await;
			if let Err(preparation_error) = preparation {
				panic!("db preparation error: {}", preparation_error);
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

			let table_creation_queries = vec![
				config::db_announcement_table_creation_query(),
				config::db_channel_update_table_creation_query(),
				config::db_channel_update_table_creation_query(),
				config::db_node_announcement_table_creation_query()
			];

			for current_table_creation_query in table_creation_queries {
				let initialization = client
					.execute(current_table_creation_query, &[])
					.await;
				if let Err(initialization_error) = initialization {
					panic!("db init error: {}", initialization_error);
				}
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
		let insert_limiter = Arc::new(Semaphore::new(INSERT_PARALELLISM));
		let connections_cache = Arc::new(Mutex::new(Vec::with_capacity(INSERT_PARALELLISM)));
		#[cfg(test)]
		let mut tasks_spawned = Vec::new();
		// TODO: it would be nice to have some sort of timeout here so after 10 seconds of
		// inactivity, some sort of message could be broadcast signaling the activation of request
		// processing
		while let Some(gossip_message) = self.gossip_persistence_receiver.recv().await {
			i += 1; // count the persisted gossip messages

			if latest_persistence_log.elapsed().as_secs() >= 60 {
				log_info!(self.logger, "Persisting gossip message #{}", i);
				latest_persistence_log = Instant::now();
			}

			// has it been ten minutes? Just cache it
			if latest_graph_cache_time.elapsed().as_secs() >= 600 {
				self.persist_network_graph();
				latest_graph_cache_time = Instant::now();
			}
			insert_limiter.acquire().await.unwrap().forget();

			let limiter_ref = Arc::clone(&insert_limiter);
			let client = {
				let mut connections_set = connections_cache.lock().await;
				let client = if connections_set.is_empty() {
					crate::connect_to_db().await
				} else {
					connections_set.pop().unwrap()
				};
				client
			};

			let connections_cache_ref = Arc::clone(&connections_cache);
			match gossip_message {
				GossipMessage::NodeAnnouncement(_announcement, _seen_override) => {

				},
				GossipMessage::ChannelAnnouncement(announcement, seen_override) => {
					let scid = announcement.contents.short_channel_id as i64;

					// start with the type prefix, which is already known a priori
					let mut announcement_signed = Vec::new();
					announcement.write(&mut announcement_signed).unwrap();

					let _task = self.tokio_runtime.spawn(async move {
						if cfg!(test) && seen_override.is_some() {
							tokio::time::timeout(POSTGRES_INSERT_TIMEOUT, client
								.execute("INSERT INTO channel_announcements (\
								short_channel_id, \
								announcement_signed, \
								seen \
							) VALUES ($1, $2, TO_TIMESTAMP($3)) ON CONFLICT (short_channel_id) DO NOTHING", &[
									&scid,
									&announcement_signed,
									&(seen_override.unwrap() as f64)
								])).await.unwrap().unwrap();
						} else {
							tokio::time::timeout(POSTGRES_INSERT_TIMEOUT, client
								.execute("INSERT INTO channel_announcements (\
								short_channel_id, \
								announcement_signed \
							) VALUES ($1, $2) ON CONFLICT (short_channel_id) DO NOTHING", &[
									&scid,
									&announcement_signed
								])).await.unwrap().unwrap();
						}
						let mut connections_set = connections_cache_ref.lock().await;
						connections_set.push(client);
						limiter_ref.add_permits(1);
					});
					#[cfg(test)]
					tasks_spawned.push(_task);
				}
				GossipMessage::ChannelUpdate(update, seen_override) => {
					let scid = update.contents.short_channel_id as i64;

					let timestamp = update.contents.timestamp as i64;

					let direction = (update.contents.flags & 1) == 1;
					let disable = (update.contents.flags & 2) > 0;

					let cltv_expiry_delta = update.contents.cltv_expiry_delta as i32;
					let htlc_minimum_msat = update.contents.htlc_minimum_msat as i64;
					let fee_base_msat = update.contents.fee_base_msat as i32;
					let fee_proportional_millionths =
						update.contents.fee_proportional_millionths as i32;
					let htlc_maximum_msat = update.contents.htlc_maximum_msat as i64;

					// start with the type prefix, which is already known a priori
					let mut update_signed = Vec::new();
					update.write(&mut update_signed).unwrap();

					let insertion_statement = if cfg!(test) {
						"INSERT INTO channel_updates (\
							short_channel_id, \
							timestamp, \
							seen, \
							channel_flags, \
							direction, \
							disable, \
							cltv_expiry_delta, \
							htlc_minimum_msat, \
							fee_base_msat, \
							fee_proportional_millionths, \
							htlc_maximum_msat, \
							blob_signed \
						) VALUES ($1, $2, TO_TIMESTAMP($3), $4, $5, $6, $7, $8, $9, $10, $11, $12)  ON CONFLICT DO NOTHING"
					} else {
						"INSERT INTO channel_updates (\
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
						) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)  ON CONFLICT DO NOTHING"
					};

					// this may not be used outside test cfg
					let _seen_timestamp = seen_override.unwrap_or(timestamp as u32) as f64;

					let _task = self.tokio_runtime.spawn(async move {
						tokio::time::timeout(POSTGRES_INSERT_TIMEOUT, client
							.execute(insertion_statement, &[
								&scid,
								&timestamp,
								#[cfg(test)]
									&_seen_timestamp,
								&(update.contents.flags as i16),
								&direction,
								&disable,
								&cltv_expiry_delta,
								&htlc_minimum_msat,
								&fee_base_msat,
								&fee_proportional_millionths,
								&htlc_maximum_msat,
								&update_signed
							])).await.unwrap().unwrap();
						let mut connections_set = connections_cache_ref.lock().await;
						connections_set.push(client);
						limiter_ref.add_permits(1);
					});
					#[cfg(test)]
					tasks_spawned.push(_task);
				}
			}
		}
		#[cfg(test)]
		for task in tasks_spawned {
			task.await.unwrap();
		}
	}

	fn persist_network_graph(&self) {
		log_info!(self.logger, "Caching network graph…");
		let cache_path = config::network_graph_cache_path();
		let file = OpenOptions::new()
			.create(true)
			.write(true)
			.truncate(true)
			.open(&cache_path)
			.unwrap();
		self.network_graph.remove_stale_channels_and_tracking();
		let mut writer = BufWriter::new(file);
		self.network_graph.write(&mut writer).unwrap();
		writer.flush().unwrap();
		log_info!(self.logger, "Cached network graph!");
	}
}
