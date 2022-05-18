use lightning::ln::msgs::OptionalField;
use lightning::util::ser::Writeable;
use tokio::sync::mpsc;
use tokio_postgres::NoTls;

use crate::{config, hex_utils};
use crate::types::{DetectedGossipMessage, GossipMessage};

pub(crate) struct GossipPersister {
	pub(crate) gossip_persistence_sender: mpsc::Sender<DetectedGossipMessage>,
	gossip_persistence_receiver: mpsc::Receiver<DetectedGossipMessage>,
}

impl GossipPersister {
	pub fn new() -> Self {
		let (gossip_persistence_sender, gossip_persistence_receiver) =
			mpsc::channel::<DetectedGossipMessage>(10000);
		GossipPersister {
			gossip_persistence_sender,
			gossip_persistence_receiver,
		}
	}

	pub(crate) async fn persist_gossip(&mut self) {
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
			let initialization = client
				.execute(config::db_channel_table_creation_query().as_str(), &[])
				.await;
			if let Err(initialization_error) = initialization {
				eprintln!("db init error: {}", initialization_error);
			}

			let initialization = client
				.execute(
					config::db_channel_update_table_creation_query().as_str(),
					&[],
				)
				.await;
			if let Err(initialization_error) = initialization {
				eprintln!("db init error: {}", initialization_error);
			}
		}

		let mut i = 0u32;
		// TODO: it would be nice to have some sort of timeout here so after 10 seconds of
		// inactivity, some sort of message could be broadcast signaling the activation of request
		// processing
		while let Some(detected_gossip_message) = &self.gossip_persistence_receiver.recv().await {
			i += 1;

			if i % 100 == 1 {
				println!("Persisting gossip message #{}", i);
			}

			let timestamp_seen = detected_gossip_message.timestamp_seen;
			match &detected_gossip_message.message {
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
					announcement
						.contents
						.write(&mut announcement_unsigned)
						.unwrap();
					let announcement_hex = hex_utils::hex_str(&announcement_signed);
					let announcement_hex_unsigned = hex_utils::hex_str(&announcement_unsigned);

					let result = client
						.execute("INSERT INTO channels (\
							short_channel_id, \
							block_height, \
							chain_hash, \
							announcement_signed, \
							announcement_unsigned, \
							seen \
						) VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (short_channel_id) DO NOTHING", &[
							&scid_hex,
							&block_height,
							&chain_hash_hex,
							&announcement_hex,
							&announcement_hex_unsigned,
							&timestamp_seen,
						]).await;
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
							blob_unsigned, \
							seen \
						) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)  ON CONFLICT (composite_index) DO NOTHING", &[
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
							&timestamp_seen
						]).await;
					if result.is_err() {
						panic!("error: {}", result.err().unwrap());
					}
				}
			}
		}
	}
}
