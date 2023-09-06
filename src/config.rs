use crate::hex_utils;

use std::convert::TryInto;
use std::env;
use std::io::Cursor;
use std::net::{SocketAddr, ToSocketAddrs};
use std::time::Duration;

use bitcoin::Network;
use bitcoin::hashes::hex::FromHex;
use bitcoin::secp256k1::PublicKey;
use futures::stream::{FuturesUnordered, StreamExt};
use lightning::ln::msgs::ChannelAnnouncement;
use lightning::util::ser::Readable;
use lightning_block_sync::http::HttpEndpoint;
use tokio_postgres::Config;

pub(crate) const SCHEMA_VERSION: i32 = 12;
pub(crate) const SYMLINK_GRANULARITY_INTERVAL: u32 = 3600 * 3; // three hours
pub(crate) const MAX_SNAPSHOT_SCOPE: u32 = 3600 * 24 * 21; // three weeks
// generate symlinks based on a 3-hour-granularity
/// If the last update in either direction was more than six days ago, we send a reminder
/// That reminder may be either in the form of a channel announcement, or in the form of empty
/// updates in both directions.
pub(crate) const CHANNEL_REMINDER_AGE: Duration = Duration::from_secs(6 * 24 * 60 * 60);
/// The number of successful peer connections to await prior to continuing to gossip storage.
/// The application will still work if the number of specified peers is lower, as long as there is
/// at least one successful peer connection, but it may result in long startup times.
pub(crate) const CONNECTED_PEER_ASSERTION_LIMIT: usize = 5;
pub(crate) const DOWNLOAD_NEW_GOSSIP: bool = true;

pub(crate) fn snapshot_generation_interval() -> u32 {
	let interval = env::var("RAPID_GOSSIP_SYNC_SERVER_SNAPSHOT_INTERVAL").unwrap_or(SYMLINK_GRANULARITY_INTERVAL.to_string())
		.parse::<u32>()
		.expect("RAPID_GOSSIP_SYNC_SERVER_SNAPSHOT_INTERVAL env variable must be a u32.");
	assert!(interval > 0, "RAPID_GOSSIP_SYNC_SERVER_SNAPSHOT_INTERVAL must be positive");
	assert_eq!(interval % SYMLINK_GRANULARITY_INTERVAL, 0, "RAPID_GOSSIP_SYNC_SERVER_SNAPSHOT_INTERVAL must be a multiple of {} (seconds)", SYMLINK_GRANULARITY_INTERVAL);
	interval
}

pub(crate) fn network() -> Network {
	let network = env::var("RAPID_GOSSIP_SYNC_SERVER_NETWORK").unwrap_or("bitcoin".to_string()).to_lowercase();
	match network.as_str() {
		"mainnet" => Network::Bitcoin,
		"bitcoin" => Network::Bitcoin,
		"testnet" => Network::Testnet,
		"signet" => Network::Signet,
		"regtest" => Network::Regtest,
		_ => panic!("Invalid network"),
	}
}

pub(crate) fn log_level() -> lightning::util::logger::Level {
	let level = env::var("RAPID_GOSSIP_SYNC_SERVER_LOG_LEVEL").unwrap_or("info".to_string()).to_lowercase();
	match level.as_str() {
		"gossip" => lightning::util::logger::Level::Gossip,
		"trace" => lightning::util::logger::Level::Trace,
		"debug" => lightning::util::logger::Level::Debug,
		"info" => lightning::util::logger::Level::Info,
		"warn" => lightning::util::logger::Level::Warn,
		"error" => lightning::util::logger::Level::Error,
		_ => panic!("Invalid log level"),
	}
}

pub(crate) fn network_graph_cache_path() -> String {
	format!("{}/network_graph.bin", cache_path())
}

pub(crate) fn cache_path() -> String {
	let path = env::var("RAPID_GOSSIP_SYNC_SERVER_CACHES_PATH").unwrap_or("./res".to_string()).to_lowercase();
	path
}

pub(crate) fn db_connection_config() -> Config {
	let mut config = Config::new();
	let host = env::var("RAPID_GOSSIP_SYNC_SERVER_DB_HOST").unwrap_or("localhost".to_string());
	let user = env::var("RAPID_GOSSIP_SYNC_SERVER_DB_USER").unwrap_or("alice".to_string());
	let db = env::var("RAPID_GOSSIP_SYNC_SERVER_DB_NAME").unwrap_or("ln_graph_sync".to_string());
	config.host(&host);
	config.user(&user);
	config.dbname(&db);
	if let Ok(password) = env::var("RAPID_GOSSIP_SYNC_SERVER_DB_PASSWORD") {
		config.password(&password);
	}
	config
}

pub(crate) fn bitcoin_rest_endpoint() -> HttpEndpoint {
	let host = env::var("BITCOIN_REST_DOMAIN").unwrap_or("127.0.0.1".to_string());
	let port = env::var("BITCOIN_REST_PORT")
		.unwrap_or("8332".to_string())
		.parse::<u16>()
		.expect("BITCOIN_REST_PORT env variable must be a u16.");
	let path = env::var("BITCOIN_REST_PATH").unwrap_or("/rest/".to_string());
	HttpEndpoint::for_host(host).with_port(port).with_path(path)
}

pub(crate) fn db_config_table_creation_query() -> &'static str {
	"CREATE TABLE IF NOT EXISTS config (
		id SERIAL PRIMARY KEY,
		db_schema integer
	)"
}

pub(crate) fn db_announcement_table_creation_query() -> &'static str {
	"CREATE TABLE IF NOT EXISTS channel_announcements (
		id SERIAL PRIMARY KEY,
		short_channel_id bigint NOT NULL UNIQUE,
		announcement_signed BYTEA,
		seen timestamp NOT NULL DEFAULT NOW()
	)"
}

pub(crate) fn db_channel_update_table_creation_query() -> &'static str {
	"CREATE TABLE IF NOT EXISTS channel_updates (
		id SERIAL PRIMARY KEY,
		short_channel_id bigint NOT NULL,
		timestamp bigint NOT NULL,
		channel_flags smallint NOT NULL,
		direction boolean NOT NULL,
		disable boolean NOT NULL,
		cltv_expiry_delta integer NOT NULL,
		htlc_minimum_msat bigint NOT NULL,
		fee_base_msat integer NOT NULL,
		fee_proportional_millionths integer NOT NULL,
		htlc_maximum_msat bigint NOT NULL,
		blob_signed BYTEA NOT NULL,
		seen timestamp NOT NULL DEFAULT NOW()
	)"
}

pub(crate) fn db_index_creation_query() -> &'static str {
	"
	CREATE INDEX IF NOT EXISTS channel_updates_seen_scid ON channel_updates(seen, short_channel_id);
	CREATE INDEX IF NOT EXISTS channel_updates_scid_dir_seen_asc ON channel_updates(short_channel_id, direction, seen);
	CREATE INDEX IF NOT EXISTS channel_updates_scid_dir_seen_desc_with_id ON channel_updates(short_channel_id ASC, direction ASC, seen DESC) INCLUDE (id);
	CREATE UNIQUE INDEX IF NOT EXISTS channel_updates_key ON channel_updates (short_channel_id, direction, timestamp);
	CREATE INDEX IF NOT EXISTS channel_updates_seen ON channel_updates(seen);
	CREATE INDEX IF NOT EXISTS channel_updates_timestamp_desc ON channel_updates(timestamp DESC);
	"
}

pub(crate) async fn upgrade_db(schema: i32, client: &mut tokio_postgres::Client) {
	if schema == 1 {
		let tx = client.transaction().await.unwrap();
		tx.execute("ALTER TABLE channel_updates DROP COLUMN chain_hash", &[]).await.unwrap();
		tx.execute("ALTER TABLE channel_announcements DROP COLUMN chain_hash", &[]).await.unwrap();
		tx.execute("UPDATE config SET db_schema = 2 WHERE id = 1", &[]).await.unwrap();
		tx.commit().await.unwrap();
	}
	if schema == 1 || schema == 2 {
		let tx = client.transaction().await.unwrap();
		tx.execute("ALTER TABLE channel_updates DROP COLUMN short_channel_id", &[]).await.unwrap();
		tx.execute("ALTER TABLE channel_updates ADD COLUMN short_channel_id bigint DEFAULT null", &[]).await.unwrap();
		tx.execute("ALTER TABLE channel_updates DROP COLUMN direction", &[]).await.unwrap();
		tx.execute("ALTER TABLE channel_updates ADD COLUMN direction boolean DEFAULT null", &[]).await.unwrap();
		loop {
			let rows = tx.query("SELECT id, composite_index FROM channel_updates WHERE short_channel_id IS NULL LIMIT 50000", &[]).await.unwrap();
			if rows.is_empty() { break; }
			let mut updates = FuturesUnordered::new();
			for row in rows {
				let id: i32 = row.get("id");
				let index: String = row.get("composite_index");
				let tx_ref = &tx;
				updates.push(async move {
					let mut index_iter = index.split(":");
					let scid_hex = index_iter.next().unwrap();
					index_iter.next().unwrap();
					let direction_str = index_iter.next().unwrap();
					assert!(direction_str == "1" || direction_str == "0");
					let direction = direction_str == "1";
					let scid_be_bytes = hex_utils::to_vec(scid_hex).unwrap();
					let scid = i64::from_be_bytes(scid_be_bytes.try_into().unwrap());
					assert!(scid > 0); // Will roll over in some 150 years or so
					tx_ref.execute("UPDATE channel_updates SET short_channel_id = $1, direction = $2 WHERE id = $3", &[&scid, &direction, &id]).await.unwrap();
				});
			}
			while let Some(_) = updates.next().await {}
		}
		tx.execute("ALTER TABLE channel_updates ALTER short_channel_id DROP DEFAULT", &[]).await.unwrap();
		tx.execute("ALTER TABLE channel_updates ALTER short_channel_id SET NOT NULL", &[]).await.unwrap();
		tx.execute("ALTER TABLE channel_updates ALTER direction DROP DEFAULT", &[]).await.unwrap();
		tx.execute("ALTER TABLE channel_updates ALTER direction SET NOT NULL", &[]).await.unwrap();
		tx.execute("UPDATE config SET db_schema = 3 WHERE id = 1", &[]).await.unwrap();
		tx.commit().await.unwrap();
	}
	if schema >= 1 && schema <= 3 {
		let tx = client.transaction().await.unwrap();
		tx.execute("ALTER TABLE channel_announcements DROP COLUMN short_channel_id", &[]).await.unwrap();
		tx.execute("ALTER TABLE channel_announcements ADD COLUMN short_channel_id bigint DEFAULT null", &[]).await.unwrap();
		loop {
			let rows = tx.query("SELECT id, announcement_signed FROM channel_announcements WHERE short_channel_id IS NULL LIMIT 10000", &[]).await.unwrap();
			if rows.is_empty() { break; }
			let mut updates = FuturesUnordered::new();
			for row in rows {
				let id: i32 = row.get("id");
				let announcement: Vec<u8> = row.get("announcement_signed");
				let tx_ref = &tx;
				updates.push(async move {
					let scid = ChannelAnnouncement::read(&mut Cursor::new(announcement)).unwrap().contents.short_channel_id as i64;
					assert!(scid > 0); // Will roll over in some 150 years or so
					tx_ref.execute("UPDATE channel_announcements SET short_channel_id = $1 WHERE id = $2", &[&scid, &id]).await.unwrap();
				});
			}
			while let Some(_) = updates.next().await {}
		}
		tx.execute("ALTER TABLE channel_announcements ADD CONSTRAINT channel_announcements_short_channel_id_key UNIQUE (short_channel_id)", &[]).await.unwrap();
		tx.execute("ALTER TABLE channel_announcements ALTER short_channel_id DROP DEFAULT", &[]).await.unwrap();
		tx.execute("ALTER TABLE channel_announcements ALTER short_channel_id SET NOT NULL", &[]).await.unwrap();
		tx.execute("UPDATE config SET db_schema = 4 WHERE id = 1", &[]).await.unwrap();
		tx.commit().await.unwrap();
	}
	if schema >= 1 && schema <= 4 {
		let tx = client.transaction().await.unwrap();
		tx.execute("ALTER TABLE channel_updates ALTER composite_index SET DATA TYPE character(29)", &[]).await.unwrap();
		tx.execute("UPDATE config SET db_schema = 5 WHERE id = 1", &[]).await.unwrap();
		tx.commit().await.unwrap();
	}
	if schema >= 1 && schema <= 5 {
		let tx = client.transaction().await.unwrap();
		tx.execute("ALTER TABLE channel_updates ALTER channel_flags SET DATA TYPE smallint", &[]).await.unwrap();
		tx.execute("ALTER TABLE channel_announcements DROP COLUMN block_height", &[]).await.unwrap();
		tx.execute("UPDATE config SET db_schema = 6 WHERE id = 1", &[]).await.unwrap();
		tx.commit().await.unwrap();
	}
	if schema >= 1 && schema <= 6 {
		let tx = client.transaction().await.unwrap();
		tx.execute("ALTER TABLE channel_updates DROP COLUMN composite_index", &[]).await.unwrap();
		tx.execute("ALTER TABLE channel_updates ALTER timestamp SET NOT NULL", &[]).await.unwrap();
		tx.execute("ALTER TABLE channel_updates ALTER channel_flags SET NOT NULL", &[]).await.unwrap();
		tx.execute("ALTER TABLE channel_updates ALTER disable SET NOT NULL", &[]).await.unwrap();
		tx.execute("ALTER TABLE channel_updates ALTER cltv_expiry_delta SET NOT NULL", &[]).await.unwrap();
		tx.execute("ALTER TABLE channel_updates ALTER htlc_minimum_msat SET NOT NULL", &[]).await.unwrap();
		tx.execute("ALTER TABLE channel_updates ALTER fee_base_msat SET NOT NULL", &[]).await.unwrap();
		tx.execute("ALTER TABLE channel_updates ALTER fee_proportional_millionths SET NOT NULL", &[]).await.unwrap();
		tx.execute("ALTER TABLE channel_updates ALTER htlc_maximum_msat SET NOT NULL", &[]).await.unwrap();
		tx.execute("ALTER TABLE channel_updates ALTER blob_signed SET NOT NULL", &[]).await.unwrap();
		tx.execute("CREATE UNIQUE INDEX channel_updates_key ON channel_updates (short_channel_id, direction, timestamp)", &[]).await.unwrap();
		tx.execute("UPDATE config SET db_schema = 7 WHERE id = 1", &[]).await.unwrap();
		tx.commit().await.unwrap();
	}
	if schema >= 1 && schema <= 7 {
		let tx = client.transaction().await.unwrap();
		tx.execute("DROP INDEX IF EXISTS channels_seen", &[]).await.unwrap();
		tx.execute("DROP INDEX IF EXISTS channel_updates_scid", &[]).await.unwrap();
		tx.execute("DROP INDEX IF EXISTS channel_updates_direction", &[]).await.unwrap();
		tx.execute("DROP INDEX IF EXISTS channel_updates_seen", &[]).await.unwrap();
		tx.execute("DROP INDEX IF EXISTS channel_updates_scid_seen", &[]).await.unwrap();
		tx.execute("DROP INDEX IF EXISTS channel_updates_scid_dir_seen", &[]).await.unwrap();
		tx.execute("UPDATE config SET db_schema = 8 WHERE id = 1", &[]).await.unwrap();
		tx.commit().await.unwrap();
	}
	if schema >= 1 && schema <= 8 {
		let tx = client.transaction().await.unwrap();
		tx.execute("DROP INDEX IF EXISTS channel_updates_seen", &[]).await.unwrap();
		tx.execute("DROP INDEX IF EXISTS channel_updates_scid_seen", &[]).await.unwrap();
		tx.execute("UPDATE config SET db_schema = 9 WHERE id = 1", &[]).await.unwrap();
		tx.commit().await.unwrap();
	}
	if schema >= 1 && schema <= 9 {
		let tx = client.transaction().await.unwrap();
		tx.execute("DROP INDEX IF EXISTS channel_updates_scid_dir_seen", &[]).await.unwrap();
		tx.execute("UPDATE config SET db_schema = 10 WHERE id = 1", &[]).await.unwrap();
		tx.commit().await.unwrap();
	}
	if schema >= 1 && schema <= 10 {
		let tx = client.transaction().await.unwrap();
		tx.execute("DROP INDEX IF EXISTS channel_updates_id_with_scid_dir_blob", &[]).await.unwrap();
		tx.execute("UPDATE config SET db_schema = 11 WHERE id = 1", &[]).await.unwrap();
		tx.commit().await.unwrap();
	}
	if schema >= 1 && schema <= 11 {
		let tx = client.transaction().await.unwrap();
		tx.execute("DROP INDEX IF EXISTS channel_updates_seen_with_id_direction_blob", &[]).await.unwrap();
		tx.execute("UPDATE config SET db_schema = 12 WHERE id = 1", &[]).await.unwrap();
		tx.commit().await.unwrap();
	}
	if schema <= 1 || schema > SCHEMA_VERSION {
		panic!("Unknown schema in db: {}, we support up to {}", schema, SCHEMA_VERSION);
	}
	// PostgreSQL (at least v13, but likely later versions as well) handles insert-only tables
	// *very* poorly. After some number of inserts, it refuses to rely on indexes, assuming them to
	// be possibly-stale, until a VACUUM happens. Thus, we set the vacuum factor really low here,
	// pushing PostgreSQL to vacuum often.
	// See https://www.cybertec-postgresql.com/en/postgresql-autovacuum-insert-only-tables/
	let _ = client.execute("ALTER TABLE channel_updates SET ( autovacuum_vacuum_insert_scale_factor = 0.005 );", &[]).await;
	let _ = client.execute("ALTER TABLE channel_announcements SET ( autovacuum_vacuum_insert_scale_factor = 0.005 );", &[]).await;
}

pub(crate) fn ln_peers() -> Vec<(PublicKey, SocketAddr)> {
	const WALLET_OF_SATOSHI: &str = "035e4ff418fc8b5554c5d9eea66396c227bd429a3251c8cbc711002ba215bfc226@170.75.163.209:9735";
	let list = env::var("LN_PEERS").unwrap_or(WALLET_OF_SATOSHI.to_string());
	let mut peers = Vec::new();
	for peer_info in list.split(',') {
		peers.push(resolve_peer_info(peer_info).expect("Invalid peer info in LN_PEERS"));
	}
	peers
}

fn resolve_peer_info(peer_info: &str) -> Result<(PublicKey, SocketAddr), &str> {
	let mut peer_info = peer_info.splitn(2, '@');

	let pubkey = peer_info.next().ok_or("Invalid peer info. Should be formatted as: `pubkey@host:port`")?;
	let pubkey = Vec::from_hex(pubkey).map_err(|_| "Invalid node pubkey")?;
	let pubkey = PublicKey::from_slice(&pubkey).map_err(|_| "Invalid node pubkey")?;

	let socket_address = peer_info.next().ok_or("Invalid peer info. Should be formatted as: `pubkey@host:port`")?;
	let socket_address = socket_address
		.to_socket_addrs()
		.map_err(|_| "Cannot resolve node address")?
		.next()
		.ok_or("Cannot resolve node address")?;

	Ok((pubkey, socket_address))
}

#[cfg(test)]
mod tests {
	use super::resolve_peer_info;
	use bitcoin::hashes::hex::ToHex;

	#[test]
	fn test_resolve_peer_info() {
		let wallet_of_satoshi = "035e4ff418fc8b5554c5d9eea66396c227bd429a3251c8cbc711002ba215bfc226@170.75.163.209:9735";
		let (pubkey, socket_address) = resolve_peer_info(wallet_of_satoshi).unwrap();
		assert_eq!(pubkey.serialize().to_hex(), "035e4ff418fc8b5554c5d9eea66396c227bd429a3251c8cbc711002ba215bfc226");
		assert_eq!(socket_address.to_string(), "170.75.163.209:9735");

		let ipv6 = "033d8656219478701227199cbd6f670335c8d408a92ae88b962c49d4dc0e83e025@[2001:db8::1]:80";
		let (pubkey, socket_address) = resolve_peer_info(ipv6).unwrap();
		assert_eq!(pubkey.serialize().to_hex(), "033d8656219478701227199cbd6f670335c8d408a92ae88b962c49d4dc0e83e025");
		assert_eq!(socket_address.to_string(), "[2001:db8::1]:80");

		let localhost = "033d8656219478701227199cbd6f670335c8d408a92ae88b962c49d4dc0e83e025@localhost:9735";
		let (pubkey, socket_address) = resolve_peer_info(localhost).unwrap();
		assert_eq!(pubkey.serialize().to_hex(), "033d8656219478701227199cbd6f670335c8d408a92ae88b962c49d4dc0e83e025");
		let socket_address = socket_address.to_string();
		assert!(socket_address == "127.0.0.1:9735" || socket_address == "[::1]:9735");
	}
}
