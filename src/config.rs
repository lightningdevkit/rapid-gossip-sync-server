use std::convert::TryInto;
use std::env;
use std::net::SocketAddr;
use std::io::Cursor;
use bitcoin::secp256k1::PublicKey;
use lightning::ln::msgs::ChannelAnnouncement;
use lightning::util::ser::Readable;
use lightning_block_sync::http::HttpEndpoint;
use tokio_postgres::Config;
use crate::hex_utils;

use futures::stream::{FuturesUnordered, StreamExt};

pub(crate) const SCHEMA_VERSION: i32 = 5;
pub(crate) const SNAPSHOT_CALCULATION_INTERVAL: u32 = 3600 * 24; // every 24 hours, in seconds
pub(crate) const DOWNLOAD_NEW_GOSSIP: bool = true;

pub(crate) fn network_graph_cache_path() -> &'static str {
	"./res/network_graph.bin"
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
		block_height integer,
		announcement_signed BYTEA,
		seen timestamp NOT NULL DEFAULT NOW()
	)"
}

pub(crate) fn db_channel_update_table_creation_query() -> &'static str {
	// We'll run out of room in composite index at block 8,388,608 or in the year 2286
	"CREATE TABLE IF NOT EXISTS channel_updates (
		id SERIAL PRIMARY KEY,
		composite_index character(29) UNIQUE,
		short_channel_id bigint NOT NULL,
		timestamp bigint,
		channel_flags integer,
		direction boolean NOT NULL,
		disable boolean,
		cltv_expiry_delta integer,
		htlc_minimum_msat bigint,
		fee_base_msat integer,
		fee_proportional_millionths integer,
		htlc_maximum_msat bigint,
		blob_signed BYTEA,
		seen timestamp NOT NULL DEFAULT NOW()
	)"
}

pub(crate) fn db_index_creation_query() -> &'static str {
	"
	CREATE INDEX IF NOT EXISTS channels_seen ON channel_announcements(seen);
	CREATE INDEX IF NOT EXISTS channel_updates_scid ON channel_updates(short_channel_id);
	CREATE INDEX IF NOT EXISTS channel_updates_direction ON channel_updates (short_channel_id, direction);
	CREATE INDEX IF NOT EXISTS channel_updates_seen ON channel_updates(seen);
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
			while let Some(_) = updates.next().await { }
		}
		tx.execute("CREATE INDEX channel_updates_scid ON channel_updates(short_channel_id)", &[]).await.unwrap();
		tx.execute("CREATE INDEX channel_updates_direction ON channel_updates (short_channel_id, direction)", &[]).await.unwrap();
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
			while let Some(_) = updates.next().await { }
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
	if schema <= 1 || schema > SCHEMA_VERSION {
		panic!("Unknown schema in db: {}, we support up to {}", schema, SCHEMA_VERSION);
	}
}

/// EDIT ME
pub(crate) fn ln_peers() -> Vec<(PublicKey, SocketAddr)> {
	vec![
		// Bitfinex
		// (hex_utils::to_compressed_pubkey("033d8656219478701227199cbd6f670335c8d408a92ae88b962c49d4dc0e83e025").unwrap(), "34.65.85.39:9735".parse().unwrap()),

		// Matt Corallo
		// (hex_utils::to_compressed_pubkey("03db10aa09ff04d3568b0621750794063df401e6853c79a21a83e1a3f3b5bfb0c8").unwrap(), "69.59.18.80:9735".parse().unwrap())

		// River Financial
		// (hex_utils::to_compressed_pubkey("03037dc08e9ac63b82581f79b662a4d0ceca8a8ca162b1af3551595b8f2d97b70a").unwrap(), "104.196.249.140:9735".parse().unwrap())

		// Wallet of Satoshi | 035e4ff418fc8b5554c5d9eea66396c227bd429a3251c8cbc711002ba215bfc226@170.75.163.209:9735
		(hex_utils::to_compressed_pubkey("035e4ff418fc8b5554c5d9eea66396c227bd429a3251c8cbc711002ba215bfc226").unwrap(), "170.75.163.209:9735".parse().unwrap())
	]
}
