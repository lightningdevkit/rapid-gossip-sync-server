use std::env;
use std::net::SocketAddr;
use bitcoin::secp256k1::PublicKey;
use lightning_block_sync::http::HttpEndpoint;
use tokio_postgres::Config;
use crate::hex_utils;

pub(crate) const SNAPSHOT_CALCULATION_INTERVAL: u64 = 3600 * 24; // every 24 hours, in seconds
pub(crate) const DOWNLOAD_NEW_GOSSIP: bool = true;

pub(crate) fn network_graph_cache_path() -> &'static str {
	"./res/network_graph.bin"
}

pub(crate) fn db_connection_config() -> Config {
	let mut config = Config::new();
	let host = env::var("RUST_LN_SYNC_DB_HOST").unwrap_or("localhost".to_string());
	let user = env::var("RUST_LN_SYNC_DB_USER").unwrap_or("alice".to_string());
	let db = env::var("RUST_LN_SYNC_DB_NAME").unwrap_or("ln_graph_sync".to_string());
	config.host(&host);
	config.user(&user);
	config.dbname(&db);
	if let Ok(password) = env::var("RUST_LN_SYNC_DB_PASSWORD") {
		config.password(&password);
	}
	config
}

pub(crate) fn bitcoin_rest_endpoint() -> HttpEndpoint {
	let host = env::var("BITCOIN_REST_DOMAIN").unwrap_or("127.0.0.1".to_string());
	let port = env::var("BITCOIN_REST_PORT")
		.unwrap_or("80".to_string())
		.parse::<u16>()
		.expect("BITCOIN_REST_PORT env variable must be a u16.");
	let path = env::var("BITCOIN_REST_PATH").unwrap_or("/rest/".to_string());
	HttpEndpoint::for_host(host).with_port(port).with_path(path)
}

pub(crate) fn db_config_table_creation_query() -> &'static str {
	"CREATE TABLE IF NOT EXISTS config (
		id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
		db_schema integer
	)"
}

pub(crate) fn db_announcement_table_creation_query() -> &'static str {
	"CREATE TABLE IF NOT EXISTS channel_announcements (
		id SERIAL PRIMARY KEY,
		short_channel_id character varying(255) NOT NULL UNIQUE,
		block_height integer,
		chain_hash character varying(255),
		announcement_signed text,
		seen oid NOT NULL
	)"
}

pub(crate) fn db_channel_update_table_creation_query() -> &'static str {
	"CREATE TABLE IF NOT EXISTS channel_updates (
		id SERIAL PRIMARY KEY,
		composite_index character varying(255) UNIQUE,
		chain_hash character varying(255),
		short_channel_id character varying(255),
		timestamp bigint,
		channel_flags integer,
		direction integer,
		disable boolean,
		cltv_expiry_delta integer,
		htlc_minimum_msat bigint,
		fee_base_msat integer,
		fee_proportional_millionths integer,
		htlc_maximum_msat bigint,
		blob_signed text,
		seen oid NOT NULL
	)"
}

pub(crate) fn db_index_creation_query() -> &'static str {
	"
	CREATE INDEX IF NOT EXISTS channels_seen ON channel_announcements(seen);
	CREATE INDEX IF NOT EXISTS channel_updates_scid ON channel_updates(short_channel_id);
	CREATE INDEX IF NOT EXISTS channel_updates_direction ON channel_updates(direction);
	CREATE INDEX IF NOT EXISTS channel_updates_seen ON channel_updates(seen);
	"
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
