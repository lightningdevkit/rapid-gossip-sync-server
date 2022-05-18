use std::net::SocketAddr;
use bitcoin::secp256k1::PublicKey;
use crate::hex_utils;

/// EDIT ME
pub(crate) fn db_connection_string() -> String {
	"host=localhost user=arik dbname=ln_graph_sync".to_string()
}

pub(crate) fn db_channel_table_creation_query() -> String {
	"CREATE TABLE IF NOT EXISTS channels (
		id SERIAL PRIMARY KEY,
		short_channel_id character varying(255) NOT NULL UNIQUE,
		block_height integer,
		chain_hash character varying(255),
		announcement_signed text,
		announcement_unsigned text,
		seen oid NOT NULL,
		created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
	)".to_string()
}

pub(crate) fn db_channel_update_table_creation_query() -> String {
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
		blob_unsigned text,
		seen oid NOT NULL,
		created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
		channel_id integer
	)".to_string()
}

/// EDIT ME
pub(crate) fn ln_peers() -> Vec<(PublicKey, SocketAddr)> {
	vec![
		// Alex Bosworth
		(hex_utils::to_compressed_pubkey("033d8656219478701227199cbd6f670335c8d408a92ae88b962c49d4dc0e83e025").unwrap(), "34.65.85.39:9735".parse().unwrap()),

		// Matt Corallo
		(hex_utils::to_compressed_pubkey("03db10aa09ff04d3568b0621750794063df401e6853c79a21a83e1a3f3b5bfb0c8").unwrap(), "69.59.18.80:9735".parse().unwrap())
	]
}
