use std::collections::HashMap;
use std::convert::TryInto;
use std::sync::{Arc, RwLock};

use bitcoin::{BlockHash, TxOut};
use bitcoin::blockdata::block::Block;
use bitcoin::hashes::Hash;
use bitcoin::hashes::hex::ToHex;
use futures::executor;
use lightning::chain;
use lightning::chain::AccessError;
use lightning_block_sync::BlockSource;
use lightning_block_sync::http::BinaryResponse;
use lightning_block_sync::rest::RestClient;

use crate::config;

pub(crate) struct ChainVerifier {
	rest_client: Arc<RestClient>,
	block_cache: RwLock<HashMap<u32, Block>>,
}

struct RestBinaryResponse(Vec<u8>);

impl ChainVerifier {
	pub(crate) fn new() -> Self {
		let rest_client = RestClient::new(config::bitcoin_rest_endpoint()).unwrap();
		ChainVerifier {
			rest_client: Arc::new(rest_client),
			block_cache: RwLock::new(HashMap::new()),
		}
	}

	fn retrieve_block(&self, block_height: u32) -> Result<Block, AccessError> {
		{
			let cache = self.block_cache.read().unwrap();
			if cache.contains_key(&block_height) {
				return Ok(cache.get(&block_height).unwrap().clone());
			}
			// if the key is absent, the cache must be dropped so the write lock can be obtained
		}

		let rest_client = self.rest_client.clone();
		let block = executor::block_on(async move {
			let block_hash_result = rest_client.request_resource::<BinaryResponse, RestBinaryResponse>(&format!("blockhashbyheight/{}.bin", block_height)).await;
			let block_hash: Vec<u8> = block_hash_result.map_err(|error| {
				eprintln!("Could't find block hash at height {}: {}", block_height, error.to_string());
				AccessError::UnknownChain
			})?.0;
			let block_hash = BlockHash::from_slice(&block_hash).unwrap();

			println!("Fetching block {} ({})", block_height, block_hash.to_hex());
			let block_result = rest_client.get_block(&block_hash).await;
			let block = block_result.map_err(|error| {
				eprintln!("Couldn't retrieve block {}: {:?} ({})", block_height, error, block_hash);
				AccessError::UnknownChain
			})?;
			Ok(block)
		})?;

		let mut mutable_cache = self.block_cache.write().unwrap();
		mutable_cache.insert(block_height, block.clone());
		Ok(block)
	}
}

impl chain::Access for ChainVerifier {
	fn get_utxo(&self, _genesis_hash: &BlockHash, short_channel_id: u64) -> Result<TxOut, AccessError> {
		let block_height = (short_channel_id >> 5 * 8) as u32; // block height is most significant three bytes
		let transaction_index = ((short_channel_id >> 2 * 8) & 0xffffff) as u32;
		let output_index = (short_channel_id & 0xffff) as u16;

		let block = self.retrieve_block(block_height)?;
		let transaction = block.txdata.get(transaction_index as usize).ok_or_else(|| {
			eprintln!("Transaction index {} out of bounds in block {} ({})", transaction_index, block_height, block.block_hash().to_string());
			AccessError::UnknownTx
		})?;
		let output = transaction.output.get(output_index as usize).ok_or_else(|| {
			eprintln!("Output index {} out of bounds in transaction {}", output_index, transaction.txid().to_string());
			AccessError::UnknownTx
		})?;
		Ok(output.clone())
	}
}

impl TryInto<RestBinaryResponse> for BinaryResponse {
	type Error = std::io::Error;

	fn try_into(self) -> Result<RestBinaryResponse, Self::Error> {
		Ok(RestBinaryResponse(self.0))
	}
}
