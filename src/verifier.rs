use std::convert::TryInto;

use bitcoin::{BlockHash, TxOut};
use bitcoin::blockdata::block::Block;
use bitcoin::hashes::Hash;
use lightning::chain;
use lightning::chain::AccessError;
use lightning_block_sync::{BlockData, BlockSource};
use lightning_block_sync::http::BinaryResponse;
use lightning_block_sync::rest::RestClient;

use crate::config;

pub(crate) struct ChainVerifier {
	rest_client: RestClient,
}

struct RestBinaryResponse(Vec<u8>);

impl ChainVerifier {
	pub(crate) fn new() -> Self {
		ChainVerifier {
			rest_client: RestClient::new(config::bitcoin_rest_endpoint()).unwrap(),
		}
	}

	fn retrieve_block(&self, block_height: u32) -> Result<Block, AccessError> {
		tokio::task::block_in_place(move || { tokio::runtime::Handle::current().block_on(async move {
			let uri = format!("blockhashbyheight/{}.bin", block_height);
			let block_hash_result =
				self.rest_client.request_resource::<BinaryResponse, RestBinaryResponse>(&uri).await;
			let block_hash: Vec<u8> = block_hash_result.map_err(|error| {
				eprintln!("Could't find block hash at height {}: {}", block_height, error.to_string());
				AccessError::UnknownChain
			})?.0;
			let block_hash = BlockHash::from_slice(&block_hash).unwrap();

			let block_result = self.rest_client.get_block(&block_hash).await;
			match block_result {
				Ok(BlockData::FullBlock(block)) => {
					Ok(block)
				},
				Ok(_) => unreachable!(),
				Err(error) => {
					eprintln!("Couldn't retrieve block {}: {:?} ({})", block_height, error, block_hash);
					Err(AccessError::UnknownChain)
				}
			}
		}) })
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
