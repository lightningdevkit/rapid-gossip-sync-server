use std::convert::TryInto;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::Mutex;

use bitcoin::{BlockHash, TxOut};
use bitcoin::blockdata::block::Block;
use lightning::log_error;
use lightning::routing::gossip::{NetworkGraph, P2PGossipSync};
use lightning::routing::utxo::{UtxoFuture, UtxoLookup, UtxoResult, UtxoLookupError};
use lightning::util::logger::Logger;
use lightning_block_sync::{BlockData, BlockSource};
use lightning_block_sync::gossip::UtxoSource;
use lightning_block_sync::http::BinaryResponse;
use lightning_block_sync::rest::RestClient;

use crate::config;
use crate::types::GossipPeerManager;

enum AnyChainSource {
	Rest(RestClient)
}

impl BlockSource for AnyChainSource {
    fn get_header<'a>(&'a self, header_hash: &'a BlockHash, height_hint: Option<u32>) -> lightning_block_sync::AsyncBlockSourceResult<'a, lightning_block_sync::BlockHeaderData> {
        match self {
			AnyChainSource::Rest(client) => client.get_header(header_hash, height_hint)
		}
    }

    fn get_block<'a>(&'a self, header_hash: &'a BlockHash) -> lightning_block_sync::AsyncBlockSourceResult<'a, BlockData> {
        match self {
			AnyChainSource::Rest(client) => client.get_block(header_hash)
		}
    }

    fn get_best_block<'a>(&'a self) -> lightning_block_sync::AsyncBlockSourceResult<(BlockHash, Option<u32>)> {
        match self {
			AnyChainSource::Rest(client) => client.get_best_block()
		}
    }
}

impl UtxoSource for AnyChainSource {
    fn get_block_hash_by_height<'a>(&'a self, block_height: u32) -> lightning_block_sync::AsyncBlockSourceResult<'a, BlockHash> {
        match self {
			AnyChainSource::Rest(client) => client.get_block_hash_by_height(block_height)
		}
    }

    fn is_output_unspent<'a>(&'a self, outpoint: bitcoin::OutPoint) -> lightning_block_sync::AsyncBlockSourceResult<'a, bool> {
        match self {
			AnyChainSource::Rest(client) => client.is_output_unspent(outpoint)
		}
    }
}

pub(crate) struct ChainVerifier<L: Deref + Clone + Send + Sync + 'static> where L::Target: Logger {
	chain_source: Arc<AnyChainSource>,
	graph: Arc<NetworkGraph<L>>,
	outbound_gossiper: Arc<P2PGossipSync<Arc<NetworkGraph<L>>, Arc<Self>, L>>,
	peer_handler: Mutex<Option<GossipPeerManager<L>>>,
	logger: L
}

struct RestBinaryResponse(Vec<u8>);

impl<L: Deref + Clone + Send + Sync + 'static> ChainVerifier<L> where L::Target: Logger {
	pub(crate) fn new(graph: Arc<NetworkGraph<L>>, outbound_gossiper: Arc<P2PGossipSync<Arc<NetworkGraph<L>>, Arc<Self>, L>>, logger: L) -> Self {
		let chain_source = Arc::new(AnyChainSource::Rest(RestClient::new(config::bitcoin_rest_endpoint()).unwrap()));

		ChainVerifier {
			chain_source,
			outbound_gossiper,
			graph,
			peer_handler: Mutex::new(None),
			logger
		}
	}
	pub(crate) fn set_ph(&self, peer_handler: GossipPeerManager<L>) {
		*self.peer_handler.lock().unwrap() = Some(peer_handler);
	}

	async fn retrieve_utxo(chain_source: Arc<AnyChainSource>, short_channel_id: u64, logger: L) -> Result<TxOut, UtxoLookupError> {
		let block_height = (short_channel_id >> 5 * 8) as u32; // block height is most significant three bytes
		let transaction_index = ((short_channel_id >> 2 * 8) & 0xffffff) as u32;
		let output_index = (short_channel_id & 0xffff) as u16;

		let mut block = Self::retrieve_block(chain_source, block_height, logger.clone()).await?;
		if transaction_index as usize >= block.txdata.len() {
			log_error!(logger, "Could't find transaction {} in block {}", transaction_index, block_height);
			return Err(UtxoLookupError::UnknownTx);
		}
		let mut transaction = block.txdata.swap_remove(transaction_index as usize);
		if output_index as usize >= transaction.output.len() {
			log_error!(logger, "Could't find output {} in transaction {}", output_index, transaction.txid());
			return Err(UtxoLookupError::UnknownTx);
		}
		Ok(transaction.output.swap_remove(output_index as usize))
	}

	async fn retrieve_block(chain_source: Arc<AnyChainSource>, block_height: u32, logger: L) -> Result<Block, UtxoLookupError> {
		let block_hash = chain_source.get_block_hash_by_height(block_height).await.map_err(|error| {
			log_error!(logger, "Could't find block hash at height {}: {:?}", block_height, error);
			UtxoLookupError::UnknownChain
		})?;

		let block_result = chain_source.get_block(&block_hash).await;
		match block_result {
			Ok(BlockData::FullBlock(block)) => {
				Ok(block)
			},
			Ok(_) => unreachable!(),
			Err(error) => {
				log_error!(logger, "Couldn't retrieve block {}: {:?} ({})", block_height, error, block_hash);
				Err(UtxoLookupError::UnknownChain)
			}
		}
	}
}

impl<L: Deref + Clone + Send + Sync + 'static> UtxoLookup for ChainVerifier<L> where L::Target: Logger {
	fn get_utxo(&self, _genesis_hash: &BlockHash, short_channel_id: u64) -> UtxoResult {
		let res = UtxoFuture::new();
		let fut = res.clone();
		let graph_ref = Arc::clone(&self.graph);
		let client_ref = Arc::clone(&self.chain_source);
		let gossip_ref = Arc::clone(&self.outbound_gossiper);
		let pm_ref = self.peer_handler.lock().unwrap().clone();
		let logger_ref = self.logger.clone();
		tokio::spawn(async move {
			let res = Self::retrieve_utxo(client_ref, short_channel_id, logger_ref).await;
			fut.resolve(&*graph_ref, &*gossip_ref, res);
			if let Some(pm) = pm_ref { pm.process_events(); }
		});
		UtxoResult::Async(res)
	}
}

impl TryInto<RestBinaryResponse> for BinaryResponse {
	type Error = std::io::Error;

	fn try_into(self) -> Result<RestBinaryResponse, Self::Error> {
		Ok(RestBinaryResponse(self.0))
	}
}
