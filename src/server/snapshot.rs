use std::sync::Arc;

use lightning::routing::network_graph::NetworkGraph;

pub(crate) struct Snapshotter {
	network_graph: Arc<NetworkGraph>,
}

impl Snapshotter {
	pub fn new(network_graph: Arc<NetworkGraph>) -> Self {
		Self { network_graph }
	}

	pub(crate) async fn snapshot_gossip(&self) {
		// this is gonna be a never-ending background job
		loop {
			// 1. get the current timestamp
			// 2. sleep until the next round 24 hours
			// 3. refresh all snapshots

			// the stored snapshots should adhere to the following format
			// from one day ago
			// from two days ago
			// …
			// from a week ago
			// from two weeks ago
			// from three weeks ago
			// full
			// That means that at any given moment, there should only ever be
			// 6 (daily) + 3 (weekly) + 1 (total) = 10 cached snapshots
			// The snapshots, unlike dynamic updates, should account for all intermediate
			// channel updates
			//
			// TODO(arik): implement this

			// super::serialize_delta()
			let full_snapshot = super::serialize_delta(self.network_graph.clone(), 0, true, true);
		}
	}
}
