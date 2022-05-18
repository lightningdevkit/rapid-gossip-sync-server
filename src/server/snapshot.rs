pub(crate) struct Snapshotter {

}

impl Snapshotter {
	pub fn new() -> Self {
		Self {}
	}

	pub(crate) async fn snapshot_gossip() {
		// this is gonna be a background job
		loop {
			// 1. get the current time
			// 2. sleep until the next round 24 hours
			// 3. refresh all snapshots

			/// the stored snapshots should adhere to the following format
			/// from one day ago
			/// from two days ago
			/// …
			/// from a week ago
			/// from two weeks ago
			/// from three weeks ago
			/// full
			/// That means that at any given moment, there should only ever be
			/// 6 (daily) + 3 (weekly) + 1 (total) = 10 cached snapshots
			/// The snapshots, unlike dynamic updates, should account for all intermediate
			/// channel updates
			///
			/// TODO(arik): implement this
		}
	}
}
