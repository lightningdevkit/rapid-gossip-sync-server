use std::fs;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use lightning::routing::gossip::NetworkGraph;
use crate::TestLogger;

pub(crate) struct Snapshotter {
	network_graph: Arc<NetworkGraph<Arc<TestLogger>>>,
}

impl Snapshotter {
	pub fn new(network_graph: Arc<NetworkGraph<Arc<TestLogger>>>) -> Self {
		Self { network_graph }
	}

	pub(crate) async fn snapshot_gossip(&self) {
		println!("Initiating snapshotting service");

		let round_day_seconds: u64 = 24 * 3600; // 24 hours
		let snapshot_sync_day_factors = [1, 2, 3, 4, 5, 6, 7, 14, 21, u64::MAX];

		// this is gonna be a never-ending background job
		loop {
			// 1. get the current timestamp
			let timestamp_seen = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
			let filename_timestamp = Self::round_down_to_nearest_multiple(timestamp_seen, round_day_seconds);
			println!("Capturing snapshots at {} for: {}", timestamp_seen, filename_timestamp);

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
			let mut snapshot_sync_timestamps: Vec<(u64, u64)> = Vec::new();
			for factor in &snapshot_sync_day_factors {
				// basically timestamp - day_seconds * factor
				let timestamp = timestamp_seen.saturating_sub(round_day_seconds.saturating_mul(factor.clone()));
				snapshot_sync_timestamps.push((factor.clone(), timestamp));
			};

			for (days, current_sync_timestamp) in &snapshot_sync_timestamps {
				let network_graph_clone = self.network_graph.clone();
				{
					println!("Calculating {}-day snapshot", days);
					// calculate the snapshot
					let snapshot = super::serialize_delta(network_graph_clone, current_sync_timestamp.clone() as u32, true, true).await;

					// persist the snapshot
					let snapshot_directory = "./res/snapshots";
					let snapshot_filename = format!("snapshot-after_{}-days_{}-calculated_{}.lngossip", current_sync_timestamp, days, filename_timestamp);
					let snapshot_path = format!("{}/{}", snapshot_directory, snapshot_filename);
					println!("Persisting {}-day snapshot: {} ({} messages, {} announcements, {} updates ({} full, {} incremental))", days, snapshot_filename, snapshot.message_count, snapshot.announcement_count, snapshot.update_count, snapshot.update_count_full, snapshot.update_count_incremental);

					// TODO: start writing the compressed snapshot again!
					fs::write(&snapshot_path, snapshot.compressed.unwrap()).unwrap();

					// remove the old snapshots for the given time interval
					let other_snapshots = fs::read_dir(snapshot_directory).unwrap();
					for entry_result in other_snapshots {
						if entry_result.is_err() {
							continue;
						};
						let entry = entry_result.as_ref().unwrap();
						if entry.file_type().is_err() {
							continue;
						};
						let file_type = entry.file_type().unwrap();
						if !file_type.is_file() {
							continue;
						}
						let file_name_result = entry.file_name().into_string();
						if file_name_result.is_err() {
							continue;
						}
						let file_name = file_name_result.unwrap();
						let substring = format!("-days_{}-", days);
						if file_name.starts_with("snapshot-after") && file_name.contains(&substring) && file_name != snapshot_filename {
							println!("Removing expired {}-day snapshot: {}", days, file_name);
							fs::remove_file(entry.path()).unwrap();
						}
					}
				}
			}

			let remainder = timestamp_seen % round_day_seconds;
			let time_until_next_day = round_day_seconds - remainder;

			println!("Sleeping until next snapshot capture: {}s", time_until_next_day);
			// add in an extra five seconds to assure the rounding down works correctly
			let sleep = tokio::time::sleep(Duration::from_secs(time_until_next_day + 5));
			sleep.await;
		}
	}

	fn round_down_to_nearest_multiple(number: u64, multiple: u64) -> u64 {
		let round_multiple_delta = number % multiple;
		number - round_multiple_delta
	}
}
