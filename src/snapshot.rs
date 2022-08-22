use std::collections::HashMap;
use std::fs;
use std::os::unix::fs::symlink;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use lightning::routing::gossip::NetworkGraph;

use crate::{config, TestLogger};

pub(crate) struct Snapshotter {
	network_graph: Arc<NetworkGraph<Arc<TestLogger>>>,
}

impl Snapshotter {
	pub fn new(network_graph: Arc<NetworkGraph<Arc<TestLogger>>>) -> Self {
		Self { network_graph }
	}

	pub(crate) async fn snapshot_gossip(&self) {
		println!("Initiating snapshotting service");

		let snapshot_sync_day_factors = [1, 2, 3, 4, 5, 6, 7, 14, 21, u64::MAX];
		let round_day_seconds = config::SNAPSHOT_CALCULATION_INTERVAL as u64;

		let pending_snapshot_directory = "./res/snapshots_pending";
		let pending_symlink_directory = "./res/symlinks_pending";
		let finalized_snapshot_directory = "./res/snapshots";
		let finalized_symlink_directory = "./res/symlinks";
		let relative_symlink_to_snapshot_path = "../snapshots";

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


			// purge and recreate the pending directories
			if fs::metadata(&pending_snapshot_directory).is_ok(){
				fs::remove_dir_all(&pending_snapshot_directory).expect("Failed to remove pending snapshot directory.");
			}
			if fs::metadata(&pending_symlink_directory).is_ok(){
				fs::remove_dir_all(&pending_symlink_directory).expect("Failed to remove pending symlink directory.");
			}
			fs::create_dir_all(&pending_snapshot_directory).expect("Failed to create pending snapshot directory");
			fs::create_dir_all(&pending_symlink_directory).expect("Failed to create pending symlink directory");

			let mut snapshot_sync_timestamps: Vec<(u64, u64)> = Vec::new();
			for factor in &snapshot_sync_day_factors {
				// basically timestamp - day_seconds * factor
				let timestamp = timestamp_seen.saturating_sub(round_day_seconds.saturating_mul(factor.clone()));
				snapshot_sync_timestamps.push((factor.clone(), timestamp));
			};

			let mut snapshot_filenames_by_day_range: HashMap<u64, String> = HashMap::with_capacity(10);

			for (day_range, current_last_sync_timestamp) in &snapshot_sync_timestamps {
				let network_graph_clone = self.network_graph.clone();
				{
					println!("Calculating {}-day snapshot", day_range);
					// calculate the snapshot
					let snapshot = super::serialize_delta(network_graph_clone, current_last_sync_timestamp.clone() as u32, true).await;

					// persist the snapshot and update the symlink
					let snapshot_filename = format!("snapshot__calculated-at:{}__range:{}-days__previous-sync:{}.lngossip", filename_timestamp, day_range, current_last_sync_timestamp);
					let snapshot_path = format!("{}/{}", pending_snapshot_directory, snapshot_filename);
					println!("Persisting {}-day snapshot: {} ({} messages, {} announcements, {} updates ({} full, {} incremental))", day_range, snapshot_filename, snapshot.message_count, snapshot.announcement_count, snapshot.update_count, snapshot.update_count_full, snapshot.update_count_incremental);
					fs::write(&snapshot_path, snapshot.data).unwrap();
					snapshot_filenames_by_day_range.insert(day_range.clone(), snapshot_filename);
				}
			}

			for i in 1..10_001u64 {
				// let's create symlinks

				// first, determine which snapshot range should be referenced
				// find min(x) in snapshot_sync_day_factors where x >= i
				let referenced_day_range = snapshot_sync_day_factors.iter().find(|x| {
					x >= &&i
				}).unwrap().clone();

				let snapshot_filename = snapshot_filenames_by_day_range.get(&referenced_day_range).unwrap();
				let simulated_last_sync_timestamp = timestamp_seen.saturating_sub(round_day_seconds.saturating_mul(i));
				let relative_snapshot_path = format!("{}/{}", relative_symlink_to_snapshot_path, snapshot_filename);
				let canonical_last_sync_timestamp = Self::round_down_to_nearest_multiple(simulated_last_sync_timestamp, round_day_seconds);
				let symlink_path = format!("{}/{}.bin", pending_symlink_directory, canonical_last_sync_timestamp);

				println!("Symlinking: {} -> {} ({} -> {}", i, referenced_day_range, symlink_path, relative_snapshot_path);
				symlink(&relative_snapshot_path, &symlink_path).unwrap();
			}

			if fs::metadata(&finalized_snapshot_directory).is_ok(){
				fs::remove_dir_all(&finalized_snapshot_directory).expect("Failed to remove finalized snapshot directory.");
			}
			if fs::metadata(&finalized_symlink_directory).is_ok(){
				fs::remove_dir_all(&finalized_symlink_directory).expect("Failed to remove pending symlink directory.");
			}
			fs::rename(&pending_snapshot_directory, &finalized_snapshot_directory).expect("Failed to finalize snapshot directory.");
			fs::rename(&pending_symlink_directory, &finalized_symlink_directory).expect("Failed to finalize symlink directory.");


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
