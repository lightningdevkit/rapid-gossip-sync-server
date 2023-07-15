use std::collections::HashMap;
use std::fs;
use std::os::unix::fs::symlink;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use lightning::routing::gossip::NetworkGraph;

use crate::{config, TestLogger};
use crate::config::cache_path;

pub(crate) struct Snapshotter {
	network_graph: Arc<NetworkGraph<TestLogger>>,
}

impl Snapshotter {
	pub fn new(network_graph: Arc<NetworkGraph<TestLogger>>) -> Self {
		Self { network_graph }
	}

	pub(crate) async fn snapshot_gossip(&self) {
		println!("Initiating snapshotting service");

		let snapshot_sync_day_factors = [1, 2, 3, 4, 5, 6, 7, 14, 21, u64::MAX];
		let round_day_seconds = config::SNAPSHOT_CALCULATION_INTERVAL as u64;

		let pending_snapshot_directory = format!("{}/snapshots_pending", cache_path());
		let pending_symlink_directory = format!("{}/symlinks_pending", cache_path());
		let finalized_snapshot_directory = format!("{}/snapshots", cache_path());
		let finalized_symlink_directory = format!("{}/symlinks", cache_path());
		let relative_symlink_to_snapshot_path = "../snapshots";

		// this is gonna be a never-ending background job
		loop {
			// 1. get the current timestamp
			let snapshot_generation_timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
			let reference_timestamp = Self::round_down_to_nearest_multiple(snapshot_generation_timestamp, round_day_seconds);
			println!("Capturing snapshots at {} for: {}", snapshot_generation_timestamp, reference_timestamp);

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
				let timestamp = reference_timestamp.saturating_sub(round_day_seconds.saturating_mul(factor.clone()));
				snapshot_sync_timestamps.push((factor.clone(), timestamp));
			};

			let mut snapshot_filenames_by_day_range: HashMap<u64, String> = HashMap::with_capacity(10);

			for (day_range, current_last_sync_timestamp) in &snapshot_sync_timestamps {
				let network_graph_clone = self.network_graph.clone();
				{
					println!("Calculating {}-day snapshot", day_range);
					// calculate the snapshot
					let snapshot = super::serialize_delta(network_graph_clone, current_last_sync_timestamp.clone() as u32).await;

					// persist the snapshot and update the symlink
					let snapshot_filename = format!("snapshot__calculated-at:{}__range:{}-days__previous-sync:{}.lngossip", reference_timestamp, day_range, current_last_sync_timestamp);
					let snapshot_path = format!("{}/{}", pending_snapshot_directory, snapshot_filename);
					println!("Persisting {}-day snapshot: {} ({} messages, {} announcements, {} updates ({} full, {} incremental))", day_range, snapshot_filename, snapshot.message_count, snapshot.announcement_count, snapshot.update_count, snapshot.update_count_full, snapshot.update_count_incremental);
					fs::write(&snapshot_path, snapshot.data).unwrap();
					snapshot_filenames_by_day_range.insert(day_range.clone(), snapshot_filename);
				}
			}

			{
				// create dummy symlink
				let dummy_filename = "empty_delta.lngossip";
				let dummy_snapshot = super::serialize_empty_blob(reference_timestamp);
				let dummy_snapshot_path = format!("{}/{}", pending_snapshot_directory, dummy_filename);
				fs::write(&dummy_snapshot_path, dummy_snapshot).unwrap();

				let dummy_symlink_path = format!("{}/{}.bin", pending_symlink_directory, reference_timestamp);
				let relative_dummy_snapshot_path = format!("{}/{}", relative_symlink_to_snapshot_path, dummy_filename);
				println!("Symlinking dummy: {} -> {}", dummy_symlink_path, relative_dummy_snapshot_path);
				symlink(&relative_dummy_snapshot_path, &dummy_symlink_path).unwrap();
			}

			for i in 0..10_001u64 {
				// let's create non-dummy-symlinks

				// first, determine which snapshot range should be referenced
				let referenced_day_range = if i == 0 {
					// special-case 0 to always refer to a full/initial sync
					u64::MAX
				} else {
					// find min(x) in snapshot_sync_day_factors where x >= i
					snapshot_sync_day_factors.iter().find(|x| {
						x >= &&i
					}).unwrap().clone()
				};

				let snapshot_filename = snapshot_filenames_by_day_range.get(&referenced_day_range).unwrap();
				let relative_snapshot_path = format!("{}/{}", relative_symlink_to_snapshot_path, snapshot_filename);

				let canonical_last_sync_timestamp = if i == 0 {
					// special-case 0 to always refer to a full/initial sync
					0
				} else {
					reference_timestamp.saturating_sub(round_day_seconds.saturating_mul(i))
				};
				let symlink_path = format!("{}/{}.bin", pending_symlink_directory, canonical_last_sync_timestamp);

				println!("Symlinking: {} -> {} ({} -> {}", i, referenced_day_range, symlink_path, relative_snapshot_path);
				symlink(&relative_snapshot_path, &symlink_path).unwrap();
			}

			let update_time_path = format!("{}/update_time.txt", pending_symlink_directory);
			let update_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
			fs::write(&update_time_path, format!("{}", update_time)).unwrap();

			if fs::metadata(&finalized_snapshot_directory).is_ok(){
				fs::remove_dir_all(&finalized_snapshot_directory).expect("Failed to remove finalized snapshot directory.");
			}
			if fs::metadata(&finalized_symlink_directory).is_ok(){
				fs::remove_dir_all(&finalized_symlink_directory).expect("Failed to remove pending symlink directory.");
			}
			fs::rename(&pending_snapshot_directory, &finalized_snapshot_directory).expect("Failed to finalize snapshot directory.");
			fs::rename(&pending_symlink_directory, &finalized_symlink_directory).expect("Failed to finalize symlink directory.");

			// constructing the snapshots may have taken a while
			let current_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
			let remainder = current_time % round_day_seconds;
			let time_until_next_day = round_day_seconds - remainder;

			println!("Sleeping until next snapshot capture: {}s", time_until_next_day);
			// add in an extra five seconds to assure the rounding down works correctly
			let sleep = tokio::time::sleep(Duration::from_secs(time_until_next_day + 5));
			sleep.await;
		}
	}

	pub(super) fn round_down_to_nearest_multiple(number: u64, multiple: u64) -> u64 {
		let round_multiple_delta = number % multiple;
		number - round_multiple_delta
	}
}
