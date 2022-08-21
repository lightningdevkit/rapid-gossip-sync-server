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

		let snapshot_directory = "./res/snapshots";
		let symlink_directory = "./res/symlinks";
		fs::create_dir_all(&snapshot_directory).expect("Failed to create snapshot directory");
		fs::create_dir_all(&symlink_directory).expect("Failed to create symlink directory");

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

			for (days, current_last_sync_timestamp) in &snapshot_sync_timestamps {
				let network_graph_clone = self.network_graph.clone();
				{
					println!("Calculating {}-day snapshot", days);
					// calculate the snapshot
					let snapshot = super::serialize_delta(network_graph_clone, current_last_sync_timestamp.clone() as u32, true).await;

					// persist the snapshot and update the symlink
					let snapshot_filename = format!("snapshot__calculated-at:{}__range:{}-days__previous-sync:{}.lngossip", filename_timestamp, days, current_last_sync_timestamp);
					let snapshot_path = format!("{}/{}", snapshot_directory, snapshot_filename);
					println!("Persisting {}-day snapshot: {} ({} messages, {} announcements, {} updates ({} full, {} incremental))", days, snapshot_filename, snapshot.message_count, snapshot.announcement_count, snapshot.update_count, snapshot.update_count_full, snapshot.update_count_incremental);
					fs::write(&snapshot_path, snapshot.data).unwrap();
					// with the file persister, a canonical snapshot path can be determined
					let canonical_snapshot_path = fs::canonicalize(&snapshot_path).unwrap();

					// symlinks require intermediate days to also be filled
					// for most days, the only delta is 0
					let mut symlink_gap_days = vec![0];
					if [14, 21].contains(days) {
						symlink_gap_days.extend_from_slice(&[1, 2, 3, 4, 5, 6]);
					}
					// TODO: create symlinks for days 22 through 100 pointing to the full sync
					for current_day_gap in symlink_gap_days {
						println!("Creating symlink for {}-day-delta with {}-day gap (or emulated {}-day-delta)", days, current_day_gap, days - current_day_gap);
						let canonical_last_sync_timestamp = Self::round_down_to_nearest_multiple(current_last_sync_timestamp.clone(), round_day_seconds).saturating_sub(round_day_seconds * current_day_gap);
						let symlink_path = format!("{}/{}.bin", symlink_directory, canonical_last_sync_timestamp);

						if let Ok(metadata) = fs::symlink_metadata(&symlink_path) {
							println!("Symlink metadata: {:#?}", metadata);
							// symlink exists
							if let Ok(previous_snapshot_path) = fs::canonicalize(&symlink_path) {
								println!("Removing symlinked file: {:?}", previous_snapshot_path);
								fs::remove_file(previous_snapshot_path).expect("Failed to remove symlinked file.");
							}
							println!("Removing symlink: {:?}", symlink_path);
							fs::remove_file(&symlink_path).unwrap();
						}
						println!("Recreating symlink: {} -> {}", symlink_path, snapshot_path);
						symlink(&canonical_snapshot_path, &symlink_path).unwrap();
					}

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
						let substring = format!("range:{}-days", days);
						if file_name.starts_with("snapshot__") && file_name.contains(&substring) && file_name != snapshot_filename {
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
