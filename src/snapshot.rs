use std::collections::HashMap;
use std::fs;
use std::ops::Deref;
use std::os::unix::fs::symlink;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use lightning::log_info;

use lightning::routing::gossip::NetworkGraph;
use lightning::util::logger::Logger;

use crate::config;
use crate::config::cache_path;

pub(crate) struct Snapshotter<L: Deref + Clone> where L::Target: Logger {
	network_graph: Arc<NetworkGraph<L>>,
	logger: L
}

impl<L: Deref + Clone> Snapshotter<L> where L::Target: Logger {
	pub fn new(network_graph: Arc<NetworkGraph<L>>, logger: L) -> Self {
		Self { network_graph, logger }
	}

	pub(crate) async fn snapshot_gossip(&self) {
		log_info!(self.logger, "Initiating snapshotting service");

		let calc_interval = config::calculate_interval();
		let snapshot_sync_day_factors = [1, 2, 3, 4, 5, 6, 7, 14, 21, u64::MAX];
		const DAY_SECONDS: u64 = 60 * 60 * 24;
		let round_day_seconds = calc_interval as u64;

		let pending_snapshot_directory = format!("{}/snapshots_pending", cache_path());
		let pending_symlink_directory = format!("{}/symlinks_pending", cache_path());
		let finalized_snapshot_directory = format!("{}/snapshots", cache_path());
		let finalized_symlink_directory = format!("{}/symlinks", cache_path());
		let relative_symlink_to_snapshot_path = "../snapshots";

		// this is gonna be a never-ending background job
		loop {
			// 1. get the current timestamp
			let snapshot_generation_timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
			let reference_timestamp = Self::round_down_to_nearest_multiple(snapshot_generation_timestamp, config::SNAPSHOT_CALCULATION_INTERVAL as u64);
			log_info!(self.logger, "Capturing snapshots at {} for: {}", snapshot_generation_timestamp, reference_timestamp);

			// 2. sleep until the next round interval
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
				let timestamp = reference_timestamp.saturating_sub(DAY_SECONDS.saturating_mul(factor.clone()));
				snapshot_sync_timestamps.push((factor.clone(), timestamp));
			};

			let mut snapshot_filenames_by_day_range: HashMap<u64, String> = HashMap::with_capacity(10);

			for (day_range, current_last_sync_timestamp) in &snapshot_sync_timestamps {
				let network_graph_clone = self.network_graph.clone();
				{
					log_info!(self.logger, "Calculating {}-day snapshot", day_range);
					// calculate the snapshot
					let snapshot = super::serialize_delta(network_graph_clone, current_last_sync_timestamp.clone() as u32, self.logger.clone()).await;

					// persist the snapshot and update the symlink
					let snapshot_filename = format!("snapshot__calculated-at:{}__range:{}-days__previous-sync:{}.lngossip", reference_timestamp, day_range, current_last_sync_timestamp);
					let snapshot_path = format!("{}/{}", pending_snapshot_directory, snapshot_filename);
					log_info!(self.logger, "Persisting {}-day snapshot: {} ({} messages, {} announcements, {} updates ({} full, {} incremental))", day_range, snapshot_filename, snapshot.message_count, snapshot.announcement_count, snapshot.update_count, snapshot.update_count_full, snapshot.update_count_incremental);
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
				log_info!(self.logger, "Symlinking dummy: {} -> {}", dummy_symlink_path, relative_dummy_snapshot_path);
				symlink(&relative_dummy_snapshot_path, &dummy_symlink_path).unwrap();
			}

			// Number of intervals since Jan 1, 2022, a few months before RGS server was released.
			let symlink_count = (reference_timestamp - 1640995200) / config::SNAPSHOT_CALCULATION_INTERVAL as u64;
			for i in 0..symlink_count {
				// let's create non-dummy-symlinks

				// first, determine which snapshot range should be referenced
				let referenced_day_range = if i == 0 {
					// special-case 0 to always refer to a full/initial sync
					u64::MAX
				} else {
					/*
					We have snapshots for 6-day- and 7-day-intervals, but the next interval is
					14 days. So if somebody requests an update with a timestamp that is 10 days old,
					there is no longer a snapshot for that specific interval.

					The correct snapshot will be the next highest interval, i. e. for 14 days.

					The `snapshot_sync_day_factors` array is sorted ascendingly, so find() will
					return on the first iteration that is at least equal to the requested interval.

					Note, however, that the last value in the array is u64::max, which means that
					multiplying it with DAY_SECONDS will overflow. To avoid that, we use
					saturating_mul.
					 */

					// find min(x) in snapshot_sync_day_factors where x >= i
					snapshot_sync_day_factors.iter().find(|x| {
						DAY_SECONDS.saturating_mul(**x) >= i * config::SNAPSHOT_CALCULATION_INTERVAL as u64
					}).unwrap().clone()
				};
				log_info!(self.logger, "i: {}, referenced day range: {}", i, referenced_day_range);

				let snapshot_filename = snapshot_filenames_by_day_range.get(&referenced_day_range).unwrap();
				let relative_snapshot_path = format!("{}/{}", relative_symlink_to_snapshot_path, snapshot_filename);

				let canonical_last_sync_timestamp = if i == 0 {
					// special-case 0 to always refer to a full/initial sync
					0
				} else {
					reference_timestamp.saturating_sub((config::SNAPSHOT_CALCULATION_INTERVAL as u64).saturating_mul(i))
				};
				let symlink_path = format!("{}/{}.bin", pending_symlink_directory, canonical_last_sync_timestamp);

				log_info!(self.logger, "Symlinking: {} -> {} ({} -> {}", i, referenced_day_range, symlink_path, relative_snapshot_path);
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
			let remainder = current_time % config::SNAPSHOT_CALCULATION_INTERVAL as u64;
			let time_until_next_day = config::SNAPSHOT_CALCULATION_INTERVAL as u64 - remainder;

			log_info!(self.logger, "Sleeping until next snapshot capture: {}s", time_until_next_day);
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
