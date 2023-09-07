use std::sync::Arc;

use lightning::sign::KeysManager;
use lightning::ln::msgs::{ChannelAnnouncement, ChannelUpdate};
use lightning::ln::peer_handler::{ErroringMessageHandler, IgnoringMessageHandler, PeerManager};
use lightning::util::logger::{Logger, Record};
use crate::config;

use crate::downloader::GossipRouter;
use crate::verifier::ChainVerifier;

pub(crate) type GossipChainAccess<L> = Arc<ChainVerifier<L>>;
pub(crate) type GossipPeerManager<L> = Arc<PeerManager<lightning_net_tokio::SocketDescriptor, ErroringMessageHandler, Arc<GossipRouter<L>>, IgnoringMessageHandler, L, IgnoringMessageHandler, Arc<KeysManager>>>;

#[derive(Debug)]
pub(crate) enum GossipMessage {
	ChannelAnnouncement(
		ChannelAnnouncement,
		Option<u32> // optionally include a persistence timestamp
	),
	ChannelUpdate(ChannelUpdate),
}

#[derive(Clone, Copy)]
pub struct RGSSLogger {}

impl RGSSLogger {
	pub fn new() -> RGSSLogger {
		Self {}
	}
}

impl Logger for RGSSLogger {
	fn log(&self, record: &Record) {
		let threshold = config::log_level();
		if record.level < threshold {
			return;
		}
		println!("{:<5} [{} : {}, {}] {}", record.level.to_string(), record.module_path, record.file, record.line, record.args);
	}
}

#[cfg(test)]
pub mod tests {
	use std::collections::HashMap;
	use std::sync::{Mutex};
	use lightning::util::logger::{Level, Logger, Record};

	pub struct TestLogger {
		level: Level,
		pub(crate) id: String,
		pub lines: Mutex<HashMap<(String, String), usize>>,
	}

	impl TestLogger {
		pub fn new() -> TestLogger {
			Self::with_id("".to_owned())
		}
		pub fn with_id(id: String) -> TestLogger {
			TestLogger {
				level: Level::Trace,
				id,
				lines: Mutex::new(HashMap::new()),
			}
		}
		pub fn enable(&mut self, level: Level) {
			self.level = level;
		}
		pub fn assert_log(&self, module: String, line: String, count: usize) {
			let log_entries = self.lines.lock().unwrap();
			assert_eq!(log_entries.get(&(module, line)), Some(&count));
		}

		/// Search for the number of occurrence of the logged lines which
		/// 1. belongs to the specified module and
		/// 2. contains `line` in it.
		/// And asserts if the number of occurrences is the same with the given `count`
		pub fn assert_log_contains(&self, module: &str, line: &str, count: usize) {
			let log_entries = self.lines.lock().unwrap();
			let l: usize = log_entries.iter().filter(|&(&(ref m, ref l), _c)| {
				m == module && l.contains(line)
			}).map(|(_, c)| { c }).sum();
			assert_eq!(l, count)
		}
	}

	impl Logger for TestLogger {
		fn log(&self, record: &Record) {
			*self.lines.lock().unwrap().entry((record.module_path.to_string(), format!("{}", record.args))).or_insert(0) += 1;
			if record.level >= self.level {
				// #[cfg(feature = "std")]
				println!("{:<5} {} [{} : {}, {}] {}", record.level.to_string(), self.id, record.module_path, record.file, record.line, record.args);
			}
		}
	}
}
