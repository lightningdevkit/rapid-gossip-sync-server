use std::{
    env::{self, VarError}, io::Error, io::ErrorKind,
};
use rapid_gossip_sync_server::RapidSyncProcessor;

const WORKER_THREADS_ENV_VAR: &str = "RAPID_GOSSIP_SYNC_SERVER_WORKERS";

fn main() {
    let mut rt = tokio::runtime::Builder::new_multi_thread();
    match get_tokio_workers() {
        Ok(Some(workers)) => {
            println!("Using {workers} Tokio worker threads");
            rt.worker_threads(workers);
        }
        Err(e) => {
            println!("{WORKER_THREADS_ENV_VAR} var was set, but could not be used. {e}");
            return;
        }
        _ => println!("Using default number of Tokio worker threads"),
    }
    rt.enable_all()
        .build()
        .unwrap()
        .block_on(RapidSyncProcessor::new().start_sync())
}

fn get_tokio_workers() -> Result<Option<usize>, Error> {
    match env::var(WORKER_THREADS_ENV_VAR) {
        Ok(workers_string) => match workers_string.parse() {
            Ok(workers) => Ok(Some(workers)),
            Err(e) => Err(Error::new(ErrorKind::InvalidInput, format!("cannot parse usize: {e}"))),
        },
        Err(VarError::NotUnicode(v)) => Err(Error::new(ErrorKind::InvalidInput, format!("not unicode: {v:?}"))),
        Err(VarError::NotPresent) => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    #[test]
    fn test_get_tokio_workers_no_var_set() {
        // Clear the environment variable to simulate it not being present.
        env::remove_var(WORKER_THREADS_ENV_VAR);

        // In this case, we expect Ok(None).
        assert_eq!(get_tokio_workers().unwrap(), None);
    }

    #[test]
    fn test_get_tokio_workers_var_set() {
        // Set the environment variable to a valid usize.
        env::set_var(WORKER_THREADS_ENV_VAR, "10");

        // In this case, we expect Ok(Some(10)).
        assert_eq!(get_tokio_workers().unwrap(), Some(10));
    }

    #[test]
    fn test_get_tokio_workers_not_parsable_var_set() {
        // Set the environment variable to a not parsable value.
        env::set_var(WORKER_THREADS_ENV_VAR, "cannotparse");

        // In this case, we expect an error.
        assert!(get_tokio_workers().is_err());
    }
}