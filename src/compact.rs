use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::{Duration, sleep};
use tracing::error;

use crate::clock::SharedClock;
use crate::config::SharedConfig;
use crate::debug_sync::{DebugRuntime, process_due_reconciliation_jobs};
use crate::store::Store;
use std::sync::Arc;

pub struct CompactorTasks {
    compactor: JoinHandle<()>,
    reconciliation: JoinHandle<()>,
}

impl CompactorTasks {
    pub async fn shutdown(self) {
        let _ = self.compactor.await;
        let _ = self.reconciliation.await;
    }
}

pub fn spawn_compactor(
    config: SharedConfig,
    store: Store,
    debug_runtime: Arc<DebugRuntime>,
    clock: SharedClock,
    shutdown: watch::Receiver<bool>,
) -> CompactorTasks {
    let compactor_store = store.clone();
    let compactor_clock = clock.clone();
    let mut compactor_shutdown = shutdown.clone();
    let compactor = tokio::spawn(async move {
        loop {
            if *compactor_shutdown.borrow() {
                return;
            }
            let snapshot = config.read().await.clone();
            if let Err(error) = compactor_store.compact_old_partitions(
                compactor_clock.now_utc(),
                snapshot.storage.compact_after_channel_days,
                snapshot.storage.compact_after_user_months,
            ) {
                error!("compactor iteration failed: {error}");
            }
            tokio::select! {
                _ = compactor_shutdown.changed() => return,
                _ = sleep(Duration::from_secs(snapshot.storage.compact_interval_seconds)) => {}
            }
        }
    });
    let reconciliation_clock = clock;
    let mut reconciliation_shutdown = shutdown;
    let reconciliation = tokio::spawn(async move {
        loop {
            if *reconciliation_shutdown.borrow() {
                return;
            }
            if let Err(error) =
                process_due_reconciliation_jobs(
                    debug_runtime.clone(),
                    store.clone(),
                    reconciliation_clock.now_utc(),
                )
                .await
            {
                error!("reconciliation iteration failed: {error}");
            }
            tokio::select! {
                _ = reconciliation_shutdown.changed() => return,
                _ = sleep(Duration::from_secs(60)) => {}
            }
        }
    });
    CompactorTasks {
        compactor,
        reconciliation,
    }
}

#[cfg(test)]
mod tests {
    use super::spawn_compactor;
    use crate::clock::SharedClock;
    use crate::config::Config;
    use crate::debug_sync::DebugRuntime;
    use crate::store::Store;
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::Arc;
    use tempfile::TempDir;
    use tokio::sync::{RwLock, watch};

    fn test_store_and_config() -> (Store, Arc<RwLock<Config>>) {
        let temp = TempDir::new().unwrap();
        let root = temp.keep();
        let logs_directory = root.join("logs");
        let config_path = root.join("config.json");
        let mut config = Config {
            config_path,
            config_file_permissions: None,
            bot_verified: false,
            logs_directory,
            archive: true,
            admin_api_key: String::new(),
            username: "justinfan0".to_string(),
            oauth: String::new(),
            listen_address: "127.0.0.1:0".to_string(),
            admins: Vec::new(),
            channels: Vec::new(),
            client_id: String::new(),
            client_secret: String::new(),
            log_level: "info".to_string(),
            opt_out: HashMap::new(),
            compression: Default::default(),
            http: Default::default(),
            ingest: Default::default(),
            helix: Default::default(),
            irc: Default::default(),
            storage: Default::default(),
            ops: Default::default(),
        };
        config.storage.sqlite_path = PathBuf::from(root.join("justlog.sqlite3"));
        config.storage.compact_interval_seconds = 60;
        config.normalize().unwrap();
        let store = Store::open(&config).unwrap();
        (store, Arc::new(RwLock::new(config)))
    }

    #[tokio::test]
    async fn compactor_tasks_exit_after_shutdown_signal() {
        let (store, config) = test_store_and_config();
        let (tx, rx) = watch::channel(false);
        let tasks = spawn_compactor(
            config,
            store,
            Arc::new(DebugRuntime::disabled()),
            SharedClock::real(),
            rx,
        );
        let _ = tx.send(true);
        tasks.shutdown().await;
    }
}
