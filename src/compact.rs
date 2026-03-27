use chrono::Utc;
use tokio::time::{Duration, sleep};
use tracing::error;

use crate::config::SharedConfig;
use crate::debug_sync::{DebugRuntime, process_due_reconciliation_jobs};
use crate::store::Store;
use std::sync::Arc;

pub fn spawn_compactor(config: SharedConfig, store: Store, debug_runtime: Arc<DebugRuntime>) {
    let compactor_store = store.clone();
    tokio::spawn(async move {
        loop {
            let snapshot = config.read().await.clone();
            if let Err(error) = compactor_store.compact_old_partitions(
                Utc::now(),
                snapshot.storage.compact_after_channel_days,
                snapshot.storage.compact_after_user_months,
            ) {
                error!("compactor iteration failed: {error}");
            }
            sleep(Duration::from_secs(
                snapshot.storage.compact_interval_seconds,
            ))
            .await;
        }
    });
    tokio::spawn(async move {
        loop {
            if let Err(error) =
                process_due_reconciliation_jobs(debug_runtime.clone(), store.clone(), Utc::now())
                    .await
            {
                error!("reconciliation iteration failed: {error}");
            }
            sleep(Duration::from_secs(60)).await;
        }
    });
}
