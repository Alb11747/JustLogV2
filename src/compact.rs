use chrono::Utc;
use tokio::time::{Duration, sleep};
use tracing::error;

use crate::config::SharedConfig;
use crate::store::Store;

pub fn spawn_compactor(config: SharedConfig, store: Store) {
    tokio::spawn(async move {
        loop {
            let snapshot = config.read().await.clone();
            if let Err(error) = store.compact_old_partitions(
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
}
