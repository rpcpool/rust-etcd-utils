use etcd_client::{Compare, CompareOp, Txn, TxnOp, WatchOptions};
use serde::{de::DeserializeOwned, Serialize};

use crate::{lock::ManagedLockGuard, retry::retry_etcd_txn, sync::watch, watcher::WatchClientExt};

pub struct LogWatcher<T> {
    rx: watch::Receiver<T>,
}

pub struct ExclusiveLogUpdater<'a, T> {
    etcd: etcd_client::Client,
    managed_lock_scope: ManagedLockGuard<'a>,
    last_log_version: i64,
    log_name: String,
    _phantom: std::marker::PhantomData<T>,
}

#[derive(Debug, thiserror::Error)]
pub enum WriteError {
    #[error("serialization error: {0}")]
    SerializationError(String),
    #[error("Failed to append to log due to lost exclusivity: lock or lease lost")]
    LostExclusivity,
    #[error("etcd error: {0}")]
    EtcdError(#[from] etcd_client::Error),
}

impl<'a, T> ExclusiveLogUpdater<'a, T>
where
    T: Serialize,
{
    pub async fn with_scope(
        log_name: impl AsRef<str>,
        lock_scope: ManagedLockGuard<'a>,
    ) -> Result<Self, etcd_client::Error> {
        let mut etcd = lock_scope.managed_lock.etcd.clone();
        let mut resp = etcd.get(log_name.as_ref(), None).await?;

        let last_log_version = resp
            .take_kvs()
            .into_iter()
            .max_by_key(|kv| kv.mod_revision())
            .map(|kv| kv.version())
            .unwrap_or(0);

        tracing::trace!("last log version: {}", last_log_version);

        Ok(Self {
            etcd,
            last_log_version,
            managed_lock_scope: lock_scope,
            log_name: log_name.as_ref().to_string(),
            _phantom: std::marker::PhantomData,
        })
    }

    pub async fn write(&mut self, value: T) -> Result<i64, WriteError> {
        let ser_value = serde_json::to_string(&value)
            .map_err(|e| WriteError::SerializationError(e.to_string()))?;

        tracing::trace!(
            "Writing to log `{}`, last version: {}",
            self.log_name,
            self.last_log_version
        );
        let txn = Txn::new()
            .when(vec![
                Compare::version(self.managed_lock_scope.get_key(), CompareOp::Greater, 0),
                Compare::version(
                    self.log_name.as_str(),
                    CompareOp::Equal,
                    self.last_log_version,
                ),
            ])
            .and_then(vec![TxnOp::put(self.log_name.as_str(), ser_value, None)]);

        let txn_resp = retry_etcd_txn(self.etcd.clone(), txn)
            .await
            .map_err(WriteError::EtcdError)?;

        if !txn_resp.succeeded() {
            tracing::warn!("Failed to append to protected log: {:?}", txn_resp);
            return Err(WriteError::LostExclusivity);
        }

        self.last_log_version += 1;
        Ok(self.last_log_version)
    }
}

#[derive(Debug, thiserror::Error)]
#[error("channel error: {0}")]
pub struct ReadError(String);

impl<T> LogWatcher<T>
where
    T: DeserializeOwned + Send + 'static,
{
    ///
    /// Creates a new log watcher for the given log name.
    ///
    pub async fn new(
        mut etcd: etcd_client::Client,
        log_name: impl AsRef<str>,
    ) -> Result<Self, etcd_client::Error> {
        let mut get_resp = etcd.get(log_name.as_ref(), None).await?;

        let maybe_watch_opts = get_resp
            .take_kvs()
            .into_iter()
            .map(|kv| kv.mod_revision())
            .max()
            .map(|max_mod_rev| WatchOptions::new().with_start_revision(max_mod_rev));

        let mut rx = etcd
            .watch_client()
            .json_put_watch_channel::<T>(log_name.as_ref(), maybe_watch_opts);

        let (mut wtx, wrx) = watch::watch::<T>();

        let _channel_handle = tokio::spawn(async move {
            loop {
                let (_revision, val) = rx.recv().await.expect("watch channel closed");
                let _ = wtx.update(val).await;
            }
        });

        Ok(Self { rx: wrx })
    }

    ///
    /// Observes the log for new entries.
    ///
    pub async fn observe(&mut self) -> Option<T> {
        self.rx.recv().await
    }
}
