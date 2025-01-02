///
/// This module provides a lock manager to create "managed" locks.
///
/// Managed lock's lifecycle is managed by the lock manager background thread, that includes:
///    - Automatic lease refresh
///    - Lock revocation when the lock is dropped
///
/// You can clone [`LockManager`] to share it across threads, it is really cheap to do so.
///
/// See [`spawn_lock_manager`] to create a new lock manager.
///
/// # Examples
///
/// ```no_run
///
/// use etcd_client::Client;
/// use rust_etcd_utils::{lease::ManagedLeaseFactory, lock::spawn_lock_manager, ManagedLock};
///
/// let etcd = Client::connect(["http://localhost:2379"], None).await.expect("failed to connect to etcd");
///
/// let managed_lease_factory = ManagedLeaseFactory::new(etcd.clone());
///
/// let (lock_man_handle, lock_man) = spawn_lock_manager(etcd.clone(), managed_lease_factory.clone());
///
/// // Do something with the lock manager
///
/// let managed_lock: ManagedLock = lock_man.try_lock("test").await.expect("failed to lock");
///
///
/// drop(lock_man);
///
/// // Wait for the lock manager background thread to stop
/// lock_man_handle.await.expect("failed to release lock manager handle");
/// ```
use {
    super::{
        lease::{ManagedLease, ManagedLeaseFactory},
        retry::retry_etcd_legacy,
        Revision,
    },
    crate::{
        lease::{LeaseExpiredNotify, ManagedLeaseWeak},
        retry::{retry_etcd, retry_etcd_txn},
        watcher::WatchClientExt,
    },
    core::fmt,
    etcd_client::{Compare, CompareOp, GetOptions, LockOptions, Txn, TxnOp, TxnResponse},
    futures::{future::join_all, FutureExt},
    retry::delay::Fixed,
    std::{
        future::Future,
        pin::Pin,
        task::{Context, Poll},
        time::Duration,
    },
    thiserror::Error,
    tokio::{
        sync::{broadcast, mpsc},
        task::{JoinError, JoinHandle},
    },
    tonic::Code,
    tracing::{info, trace},
};

enum DeleteQueueCommand {
    Delete(Vec<u8>),
}

///
/// A lock manager to create "managed" locks.
///
/// Managed lock's lifecycle is managed by the lock manager background thread, that includes:
///     - Automatic lease refresh
///     - Lock revocation when the lock is dropped
///
/// You can clone [`LockManager`] to share it across threads, it is really cheap to do so.
///
/// See [`spawn_lock_manager`] to create a new lock manager.
///
#[derive(Clone)]
pub struct LockManager {
    etcd: etcd_client::Client,
    delete_queue_tx: mpsc::UnboundedSender<DeleteQueueCommand>,
    manager_lease_factory: ManagedLeaseFactory,
    try_locking_timeout: Duration,
    #[allow(dead_code)]
    // When all Sender to this channel is dropped, the Lock manager background thread will stop.
    lock_manager_handle_entangled_tx: mpsc::UnboundedSender<()>,
}

///
/// Handle to the lock manager background thread.
///
/// See [`spawn_lock_manager`] to create a new lock manager.
pub struct LockManagerHandle {
    inner: JoinHandle<()>,
}

impl Future for LockManagerHandle {
    type Output = Result<(), JoinError>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.inner.poll_unpin(cx)
    }
}

///
/// Used to notify when a lock is revoked.
///
/// Examples
///
/// ```no_run
///
/// use etcd_client::Client;
/// use rust_etcd_utils::{lease::ManagedLeaseFactory, lock::spawn_lock_manager, ManagedLock};
///
/// let etcd = Client::connect(["http://localhost:2379"], None).await.expect("failed to connect to etcd");
///
/// let managed_lease_factory = ManagedLeaseFactory::new(etcd.clone());
///
/// let (lock_man_handle, lock_man) = spawn_lock_manager(etcd.clone(), managed_lease_factory.clone());
///
/// // Do something with the lock manager
///
/// let managed_lock: ManagedLock = lock_man.try_lock("test").await.expect("failed to lock");
///
/// let revoke_notify = managed_lock.get_revoke_notify();
///
/// // Can be cloned
/// let revoke_notify2 = revoke_notify.clone();
///
/// // Can create multiple instances..
///
/// let revoke_notify3 = managed_lock.get_revoke_notify();
///
/// etcd.delete("test", None).await.expect("failed to delete");
///
/// revoke_notify.wait_for_revoke().await;
/// revoke_notify2.wait_for_revoke().await;
/// revoke_notify3.wait_for_revoke().await;
///
/// println!("All revoke notify received");
/// ```
///
pub struct ManagedLockRevokeNotify {
    watch_lock_delete: broadcast::Receiver<Revision>,
    lease_expired_notify: LeaseExpiredNotify,
}

impl Clone for ManagedLockRevokeNotify {
    fn clone(&self) -> Self {
        Self {
            watch_lock_delete: self.watch_lock_delete.resubscribe(),
            lease_expired_notify: self.lease_expired_notify.clone(),
        }
    }
}

impl ManagedLockRevokeNotify {
    ///
    /// Wait for the lock to be revoked.
    ///
    pub async fn wait_for_revoke(mut self) {
        tokio::select! {
            _ = self.lease_expired_notify.recv() => {}
            _ = self.watch_lock_delete.recv() => {}
        }
    }
}

///
/// Creates a lock manager to create "managed" locks.
///
/// Managed lock's lifecycle is managed by the lock manager background thread, that includes:
///     - Automatic lease refresh
///     - Lock revocation when the lock is dropped
///
/// You can clone [[`LockManager`]] to share it across threads, it is really cheap to do so.
///
/// The lock manager background thread will stop when all the [[`LockManager`]] is dropped.
/// You can await on the [[`LockManagerHandle`]] to wait for the lock manager background thread to stop.
///
/// Dropping the [[`LockManagerHandle`]] will not stop the lock manager background thread, but it is not recommended to do so
/// as you are suppoed to `await` the handle to gracefully shutdown.
///
/// Examples
///
/// ```no_run
/// use etcd_client::Client;
/// use rust_etcd_utils::{lease::ManagedLeaseFactory, lock::spawn_lock_manager, ManagedLock};
///
/// let etcd = Client::connect(["http://localhost:2379"], None).await.expect("failed to connect to etcd");
///
/// let managed_lease_factory = ManagedLeaseFactory::new(etcd.clone());
///
/// let (lock_man_handle, lock_man) = spawn_lock_manager(etcd.clone(), managed_lease_factory.clone());
///
/// // Do something with the lock manager
///
/// let managed_lock: ManagedLock = lock_man.try_lock("test").await.expect("failed to lock");
///
///
/// drop(lock_man);
///
/// // Wait for the lock manager background thread to stop
/// lock_man_handle.await.expect("failed to release lock manager handle");
/// ```
///
/// Cloning the lock manager is cheap and can be shared across threads.
///
/// ```no_run
///
/// use etcd_client::Client;
/// use rust_etcd_utils::{lease::ManagedLeaseFactory, lock::spawn_lock_manager, lock::ManagedLock, lock::TryLockError};
///
/// let etcd = Client::connect(["http://localhost:2379"], None).await.expect("failed to connect to etcd");
/// let managed_lease_factory = ManagedLeaseFactory::new(etcd.clone());
/// let (_, lock_man) = spawn_lock_manager(etcd.clone(), managed_lease_factory.clone());
///     
/// let lock_man2 = lock_man.clone();
/// let task1 = tokio::spawn(async move {
///     let managed_lock: ManagedLock = lock_man2.try_lock("test").await.expect("failed to lock");
///
///     tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
/// });
///
///
/// let err = lock_man.try_lock("test").await;
///
/// assert!(matches!(err, Err(TryLockError::AlreadyTaken)));
/// ```
///
pub fn spawn_lock_manager(
    etcd: etcd_client::Client,
    managed_lease_factory: ManagedLeaseFactory,
) -> (LockManagerHandle, LockManager) {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let etcd2 = etcd.clone();

    let (entangled_tx, mut entangled_rx) = tokio::sync::mpsc::unbounded_channel();
    let tx2 = tx.clone();
    let handle = tokio::spawn(async move {
        let _tx2 = tx2;
        loop {
            let cmd = tokio::select! {
                cmd = rx.recv() => {
                    cmd.expect("command rx droped")
                }
                maybe = entangled_rx.recv() => {
                    match maybe {
                        Some(_) => unreachable!("entangled_rx should not have any message"),
                        None => {
                            // If entangled_rx is closed, we should stop the loop because it means there is no LockManager instance alive.
                            break
                        },
                    }
                }
            };
            match cmd {
                DeleteQueueCommand::Delete(lock_id) => {
                    let kv_client = etcd2.kv_client();
                    let lock_id2 = lock_id.clone();
                    let result = retry_etcd_legacy(Fixed::from_millis(10), move || {
                        let lock_id = lock_id2.clone();
                        let mut kv_client = kv_client.clone();
                        async move { kv_client.delete(lock_id, None).await }
                    })
                    .await;
                    match result {
                        Ok(_) => {
                            let lock_id = String::from_utf8(lock_id).expect("lock id is not utf8");
                            info!("Deleted lock {lock_id}");
                        }
                        Err(e) => {
                            if !matches!(e, etcd_client::Error::GRpcStatus(ref status) if status.code() == Code::NotFound)
                            {
                                tracing::error!("Failed to revoke lock: {e}");
                                // panic!("Failed to delete lock: {e}");
                            }
                        }
                    }
                }
            }
        }
        let mut futures = vec![];
        // Drain any remaining delete commands
        while let Ok(cmd) = rx.try_recv() {
            match cmd {
                DeleteQueueCommand::Delete(lock_id) => {
                    let mut kv_client = etcd2.kv_client();
                    let fut = async move { kv_client.delete(lock_id, None).await };
                    futures.push(fut);
                }
            }
        }
        // Since we are closing the channel, we can ignore the result of the futures
        let _ = join_all(futures).await;
    });
    let handle = LockManagerHandle { inner: handle };
    (
        handle,
        LockManager {
            etcd,
            delete_queue_tx: tx,
            try_locking_timeout: Duration::from_secs(1),
            manager_lease_factory: managed_lease_factory,
            lock_manager_handle_entangled_tx: entangled_tx,
        },
    )
}

///
/// Error that can occur when trying to lock a key.
///
#[derive(Debug, thiserror::Error)]
pub enum LockingError {
    #[error("Etcd error: {0:?}")]
    EtcdError(etcd_client::Error),
}

impl LockManager {
    ///
    /// Tries to lock a key with automatic lease refresh and lock revocation when dropped.
    ///
    /// If the key is already held by another lock, it will return an error immediately.
    ///
    pub async fn try_lock<S>(
        &self,
        name: S,
        lease_duration: Duration,
    ) -> Result<ManagedLock, TryLockError>
    where
        S: AsRef<str>,
    {
        let name = name.as_ref();
        if self.delete_queue_tx.is_closed() {
            panic!("LockManager lifecycle thread is stopped.");
        }
        let gopts = GetOptions::new().with_prefix();
        trace!("Trying to lock {name}...");
        let get_response = retry_etcd(
            self.etcd.clone(),
            (name.to_string(), gopts),
            move |etcd, (name, gopts)| async move { etcd.kv_client().get(name, Some(gopts)).await },
        )
        .await
        .map_err(TryLockError::EtcdError)?;

        if get_response.count() > 0 {
            return Err(TryLockError::AlreadyTaken);
        }

        let managed_lease = self
            .manager_lease_factory
            .new_lease(lease_duration, None)
            .await
            .map_err(TryLockError::EtcdError)?;
        let lease_id = managed_lease.lease_id;

        let lock_fut = retry_etcd(
            self.etcd.clone(),
            (name.to_string(), LockOptions::new().with_lease(lease_id)),
            |mut etcd, (name, opts)| async move { etcd.lock(name, Some(opts)).await },
        );

        let lease_expire_notify = managed_lease.get_lease_expire_notify();

        let (revision, lock_key) = tokio::select! {
            _ = tokio::time::sleep(self.try_locking_timeout) => {
                return Err(TryLockError::LockingDeadlineExceeded)
            }
            result = lock_fut => {
                let lock_response = match result {
                    Ok(lock_response) => lock_response,
                    Err(e) => {
                        match e {
                            etcd_client::Error::GRpcStatus(status) => {
                                if status.code() == Code::Unknown {
                                    if status.message() == "etcdserver: requested lease not found" {
                                        return Err(TryLockError::LeaseExpired)
                                    } else {
                                        return Err(TryLockError::EtcdError(etcd_client::Error::GRpcStatus(status)))
                                    }
                                } else {
                                    return Err(TryLockError::EtcdError(etcd_client::Error::GRpcStatus(status)))
                                }
                            }
                            _ => return Err(TryLockError::EtcdError(e))
                        }
                    }
                };
                (lock_response.header().expect("empty header for etcd lock").revision(), lock_response.key().to_vec())
            }
            _ = lease_expire_notify.recv() => {
                return Err(TryLockError::LeaseExpired)
            }
        };

        let watch_lock_delete = self
            .etcd
            .watch_client()
            .watch_lock_key_change(lock_key.clone(), revision);
        Ok(ManagedLock {
            lock_key,
            managed_lease,
            etcd: self.etcd.clone(),
            created_at_revision: revision,
            delete_signal_tx: self.delete_queue_tx.clone(),
            revoke_callback_rx: watch_lock_delete.subscribe(),
        })
    }

    ///
    /// Similar to [`LockManager::try_lock`] but with a custom lease.
    ///
    pub async fn try_lock_with_lease<S>(
        &self,
        name: S,
        managed_lease: ManagedLease,
    ) -> Result<ManagedLock, TryLockError>
    where
        S: AsRef<str>,
    {
        let name = name.as_ref();
        if self.delete_queue_tx.is_closed() {
            panic!("LockManager lifecycle thread is stopped.");
        }
        let gopts = GetOptions::new().with_prefix();
        const TRY_LOCKING_DURATION: Duration = Duration::from_millis(1000);
        trace!("Trying to lock {name}...");
        let get_response = retry_etcd(
            self.etcd.clone(),
            (name.to_string(), gopts),
            move |etcd, (name, gopts)| async move { etcd.kv_client().get(name, Some(gopts)).await },
        )
        .await
        .map_err(TryLockError::EtcdError)?;

        if get_response.count() > 0 {
            return Err(TryLockError::AlreadyTaken);
        }

        let lease_id = managed_lease.lease_id;

        let lock_fut = retry_etcd(
            self.etcd.clone(),
            (name.to_string(), LockOptions::new().with_lease(lease_id)),
            |mut etcd, (name, opts)| async move { etcd.lock(name, Some(opts)).await },
        );

        let lease_expire_notify = managed_lease.get_lease_expire_notify();
        let (revision, lock_key) = tokio::select! {
            _ = tokio::time::sleep(TRY_LOCKING_DURATION) => {
                return Err(TryLockError::LockingDeadlineExceeded)
            }
            result = lock_fut => {
                let lock_response = match result {
                    Ok(lock_response) => lock_response,
                    Err(e) => {
                        match e {
                            etcd_client::Error::GRpcStatus(status) => {
                                if status.code() == Code::Unknown {
                                    if status.message() == "etcdserver: requested lease not found" {
                                        return Err(TryLockError::LeaseExpired)
                                    } else {
                                        return Err(TryLockError::EtcdError(etcd_client::Error::GRpcStatus(status)))
                                    }
                                } else {
                                    return Err(TryLockError::EtcdError(etcd_client::Error::GRpcStatus(status)))
                                }
                            }
                            _ => return Err(TryLockError::EtcdError(e))
                        }
                    }
                };

                (lock_response.header().expect("empty header for etcd lock").revision(), lock_response.key().to_vec())
            }
            _ = lease_expire_notify.recv() => {
                return Err(TryLockError::LeaseExpired)
            }
        };

        let watch_lock_delete = self
            .etcd
            .watch_client()
            .watch_lock_key_change(lock_key.clone(), revision);
        Ok(ManagedLock {
            lock_key,
            managed_lease,
            etcd: self.etcd.clone(),
            created_at_revision: revision,
            delete_signal_tx: self.delete_queue_tx.clone(),
            revoke_callback_rx: watch_lock_delete.subscribe(),
        })
    }

    ///
    /// Locks a key with automatic lease refresh and lock revocation when dropped.
    /// If the key is already held by another lock, it will wait until the lock is released.
    ///
    /// Be aware this method can await indefinitely if the key is never released.
    pub async fn lock<S>(
        &self,
        name: S,
        lease_duration: Duration,
    ) -> Result<ManagedLock, etcd_client::Error>
    where
        S: AsRef<str>,
    {
        if self.delete_queue_tx.is_closed() {
            panic!("LockManager lifecycle thread is stopped.");
        }
        let managed_lease = self
            .manager_lease_factory
            .new_lease(lease_duration, None)
            .await?;
        self.lock_with_lease(name, managed_lease).await
    }

    ///
    /// Similar to [`LockManager::lock`] but with a custom lease.
    ///
    pub async fn lock_with_lease<S>(
        &self,
        name: S,
        managed_lease: ManagedLease,
    ) -> Result<ManagedLock, etcd_client::Error>
    where
        S: AsRef<str>,
    {
        if self.delete_queue_tx.is_closed() {
            panic!("LockManager lifecycle thread is stopped.");
        }
        let name = name.as_ref();

        let lease_id = managed_lease.lease_id;

        let lock_fut = retry_etcd(
            self.etcd.clone(),
            (name.to_string(), LockOptions::new().with_lease(lease_id)),
            |mut etcd, (name, opts)| async move { etcd.lock(name, Some(opts)).await },
        );

        let lock_response = tokio::select! {
            result = lock_fut => {
                result?
            }
        };

        let (revision, lock_key) = (
            lock_response
                .header()
                .expect("empty header for etcd lock")
                .revision(),
            lock_response.key().to_vec(),
        );

        let watch_lock_delete = self
            .etcd
            .watch_client()
            .watch_lock_key_change(lock_key.clone(), revision);

        let managed_lock = ManagedLock {
            lock_key,
            managed_lease,
            etcd: self.etcd.clone(),
            created_at_revision: revision,
            delete_signal_tx: self.delete_queue_tx.clone(),
            revoke_callback_rx: watch_lock_delete.subscribe(),
        };

        Ok(managed_lock)
    }
}

///
/// A Lock instance with automatic lease refresh and lock revocation when dropped.
///
pub struct ManagedLock {
    pub(crate) lock_key: Vec<u8>,
    managed_lease: ManagedLease,
    pub created_at_revision: Revision,
    pub(crate) etcd: etcd_client::Client,
    delete_signal_tx: tokio::sync::mpsc::UnboundedSender<DeleteQueueCommand>,
    revoke_callback_rx: broadcast::Receiver<Revision>,
}

impl fmt::Debug for ManagedLock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ManagedLock")
            .field("lock_key", &String::from_utf8_lossy(&self.lock_key))
            .field("lease_id", &self.managed_lease.lease_id)
            .field("created_At_revision", &self.created_at_revision)
            .finish()
    }
}

impl Drop for ManagedLock {
    fn drop(&mut self) {
        info!(
            "Destructor called for ManagedLock({})",
            String::from_utf8_lossy(&self.lock_key)
        );
        let _ = self
            .delete_signal_tx
            .send(DeleteQueueCommand::Delete(self.lock_key.clone()));
    }
}

///
/// Error that can occur when using a managed lock.
///
#[derive(Debug)]
pub enum LockError {
    LockRevoked,
}

impl fmt::Display for LockError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LockError::LockRevoked => f.write_str("lock revoked"),
        }
    }
}

///
/// Acquired only via [`ManagedLock::scope`] or [`ManagedLock::scope_with`].
///
/// This guard represents the scope of the managed lock, and can be used to express restriction on code execution so
/// that it is only executed within the lock lifetime.
///
/// This guard can also be used with other modules in the library such as the `log` module.
///
pub struct ManagedLockGuard<'a> {
    pub(crate) managed_lock: &'a ManagedLock,
}

impl<'a> ManagedLockGuard<'a> {
    pub(crate) fn get_key(&self) -> &[u8] {
        self.managed_lock.lock_key.as_slice()
    }
}

impl ManagedLock {
    pub fn lease_id(&self) -> i64 {
        self.managed_lease.lease_id
    }

    ///
    /// Execute an etcd transaction if and only if the lock is still alive.
    ///
    pub async fn txn(&self, operations: impl Into<Vec<TxnOp>>) -> TxnResponse {
        let txn = Txn::new()
            .when(vec![Compare::version(
                self.lock_key.clone(),
                CompareOp::Greater,
                0,
            )])
            .and_then(operations);

        retry_etcd_txn(self.etcd.clone(), txn)
            .await
            .expect("failed txn")
    }

    ///
    /// Get a revoke notify handle to be notified when the lock is revoked.
    ///
    pub fn get_revoke_notify(&self) -> ManagedLockRevokeNotify {
        ManagedLockRevokeNotify {
            watch_lock_delete: self.revoke_callback_rx.resubscribe(),
            lease_expired_notify: self.managed_lease.get_lease_expire_notify(),
        }
    }

    ///
    /// Check if the lock is still alive.
    ///
    pub async fn is_alive(&self) -> bool {
        let get_response = self
            .etcd
            .kv_client()
            .get(self.lock_key.as_slice(), None)
            .await
            .expect("failed to communicate with etcd");
        get_response.count() == 1
    }

    ///
    /// Get the underlying unique lock key.
    ///
    pub fn get_key(&self) -> Vec<u8> {
        self.lock_key.clone()
    }

    ///
    /// This function make sure the future is executed within a valid managed lock lifetime.
    ///
    /// If the lock is revoked, it will cancel the future and return a LockError::LockRevoked.
    ///
    /// Make sure the future returned by the closure is cancel safe.
    ///
    /// Examples
    ///
    /// ```no_run
    /// use etcd_client::Client;
    /// use rust_etcd_utils::{lease::ManagedLeaseFactory, lock::spawn_lock_manager, ManagedLock};
    ///
    /// let etcd = Client::connect(["http://localhost:2379"], None).await.expect("failed to connect to etcd");
    ///
    /// let managed_lease_factory = ManagedLeaseFactory::new(etcd.clone());
    ///
    /// let (lock_man_handle, lock_man) = spawn_lock_manager(etcd.clone(), managed_lease_factory.clone());
    ///
    /// // Do something with the lock manager
    ///
    /// let managed_lock: ManagedLock = lock_man.try_lock("test").await.expect("failed to lock");
    ///
    /// managed_lock.scope(async move {
    ///    // execute only if my lock is valid
    ///    access_protected_ressource().await;
    /// });
    ///
    /// ```
    pub async fn scope<T, Fut>(&self, fut: Fut) -> Result<T, LockError>
    where
        T: Send + 'static,
        Fut: Future<Output = T> + Send + 'static,
    {
        self.scope_with(move |_| fut).await
    }

    ///
    /// Similar to [`ManagedLock::scope`] but accept a closure to compute the future to execute against the lock.
    ///
    pub async fn scope_with<'a, T, F, Fut>(&'a self, func: F) -> Result<T, LockError>
    where
        T: Send + 'a,
        F: FnOnce(ManagedLockGuard<'a>) -> Fut,
        Fut: Future<Output = T> + Send + 'a,
    {
        let mut rx = self.revoke_callback_rx.resubscribe();

        match rx.try_recv() {
            Ok(_) => {
                return Err(LockError::LockRevoked);
            }
            Err(broadcast::error::TryRecvError::Closed) => {
                return Err(LockError::LockRevoked);
            }
            _ => {}
        }
        tokio::select! {
            result = func(ManagedLockGuard { managed_lock: self }) => Ok(result),
            _ = rx.recv() => Err(LockError::LockRevoked),
        }
    }

    ///
    /// Get a weak reference to the managed lease.
    ///
    pub fn get_managed_lease_weak_ref(&self) -> ManagedLeaseWeak {
        self.managed_lease.get_weak()
    }
}

///
/// Error that can occur when trying to lock a key.
///
#[derive(Debug, Error)]
pub enum TryLockError {
    #[error("Already taken")]
    AlreadyTaken,
    #[error("Locking deadline exceeded")]
    LockingDeadlineExceeded,
    #[error("Lease expired before the lock")]
    LeaseExpired,
    #[error("Etcd error: {0:?}")]
    EtcdError(etcd_client::Error),
}
