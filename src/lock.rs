use {
    super::{
        lease::{ManagedLease, ManagedLeaseFactory},
        retry::retry_etcd_legacy,
        Revision,
    },
    crate::retry::{retry_etcd, retry_etcd_txn},
    core::fmt,
    etcd_client::{Compare, CompareOp, GetOptions, LockOptions, Txn, TxnOp, TxnResponse},
    futures::{future::join_all, FutureExt},
    retry::delay::Fixed,
    std::{future::Future, pin::Pin, task::{Context, Poll}, time::Duration},
    thiserror::Error,
    tokio::{sync::{mpsc, oneshot}, task::{JoinError, JoinHandle}},
    tonic::Code,
    tracing::{info, trace},
};


enum DeleteQueueCommand {
    Delete(Vec<u8>, mpsc::UnboundedSender<()>),
}

#[derive(Clone)]
pub struct LockManager {
    etcd: etcd_client::Client,
    delete_queue_tx: mpsc::UnboundedSender<DeleteQueueCommand>,
    manager_lease_factory: ManagedLeaseFactory,

    #[allow(dead_code)]
    // When all Sender to this channel is dropped, the Lock manager background thread will stop.
    lock_manager_handle_entangled_tx: mpsc::UnboundedSender<()>,
}


pub struct LockManagerHandle { 
    inner: JoinHandle<()>
}

impl Future for LockManagerHandle {
    type Output = Result<(), JoinError>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.inner.poll_unpin(cx)
    }
}

pub struct ManagedLockDeleteCallback {
    delete_callback_rx: mpsc::UnboundedReceiver<()>,
}

impl Future for ManagedLockDeleteCallback {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.delete_callback_rx.poll_recv(cx) {
            Poll::Ready(Some(_)) => Poll::Ready(()),
            Poll::Ready(None) => Poll::Ready(()),
            Poll::Pending => Poll::Pending,
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
/// ```
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
/// ```
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
) -> (LockManagerHandle, LockManager) 
{
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
                DeleteQueueCommand::Delete(lock_id, delete_callback) => {
                    let kv_client = etcd2.kv_client();
                    let lock_id2 = lock_id.clone();
                    let result = retry_etcd_legacy(Fixed::from_millis(10), move || {
                        let lock_id = lock_id2.clone();
                        let mut kv_client = kv_client.clone();
                        async move { kv_client.delete(lock_id, None).await }
                    })
                    .await;
                    let _ = delete_callback.send(());
                    match result {
                        Ok(_) => {
                            let lock_id =
                                String::from_utf8(lock_id).expect("lock id is not utf8");
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
                DeleteQueueCommand::Delete(lock_id, delete_callback) => {
                    let mut kv_client = etcd2.kv_client();
                    let fut = async move {
                        let result = kv_client.delete(lock_id, None).await;
                        let _ = delete_callback.send(());
                        result
                    };
                    futures.push(fut);
                }
            }
        }
        // Since we are closing the channel, we can ignore the result of the futures
        let _ = join_all(futures).await;
    });
    let handle = LockManagerHandle { inner: handle };
    (handle, LockManager {
        etcd,
        delete_queue_tx: tx,
        manager_lease_factory: managed_lease_factory,
        lock_manager_handle_entangled_tx: entangled_tx,
    })
}


impl LockManager {

    pub async fn try_lock<S>(
        &self, 
        name: S, 
        lease_duration: Duration
    ) -> Result<ManagedLock, TryLockError>
        where S: AsRef<str>
    {
        self.try_lock_with_delete_callback(name, lease_duration)
            .await
            .map(|(lock, _)| lock)
    }

    pub async fn try_lock_with_delete_callback<S>(
        &self, 
        name: S, 
        lease_duration: Duration
    ) -> Result<(ManagedLock, ManagedLockDeleteCallback), TryLockError> 
        where S: AsRef<str>
    {
        let name = name.as_ref();
        if self.delete_queue_tx.is_closed() {
            panic!("LockManager lifecycle thread is stopped.");
        }
        let gopts = GetOptions::new().with_prefix();
        const TRY_LOCKING_DURATION: Duration = Duration::from_millis(500);
        trace!("Trying to lock {name}...");
        let get_response = retry_etcd(
            self.etcd.clone(),
            (name.to_string(), gopts),
            move |etcd, (name, gopts)| async move { etcd.kv_client().get(name, Some(gopts)).await },
        )
        .await
        .expect("failed to communicate with etcd");

        if get_response.count() > 1 {
            return Err(TryLockError::InvalidLockName);
        }
        if get_response.count() == 1 {
            return Err(TryLockError::AlreadyTaken);
        }

        let managed_lease = self
            .manager_lease_factory
            .new_lease(lease_duration, None)
            .await;
        let lease_id = managed_lease.lease_id;

        let lock_fut = retry_etcd(
            self.etcd.clone(), 
            (name.to_string(), LockOptions::new().with_lease(lease_id)), 
            |mut etcd, (name, opts)| {
                async move { etcd.lock(name, Some(opts)).await }
            }
        );

        let (revision, lock_key) = tokio::select! {
            _ = tokio::time::sleep(TRY_LOCKING_DURATION) => {
                return Err(TryLockError::LockingDeadlineExceeded)
            }
            result = lock_fut => {
                let lock_response = result.expect("failed to lock");
                (lock_response.header().expect("empty header for etcd lock").revision(), lock_response.key().to_vec())
            }
        };

        let (delete_callback_tx, delete_callback_rx) = mpsc::unbounded_channel();

        Ok((ManagedLock {
            lock_key,
            managed_lease,
            etcd: self.etcd.clone(),
            created_at_revision: revision,
            delete_signal_tx: self.delete_queue_tx.clone(),
            delete_callback_tx,
        }, ManagedLockDeleteCallback { delete_callback_rx }),
        )
    }

}

///
/// A Lock instance with automatic lease refresh and lock revocation when dropped.
///
pub struct ManagedLock {
    pub(crate) lock_key: Vec<u8>,
    managed_lease: ManagedLease,
    pub created_at_revision: Revision,
    etcd: etcd_client::Client,
    delete_signal_tx: tokio::sync::mpsc::UnboundedSender<DeleteQueueCommand>,
    delete_callback_tx: mpsc::UnboundedSender<()>,
}

impl Drop for ManagedLock {
    fn drop(&mut self) {
        let _ = self
            .delete_signal_tx
            .send(DeleteQueueCommand::Delete(self.lock_key.clone(), self.delete_callback_tx.clone()));
    }
}

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

impl ManagedLock {
    pub fn lease_id(&self) -> i64 {
        self.managed_lease.lease_id
    }

    pub async fn txn(&self, operations: impl Into<Vec<TxnOp>>) -> TxnResponse {
        let txn = Txn::new()
            .when(vec![Compare::version(
                self.lock_key.clone(),
                CompareOp::Greater,
                0,
            )])
            .and_then(operations);
        
        retry_etcd_txn(self.etcd.clone(), txn).await.expect("failed txn")
    }

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
    /// This function make sure the future is executed within a managed lock.
    /// If the lock is revoked, it will return an error.
    /// If the lock is orphaned, it will return an error.
    ///
    /// A lock become orphan if the lock manager is stopped.
    pub async fn scope<T, F, Fut>(&self, f: F) -> Result<T, LockError>
    where
        T: Send + 'static,
        F: FnOnce() -> Fut,
        Fut: Future<Output = T> + Send + 'static,
    {
        if !self.is_alive().await {
            return Err(LockError::LockRevoked);
        }
        Ok(f().await)
    }
}

#[derive(Debug, PartialEq, Eq, Error)]
pub enum TryLockError {
    InvalidLockName,
    AlreadyTaken,
    LockingDeadlineExceeded,
}

impl fmt::Display for TryLockError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidLockName => f.write_str("InvalidLockName"),
            Self::AlreadyTaken => f.write_str("AlreadyTaken"),
            Self::LockingDeadlineExceeded => f.write_str("LockingDeadlineExceeded"),
        }
    }
}
