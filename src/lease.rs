use {
    crate::retry::retry_etcd,
    futures::StreamExt,
    std::time::Duration,
    tokio::{
        sync::{broadcast, mpsc, oneshot},
        task::{JoinHandle, JoinSet},
        time::Instant,
    },
    tracing::{error, warn},
};

// Jiffy is interval between system timer interrupts, typically 10ms for linux systems.
const AT_LEAST_10_JIFFIES: Duration = Duration::from_millis(100);

///
/// Managed lease instance that will keep the lease alive until it is dropped.
///
/// See [`ManagedLeaseFactory::new_lease`] for more information.
///
pub struct ManagedLease {
    etcd: etcd_client::Client,
    pub lease_id: i64,
    // Let this field dead, because when drop it will trigger a task to wake up and gracefully revoke lease.
    #[allow(dead_code)]
    _tx_terminate: oneshot::Sender<()>,

    rx_lease_expire: broadcast::Receiver<()>,
}

///
/// Weak reference to the managed lease.
///
/// This can be used to check if the lease is still alive.
/// The reference is weak because dropping this reference will not revoke the lease.
pub struct ManagedLeaseWeak {
    lease_id: i64,
    etcd: etcd_client::Client,
    rx_lease_expire: broadcast::Receiver<()>,
}

impl ManagedLeaseWeak {
    pub fn lease_id(&self) -> i64 {
        self.lease_id
    }

    pub fn get_lease_expire_notify(&self) -> LeaseExpiredNotify {
        LeaseExpiredNotify {
            inner: self.rx_lease_expire.resubscribe(),
        }
    }

    pub async fn is_alive(&self) -> Result<bool, etcd_client::Error> {
        let result = retry_etcd(
            self.etcd.clone(),
            (self.lease_id,),
            |etcd, (lease_id,)| async move {
                let resp = etcd.lease_client().time_to_live(lease_id, None).await?;
                Ok(resp.ttl() > 0)
            },
        )
        .await;

        match result {
            Ok(is_alive) => Ok(is_alive),
            Err(e) => match e {
                etcd_client::Error::GRpcStatus(status) => {
                    if status.code() == tonic::Code::NotFound {
                        Ok(false)
                    } else {
                        Err(etcd_client::Error::GRpcStatus(status))
                    }
                }
                _ => Err(e),
            },
        }
    }
}

impl ManagedLease {
    pub fn lease_id(&self) -> i64 {
        self.lease_id
    }

    pub fn get_lease_expire_notify(&self) -> LeaseExpiredNotify {
        LeaseExpiredNotify {
            inner: self.rx_lease_expire.resubscribe(),
        }
    }

    pub async fn is_alive(&self) -> Result<bool, etcd_client::Error> {
        let result = retry_etcd(
            self.etcd.clone(),
            (self.lease_id,),
            |etcd, (lease_id,)| async move {
                let resp = etcd.lease_client().time_to_live(lease_id, None).await?;
                Ok(resp.ttl() > 0)
            },
        )
        .await;

        match result {
            Ok(is_alive) => Ok(is_alive),
            Err(e) => match e {
                etcd_client::Error::GRpcStatus(status) => {
                    if status.code() == tonic::Code::NotFound {
                        Ok(false)
                    } else {
                        Err(etcd_client::Error::GRpcStatus(status))
                    }
                }
                _ => Err(e),
            },
        }
    }

    pub fn get_weak(&self) -> ManagedLeaseWeak {
        ManagedLeaseWeak {
            lease_id: self.lease_id,
            etcd: self.etcd.clone(),
            rx_lease_expire: self.rx_lease_expire.resubscribe(),
        }
    }
}

///
/// Managed lease factory that will create a new lease and keep it alive until it is dropped.
///
#[derive(Clone)]
pub struct ManagedLeaseFactory {
    cnc_tx: mpsc::Sender<ManagedLeaseRuntimeCommand>,
}

///
/// Notify when the lease has expired.
///
pub struct LeaseExpiredNotify {
    inner: broadcast::Receiver<()>,
}

impl LeaseExpiredNotify {
    ///
    /// Wait until the lease has expired.
    ///
    pub async fn recv(mut self) {
        let _ = self.inner.recv().await;
    }
}

impl Clone for LeaseExpiredNotify {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.resubscribe(),
        }
    }
}

struct CreateLeaseCommand {
    ttl: Duration,
    keepalive_interval: Option<Duration>,
    auto_refresh_limit: Option<usize>,
    callback: oneshot::Sender<Result<ManagedLease, CreateLeaseError>>,
}

enum ManagedLeaseRuntimeCommand {
    CreateLease(CreateLeaseCommand),
}

///
/// Managed lease factory runtime that will handle the lease creation and keep alive.
/// This is a separate task that will run in the background.
///
struct ManagedLeaseFactoryRuntime {
    ///
    /// The etcd client to use.
    ///
    etcd: etcd_client::Client,

    ///
    /// The runtime handle to spawn tasks on.
    ///
    rt: tokio::runtime::Handle,

    ///
    /// The join set to manage the tasks.
    ///
    js: JoinSet<()>,

    ///
    /// The channel to notify the runtime to shutdown.
    ///
    cnc_rx: mpsc::Receiver<ManagedLeaseRuntimeCommand>,
}

#[derive(Debug, thiserror::Error)]
pub enum CreateLeaseError {
    #[error("lease creation failed")]
    EtcdError(#[from] etcd_client::Error),
    #[error("invalid lease ttl, must be at least 2 seconds")]
    InvalidTTL,
}

impl ManagedLeaseFactoryRuntime {
    async fn handle_create_lease(&mut self, cmd: CreateLeaseCommand) {
        let CreateLeaseCommand {
            ttl,
            keepalive_interval,
            auto_refresh_limit,
            callback,
        } = cmd;
        let ttl_secs = ttl.as_secs() as i64;
        let lease_result = retry_etcd(self.etcd.clone(), (), move |mut etcd, _| async move {
            etcd.lease_grant(ttl_secs, None).await
        })
        .await;
        let lease_id = match lease_result {
            Ok(lease) => lease.id(),
            Err(e) => {
                let _ = callback.send(Err(e.into()));
                return;
            }
        };
        let (stop_tx, mut stop_rx) = oneshot::channel::<()>();
        let client = self.etcd.clone();
        let (tx_expired, rx_expired) = broadcast::channel(1);
        let _ah = self.js.spawn_on(async move {
            let mut refresh_count = 0;
            'outer: loop {
                let first_keep_alive  = Instant::now();
                let (mut keeper, mut keep_alive_resp_stream) = retry_etcd(
                    client.clone(),
                    (lease_id,),
                    move |mut client, (lease_id,)| {
                        async move {
                            client.lease_keep_alive(lease_id).await
                        }
                    })
                        .await
                        .expect("failed to keep alive lease");  // if we have an error this will break out the entire loop
                let mut last_keep_alive = first_keep_alive;
                let keepalive_interval =
                    keepalive_interval.unwrap_or(Duration::from_secs((ttl_secs / 2) as u64));
                let mut next_renewal = first_keep_alive + keepalive_interval;
                'inner: loop {

                    if let Some(limit) = auto_refresh_limit {
                        if refresh_count >= limit {
                            warn!("auto refresh limit reached, stopping lease {lease_id:?} after {refresh_count} refreshes");
                            break 'outer;
                        }
                    }

                    tokio::select! {
                        _ = tokio::time::sleep_until(next_renewal) => {
                            let since_last_keep_alive = last_keep_alive.elapsed();
                            if since_last_keep_alive > keepalive_interval {
                                let dt = since_last_keep_alive - keepalive_interval;
                                if dt >= AT_LEAST_10_JIFFIES {
                                    warn!("last keep alive was {dt:?} late");
                                }
                            }
                            if let Err(e) = keeper.keep_alive().await {
                                error!("failed to keep alive lease {lease_id:?}, got {e:?}");
                                break 'inner;
                            }
                            last_keep_alive = Instant::now();
                            next_renewal += keepalive_interval;
                            let res = keep_alive_resp_stream.next().await;
                            match res {
                                Some(Ok(keep_alive_resp)) => {
                                    refresh_count += 1;
                                    if keep_alive_resp.ttl() == 0 {
                                        error!("lease {lease_id:?} expired");
                                        break 'outer;
                                    }
                                    let ttl = keep_alive_resp.ttl();
                                    if ttl < ttl_secs {
                                        warn!("lease {lease_id:?} ttl reduced to {ttl}, since_last_keep_alive: {since_last_keep_alive:?}");
                                    }
                                    tracing::trace!("keep alive lease {lease_id:?} at {since_last_keep_alive:?}");
                                }
                                Some(Err(e)) => {
                                    warn!("keep alive stream for lease {lease_id:?} errored: {e:?}");
                                    break 'inner;
                                }
                                None => {
                                    warn!("keep alive stream for lease {lease_id:?} ended");
                                    break 'inner;
                                }
                            }
                        }
                        _ = &mut stop_rx => {
                            let since_last_keep_alive = last_keep_alive.elapsed();
                            tracing::info!("revoking lease {lease_id:?}, last keep alive: {since_last_keep_alive:?}");
                            let result = retry_etcd(
                                client.clone(),
                                (lease_id,),
                                move |mut client, (lease_id,)| {
                                    async move {
                                        match client.lease_revoke(lease_id).await {
                                            Ok(_) => Ok(()),
                                            Err(etcd_client::Error::GRpcStatus(status)) => {
                                                if status.code() == tonic::Code::NotFound {
                                                    tracing::warn!("lease {lease_id:?} was already deleted");
                                                    Ok(())
                                                } else {
                                                    Err(etcd_client::Error::GRpcStatus(status))
                                                }
                                            }
                                            Err(e) => Err(e),
                                        }
                                    }
                                }
                            );
                            if let Err(e) = result.await {
                                error!("failed to revoke lease {lease_id:?}, got {e:?}");
                            }
                            break 'outer;
                        }
                    }
                }
            }
            let _ = tx_expired.send(());
        }, &self.rt);
        let lease = ManagedLease {
            etcd: self.etcd.clone(),
            lease_id,
            _tx_terminate: stop_tx,
            rx_lease_expire: rx_expired,
        };
        let _ = callback.send(Ok(lease));
    }

    async fn handle_command(&mut self, cmd: ManagedLeaseRuntimeCommand) {
        match cmd {
            ManagedLeaseRuntimeCommand::CreateLease(cmd) => {
                self.handle_create_lease(cmd).await;
            }
        }
    }

    async fn run(mut self) {
        loop {
            // Loops ends when both the command channel and the join set are closed.
            // When command-and-control channel is closed, it means no `ManagedLease` exists anymore.
            // However, the join set may still have tasks running, we must wait for them to finish.
            tokio::select! {
                Some(cmd) = self.cnc_rx.recv() => {
                    self.handle_command(cmd).await;
                }
                Some(res) = self.js.join_next() => {
                    match res {
                        Ok(_) => {
                            // task completed successfully
                            tracing::trace!("managed lease task completed");
                        }
                        Err(e) => {
                            tracing::warn!("task failed: {e:?}");
                        }
                    }
                }
                else => {
                    break;
                }
            }
        }
        tracing::trace!("managed lease factory runtime exiting");
    }
}

impl ManagedLeaseFactory {
    ///
    /// Create a new managed lease factory.
    /// This will spawn a new task that will handle the lease creation and keep alive.
    ///
    pub fn spawn(etcd: etcd_client::Client) -> (Self, JoinHandle<()>) {
        Self::spawn_on(etcd, tokio::runtime::Handle::current())
    }

    ///
    /// Create a new managed lease factory.
    /// This will spawn a new task that will handle the lease creation and keep alive.
    ///
    /// Arguments:
    /// * `etcd` - The etcd client to use.
    /// * `rt` - The runtime handle to spawn tasks on.
    pub fn spawn_on(
        etcd: etcd_client::Client,
        rt: tokio::runtime::Handle,
    ) -> (Self, JoinHandle<()>) {
        let (cnc_tx, cnc_rx) = mpsc::channel(100);
        let lease_rt = ManagedLeaseFactoryRuntime {
            etcd,
            rt: rt.clone(),
            js: JoinSet::new(),
            cnc_rx,
        };
        let jh = rt.spawn(lease_rt.run());
        (
            Self {
                cnc_tx: cnc_tx.clone(),
            },
            jh,
        )
    }

    ///
    /// Create a new managed lease with the given time-to-live (TTL), keepalive interval and auto refresh limit.
    /// The lease will be kept alive until it is dropped OR until the lease has been refresh `auto_refresh_limit` times.
    ///
    /// Arguments:
    ///
    /// * `ttl` - The time-to-live for the lease.
    /// * `keepalive_interval` - The interval to keep the lease alive.
    /// * `auto_refresh_limit` - The number of times to auto refresh the lease.
    ///
    pub async fn new_lease_with_auto_refresh_limit(
        &self,
        ttl: Duration,
        keepalive_interval: Option<Duration>,
        auto_refresh_limit: Option<usize>,
    ) -> Result<ManagedLease, etcd_client::Error> {
        let ttl_secs: i64 = ttl.as_secs() as i64;
        assert!(ttl_secs >= 2, "lease ttl must be at least two (2) seconds");
        let (callback_tx, callback_rx) = oneshot::channel();
        let command = CreateLeaseCommand {
            ttl,
            keepalive_interval,
            auto_refresh_limit,
            callback: callback_tx,
        };
        self.cnc_tx
            .send(ManagedLeaseRuntimeCommand::CreateLease(command))
            .await
            .expect("failed to send command to managed lease factory");

        let result = callback_rx
            .await
            .expect("failed to receive result from managed lease factory");
        match result {
            Ok(lease) => Ok(lease),
            Err(e) => match e {
                CreateLeaseError::EtcdError(e) => Err(e),
                CreateLeaseError::InvalidTTL => {
                    panic!("lease ttl must be at least two (2) seconds");
                }
            },
        }
    }

    ///
    /// Create a new managed lease with the given time-to-live (TTL) and keepalive interval.
    ///
    /// Managed lease have automatic keep alive mechanism that will keep the lease alive until it is dropped.
    ///
    /// The ttl must be at least two (2) seconds.
    ///
    /// Keepalive interval is optional, if not provided it will be half of the ttl.
    ///
    /// Arguments:
    ///
    /// * `ttl` - The time-to-live for the lease.
    /// * `keepalive_interval` - The interval to keep the lease alive.
    ///
    pub async fn new_lease(
        &self,
        ttl: Duration,
        keepalive_interval: Option<Duration>,
    ) -> Result<ManagedLease, etcd_client::Error> {
        self.new_lease_with_auto_refresh_limit(ttl, keepalive_interval, None)
            .await
    }
}
