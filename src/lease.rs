use {
    crate::retry::retry_etcd,
    futures::StreamExt,
    std::{sync::Arc, time::Duration},
    tokio::{
        sync::{oneshot, Mutex},
        task::JoinSet,
        time::Instant,
    },
    tracing::{error, trace, warn},
};

// Jiffy is interval between system timer interrupts, typically 10ms for linux systems.
const AT_LEAST_10_JIFFIES: Duration = Duration::from_millis(100);

///
/// Managed lease instance that will keep the lease alive until it is dropped.
///
/// See [`ManagedLeaseFactory::new_lease`] for more information.
///
pub struct ManagedLease {
    pub lease_id: i64,
    // Let this field dead, because when drop it will trigger a task to wake up and gracefully revoke lease.
    #[allow(dead_code)]
    _tx_terminate: oneshot::Sender<()>,
}

///
/// Managed lease factory that will create a new lease and keep it alive until it is dropped.
///
#[derive(Clone)]
pub struct ManagedLeaseFactory {
    etcd: etcd_client::Client,
    js: Arc<Mutex<JoinSet<()>>>,
}

impl ManagedLeaseFactory {
    pub fn new(etcd: etcd_client::Client) -> Self {

        let js = Arc::new(Mutex::new(JoinSet::new()));
        let js2 = Arc::clone(&js);

        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;
                {
                    let mut lock = js2.lock().await;
                    while let Some(result) = lock.try_join_next() {
                        if let Err(e) = result {
                            error!("detected managed lease thread failed with: {e:?}");
                        } else {
                            tracing::info!("detected managed lease thread finished");
                        }
                    }
                }
            }
        });


        Self {
            etcd,
            js: js,
        }
    }

    ///
    /// Shutdown the lease factory and revoke all leases.
    ///
    /// Becareful calling this method as it will wait for all lease to be revoked.
    ///
    pub async fn shutdown(self, timeout: Duration) {
        let mut lock = self.js.lock().await;
        let _ = tokio::time::timeout(timeout, lock.shutdown()).await;
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
    pub async fn new_lease(
        &self,
        ttl: Duration,
        keepalive_interval: Option<Duration>,
    ) -> Result<ManagedLease, etcd_client::Error> {
        let ttl_secs: i64 = ttl.as_secs() as i64;
        assert!(ttl_secs >= 2, "lease ttl must be at least two (2) seconds");
        let lease_id = retry_etcd(self.etcd.clone(), (), move |mut etcd, _| async move {
            etcd.lease_grant(ttl_secs, None).await
        })
        .await?
        .id();
        let (stop_tx, mut stop_rx) = oneshot::channel();
        let client = self.etcd.clone();
        let mut lock = self.js.lock().await;

        while let Some(result) = lock.try_join_next() {
            if let Err(e) = result {
                error!("failed to join handle, got {e:?}");
            }
        }

        let _ = lock.spawn(async move {
            'outer: loop {
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

                let keepalive_interval =
                    keepalive_interval.unwrap_or(Duration::from_secs((ttl_secs / 2) as u64));
                'inner: loop {
                    let next_renewal = Instant::now() + keepalive_interval - AT_LEAST_10_JIFFIES;
                    let t = Instant::now();
                    tokio::select! {
                        _ = tokio::time::sleep_until(next_renewal) => {
                            let sent_keepalive_at = Instant::now();
                            
                            let since_last_keep_alive = t.elapsed();
                            tracing::trace!("my ttl_secs: {ttl_secs}, got {since_last_keep_alive:?}");
                            let t2 = Instant::now();
                            if let Err(e) = keeper.keep_alive().await {
                                error!("failed to keep alive lease {lease_id:?}, got {e:?}");
                                break 'inner;
                            }
                            let res = keep_alive_resp_stream.next().await;
                            let keep_alive_rtt = t2.elapsed();
                            tracing::trace!("my ttl_secs: {ttl_secs}, keep alive rtt: {keep_alive_rtt:?}");
                            match res {
                                Some(Ok(keep_alive_resp)) => {
                                    if keep_alive_resp.ttl() == 0 {
                                        error!("lease {lease_id:?} expired");
                                        break 'outer;
                                    }
                                    // next_renewal = Instant::now() + keepalive_interval;
                                    tracing::trace!("keep alive lease {lease_id:?} at {sent_keepalive_at:?}");
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
                        // result = keep_alive_resp_stream.next() => {
                        //     match result {
                        //         Some(Ok(_)) => {
                        //             tracing::warn!("unexpected keep alive response");
                        //         }
                        //         Some(Err(e)) => {
                        //             warn!("keep alive stream for lease {lease_id:?} errored: {e:?}");
                        //             break 'inner;
                        //         }
                        //         None => {
                        //             warn!("keep alive stream for lease {lease_id:?} ended");
                        //             break 'inner;
                        //         }
                        //     }
                        // }
                        _ = &mut stop_rx => {
                            let since_last_keep_alive = t.elapsed();
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
        });

        Ok(ManagedLease {
            lease_id,
            _tx_terminate: stop_tx,
        })
    }
}
