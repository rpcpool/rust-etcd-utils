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

pub struct ManagedLease {
    pub lease_id: i64,
    // Let this field dead, because when drop it will trigger a task to wake up and gracefully revoke lease.
    #[allow(dead_code)]
    _tx_terminate: oneshot::Sender<()>,
}

#[derive(Clone)]
pub struct ManagedLeaseFactory {
    etcd: etcd_client::Client,
    js: Arc<Mutex<JoinSet<()>>>,
}

impl ManagedLeaseFactory {
    pub fn new(etcd: etcd_client::Client) -> Self {
        Self {
            etcd,
            js: Arc::new(Mutex::new(JoinSet::new())),
        }
    }

    pub async fn shutdown(self, timeout: Duration) {
        let mut lock = self.js.lock().await;
        let _ = tokio::time::timeout(timeout, lock.shutdown()).await;
    }

    pub async fn new_lease(
        &self,
        ttl: Duration,
        keepalive_interval: Option<Duration>,
    ) -> ManagedLease {
        let ttl_secs: i64 = ttl.as_secs() as i64;
        assert!(ttl_secs >= 2, "lease ttl must be at least two (2) seconds");
        let lease_id = retry_etcd(self.etcd.clone(), (), move |mut etcd, _| async move {
            etcd.lease_grant(ttl_secs, None).await
        })
        .await
        .expect("failed to grant lease")
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
                    tokio::select! {
                        _ = tokio::time::sleep_until(next_renewal) => {
                            let sent_keepalive_at = Instant::now();
                            if let Err(e) = keeper.keep_alive().await {
                                error!("failed to keep alive lease {lease_id:?}, got {e:?}");
                                break 'inner;
                            }
                            let res = keep_alive_resp_stream.next().await;
                            match res {
                                Some(Ok(_)) => {
                                    // next_renewal = Instant::now() + keepalive_interval;
                                    trace!("keep alive lease {lease_id:?} at {sent_keepalive_at:?}");
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
                            let result = retry_etcd(
                                client.clone(),
                                (lease_id,),
                                move |mut client, (lease_id,)| {
                                    async move {
                                        match client.lease_revoke(lease_id).await {
                                            Ok(_) => Ok(()),
                                            Err(etcd_client::Error::GRpcStatus(status)) => {
                                                if status.code() == tonic::Code::NotFound {
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

        ManagedLease {
            lease_id,
            _tx_terminate: stop_tx,
        }
    }
}
