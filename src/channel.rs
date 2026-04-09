use {
    etcd_client::{BalancedChannelBuilder, Channel, Client, ConnectOptions, Error},
    std::{
        collections::{HashMap, HashSet},
        sync::{Arc, Mutex},
        time::Duration,
    },
    tokio::{
        sync::mpsc,
        time::{interval, timeout},
    },
    tonic::transport::{self, Endpoint},
    tower::{balance::p2c::Balance, buffer::Buffer, util::BoxCloneSyncService},
    tracing::{debug, info, warn},
};

use crate::tonic::discovery::QuarantiningDiscover;

type Uri = tonic::codegen::http::Uri;
type EndpointUpdater = mpsc::Sender<transport::channel::Change<Uri, Endpoint>>;
type EndpointChange = transport::channel::Change<Uri, Endpoint>;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReliableChannelStats {
    pub quarantine_transitions: u64,
    pub active_endpoints: usize,
    pub quarantined_endpoints: usize,
}

#[derive(Debug, Clone, Default)]
pub struct ReliableChannelStatsRegistry {
    inner: Arc<Mutex<ReliableChannelStats>>,
    endpoint_status: Arc<Mutex<HashMap<String, EndpointStatus>>>,
    call_counter: Arc<dashmap::DashMap<Uri, usize>>,
}

impl ReliableChannelStatsRegistry {
    pub fn snapshot(&self) -> ReliableChannelStats {
        *self
            .inner
            .lock()
            .expect("reliable channel stats lock poisoned")
    }

    pub fn call_counts_snapshot(&self) -> HashMap<String, usize> {
        self.call_counter
            .iter()
            .map(|entry| (entry.key().to_string(), *entry.value()))
            .collect()
    }

    pub fn get_call_count(&self, endpoint: &str) -> Option<usize> {
        self.call_counter
            .iter()
            .find(|entry| entry.key().to_string().contains(endpoint))
            .map(|entry| *entry.value())
    }

    pub fn endpoint_status_snapshot(&self) -> HashMap<String, EndpointStatus> {
        self.endpoint_status
            .lock()
            .expect("endpoint status lock poisoned")
            .clone()
    }

    pub fn get_endpoint_status(&self, endpoint: &str) -> Option<EndpointStatus> {
        self.endpoint_status
            .lock()
            .expect("endpoint status lock poisoned")
            .get(endpoint)
            .copied()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EndpointStatus {
    Active,
    Quarantined,
}

/// Reliable balanced channel builder with endpoint quarantine and periodic recovery probes.
///
/// Behavior:
/// - Inserted endpoints are probed before entering the routable pool.
/// - Failing endpoints are quarantined and retried periodically.
/// - Error-reporting transport failures automatically remove endpoints from balancer routing.
#[derive(Debug, Clone)]
pub struct ReliableBalancedChannelBuilder {
    pub probe_timeout: Duration,
    pub quarantine_retry_interval: Duration,
    stats_registry: ReliableChannelStatsRegistry,
}

impl Default for ReliableBalancedChannelBuilder {
    fn default() -> Self {
        Self {
            probe_timeout: Duration::from_secs(5),
            quarantine_retry_interval: Duration::from_secs(15),
            stats_registry: ReliableChannelStatsRegistry::default(),
        }
    }
}

impl ReliableBalancedChannelBuilder {
    pub fn stats_registry(&self) -> ReliableChannelStatsRegistry {
        self.stats_registry.clone()
    }
}

impl BalancedChannelBuilder for ReliableBalancedChannelBuilder {
    type Error = transport::Error;

    fn balanced_channel(
        self,
        buffer_size: usize,
    ) -> Result<(Channel, EndpointUpdater), Self::Error> {
        let balance_buffer_size = buffer_size.max(16);
        let (discover_updater, discover_updates_rx) = mpsc::channel(balance_buffer_size);
        let (user_updater, user_updates_rx) = mpsc::channel(buffer_size.max(16));
        let (spy_quarantine_tx, spy_quarantine_rx) = mpsc::unbounded_channel();

        let discover = QuarantiningDiscover::new_with_quarantine_events_and_call_counter(
            discover_updates_rx,
            Some(spy_quarantine_tx),
            Some(self.stats_registry.call_counter.clone()),
        );
        let balanced = Balance::new(discover);
        let buffered = Buffer::new(balanced, balance_buffer_size);
        let custom = BoxCloneSyncService::new(buffered);

        tokio::spawn(run_endpoint_manager(
            user_updates_rx,
            discover_updater,
            spy_quarantine_rx,
            self.stats_registry.clone(),
            self.probe_timeout,
            self.quarantine_retry_interval,
        ));

        Ok((Channel::Custom(custom), user_updater))
    }
}

async fn run_endpoint_manager(
    mut user_updates_rx: mpsc::Receiver<EndpointChange>,
    discover_updater: EndpointUpdater,
    mut spy_quarantine_rx: mpsc::UnboundedReceiver<Uri>,
    stats_registry: ReliableChannelStatsRegistry,
    probe_timeout: Duration,
    quarantine_retry_interval: Duration,
) {
    let mut desired: HashMap<Uri, Endpoint> = HashMap::new();
    let mut active: HashSet<Uri> = HashSet::new();
    let mut quarantined: HashSet<Uri> = HashSet::new();
    let mut tick = interval(quarantine_retry_interval);
    let shared = EndpointManagerShared {
        tonic_updater: &discover_updater,
        stats_registry: &stats_registry,
        probe_timeout,
    };

    loop {
        tokio::select! {
            maybe_change = user_updates_rx.recv() => {
                let Some(change) = maybe_change else {
                    break;
                };
                apply_user_change(
                    change,
                    &mut desired,
                    &mut active,
                    &mut quarantined,
                    &shared,
                ).await;
            }
            maybe_quarantine = spy_quarantine_rx.recv() => {
                let Some(uri) = maybe_quarantine else {
                    continue;
                };
                apply_spy_quarantine(
                    uri,
                    &mut active,
                    &mut quarantined,
                    &shared,
                );
            }
            _ = tick.tick() => {
                retry_quarantined(
                    &desired,
                    &mut active,
                    &mut quarantined,
                    &shared,
                ).await;
            }
        }
    }
}

struct EndpointManagerShared<'a> {
    tonic_updater: &'a EndpointUpdater,
    stats_registry: &'a ReliableChannelStatsRegistry,
    probe_timeout: Duration,
}

async fn apply_user_change(
    change: EndpointChange,
    desired: &mut HashMap<Uri, Endpoint>,
    active: &mut HashSet<Uri>,
    quarantined: &mut HashSet<Uri>,
    shared: &EndpointManagerShared<'_>,
) {
    match change {
        EndpointChange::Insert(uri, endpoint) => {
            desired.insert(uri.clone(), endpoint.clone());
            if endpoint_healthy(&endpoint, shared.probe_timeout).await {
                quarantined.remove(&uri);
                if active.insert(uri.clone()) {
                    set_endpoint_status(shared.stats_registry, &uri, EndpointStatus::Active);
                    let _ = shared
                        .tonic_updater
                        .send(EndpointChange::Insert(uri.clone(), endpoint))
                        .await;
                    info!(endpoint = %uri, "endpoint added to active pool");
                }
            } else {
                if quarantined.insert(uri.clone()) {
                    increment_quarantine_transition(shared.stats_registry);
                }
                set_endpoint_status(shared.stats_registry, &uri, EndpointStatus::Quarantined);
                if active.remove(&uri) {
                    let _ = shared
                        .tonic_updater
                        .send(EndpointChange::Remove(uri.clone()))
                        .await;
                }
                warn!(endpoint = %uri, "endpoint moved to quarantine");
            }
        }
        EndpointChange::Remove(uri) => {
            desired.remove(&uri);
            quarantined.remove(&uri);
            remove_endpoint_status(shared.stats_registry, &uri);
            if active.remove(&uri) {
                let _ = shared
                    .tonic_updater
                    .send(EndpointChange::Remove(uri.clone()))
                    .await;
            }
            debug!(endpoint = %uri, "endpoint removed by caller");
        }
    }

    set_current_counts(shared.stats_registry, active.len(), quarantined.len());
}

fn apply_spy_quarantine(
    uri: Uri,
    active: &mut HashSet<Uri>,
    quarantined: &mut HashSet<Uri>,
    shared: &EndpointManagerShared<'_>,
) {
    if active.remove(&uri) {
        if quarantined.insert(uri.clone()) {
            increment_quarantine_transition(shared.stats_registry);
        }
        set_endpoint_status(shared.stats_registry, &uri, EndpointStatus::Quarantined);
        warn!(endpoint = %uri, "endpoint quarantined from error-reporting transport error");
    }

    set_current_counts(shared.stats_registry, active.len(), quarantined.len());
}

async fn retry_quarantined(
    desired: &HashMap<Uri, Endpoint>,
    active: &mut HashSet<Uri>,
    quarantined: &mut HashSet<Uri>,
    shared: &EndpointManagerShared<'_>,
) {
    let quarantine_uris: Vec<Uri> = quarantined.iter().cloned().collect();
    for uri in quarantine_uris {
        let Some(endpoint) = desired.get(&uri) else {
            quarantined.remove(&uri);
            continue;
        };

        if endpoint_healthy(endpoint, shared.probe_timeout).await {
            tracing::trace!("endpoint probe succeeded during quarantine retry");
            quarantined.remove(&uri);
            if active.insert(uri.clone()) {
                set_endpoint_status(shared.stats_registry, &uri, EndpointStatus::Active);
                let _ = shared
                    .tonic_updater
                    .send(EndpointChange::Insert(uri.clone(), endpoint.clone()))
                    .await;
                info!(endpoint = %uri, "endpoint recovered from quarantine");
            }
        }
    }

    set_current_counts(shared.stats_registry, active.len(), quarantined.len());
}

fn increment_quarantine_transition(stats_registry: &ReliableChannelStatsRegistry) {
    let mut stats = stats_registry
        .inner
        .lock()
        .expect("reliable channel stats lock poisoned");
    stats.quarantine_transitions = stats.quarantine_transitions.saturating_add(1);
}

fn set_current_counts(
    stats_registry: &ReliableChannelStatsRegistry,
    active_count: usize,
    quarantined_count: usize,
) {
    let mut stats = stats_registry
        .inner
        .lock()
        .expect("reliable channel stats lock poisoned");
    stats.active_endpoints = active_count;
    stats.quarantined_endpoints = quarantined_count;
}

fn set_endpoint_status(
    stats_registry: &ReliableChannelStatsRegistry,
    uri: &Uri,
    status: EndpointStatus,
) {
    stats_registry
        .endpoint_status
        .lock()
        .expect("endpoint status lock poisoned")
        .insert(uri.to_string(), status);
}

fn remove_endpoint_status(stats_registry: &ReliableChannelStatsRegistry, uri: &Uri) {
    stats_registry
        .endpoint_status
        .lock()
        .expect("endpoint status lock poisoned")
        .remove(&uri.to_string());
}

async fn endpoint_healthy(endpoint: &Endpoint, probe_timeout: Duration) -> bool {
    match timeout(probe_timeout, endpoint.clone().connect()).await {
        Ok(Ok(_)) => true,
        Ok(Err(e)) => {
            debug!(error = %e, "endpoint probe failed");
            false
        }
        Err(_) => false,
    }
}

/// Connect using the reliable balanced channel builder.
pub async fn connect_with_reliable_balanced_channel<E, S>(
    endpoints: S,
    options: Option<ConnectOptions>,
) -> Result<Client, Error>
where
    E: AsRef<str>,
    S: AsRef<[E]>,
{
    Client::connect_with_balanced_channel(
        endpoints,
        options,
        ReliableBalancedChannelBuilder::default(),
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn endpoint(url: &'static str) -> Endpoint {
        Endpoint::from_static(url)
    }

    fn uri(url: &'static str) -> Uri {
        url.parse().expect("valid uri")
    }

    #[tokio::test]
    async fn apply_spy_quarantine_is_noop_for_non_active_endpoint() {
        let stats_registry = ReliableChannelStatsRegistry::default();
        let (tx, _rx) = mpsc::channel(4);
        let shared = EndpointManagerShared {
            tonic_updater: &tx,
            stats_registry: &stats_registry,
            probe_timeout: Duration::from_millis(10),
        };
        let mut active = HashSet::new();
        let mut quarantined = HashSet::new();
        let target = uri("http://127.0.0.1:21001");

        apply_spy_quarantine(target.clone(), &mut active, &mut quarantined, &shared);

        assert!(!quarantined.contains(&target));
        assert_eq!(stats_registry.snapshot().quarantine_transitions, 0);
        assert!(stats_registry.endpoint_status_snapshot().is_empty());
    }

    #[tokio::test]
    async fn apply_spy_quarantine_moves_active_to_quarantined_and_updates_stats() {
        let stats_registry = ReliableChannelStatsRegistry::default();
        let (tx, _rx) = mpsc::channel(4);
        let shared = EndpointManagerShared {
            tonic_updater: &tx,
            stats_registry: &stats_registry,
            probe_timeout: Duration::from_millis(10),
        };
        let mut active = HashSet::new();
        let mut quarantined = HashSet::new();
        let target = uri("http://127.0.0.1:21002");
        active.insert(target.clone());

        apply_spy_quarantine(target.clone(), &mut active, &mut quarantined, &shared);

        assert!(!active.contains(&target));
        assert!(quarantined.contains(&target));
        assert_eq!(stats_registry.snapshot().quarantine_transitions, 1);
        let status = stats_registry
            .get_endpoint_status(target.to_string().as_str())
            .expect("status should exist");
        assert_eq!(status, EndpointStatus::Quarantined);
    }

    #[tokio::test]
    async fn apply_user_remove_clears_status_without_emitting_remove_for_inactive_endpoint() {
        let stats_registry = ReliableChannelStatsRegistry::default();
        let (tx, mut rx) = mpsc::channel(4);
        let shared = EndpointManagerShared {
            tonic_updater: &tx,
            stats_registry: &stats_registry,
            probe_timeout: Duration::from_millis(10),
        };
        let mut desired = HashMap::new();
        let mut active = HashSet::new();
        let mut quarantined = HashSet::new();
        let target = uri("http://127.0.0.1:21003");
        let target_str = target.to_string();

        desired.insert(target.clone(), endpoint("http://127.0.0.1:21003"));
        quarantined.insert(target.clone());
        set_endpoint_status(&stats_registry, &target, EndpointStatus::Quarantined);

        apply_user_change(
            EndpointChange::Remove(target.clone()),
            &mut desired,
            &mut active,
            &mut quarantined,
            &shared,
        )
        .await;

        assert!(!desired.contains_key(&target));
        assert!(!quarantined.contains(&target));
        assert_eq!(
            stats_registry.get_endpoint_status(target_str.as_str()),
            None
        );
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn retry_quarantined_removes_orphaned_uris_not_in_desired() {
        let stats_registry = ReliableChannelStatsRegistry::default();
        let (tx, mut rx) = mpsc::channel(4);
        let shared = EndpointManagerShared {
            tonic_updater: &tx,
            stats_registry: &stats_registry,
            probe_timeout: Duration::from_millis(10),
        };
        let desired = HashMap::new();
        let mut active = HashSet::new();
        let mut quarantined = HashSet::new();
        let orphan = uri("http://127.0.0.1:21004");
        quarantined.insert(orphan.clone());

        retry_quarantined(&desired, &mut active, &mut quarantined, &shared).await;

        assert!(!quarantined.contains(&orphan));
        assert!(rx.try_recv().is_err());
        let stats = stats_registry.snapshot();
        assert_eq!(stats.active_endpoints, 0);
        assert_eq!(stats.quarantined_endpoints, 0);
    }

    #[tokio::test]
    async fn apply_user_insert_unhealthy_endpoint_quarantines_without_insert_event() {
        let stats_registry = ReliableChannelStatsRegistry::default();
        let (tx, mut rx) = mpsc::channel(4);
        let shared = EndpointManagerShared {
            tonic_updater: &tx,
            stats_registry: &stats_registry,
            probe_timeout: Duration::from_millis(30),
        };
        let mut desired = HashMap::new();
        let mut active = HashSet::new();
        let mut quarantined = HashSet::new();
        let target = uri("http://127.0.0.1:1");
        let ep = endpoint("http://127.0.0.1:1");

        apply_user_change(
            EndpointChange::Insert(target.clone(), ep),
            &mut desired,
            &mut active,
            &mut quarantined,
            &shared,
        )
        .await;

        assert!(desired.contains_key(&target));
        assert!(!active.contains(&target));
        assert!(quarantined.contains(&target));
        assert!(rx.try_recv().is_err());
        assert_eq!(stats_registry.snapshot().quarantine_transitions, 1);
        let status = stats_registry
            .get_endpoint_status(target.to_string().as_str())
            .expect("status should exist");
        assert_eq!(status, EndpointStatus::Quarantined);
    }
}
