use std::{
    collections::HashSet,
    hash::Hash,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use futures::Sink;
use futures::channel::mpsc as futures_mpsc;
use tokio::sync::mpsc;
use tokio_stream::Stream;
use tonic::transport::{Endpoint, channel::Change};
use tower::{Service, discover::Change as TowerChange};

use crate::tonic::transport::ErrorReportingTransport;

type QuarantinableTransport<Key, Transport> =
    ErrorReportingTransport<Key, Transport, TransportErrorSink<Key>>;

pub trait TransportBuilder {
    type Transport: Service<http::Request<tonic::body::Body>, Error = tonic::transport::Error>;

    fn build_transport(&self) -> Self::Transport;
}

impl TransportBuilder for Endpoint {
    type Transport = tonic::transport::Channel;

    fn build_transport(&self) -> Self::Transport {
        self.connect_lazy()
    }
}

#[derive(Clone)]
pub struct TransportErrorSink<K> {
    discover_tx: futures_mpsc::UnboundedSender<K>,
    quarantine_event_tx: Option<mpsc::UnboundedSender<K>>,
}

impl<K: Clone> TransportErrorSink<K> {
    fn unbounded_send(&self, item: K) -> Result<(), futures_mpsc::TrySendError<K>> {
        let _ = self
            .quarantine_event_tx
            .as_ref()
            .map(|tx| tx.send(item.clone()));
        self.discover_tx.unbounded_send(item)
    }
}

impl<K: Clone> Sink<K> for TransportErrorSink<K> {
    type Error = std::io::Error;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: K) -> Result<(), Self::Error> {
        self.get_mut()
            .unbounded_send(item)
            .map_err(|_| std::io::Error::other("failed to publish transport error"))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

/// A `Discover` implementation similar to tonic's internal dynamic service stream,
/// but with error-reporting-driven endpoint quarantine.
///
/// Endpoints are inserted from the `changes` channel. If a wrapped channel reports a
/// transport error through the error-reporting sink, the key is moved to quarantine and a
/// `Remove` change is emitted for the balancer.
pub struct QuarantiningDiscover<K, TB> {
    changes: mpsc::Receiver<Change<K, TB>>,
    transport_error_rx: futures_mpsc::UnboundedReceiver<K>,
    transport_error_tx: TransportErrorSink<K>,
    shared_call_counter: Option<Arc<dashmap::DashMap<K, usize>>>,
    active: HashSet<K>,
    quarantined: HashSet<K>,
}

impl<K, TB> QuarantiningDiscover<K, TB> {
    pub fn new(changes: mpsc::Receiver<Change<K, TB>>) -> Self {
        Self::new_with_quarantine_events_and_call_counter(changes, None, None)
    }

    pub fn new_with_quarantine_events(
        changes: mpsc::Receiver<Change<K, TB>>,
        quarantine_event_tx: Option<mpsc::UnboundedSender<K>>,
    ) -> Self {
        Self::new_with_quarantine_events_and_call_counter(changes, quarantine_event_tx, None)
    }

    pub fn new_with_quarantine_events_and_call_counter(
        changes: mpsc::Receiver<Change<K, TB>>,
        quarantine_event_tx: Option<mpsc::UnboundedSender<K>>,
        shared_call_counter: Option<Arc<dashmap::DashMap<K, usize>>>,
    ) -> Self {
        let (discover_tx, transport_error_rx) = futures_mpsc::unbounded();
        let transport_error_tx = TransportErrorSink {
            discover_tx,
            quarantine_event_tx,
        };
        Self {
            changes,
            transport_error_rx,
            transport_error_tx,
            shared_call_counter,
            active: HashSet::new(),
            quarantined: HashSet::new(),
        }
    }

    pub fn quarantined_keys(&self) -> &HashSet<K> {
        &self.quarantined
    }
}

impl<K, TB> QuarantiningDiscover<K, TB>
where
    K: Hash + Eq + Clone + Send + 'static,
    TB: TransportBuilder,
{
    fn poll_transport_errors(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<
        Option<Result<TowerChange<K, QuarantinableTransport<K, TB::Transport>>, tower::BoxError>>,
    > {
        loop {
            match Pin::new(&mut self.transport_error_rx).poll_next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Ready(Some(key)) => {
                    if !self.quarantined.insert(key.clone()) {
                        continue;
                    }
                    if self.active.remove(&key) {
                        tracing::trace!("removing endpoint from discover due to transport error");
                        return Poll::Ready(Some(Ok(TowerChange::Remove(key))));
                    }
                }
            }
        }
    }

    fn poll_changes(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<
        Option<Result<TowerChange<K, QuarantinableTransport<K, TB::Transport>>, tower::BoxError>>,
    > {
        loop {
            match Pin::new(&mut self.changes).poll_recv(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Ready(Some(change)) => match change {
                    Change::Insert(key, endpoint) => {
                        // Explicit insert acts as a retry signal and clears quarantine state.
                        self.quarantined.remove(&key);
                        self.active.insert(key.clone());
                        let connection: QuarantinableTransport<K, TB::Transport> =
                            crate::tonic::error_reporting::ErrorReportingTransport::new_with_call_counter(
                                endpoint.build_transport(),
                                key.clone(),
                                self.transport_error_tx.clone(),
                                self.shared_call_counter.clone(),
                            );
                        tracing::trace!("inserting endpoint into discover");
                        return Poll::Ready(Some(Ok(TowerChange::Insert(key, connection))));
                    }
                    Change::Remove(key) => {
                        tracing::trace!("removing endpoint from discover");
                        self.quarantined.remove(&key);
                        if self.active.remove(&key) {
                            return Poll::Ready(Some(Ok(TowerChange::Remove(key))));
                        }
                    }
                },
            }
        }
    }
}

impl<K, TB> Stream for QuarantiningDiscover<K, TB>
where
    K: Hash + Eq + Clone + Send + 'static,
    TB: TransportBuilder,
{
    type Item = Result<TowerChange<K, QuarantinableTransport<K, TB::Transport>>, tower::BoxError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.as_mut().get_mut();
        match this.poll_transport_errors(cx) {
            Poll::Ready(Some(change)) => return Poll::Ready(Some(change)),
            Poll::Ready(None) | Poll::Pending => {}
        }

        this.poll_changes(cx)
    }
}

impl<K: Hash + Eq + Clone + Send + 'static, TB: TransportBuilder> Unpin
    for QuarantiningDiscover<K, TB>
{
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::SinkExt;
    use futures::task::noop_waker_ref;
    use tower::discover::Discover;

    #[tokio::test]
    async fn quarantines_key_when_error_reporting_transport_reports_transport_error() {
        let (tx, rx) = mpsc::channel(4);
        let mut discover = QuarantiningDiscover::new(rx);
        let key = "ep-1".to_string();

        let endpoint = Endpoint::from_static("http://127.0.0.1:2379");
        tx.send(Change::Insert(key.clone(), endpoint))
            .await
            .expect("insert change should be sent");

        let waker = noop_waker_ref();
        let mut cx = Context::from_waker(waker);

        let insert = Pin::new(&mut discover).poll_discover(&mut cx);
        assert!(matches!(
            insert,
            Poll::Ready(Some(Ok(TowerChange::Insert(_, _))))
        ));

        discover
            .transport_error_tx
            .unbounded_send(key.clone())
            .expect("transport error key should be sent");

        let remove = Pin::new(&mut discover).poll_discover(&mut cx);
        assert!(matches!(remove, Poll::Ready(Some(Ok(TowerChange::Remove(k)))) if k == key));
        assert!(discover.quarantined_keys().contains(&key));
    }

    #[tokio::test]
    async fn duplicate_transport_errors_do_not_emit_duplicate_remove() {
        let (tx, rx) = mpsc::channel(4);
        let mut discover = QuarantiningDiscover::new(rx);
        let key = "ep-dup".to_string();

        let endpoint = Endpoint::from_static("http://127.0.0.1:2379");
        tx.send(Change::Insert(key.clone(), endpoint))
            .await
            .expect("insert change should be sent");

        let waker = noop_waker_ref();
        let mut cx = Context::from_waker(waker);

        let insert = Pin::new(&mut discover).poll_discover(&mut cx);
        assert!(matches!(
            insert,
            Poll::Ready(Some(Ok(TowerChange::Insert(_, _))))
        ));

        discover
            .transport_error_tx
            .unbounded_send(key.clone())
            .expect("first transport error should be sent");
        let first_remove = Pin::new(&mut discover).poll_discover(&mut cx);
        assert!(matches!(first_remove, Poll::Ready(Some(Ok(TowerChange::Remove(k)))) if k == key));

        discover
            .transport_error_tx
            .unbounded_send(key)
            .expect("second transport error should be sent");
        let second_poll = Pin::new(&mut discover).poll_discover(&mut cx);
        assert!(matches!(second_poll, Poll::Pending));
    }

    #[tokio::test]
    async fn transport_error_for_inactive_key_marks_quarantine_without_remove() {
        let (_tx, rx) = mpsc::channel::<Change<String, Endpoint>>(4);
        let mut discover = QuarantiningDiscover::new(rx);
        let key = "ep-inactive".to_string();

        discover
            .transport_error_tx
            .unbounded_send(key.clone())
            .expect("transport error key should be sent");

        let waker = noop_waker_ref();
        let mut cx = Context::from_waker(waker);
        let poll = Pin::new(&mut discover).poll_discover(&mut cx);

        assert!(matches!(poll, Poll::Pending));
        assert!(discover.quarantined_keys().contains(&key));
    }

    #[tokio::test]
    async fn remove_for_inactive_key_clears_quarantine_without_emitting_remove() {
        let (tx, rx) = mpsc::channel(4);
        let mut discover = QuarantiningDiscover::new(rx);
        let key = "ep-remove-inactive".to_string();

        let endpoint = Endpoint::from_static("http://127.0.0.1:2379");
        tx.send(Change::Insert(key.clone(), endpoint))
            .await
            .expect("insert change should be sent");

        let waker = noop_waker_ref();
        let mut cx = Context::from_waker(waker);

        let insert = Pin::new(&mut discover).poll_discover(&mut cx);
        assert!(matches!(
            insert,
            Poll::Ready(Some(Ok(TowerChange::Insert(_, _))))
        ));

        discover
            .transport_error_tx
            .unbounded_send(key.clone())
            .expect("transport error key should be sent");
        let remove_after_error = Pin::new(&mut discover).poll_discover(&mut cx);
        assert!(
            matches!(remove_after_error, Poll::Ready(Some(Ok(TowerChange::Remove(k)))) if k == key)
        );
        assert!(discover.quarantined_keys().contains(&key));

        tx.send(Change::Remove(key.clone()))
            .await
            .expect("remove change should be sent");
        let remove_inactive = Pin::new(&mut discover).poll_discover(&mut cx);
        assert!(matches!(remove_inactive, Poll::Pending));
        assert!(!discover.quarantined_keys().contains(&key));
    }

    #[tokio::test]
    async fn explicit_reinsert_clears_quarantine_and_emits_insert() {
        let (tx, rx) = mpsc::channel(4);
        let mut discover = QuarantiningDiscover::new(rx);
        let key = "ep-reinsert".to_string();

        let endpoint = Endpoint::from_static("http://127.0.0.1:2379");
        tx.send(Change::Insert(key.clone(), endpoint.clone()))
            .await
            .expect("insert change should be sent");

        let waker = noop_waker_ref();
        let mut cx = Context::from_waker(waker);

        let first_insert = Pin::new(&mut discover).poll_discover(&mut cx);
        assert!(matches!(
            first_insert,
            Poll::Ready(Some(Ok(TowerChange::Insert(_, _))))
        ));

        discover
            .transport_error_tx
            .unbounded_send(key.clone())
            .expect("transport error key should be sent");
        let remove_after_error = Pin::new(&mut discover).poll_discover(&mut cx);
        assert!(
            matches!(remove_after_error, Poll::Ready(Some(Ok(TowerChange::Remove(k)))) if k == key)
        );
        assert!(discover.quarantined_keys().contains(&key));

        tx.send(Change::Insert(key.clone(), endpoint))
            .await
            .expect("retry insert change should be sent");
        let retry_insert = Pin::new(&mut discover).poll_discover(&mut cx);
        assert!(
            matches!(retry_insert, Poll::Ready(Some(Ok(TowerChange::Insert(k, _)))) if k == key)
        );
        assert!(!discover.quarantined_keys().contains(&key));
    }

    #[tokio::test]
    async fn transport_error_sink_fanout_sends_quarantine_event() {
        let (discover_tx, _discover_rx) = futures_mpsc::unbounded::<String>();
        let (quarantine_tx, mut quarantine_rx) = mpsc::unbounded_channel::<String>();
        let mut sink = TransportErrorSink {
            discover_tx,
            quarantine_event_tx: Some(quarantine_tx),
        };

        sink.send("ep-fanout".to_string())
            .await
            .expect("sink send should succeed");

        let forwarded = quarantine_rx
            .recv()
            .await
            .expect("quarantine event should be forwarded");
        assert_eq!(forwarded, "ep-fanout");
    }

    #[tokio::test]
    async fn transport_error_sink_reports_error_when_discover_channel_closed() {
        let (discover_tx, discover_rx) = futures_mpsc::unbounded::<String>();
        drop(discover_rx);

        let mut sink = TransportErrorSink {
            discover_tx,
            quarantine_event_tx: None,
        };

        let result = sink.send("ep-closed".to_string()).await;
        assert!(result.is_err());
    }
}
