use std::{
    future::Future,
    hash::Hash,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, ready},
};

use futures::channel::mpsc as futures_mpsc;
use futures::{Sink, SinkExt};
use pin_project::pin_project;
use tower::{Service, load::Load};

/// Channel type wrapped with error-reporting transport behavior.
pub type ErrorReportingChannel<K, ES = futures_mpsc::UnboundedSender<K>> =
    ErrorReportingTransport<K, tonic::transport::Channel, ES>;

/// Wraps a tonic transport service and reports transport errors specifically.
///
/// This type does not report successful calls. It only forwards endpoint keys
/// to the configured sink when transport-layer operations fail:
/// - `poll_ready` returns a transport error
/// - `call` future resolves to a transport error
pub struct ErrorReportingTransport<Key, Transport, ErrorSink> {
    inner: Transport,
    key: Key,
    error_sink: ErrorSink,
    shared_stat_map: Option<Arc<dashmap::DashMap<Key, usize>>>,
    ready_notify_state: ReadyNotifyState<Key>,
    ready_transport_error: Option<tonic::transport::Error>,
}

enum ReadyNotifyState<K> {
    Idle,
    NeedSend(K),
    NeedFlush,
}

impl<K, T, ES> ErrorReportingTransport<K, T, ES> {
    pub fn new(inner: T, key: K, error_sink: ES) -> Self {
        Self {
            inner,
            key,
            error_sink,
            shared_stat_map: None,
            ready_notify_state: ReadyNotifyState::Idle,
            ready_transport_error: None,
        }
    }

    pub fn new_with_call_counter(
        inner: T,
        key: K,
        error_sink: ES,
        shared_stat_map: Option<Arc<dashmap::DashMap<K, usize>>>,
    ) -> Self {
        Self {
            inner,
            key,
            error_sink,
            shared_stat_map,
            ready_notify_state: ReadyNotifyState::Idle,
            ready_transport_error: None,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ErrorReportingTransportError {
    #[error("Service error: {0}")]
    Transport(#[from] tonic::transport::Error),
    #[error("error sink error: {0}")]
    ErrorSink(Box<dyn std::error::Error + Send + Sync>),
}

enum SinkNotifyState<K> {
    Idle,
    NeedSend(K),
    NeedFlush,
    Done,
}

#[pin_project]
pub struct ErrorReportingTransportFuture<Fut, Ok, Key, ErrorSink> {
    #[pin]
    inner: Fut,
    #[pin]
    error_sink: ErrorSink,
    key: Key,
    transport_error: Option<tonic::transport::Error>,
    sink_notify_state: SinkNotifyState<Key>,
    _ok: std::marker::PhantomData<Ok>,
}

impl<Fut, Ok, K, ES> Future for ErrorReportingTransportFuture<Fut, Ok, K, ES>
where
    Fut: Future<Output = Result<Ok, tonic::transport::Error>>,
    K: Clone + Send,
    ES: Sink<K> + Unpin,
    <ES as Sink<K>>::Error: std::error::Error + Send + Sync + 'static,
{
    type Output = Result<Ok, ErrorReportingTransportError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        loop {
            match this.sink_notify_state {
                SinkNotifyState::Idle => match this.inner.as_mut().poll(cx) {
                    Poll::Ready(Ok(ok)) => return Poll::Ready(Ok(ok)),
                    Poll::Ready(Err(err)) => {
                        *this.transport_error = Some(err);
                        *this.sink_notify_state = SinkNotifyState::NeedSend(this.key.clone());
                    }
                    Poll::Pending => return Poll::Pending,
                },
                SinkNotifyState::NeedSend(key) => {
                    ready!(
                        this.error_sink
                            .poll_ready_unpin(cx)
                            .map_err(|e| ErrorReportingTransportError::ErrorSink(Box::new(e)))
                    )?;
                    this.error_sink
                        .start_send_unpin(key.clone())
                        .map_err(|e| ErrorReportingTransportError::ErrorSink(Box::new(e)))?;
                    *this.sink_notify_state = SinkNotifyState::NeedFlush;
                }
                SinkNotifyState::NeedFlush => {
                    ready!(
                        this.error_sink
                            .poll_flush_unpin(cx)
                            .map_err(|e| ErrorReportingTransportError::ErrorSink(Box::new(e)))
                    )?;
                    *this.sink_notify_state = SinkNotifyState::Done;
                    let err = this
                        .transport_error
                        .take()
                        .expect("missing transport error while flushing sink");
                    return Poll::Ready(Err(ErrorReportingTransportError::Transport(err)));
                }
                SinkNotifyState::Done => {
                    panic!("ErrorReportingTransportFuture polled after completion")
                }
            }
        }
    }
}

impl<K, T, ES> Service<http::Request<tonic::body::Body>> for ErrorReportingTransport<K, T, ES>
where
    T: Service<http::Request<tonic::body::Body>, Error = tonic::transport::Error>,
    K: Clone + Eq + Hash + Send + 'static,
    ES: Sink<K> + Clone + Unpin + Send + 'static,
    <ES as Sink<K>>::Error: std::error::Error + Send + Sync + 'static,
{
    type Response = T::Response;
    type Error = ErrorReportingTransportError;
    type Future = ErrorReportingTransportFuture<T::Future, T::Response, K, ES>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        loop {
            match &self.ready_notify_state {
                ReadyNotifyState::Idle => {
                    ready!(self.error_sink.poll_ready_unpin(cx))
                        .map_err(|e| ErrorReportingTransportError::ErrorSink(Box::new(e)))?;

                    match self.inner.poll_ready(cx) {
                        Poll::Ready(Ok(())) => return Poll::Ready(Ok(())),
                        Poll::Ready(Err(err)) => {
                            self.ready_transport_error = Some(err);
                            self.ready_notify_state = ReadyNotifyState::NeedSend(self.key.clone());
                        }
                        Poll::Pending => return Poll::Pending,
                    }
                }
                ReadyNotifyState::NeedSend(key) => {
                    ready!(self.error_sink.poll_ready_unpin(cx))
                        .map_err(|e| ErrorReportingTransportError::ErrorSink(Box::new(e)))?;
                    self.error_sink
                        .start_send_unpin(key.clone())
                        .map_err(|e| ErrorReportingTransportError::ErrorSink(Box::new(e)))?;
                    self.ready_notify_state = ReadyNotifyState::NeedFlush;
                }
                ReadyNotifyState::NeedFlush => {
                    ready!(self.error_sink.poll_flush_unpin(cx))
                        .map_err(|e| ErrorReportingTransportError::ErrorSink(Box::new(e)))?;
                    self.ready_notify_state = ReadyNotifyState::Idle;
                    let err = self
                        .ready_transport_error
                        .take()
                        .expect("missing transport error while flushing poll_ready notification");
                    return Poll::Ready(Err(ErrorReportingTransportError::Transport(err)));
                }
            }
        }
    }

    fn call(&mut self, req: http::Request<tonic::body::Body>) -> Self::Future {
        if let Some(map) = &self.shared_stat_map {
            map.entry(self.key.clone())
                .and_modify(|count| *count += 1)
                .or_insert(1);
        }

        let error_sink = self.error_sink.clone();
        let key = self.key.clone();
        ErrorReportingTransportFuture {
            inner: self.inner.call(req),
            error_sink,
            key,
            transport_error: None,
            sink_notify_state: SinkNotifyState::Idle,
            _ok: std::marker::PhantomData,
        }
    }
}

impl<K, T, ES> Load for ErrorReportingTransport<K, T, ES> {
    type Metric = usize;

    fn load(&self) -> Self::Metric {
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dashmap::DashMap;
    use futures::task::noop_waker_ref;
    use std::{
        future::Future,
        sync::atomic::{AtomicUsize, Ordering},
        sync::{Arc, Mutex},
        time::Duration,
    };

    #[derive(Debug, Default)]
    struct SinkState<K> {
        sent: Vec<K>,
        flush_count: usize,
        ready_count: usize,
    }

    #[derive(Debug, Clone)]
    struct TestSink<K> {
        state: Arc<Mutex<SinkState<K>>>,
        pending_ready_once: bool,
        pending_flush_once: bool,
    }

    impl<K> TestSink<K> {
        fn new(state: Arc<Mutex<SinkState<K>>>) -> Self {
            Self {
                state,
                pending_ready_once: false,
                pending_flush_once: false,
            }
        }

        fn with_pending_once(state: Arc<Mutex<SinkState<K>>>) -> Self {
            Self {
                state,
                pending_ready_once: true,
                pending_flush_once: true,
            }
        }
    }

    impl<K: Clone + Send + 'static> Sink<K> for TestSink<K> {
        type Error = std::io::Error;

        fn poll_ready(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            self.state.lock().expect("sink state lock").ready_count += 1;
            if self.pending_ready_once {
                self.pending_ready_once = false;
                return Poll::Pending;
            }
            Poll::Ready(Ok(()))
        }

        fn start_send(self: Pin<&mut Self>, item: K) -> Result<(), Self::Error> {
            self.state.lock().expect("sink state lock").sent.push(item);
            Ok(())
        }

        fn poll_flush(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            if self.pending_flush_once {
                self.pending_flush_once = false;
                return Poll::Pending;
            }
            self.state.lock().expect("sink state lock").flush_count += 1;
            Poll::Ready(Ok(()))
        }

        fn poll_close(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
    }

    struct ErrorOnceFuture {
        err: Option<tonic::transport::Error>,
        polled_once: bool,
    }

    struct PollReadyErrorService {
        poll_ready_calls: Arc<AtomicUsize>,
        err: Option<tonic::transport::Error>,
    }

    struct PollReadyOkCountingService {
        poll_ready_calls: Arc<AtomicUsize>,
    }

    impl Service<http::Request<tonic::body::Body>> for PollReadyErrorService {
        type Response = http::Response<tonic::body::Body>;
        type Error = tonic::transport::Error;
        type Future = std::future::Ready<Result<Self::Response, Self::Error>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            self.poll_ready_calls.fetch_add(1, Ordering::SeqCst);
            Poll::Ready(Err(self
                .err
                .take()
                .expect("poll_ready called more than once on error service")))
        }

        fn call(&mut self, _req: http::Request<tonic::body::Body>) -> Self::Future {
            std::future::ready(Ok(http::Response::new(tonic::body::Body::empty())))
        }
    }

    impl Service<http::Request<tonic::body::Body>> for PollReadyOkCountingService {
        type Response = http::Response<tonic::body::Body>;
        type Error = tonic::transport::Error;
        type Future = std::future::Ready<Result<Self::Response, Self::Error>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            self.poll_ready_calls.fetch_add(1, Ordering::SeqCst);
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _req: http::Request<tonic::body::Body>) -> Self::Future {
            std::future::ready(Ok(http::Response::new(tonic::body::Body::empty())))
        }
    }

    #[derive(Clone)]
    struct OkService;

    impl Service<http::Request<tonic::body::Body>> for OkService {
        type Response = http::Response<tonic::body::Body>;
        type Error = tonic::transport::Error;
        type Future = std::future::Ready<Result<Self::Response, Self::Error>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _req: http::Request<tonic::body::Body>) -> Self::Future {
            std::future::ready(Ok(http::Response::new(tonic::body::Body::empty())))
        }
    }

    impl Future for ErrorOnceFuture {
        type Output = Result<(), tonic::transport::Error>;

        fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
            if self.polled_once {
                panic!("inner future was polled after completion");
            }
            self.polled_once = true;
            Poll::Ready(Err(self.err.take().expect("missing transport error")))
        }
    }

    #[derive(Clone)]
    struct AlwaysErrorSink;

    impl Sink<String> for AlwaysErrorSink {
        type Error = std::io::Error;

        fn poll_ready(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Err(std::io::Error::other("poll_ready failed")))
        }

        fn start_send(self: Pin<&mut Self>, _item: String) -> Result<(), Self::Error> {
            Ok(())
        }

        fn poll_flush(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn poll_close(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
    }

    async fn make_transport_error() -> tonic::transport::Error {
        tonic::transport::Endpoint::from_static("http://192.0.2.1:12345")
            .connect_timeout(Duration::from_millis(50))
            .connect()
            .await
            .expect_err("expected connect failure")
    }

    #[tokio::test]
    async fn error_reporting_transport_future_success_path_does_not_notify_sink() {
        let state = Arc::new(Mutex::new(SinkState::<String>::default()));
        let sink = TestSink::new(Arc::clone(&state));

        let fut = ErrorReportingTransportFuture {
            inner: std::future::ready(Ok::<_, tonic::transport::Error>(123)),
            error_sink: sink,
            key: "endpoint-a".to_string(),
            transport_error: None,
            sink_notify_state: SinkNotifyState::Idle,
            _ok: std::marker::PhantomData,
        };

        let out = fut.await.expect("expected success");
        assert_eq!(out, 123);

        let s = state.lock().expect("sink state lock");
        assert!(s.sent.is_empty());
        assert_eq!(s.flush_count, 0);
    }

    #[tokio::test]
    async fn error_reporting_transport_future_error_path_sends_and_flushes_once_without_repolling_inner()
     {
        let state = Arc::new(Mutex::new(SinkState::<String>::default()));
        let sink = TestSink::with_pending_once(Arc::clone(&state));
        let err = make_transport_error().await;

        let mut fut = ErrorReportingTransportFuture {
            inner: ErrorOnceFuture {
                err: Some(err),
                polled_once: false,
            },
            error_sink: sink,
            key: "endpoint-b".to_string(),
            transport_error: None,
            sink_notify_state: SinkNotifyState::Idle,
            _ok: std::marker::PhantomData,
        };

        let waker = noop_waker_ref();
        let mut cx = Context::from_waker(waker);

        assert!(matches!(Pin::new(&mut fut).poll(&mut cx), Poll::Pending));
        assert!(matches!(Pin::new(&mut fut).poll(&mut cx), Poll::Pending));
        let final_poll = Pin::new(&mut fut).poll(&mut cx);
        assert!(matches!(
            final_poll,
            Poll::Ready(Err(ErrorReportingTransportError::Transport(_)))
        ));

        let s = state.lock().expect("sink state lock");
        assert_eq!(s.sent, vec!["endpoint-b".to_string()]);
        assert_eq!(s.flush_count, 1);
    }

    #[tokio::test]
    async fn error_reporting_transport_future_drop_before_notify_send_is_safe() {
        let state = Arc::new(Mutex::new(SinkState::<String>::default()));
        let sink = TestSink {
            state: Arc::clone(&state),
            pending_ready_once: true,
            pending_flush_once: false,
        };
        let err = make_transport_error().await;

        let mut fut = ErrorReportingTransportFuture {
            inner: ErrorOnceFuture {
                err: Some(err),
                polled_once: false,
            },
            error_sink: sink,
            key: "endpoint-cancel-1".to_string(),
            transport_error: None,
            sink_notify_state: SinkNotifyState::Idle,
            _ok: std::marker::PhantomData,
        };

        let waker = noop_waker_ref();
        let mut cx = Context::from_waker(waker);
        assert!(matches!(Pin::new(&mut fut).poll(&mut cx), Poll::Pending));

        drop(fut);

        let s = state.lock().expect("sink state lock");
        assert!(s.sent.is_empty());
        assert_eq!(s.flush_count, 0);
    }

    #[tokio::test]
    async fn error_reporting_transport_future_drop_after_send_before_flush_is_safe() {
        let state = Arc::new(Mutex::new(SinkState::<String>::default()));
        let sink = TestSink {
            state: Arc::clone(&state),
            pending_ready_once: false,
            pending_flush_once: true,
        };
        let err = make_transport_error().await;

        let mut fut = ErrorReportingTransportFuture {
            inner: ErrorOnceFuture {
                err: Some(err),
                polled_once: false,
            },
            error_sink: sink,
            key: "endpoint-cancel-2".to_string(),
            transport_error: None,
            sink_notify_state: SinkNotifyState::Idle,
            _ok: std::marker::PhantomData,
        };

        let waker = noop_waker_ref();
        let mut cx = Context::from_waker(waker);
        assert!(matches!(Pin::new(&mut fut).poll(&mut cx), Poll::Pending));

        drop(fut);

        let s = state.lock().expect("sink state lock");
        assert_eq!(s.sent, vec!["endpoint-cancel-2".to_string()]);
        assert_eq!(s.flush_count, 0);
    }

    #[tokio::test]
    async fn error_reporting_transport_increments_optional_shared_call_counter() {
        let map = Arc::new(DashMap::<String, usize>::new());
        let (error_sink, _error_rx) = futures_mpsc::unbounded::<String>();

        let mut spy = ErrorReportingTransport::new_with_call_counter(
            OkService,
            "endpoint-counter".to_string(),
            error_sink,
            Some(Arc::clone(&map)),
        );

        let req = http::Request::new(tonic::body::Body::empty());
        let _ = spy.call(req).await.expect("first request should succeed");
        let req = http::Request::new(tonic::body::Body::empty());
        let _ = spy.call(req).await.expect("second request should succeed");

        let count = *map
            .get("endpoint-counter")
            .expect("missing endpoint call counter");
        assert_eq!(count, 2);
    }

    #[tokio::test]
    async fn error_reporting_poll_ready_waits_for_sink_before_polling_inner() {
        let state = Arc::new(Mutex::new(SinkState::<String>::default()));
        let sink = TestSink {
            state: Arc::clone(&state),
            pending_ready_once: true,
            pending_flush_once: false,
        };
        let poll_ready_calls = Arc::new(AtomicUsize::new(0));

        let mut spy = ErrorReportingTransport::new(
            PollReadyOkCountingService {
                poll_ready_calls: Arc::clone(&poll_ready_calls),
            },
            "endpoint-ready-gate".to_string(),
            sink,
        );

        let waker = noop_waker_ref();
        let mut cx = Context::from_waker(waker);

        assert!(matches!(spy.poll_ready(&mut cx), Poll::Pending));
        assert_eq!(poll_ready_calls.load(Ordering::SeqCst), 0);

        assert!(matches!(spy.poll_ready(&mut cx), Poll::Ready(Ok(()))));
        assert_eq!(poll_ready_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn error_reporting_poll_ready_maps_sink_poll_ready_error() {
        let poll_ready_calls = Arc::new(AtomicUsize::new(0));
        let mut spy = ErrorReportingTransport::new(
            PollReadyOkCountingService {
                poll_ready_calls: Arc::clone(&poll_ready_calls),
            },
            "endpoint-ready-sink-error".to_string(),
            AlwaysErrorSink,
        );

        let waker = noop_waker_ref();
        let mut cx = Context::from_waker(waker);
        let res = spy.poll_ready(&mut cx);

        assert!(matches!(
            res,
            Poll::Ready(Err(ErrorReportingTransportError::ErrorSink(_)))
        ));
        assert_eq!(poll_ready_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    #[should_panic(expected = "ErrorReportingTransportFuture polled after completion")]
    async fn error_reporting_transport_future_panics_if_polled_after_completion() {
        let state = Arc::new(Mutex::new(SinkState::<String>::default()));
        let sink = TestSink::new(Arc::clone(&state));
        let err = make_transport_error().await;

        let mut fut = ErrorReportingTransportFuture {
            inner: ErrorOnceFuture {
                err: Some(err),
                polled_once: false,
            },
            error_sink: sink,
            key: "endpoint-done-repoll".to_string(),
            transport_error: None,
            sink_notify_state: SinkNotifyState::Idle,
            _ok: std::marker::PhantomData,
        };

        let waker = noop_waker_ref();
        let mut cx = Context::from_waker(waker);

        let first = Pin::new(&mut fut).poll(&mut cx);
        assert!(matches!(
            first,
            Poll::Ready(Err(ErrorReportingTransportError::Transport(_)))
        ));

        let _ = Pin::new(&mut fut).poll(&mut cx);
    }

    #[tokio::test]
    async fn error_reporting_poll_ready_does_not_repoll_inner_while_sink_flush_pending() {
        let state = Arc::new(Mutex::new(SinkState::<String>::default()));
        let sink = TestSink {
            state: Arc::clone(&state),
            pending_ready_once: false,
            pending_flush_once: true,
        };
        let err = make_transport_error().await;
        let poll_ready_calls = Arc::new(AtomicUsize::new(0));

        let mut spy = ErrorReportingTransport::new(
            PollReadyErrorService {
                poll_ready_calls: Arc::clone(&poll_ready_calls),
                err: Some(err),
            },
            "endpoint-ready-error".to_string(),
            sink,
        );

        let waker = noop_waker_ref();
        let mut cx = Context::from_waker(waker);

        assert!(matches!(spy.poll_ready(&mut cx), Poll::Pending));
        assert!(matches!(
            spy.poll_ready(&mut cx),
            Poll::Ready(Err(ErrorReportingTransportError::Transport(_)))
        ));

        assert_eq!(poll_ready_calls.load(Ordering::SeqCst), 1);
        let s = state.lock().expect("sink state lock");
        assert_eq!(s.sent, vec!["endpoint-ready-error".to_string()]);
        assert_eq!(s.flush_count, 1);
    }
}
