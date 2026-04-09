use {
    super::{Revision, retry::retry_etcd_legacy},
    crate::retry::is_transient,
    etcd_client::{
        Error, EventType, WatchClient, WatchFilterType, WatchOptions, WatchResponse,
        WatchStream as EtcdClientWatchStream,
    },
    futures::{Future, Stream},
    pin_project::pin_project,
    retry::delay::Exponential,
    serde::de::DeserializeOwned,
    std::{
        collections::VecDeque,
        marker::PhantomData,
        pin::{Pin, pin},
        task::{Context, Poll, ready},
    },
    tracing::{error, info},
};

///
/// Custom types for watch events.
///
/// Unwrap the etcd watch event to a more user-friendly event.
///
pub enum WatchEvent<V> {
    Put {
        key: Vec<u8>,
        value: V,
        revision: Revision,
    },
    Delete {
        key: Vec<u8>,
        prev_value: Option<V>,
        revision: Revision,
    },
}

enum ReconnectState<S> {
    Disconnected,
    Connecting(Pin<Box<dyn Future<Output = Result<S, Error>> + Send>>),
    Streaming { stream: S },
    Terminated,
}

pub trait EtcdConnector {
    type WatchStream: Stream<Item = Result<WatchResponse, Error>> + Unpin + Send + 'static;
    type ConnectFut: Future<Output = Result<Self::WatchStream, Error>> + Send + 'static;

    fn connect_watch(&mut self, last_revision: Option<Revision>) -> Self::ConnectFut;
}

pub struct GrpcEtcdConenctor {
    watch_client: WatchClient,
    key: Vec<u8>,
    watch_options_prototype: WatchOptions,
}

impl GrpcEtcdConenctor {
    pub fn new(
        watch_client: WatchClient,
        key: Vec<u8>,
        watch_options_prototype: WatchOptions,
    ) -> Self {
        Self {
            watch_client,
            key,
            watch_options_prototype,
        }
    }
}

impl EtcdConnector for GrpcEtcdConenctor {
    type WatchStream = EtcdClientWatchStream;
    type ConnectFut = Pin<Box<dyn Future<Output = Result<Self::WatchStream, Error>> + Send>>;

    fn connect_watch(&mut self, last_revision: Option<Revision>) -> Self::ConnectFut {
        let wc = self.watch_client.clone();
        let key = self.key.clone();
        let mut wopts = self.watch_options_prototype.clone();
        if let Some(rev) = last_revision {
            wopts = wopts.with_start_revision(rev);
        }

        Box::pin(async move {
            let retry_strategy = Exponential::from_millis_with_factor(10, 10.0).take(3);
            retry_etcd_legacy(retry_strategy, move || {
                let mut wc = wc.clone();
                let key = key.clone();
                let wopts = wopts.clone();
                async move { wc.watch(key.clone(), Some(wopts)).await }
            })
            .await
        })
    }
}

pub struct AutoReconnectWatchStream<C>
where
    C: EtcdConnector,
{
    connector: C,
    state: ReconnectState<C::WatchStream>,
    last_revision: Option<Revision>,
}

impl<C> AutoReconnectWatchStream<C>
where
    C: EtcdConnector,
{
    pub fn new(connector: C) -> Self {
        Self {
            connector,
            state: ReconnectState::Disconnected,
            last_revision: None,
        }
    }
}

impl<C> Stream for AutoReconnectWatchStream<C>
where
    C: EtcdConnector + Unpin,
{
    type Item = WatchResponse;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.as_mut().get_mut();

        loop {
            match &mut this.state {
                ReconnectState::Disconnected => {
                    let fut = this.connector.connect_watch(this.last_revision);
                    this.state = ReconnectState::Connecting(Box::pin(fut));
                }
                ReconnectState::Connecting(fut) => {
                    let stream = match ready!(fut.as_mut().poll(cx)) {
                        Ok(v) => v,
                        Err(e) => {
                            error!("watch reconnect failed: {e}");
                            this.state = ReconnectState::Terminated;
                            continue;
                        }
                    };
                    this.state = ReconnectState::Streaming { stream };
                }
                ReconnectState::Streaming { stream } => match Pin::new(stream).poll_next(cx) {
                    Poll::Ready(Some(Ok(watch_resp))) => {
                        if watch_resp.canceled() {
                            error!("watch cancelled: {watch_resp:?}");
                            this.state = ReconnectState::Terminated;
                            continue;
                        }

                        if let Some(revision) = watch_resp
                            .events()
                            .iter()
                            .filter_map(|ev| ev.kv())
                            .max_by_key(|kv| kv.mod_revision())
                            .map(|kv| kv.mod_revision())
                        {
                            this.last_revision.replace(revision);
                        }

                        return Poll::Ready(Some(watch_resp));
                    }
                    Poll::Ready(Some(Err(e))) => {
                        if is_transient(&e) {
                            this.state = ReconnectState::Disconnected;
                            continue;
                        }
                        error!("watch stream failed with non-transient error: {e}");
                        this.state = ReconnectState::Terminated;
                    }
                    Poll::Ready(None) => {
                        this.state = ReconnectState::Disconnected;
                    }
                    Poll::Pending => return Poll::Pending,
                },
                ReconnectState::Terminated => return Poll::Ready(None),
            }
        }
    }
}

pub trait WatchStreamValueDecoder {
    type Item;
    type Error: std::error::Error + Send + 'static;

    fn decode_watch_response(
        &mut self,
        key: &[u8],
        value: &[u8],
    ) -> Result<Self::Item, Self::Error>;
}

pub struct JsonDecoder<V> {
    _phantom: PhantomData<V>,
}

impl<V> Default for JsonDecoder<V> {
    fn default() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

impl<V> WatchStreamValueDecoder for JsonDecoder<V>
where
    V: DeserializeOwned,
{
    type Item = V;
    type Error = serde_json::Error;

    fn decode_watch_response(
        &mut self,
        _key: &[u8],
        value: &[u8],
    ) -> Result<Self::Item, Self::Error> {
        serde_json::from_slice(value)
    }
}

#[pin_project]
pub struct ValueWatchStream<Source, Value, Decoder> {
    #[pin]
    inner: Source,
    pending: VecDeque<WatchEvent<Value>>,
    decoder: Decoder,
}

impl<S, V, D> ValueWatchStream<S, V, D> {
    pub fn new(inner: S, decoder: D) -> Self {
        Self {
            inner,
            pending: VecDeque::new(),
            decoder,
        }
    }
}

impl<S, V, D> Stream for ValueWatchStream<S, V, D>
where
    D: WatchStreamValueDecoder<Item = V>,
    S: Stream<Item = WatchResponse> + Unpin,
    V: DeserializeOwned,
{
    type Item = WatchEvent<V>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        loop {
            if let Some(next) = this.pending.pop_front() {
                return Poll::Ready(Some(next));
            }
            let watch_resp = match ready!(this.inner.as_mut().poll_next(cx)) {
                Some(v) => v,
                None => return Poll::Ready(None),
            };

            for event in watch_resp.events() {
                let parsed = match event.event_type() {
                    EventType::Put => {
                        let kv = event.kv().expect("put event with no kv");
                        let key = Vec::from(kv.key());
                        let value = this
                            .decoder
                            .decode_watch_response(&key, kv.value())
                            .expect("failed to deserialize controller state");
                        WatchEvent::Put {
                            key,
                            value,
                            revision: kv.mod_revision(),
                        }
                    }
                    EventType::Delete => {
                        let kv = event.kv().expect("delete event with no kv");
                        let prev_value = event
                            .prev_kv()
                            .map(|prev_kv| {
                                this.decoder
                                    .decode_watch_response(prev_kv.key(), prev_kv.value())
                            })
                            .transpose()
                            .expect("failed to deserialize prev controller state");
                        let key = Vec::from(kv.key());
                        WatchEvent::Delete {
                            key,
                            prev_value,
                            revision: kv.mod_revision(),
                        }
                    }
                };

                this.pending.push_back(parsed);
            }
        }
    }
}

#[pin_project]
pub struct PutWatchStream<Source, Value, Decoder> {
    #[pin]
    inner: Source,
    _phantom: PhantomData<Value>,
    decoder: Decoder,
}

impl<S, T, D> PutWatchStream<S, T, D> {
    pub fn new(inner: S, decoder: D) -> Self {
        Self {
            inner,
            _phantom: PhantomData,
            decoder,
        }
    }
}

impl<S, T, D> Stream for PutWatchStream<S, T, D>
where
    S: Stream<Item = WatchResponse> + Unpin,
    D: WatchStreamValueDecoder<Item = T>,
{
    type Item = (Revision, T);

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        loop {
            let watch_resp = match ready!(Pin::new(&mut this.inner).poll_next(cx)) {
                Some(v) => v,
                None => return Poll::Ready(None),
            };

            let max_kv = watch_resp
                .events()
                .iter()
                .filter_map(|ev| ev.kv())
                .max_by_key(|kv| kv.mod_revision());

            if let Some(kv) = max_kv {
                let revision = kv.mod_revision();
                let state = this
                    .decoder
                    .decode_watch_response(kv.key(), kv.value())
                    .expect("failed to deserialize kv value");
                return Poll::Ready(Some((revision, state)));
            }
        }
    }
}

pub struct LockKeyChangeStream<S> {
    inner: S,
    key: Vec<u8>,
    key_mod_revision: Revision,
    done: bool,
}

impl<S> LockKeyChangeStream<S> {
    pub fn new(inner: S, key: Vec<u8>, key_mod_revision: Revision) -> Self {
        Self {
            inner,
            key,
            key_mod_revision,
            done: false,
        }
    }
}

impl<S> Stream for LockKeyChangeStream<S>
where
    S: Stream<Item = WatchResponse> + Unpin,
{
    type Item = Revision;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.as_mut().get_mut();

        if this.done {
            return Poll::Ready(None);
        }

        loop {
            let watch_resp = match ready!(Pin::new(&mut this.inner).poll_next(cx)) {
                Some(v) => v,
                None => {
                    this.done = true;
                    return Poll::Ready(None);
                }
            };

            for event in watch_resp.events() {
                match event.event_type() {
                    EventType::Put => {
                        let kv = event.kv().expect("put event with no kv");
                        if kv.key() == this.key {
                            continue;
                        }
                        let revision = kv.mod_revision();
                        if revision <= this.key_mod_revision {
                            continue;
                        }
                        info!(
                            "watcher detected put event on key {:?} with revision {} > {}",
                            this.key, revision, this.key_mod_revision
                        );
                        this.done = true;
                        return Poll::Ready(Some(revision));
                    }
                    EventType::Delete => {
                        let kv = event.kv().expect("delete event with no kv");
                        let revision = kv.mod_revision();
                        if revision < this.key_mod_revision {
                            continue;
                        }
                        if kv.key() == this.key {
                            let key_label = String::from_utf8_lossy(&this.key);
                            info!(
                                "watcher detected delete event on key {:?} with revision {} >= {}",
                                key_label, revision, this.key_mod_revision
                            );
                            this.done = true;
                            return Poll::Ready(Some(revision));
                        }
                    }
                }
            }
        }
    }
}

pub type EtcdReconnectWatchStream = AutoReconnectWatchStream<GrpcEtcdConenctor>;
pub type EtcdJsonWatchStream<V> = ValueWatchStream<EtcdReconnectWatchStream, V, JsonDecoder<V>>;
pub type EtcdJsonPutWatchStream<T> = PutWatchStream<EtcdReconnectWatchStream, T, JsonDecoder<T>>;
pub type EtcdLockKeyChangeStream = LockKeyChangeStream<EtcdReconnectWatchStream>;

///
/// Extension trait for [`WatchClient`].
///
/// This trait provides utility methods for working with [`WatchClient`].
///
/// This extension trait provides rust channel of watch stream and more reliability in case of transient errors.
///
/// On transient errors, the watch stream will be retried and resume where you left off.
///
#[async_trait::async_trait]
pub trait WatchClientExt {
    fn get_watch_client(&self) -> WatchClient;

    fn json_watch_stream<V>(
        &self,
        key: impl Into<Vec<u8>>,
        watch_options: Option<WatchOptions>,
    ) -> EtcdJsonWatchStream<V>
    where
        V: DeserializeOwned + Send + 'static,
    {
        self.value_watch_stream(key, watch_options, JsonDecoder::default())
    }

    fn value_watch_stream<D>(
        &self,
        key: impl Into<Vec<u8>>,
        watch_options: Option<WatchOptions>,
        decoder: D,
    ) -> ValueWatchStream<EtcdReconnectWatchStream, D::Item, D>
    where
        D: WatchStreamValueDecoder,
    {
        let wc = self.get_watch_client();
        let key: Vec<u8> = key.into();
        let wopts_prototype = watch_options.unwrap_or_default().with_prev_key();

        let connector = GrpcEtcdConenctor::new(wc, key, wopts_prototype);
        let reconnecting_stream = AutoReconnectWatchStream::new(connector);

        ValueWatchStream::new(reconnecting_stream, decoder)
    }

    fn put_watch_stream<D>(
        &self,
        key: impl Into<Vec<u8>>,
        watch_options: Option<WatchOptions>,
        decoder: D,
    ) -> PutWatchStream<EtcdReconnectWatchStream, D::Item, D>
    where
        D: WatchStreamValueDecoder,
    {
        let wc = self.get_watch_client();
        let key: Vec<u8> = key.into();
        let wopts_prototype = watch_options
            .unwrap_or_default()
            .with_filters(vec![WatchFilterType::NoDelete]);

        let connector = GrpcEtcdConenctor::new(wc, key, wopts_prototype);
        let reconnecting_stream = AutoReconnectWatchStream::new(connector);

        PutWatchStream::new(reconnecting_stream, decoder)
    }

    fn json_put_watch_stream<T>(
        &self,
        key: impl Into<Vec<u8>>,
        watch_options: Option<WatchOptions>,
    ) -> EtcdJsonPutWatchStream<T>
    where
        T: DeserializeOwned + Send + 'static,
    {
        self.put_watch_stream(key, watch_options, JsonDecoder::default())
    }

    ///
    /// Creates a channel that watches for changes to a key in etcd.
    ///
    /// The channel will send a [`WatchEvent`] for each change to the key.
    /// The channel will be retried on transient errors.
    ///
    /// The channel will be closed if the watch is cancelled or if the stream is closed.
    ///
    /// The watch expect value to be JSON encoded.
    fn json_watch_channel<V>(
        &self,
        key: impl Into<Vec<u8>>,
        watch_options: Option<WatchOptions>,
    ) -> EtcdJsonWatchStream<V>
    where
        V: DeserializeOwned + Send + 'static,
    {
        self.json_watch_stream::<V>(key, watch_options)
    }

    fn json_put_watch_channel<T>(
        &self,
        key: impl Into<Vec<u8>>,
        watch_options: Option<WatchOptions>,
    ) -> EtcdJsonPutWatchStream<T>
    where
        T: DeserializeOwned + Send + 'static,
    {
        self.json_put_watch_stream::<T>(key, watch_options)
    }

    fn watch_lock_key_change_stream(
        &self,
        key: impl Into<Vec<u8>>,
        key_mod_revision: Revision,
    ) -> EtcdLockKeyChangeStream {
        let wc = self.get_watch_client();
        let key: Vec<u8> = key.into();
        let wopts_prototype = WatchOptions::new().with_start_revision(key_mod_revision);

        let connector = GrpcEtcdConenctor::new(wc, key.clone(), wopts_prototype);
        let reconnecting_stream = AutoReconnectWatchStream::new(connector);

        LockKeyChangeStream::new(reconnecting_stream, key, key_mod_revision)
    }
}

impl WatchClientExt for WatchClient {
    fn get_watch_client(&self) -> WatchClient {
        self.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::{StreamExt, executor::block_on, future, stream};
    use std::{
        collections::VecDeque,
        sync::{Arc, Mutex},
    };

    fn make_watch_response(canceled: bool, revision: Option<Revision>) -> WatchResponse {
        let events = revision
            .map(|rev| {
                vec![etcd_client::proto::PbEvent {
                    r#type: 0,
                    kv: Some(etcd_client::proto::PbKeyValue {
                        mod_revision: rev,
                        ..Default::default()
                    }),
                    ..Default::default()
                }]
            })
            .unwrap_or_default();

        WatchResponse(etcd_client::proto::PbWatchResponse {
            canceled,
            events,
            ..Default::default()
        })
    }

    fn transient_error() -> Error {
        Error::GRpcStatus(tonic::Status::new(tonic::Code::Unavailable, "transient"))
    }

    fn non_transient_error() -> Error {
        Error::GRpcStatus(tonic::Status::new(
            tonic::Code::InvalidArgument,
            "non-transient",
        ))
    }

    struct TestConnector {
        connect_called: Arc<Mutex<bool>>,
    }

    impl EtcdConnector for TestConnector {
        type WatchStream = stream::Empty<Result<WatchResponse, Error>>;
        type ConnectFut = future::Ready<Result<Self::WatchStream, Error>>;

        fn connect_watch(&mut self, _last_revision: Option<Revision>) -> Self::ConnectFut {
            *self.connect_called.lock().expect("mutex poisoned") = true;
            future::ready(Err(Error::WatchError("connect failed".to_string())))
        }
    }

    struct MockConnector {
        calls: Arc<Mutex<Vec<Option<Revision>>>>,
        plans: VecDeque<Result<Vec<Result<WatchResponse, Error>>, Error>>,
    }

    impl MockConnector {
        fn new(plans: Vec<Result<Vec<Result<WatchResponse, Error>>, Error>>) -> Self {
            Self {
                calls: Arc::new(Mutex::new(Vec::new())),
                plans: plans.into(),
            }
        }
    }

    impl EtcdConnector for MockConnector {
        type WatchStream = stream::Iter<std::vec::IntoIter<Result<WatchResponse, Error>>>;
        type ConnectFut = future::Ready<Result<Self::WatchStream, Error>>;

        fn connect_watch(&mut self, last_revision: Option<Revision>) -> Self::ConnectFut {
            self.calls
                .lock()
                .expect("mutex poisoned")
                .push(last_revision);
            let planned = self.plans.pop_front().expect("missing test plan");
            match planned {
                Ok(items) => future::ready(Ok(stream::iter(items))),
                Err(e) => future::ready(Err(e)),
            }
        }
    }

    #[test]
    fn reconnecting_stream_terminates_when_connect_fails() {
        let connect_called = Arc::new(Mutex::new(false));
        let mut stream = AutoReconnectWatchStream::new(TestConnector {
            connect_called: Arc::clone(&connect_called),
        });

        let next = block_on(stream.next());
        assert!(next.is_none());
        assert!(*connect_called.lock().expect("mutex poisoned"));
    }

    #[test]
    fn json_watch_stream_returns_none_when_inner_is_empty() {
        let mut stream =
            ValueWatchStream::<_, serde_json::Value, JsonDecoder<serde_json::Value>>::new(
                stream::empty(),
                JsonDecoder::default(),
            );
        let next = block_on(stream.next());
        assert!(next.is_none());
    }

    #[test]
    fn json_put_watch_stream_returns_none_when_inner_is_empty() {
        let mut stream =
            PutWatchStream::<_, serde_json::Value, JsonDecoder<serde_json::Value>>::new(
                stream::empty(),
                JsonDecoder::default(),
            );
        let next = block_on(stream.next());
        assert!(next.is_none());
    }

    #[test]
    fn lock_key_change_stream_returns_none_when_inner_is_empty() {
        let mut stream = LockKeyChangeStream::new(stream::empty(), b"/lock/key".to_vec(), 42);
        let next = block_on(stream.next());
        assert!(next.is_none());
    }

    #[test]
    fn reconnecting_stream_terminates_on_non_transient_stream_error() {
        let connector = MockConnector::new(vec![Ok(vec![Err(non_transient_error())])]);
        let calls = Arc::clone(&connector.calls);
        let mut stream = AutoReconnectWatchStream::new(connector);

        let next = block_on(stream.next());
        assert!(next.is_none());
        assert_eq!(*calls.lock().expect("mutex poisoned"), vec![None]);
    }

    #[test]
    fn reconnecting_stream_reconnects_on_transient_stream_error() {
        let connector = MockConnector::new(vec![
            Ok(vec![Err(transient_error())]),
            Ok(vec![Ok(make_watch_response(false, Some(3)))]),
        ]);
        let calls = Arc::clone(&connector.calls);
        let mut stream = AutoReconnectWatchStream::new(connector);

        let next = block_on(stream.next());
        assert!(next.is_some());
        assert_eq!(*calls.lock().expect("mutex poisoned"), vec![None, None]);
    }

    #[test]
    fn reconnecting_stream_reconnects_when_inner_stream_ends() {
        let connector = MockConnector::new(vec![
            Ok(vec![]),
            Ok(vec![Ok(make_watch_response(false, Some(7)))]),
        ]);
        let calls = Arc::clone(&connector.calls);
        let mut stream = AutoReconnectWatchStream::new(connector);

        let next = block_on(stream.next());
        assert!(next.is_some());
        assert_eq!(*calls.lock().expect("mutex poisoned"), vec![None, None]);
    }

    #[test]
    fn reconnecting_stream_stops_on_canceled_watch_response() {
        let connector = MockConnector::new(vec![Ok(vec![Ok(make_watch_response(true, None))])]);
        let mut stream = AutoReconnectWatchStream::new(connector);

        let next = block_on(stream.next());
        assert!(next.is_none());
    }

    #[test]
    fn reconnecting_stream_tracks_last_revision_for_reconnect() {
        let connector = MockConnector::new(vec![
            Ok(vec![Ok(make_watch_response(false, Some(11)))]),
            Ok(vec![]),
            Err(non_transient_error()),
        ]);
        let calls = Arc::clone(&connector.calls);
        let mut stream = AutoReconnectWatchStream::new(connector);

        let first = block_on(stream.next());
        assert!(first.is_some());
        let second = block_on(stream.next());
        assert!(second.is_none());

        let calls = calls.lock().expect("mutex poisoned");
        assert_eq!(calls[0], None);
        assert!(calls.iter().skip(1).all(|v| *v == Some(11)));
    }
}
