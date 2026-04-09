use std::time::Duration;

use futures::StreamExt;
use rust_etcd_utils::{
    Revision,
    watcher::{AutoReconnectWatchStream, EtcdConnector, WatchClientExt, WatchEvent},
};
use serde::{Deserialize, Serialize};
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};

mod common;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct DummyValue {
    value: i64,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn json_watch_stream_emits_put_then_delete_with_prev_value() {
    let mut etcd = common::get_etcd_client().await;
    let key = format!("watch-json-{}", common::random_str(12));

    let _ = etcd.delete(key.as_str(), None).await;

    let snapshot = etcd
        .get(key.as_str(), None)
        .await
        .expect("snapshot get failed");
    let start_revision = snapshot
        .header()
        .expect("missing snapshot header")
        .revision()
        + 1;

    let mut stream = etcd.watch_client().json_watch_stream::<DummyValue>(
        key.clone(),
        Some(etcd_client::WatchOptions::new().with_start_revision(start_revision)),
    );

    let put_value = DummyValue { value: 10 };
    etcd.kv_client()
        .put(
            key.as_str(),
            serde_json::to_string(&put_value).expect("json"),
            None,
        )
        .await
        .expect("put failed");

    let put_event = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("watch timeout on put")
        .expect("watch stream ended unexpectedly");

    match put_event {
        WatchEvent::Put {
            key: event_key,
            value,
            revision: _,
        } => {
            assert_eq!(event_key, key.as_bytes().to_vec());
            assert_eq!(value, put_value);
        }
        WatchEvent::Delete { .. } => panic!("expected put event"),
    }

    etcd.kv_client()
        .delete(
            key.as_str(),
            Some(etcd_client::DeleteOptions::new().with_prev_key()),
        )
        .await
        .expect("delete failed");

    let delete_event = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("watch timeout on delete")
        .expect("watch stream ended unexpectedly");

    match delete_event {
        WatchEvent::Delete {
            key: event_key,
            prev_value,
            revision: _,
        } => {
            assert_eq!(event_key, key.as_bytes().to_vec());
            assert_eq!(prev_value, Some(put_value));
        }
        WatchEvent::Put { .. } => panic!("expected delete event"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn json_put_watch_stream_emits_put_events_and_filters_delete() {
    let mut etcd = common::get_etcd_client().await;
    let key = format!("watch-json-put-{}", common::random_str(12));

    let _ = etcd.delete(key.as_str(), None).await;

    let snapshot = etcd
        .get(key.as_str(), None)
        .await
        .expect("snapshot get failed");
    let start_revision = snapshot
        .header()
        .expect("missing snapshot header")
        .revision()
        + 1;

    let mut stream = etcd.watch_client().json_put_watch_stream::<DummyValue>(
        key.clone(),
        Some(etcd_client::WatchOptions::new().with_start_revision(start_revision)),
    );

    let first = DummyValue { value: 1 };
    etcd.kv_client()
        .put(
            key.as_str(),
            serde_json::to_string(&first).expect("json"),
            None,
        )
        .await
        .expect("first put failed");

    let (_rev1, got_first) = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("watch timeout on first put")
        .expect("watch stream ended unexpectedly");
    assert_eq!(got_first, first);

    etcd.kv_client()
        .delete(key.as_str(), None)
        .await
        .expect("delete failed");

    let second = DummyValue { value: 2 };
    etcd.kv_client()
        .put(
            key.as_str(),
            serde_json::to_string(&second).expect("json"),
            None,
        )
        .await
        .expect("second put failed");

    let (_rev2, got_second) = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("watch timeout on second put")
        .expect("watch stream ended unexpectedly");
    assert_eq!(got_second, second);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn watch_lock_key_change_stream_emits_on_lock_key_delete() {
    let etcd = common::get_etcd_client().await;
    let key = format!("watch-lock-delete-{}", common::random_str(12));

    let put_resp = etcd
        .kv_client()
        .put(key.as_str(), "seed", None)
        .await
        .expect("seed put failed");
    let key_mod_revision = put_resp.header().expect("missing put header").revision();

    let mut stream = etcd
        .watch_client()
        .watch_lock_key_change_stream(key.clone(), key_mod_revision);

    etcd.kv_client()
        .delete(key.as_str(), None)
        .await
        .expect("delete failed");

    let revision = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("watch timeout on lock delete")
        .expect("watch stream ended unexpectedly");

    assert!(revision >= key_mod_revision);
}

fn make_watch_response(revision: Revision) -> etcd_client::WatchResponse {
    etcd_client::WatchResponse(etcd_client::proto::PbWatchResponse {
        canceled: false,
        events: vec![etcd_client::proto::PbEvent {
            r#type: 0,
            kv: Some(etcd_client::proto::PbKeyValue {
                mod_revision: revision,
                ..Default::default()
            }),
            ..Default::default()
        }],
        ..Default::default()
    })
}

fn transient_error() -> etcd_client::Error {
    etcd_client::Error::GRpcStatus(tonic::Status::new(tonic::Code::Unavailable, "transient"))
}

struct ScriptedConnector {
    calls: Arc<Mutex<Vec<Option<Revision>>>>,
    plans: VecDeque<
        Result<Vec<Result<etcd_client::WatchResponse, etcd_client::Error>>, etcd_client::Error>,
    >,
}

impl ScriptedConnector {
    fn new(
        plans: Vec<
            Result<Vec<Result<etcd_client::WatchResponse, etcd_client::Error>>, etcd_client::Error>,
        >,
    ) -> Self {
        Self {
            calls: Arc::new(Mutex::new(Vec::new())),
            plans: plans.into(),
        }
    }
}

impl EtcdConnector for ScriptedConnector {
    type WatchStream = futures::stream::Iter<
        std::vec::IntoIter<Result<etcd_client::WatchResponse, etcd_client::Error>>,
    >;
    type ConnectFut = futures::future::Ready<Result<Self::WatchStream, etcd_client::Error>>;

    fn connect_watch(&mut self, last_revision: Option<Revision>) -> Self::ConnectFut {
        self.calls
            .lock()
            .expect("mutex poisoned")
            .push(last_revision);

        let planned = self.plans.pop_front().expect("missing reconnect plan");
        match planned {
            Ok(items) => futures::future::ready(Ok(futures::stream::iter(items))),
            Err(e) => futures::future::ready(Err(e)),
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn autoreconnect_stream_reconnects_after_transient_error() {
    let connector = ScriptedConnector::new(vec![
        Ok(vec![Err(transient_error())]),
        Ok(vec![Ok(make_watch_response(123))]),
    ]);
    let calls = Arc::clone(&connector.calls);

    let mut stream = AutoReconnectWatchStream::new(connector);
    let next = tokio::time::timeout(Duration::from_secs(3), stream.next())
        .await
        .expect("timeout waiting for reconnect")
        .expect("stream ended unexpectedly");

    let max_revision = next
        .events()
        .iter()
        .filter_map(|ev| ev.kv())
        .map(|kv| kv.mod_revision())
        .max()
        .expect("missing kv in reconnect event");
    assert_eq!(max_revision, 123);

    let calls = calls.lock().expect("mutex poisoned");
    assert_eq!(*calls, vec![None, None]);
}
