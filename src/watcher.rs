use {
    super::{retry::retry_etcd_legacy, Revision},
    crate::retry::is_transient,
    etcd_client::{EventType, WatchClient, WatchFilterType, WatchOptions},
    retry::delay::Exponential,
    serde::de::DeserializeOwned,
    tokio::sync::{broadcast, mpsc},
    tokio_stream::StreamExt,
    tracing::{error, info, warn},
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

    fn json_watch_channel<V>(
        &self,
        key: impl Into<Vec<u8>>,
        watch_options: Option<WatchOptions>,
    ) -> mpsc::Receiver<WatchEvent<V>>
    where
        V: DeserializeOwned + Send + 'static,
    {
        let wc = self.get_watch_client();
        let (tx, rx) = tokio::sync::mpsc::channel(10);
        let key: Vec<u8> = key.into();
        tokio::spawn(async move {
            let wopts_prototype = watch_options.unwrap_or_default().with_prev_key();
            let mut last_revision = None; // 0 = latest revision
            'outer: loop {
                let mut wopts = wopts_prototype.clone();
                if let Some(rev) = last_revision {
                    wopts = wopts.with_start_revision(rev);
                }
                let wc2 = wc.clone();
                let key2 = key.clone();
                let retry_strategy = Exponential::from_millis_with_factor(10, 10.0).take(3);

                let (mut watcher, mut stream) = retry_etcd_legacy(retry_strategy, move || {
                    let mut wc = wc2.clone();
                    let key = key2.clone();
                    let wopts = wopts.clone();
                    async move { wc.watch(key.clone(), Some(wopts)).await }
                })
                .await
                .expect("watch retry failed");

                'inner: while let Some(watch_resp) = stream.next().await {
                    match watch_resp {
                        Ok(watch_resp) => {
                            if watch_resp.canceled() {
                                // This is probably because the compaction_revision < initial revision
                                error!("watch cancelled: {watch_resp:?}");
                                break 'outer;
                            }
                            for event in watch_resp.events() {
                                let watch_event = match event.event_type() {
                                    EventType::Put => {
                                        let kv = event.kv().expect("put event with no kv");
                                        let key = Vec::from(kv.key());
                                        let value = serde_json::from_slice::<V>(kv.value())
                                            .expect("failed to deserialize controller state");
                                        last_revision.replace(kv.mod_revision());
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
                                            .map(|prev_kv| prev_kv.value())
                                            .map(|prev_v| serde_json::from_slice::<V>(prev_v))
                                            .transpose()
                                            .expect("failed to deserialize prev controller state");
                                        let key = Vec::from(kv.key());
                                        last_revision.replace(kv.mod_revision());
                                        WatchEvent::Delete {
                                            key,
                                            prev_value,
                                            revision: kv.mod_revision(),
                                        }
                                    }
                                };
                                if tx.send(watch_event).await.is_err() {
                                    warn!("closed watch event receiver");
                                    break 'outer;
                                }
                            }
                        }
                        Err(e) => {
                            error!("watch stream error: {:?}", e);
                            break 'inner;
                        }
                    }
                }
                let _ = watcher.cancel().await;
            }
        });
        rx
    }

    fn json_put_watch_channel<T>(
        &self,
        key: impl Into<Vec<u8>>,
        watch_options: Option<WatchOptions>,
    ) -> mpsc::Receiver<(Revision, T)>
    where
        T: DeserializeOwned + Send + 'static,
    {
        let wc = self.get_watch_client();
        let (tx, rx) = tokio::sync::mpsc::channel(10);
        let key: Vec<u8> = key.into();
        tokio::spawn(async move {
            let mut last_revision = None; // 0 = latest revision
            let wopts_prototype = watch_options
                .unwrap_or_default()
                .with_filters(vec![WatchFilterType::NoDelete]);
            'outer: loop {
                let mut wopts = wopts_prototype.clone();
                if let Some(rev) = last_revision {
                    wopts = wopts.with_start_revision(rev);
                }

                let retry_strategy = Exponential::from_millis_with_factor(10, 10.0).take(3);
                let wc2 = wc.clone();
                let key2 = key.clone();
                let (mut watcher, mut stream) = retry_etcd_legacy(retry_strategy, move || {
                    let mut wc = wc2.clone();
                    let key = key2.clone();
                    let wopts = wopts.clone();
                    async move { wc.watch(key.clone(), Some(wopts)).await }
                })
                .await
                .expect("watch retry failed");

                'inner: while let Some(watch_resp) = stream.next().await {
                    match watch_resp {
                        Ok(watch_resp) => {
                            let max_kv = watch_resp
                                .events()
                                .iter()
                                .filter_map(|ev| ev.kv())
                                .max_by_key(|kv| kv.mod_revision());
                            if let Some(kv) = max_kv {
                                let revision = kv.mod_revision();
                                last_revision.replace(revision);

                                let state = serde_json::from_slice::<T>(kv.value())
                                    .expect("failed to deserialize kv value");
                                if tx.send((revision, state)).await.is_err() {
                                    let key_str = String::from_utf8(key).expect("key is not utf8");
                                    warn!("json watch channel closed its receiving half for {key_str}");
                                    break 'outer;
                                }
                            } else if watch_resp.canceled() {
                                // This is probably because the compaction_revision < initial revision
                                error!("watch cancelled: {watch_resp:?}");
                                break 'outer;
                            }
                        }
                        Err(e) => {
                            error!("watch stream error: {:?}", e);
                            break 'inner;
                        }
                    }
                }
                let _ = watcher.cancel().await;
            }
        });
        rx
    }

    ///
    /// Creates a broadcast channel that watches for a lock key that gets deleted.
    ///
    fn watch_lock_key_change(
        &self,
        key: impl Into<Vec<u8>>,
        key_mod_revision: Revision,
    ) -> broadcast::Sender<Revision> {
        let wc = self.get_watch_client();
        let key: Vec<u8> = key.into();

        let (tx, _) = broadcast::channel(1);

        let tx2 = tx.clone();
        tokio::spawn(async move {
            let wopts = WatchOptions::new()
                .with_start_revision(key_mod_revision);
            let tx = tx2;
            'outer: loop {
                let key2 = key.clone();
                let key = key.clone();
                let retry_strategy = Exponential::from_millis_with_factor(10, 10.0).take(3);
                let wc2 = wc.clone();
                let wopts2 = wopts.clone();
                let (mut watcher, mut stream) = retry_etcd_legacy(retry_strategy, move || {
                    let mut wc = wc2.clone();
                    let key = key2.clone();
                    let wopts = wopts2.clone();
                    async move { wc.watch(key.clone(), Some(wopts)).await }
                })
                .await
                .expect("watch retry failed");

                match stream.next().await {
                    Some(Ok(watch_resp)) => {
                        if watch_resp.canceled() {
                            // This is probably because the compaction_revision < initial revision
                            error!("watch cancelled: {watch_resp:?}");
                            break;
                        }

                        for event in watch_resp.events() {

                            match event.event_type() {
                                EventType::Put => {
                                    let kv = event.kv().expect("put event with no kv");
                                    if kv.key() == key {
                                        continue;
                                    }
                                    let revision = kv.mod_revision();
                                    if revision <= key_mod_revision {
                                        continue;
                                    }
                                    info!("watcher detected put event on key {key:?} with revision {revision} > {key_mod_revision}");
                                    let _ = tx.send(revision);
                                    let _ = watcher.cancel().await;
                                    break 'outer;
                                }
                                EventType::Delete => {
                                    let kv = event.kv().expect("delete event with no kv");
                                    let revision = kv.mod_revision();
                                    if revision < key_mod_revision {
                                        continue;
                                    }

                                    if kv.key() == key {
                                        let key_label = String::from_utf8_lossy(&key);
                                        info!("watcher detected delete event on key {key_label:?} with revision {revision} >= {key_mod_revision}");
                                        let _ = tx.send(revision);
                                        let _ = watcher.cancel().await;
                                        break 'outer;
                                    }
                                    
                                }
                            }
                        }
                    }
                    Some(Err(e)) => {
                        if !is_transient(&e) {
                            tracing::error!("watch stream error: {e}");
                            break;
                        }
                    }
                    None => {
                        tracing::warn!("watch stream closed");
                    }
                }
                let _ = watcher.cancel().await;
            }
        });
        tx
    }
}

impl WatchClientExt for WatchClient {
    fn get_watch_client(&self) -> WatchClient {
        self.clone()
    }
}
