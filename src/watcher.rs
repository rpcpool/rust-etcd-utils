use {
    super::{retry::retry_etcd_legacy, Revision},
    etcd_client::{EventType, WatchClient, WatchFilterType, WatchOptions},
    retry::delay::Exponential,
    serde::de::DeserializeOwned,
    tokio::sync::mpsc,
    tokio_stream::StreamExt,
    tracing::{error, warn},
};

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
}

impl WatchClientExt for WatchClient {
    fn get_watch_client(&self) -> WatchClient {
        self.clone()
    }
}
