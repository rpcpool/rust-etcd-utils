use std::sync::{atomic::AtomicBool, Arc};
use tokio::sync::{Mutex, Notify};

struct Inner<T> {
    value: Mutex<Option<T>>,
    notify: Notify,
    is_closed: AtomicBool,
}

pub struct Sender<T> {
    inner: Arc<Inner<T>>,
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        self.inner.notify.notify_one();
        self.inner
            .is_closed
            .store(true, std::sync::atomic::Ordering::Release);
    }
}

pub struct Receiver<T> {
    inner: Arc<Inner<T>>,
}

pub fn watch<T>() -> (Sender<T>, Receiver<T>) {
    let inner = Arc::new(Inner {
        value: Mutex::new(None),
        notify: Notify::new(),
        is_closed: AtomicBool::new(false),
    });

    (
        Sender {
            inner: inner.clone(),
        },
        Receiver { inner },
    )
}

impl<T> Inner<T> {
    // Producer can overwrite the existing value
    async fn send_and_replace(&self, value: T) {
        let mut guard = self.value.lock().await;
        *guard = Some(value); // Overwrite the current value
        self.notify.notify_one(); // Notify the consumer that a new value is available
    }

    // Consumer waits for a value if the channel is empty
    async fn consume(&self) -> Option<T> {
        if !self.is_closed.load(std::sync::atomic::Ordering::Acquire) {
            self.notify.notified().await;
        }
        {
            let mut guard = self.value.lock().await;
            guard.take()
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("Channel is closed")]
pub struct SendError<T> {
    inner: T,
}

impl<T> SendError<T> {
    #[allow(dead_code)]
    pub fn into_inner(self) -> T {
        self.inner
    }
}

impl<T> Sender<T> {
    pub async fn update(&mut self, value: T) -> Result<(), SendError<T>> {
        if Arc::strong_count(&self.inner) < 2 {
            Err(SendError { inner: value })
        } else {
            self.inner.send_and_replace(value).await;
            Ok(())
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("Channel is closed")]
pub struct RecvError;

impl<T> Receiver<T> {
    pub async fn recv(&mut self) -> Option<T> {
        self.inner.consume().await
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn receiver_should_await_if_no_value_available() {
        let (mut tx, mut rx) = watch::<i32>();
        let h = tokio::spawn(async move { rx.recv().await });

        tx.update(1).await.expect("send failed");
        let val = h.await.expect("recv failed");
        assert_eq!(val, Some(1));
    }

    #[tokio::test]
    async fn send_and_replace_should_fail_if_receiver_is_closed() {
        let (mut tx, rx) = watch::<i32>();
        drop(rx);

        let result = tx.update(1).await;

        assert!(result.is_err());
        let err = result.expect_err("error");
        assert_eq!(err.into_inner(), 1);
    }

    #[tokio::test]
    async fn rx_should_return_immediately_if_value_available() {
        let (mut tx, mut rx) = watch::<i32>();

        tx.update(1).await.expect("send failed");

        let val = rx.recv().await;
        assert_eq!(val, Some(1));
    }

    #[tokio::test]
    async fn rx_should_always_see_the_latest_value() {
        let (mut tx, mut rx) = watch::<i32>();

        tx.update(1).await.expect("send failed");
        tx.update(10).await.expect("send failed");
        tx.update(100).await.expect("send failed");

        drop(tx);
        let val = rx.recv().await;
        assert_eq!(val, Some(100));
    }

    #[tokio::test]
    async fn rx_should_wait_for_new_value() {
        let (mut tx, mut rx) = watch::<i32>();
        let h = tokio::spawn(async move {
            let mut captured = vec![];
            let x = rx.recv().await.expect("recv failed");
            captured.push(x);
            let y = rx.recv().await.expect("recv failed");
            captured.push(y);
            captured
        });

        tx.update(1).await.expect("send failed");
        tokio::time::sleep(Duration::from_millis(10)).await;
        tx.update(10).await.expect("send failed");
        let actual = h.await.expect("recv failed");
        assert_eq!(actual, vec![1, 10]);
    }

    #[tokio::test]
    async fn rx_recv_should_return_none_if_sender_is_dropped() {
        let (tx, mut rx) = watch::<i32>();
        drop(tx);

        let val = rx.recv().await;
        assert_eq!(val, None);
    }

    #[tokio::test]
    async fn rx_recv_should_not_loose_last_sent_event_before_sender_drop() {
        let (mut tx, mut rx) = watch::<i32>();
        tx.update(1).await.expect("send failed");
        drop(tx);

        let val = rx.recv().await;
        assert_eq!(val, Some(1));
        let val = rx.recv().await;
        assert_eq!(val, None);
    }
}
