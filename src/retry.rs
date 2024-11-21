use {
    retry::delay::Exponential,
    std::{error::Error, future::Future, time::Duration},
    tracing::{error, warn},
};

pub fn is_transient(err: &etcd_client::Error) -> bool {
    match err {
        etcd_client::Error::GRpcStatus(status) => match status.code() {
            tonic::Code::Ok => false,
            tonic::Code::Cancelled => false,
            tonic::Code::Unknown => status.source().is_none(),
            tonic::Code::InvalidArgument => false,
            tonic::Code::DeadlineExceeded => true,
            tonic::Code::NotFound => false,
            tonic::Code::AlreadyExists => false,
            tonic::Code::PermissionDenied => false,
            tonic::Code::ResourceExhausted => true,
            tonic::Code::FailedPrecondition => false,
            tonic::Code::Aborted => false,
            tonic::Code::OutOfRange => false,
            tonic::Code::Unimplemented => false,
            tonic::Code::Internal => true,
            tonic::Code::Unavailable => true,
            tonic::Code::DataLoss => true,
            tonic::Code::Unauthenticated => false,
        },
        _ => false,
    }
}

pub async fn retry_etcd_txn(
    etcd: etcd_client::Client,
    txn: etcd_client::Txn,
) -> Result<etcd_client::TxnResponse, etcd_client::Error> {
    retry_etcd(etcd, (txn,), move |etcd, (txn,)| async move {
        etcd.kv_client().txn(txn).await
    })
    .await
}

pub async fn retry_etcd_get(
    etcd: etcd_client::Client,
    key: String,
    opts: Option<etcd_client::GetOptions>,
) -> Result<etcd_client::GetResponse, etcd_client::Error> {
    retry_etcd(etcd, (key, opts), move |etcd, (key, opts)| async move {
        etcd.kv_client().get(key, opts).await
    })
    .await
}

pub async fn retry_etcd<A, T, F, Fut>(
    etcd: etcd_client::Client,
    reusable_args: A,
    f: F,
) -> Result<T, etcd_client::Error>
where
    A: Clone + Send + 'static,
    Fut: Future<Output = Result<T, etcd_client::Error>> + Send + 'static,
    F: FnMut(etcd_client::Client, A) -> Fut,
    T: Send + 'static,
{
    let retry_strategy = Exponential::from_millis_with_factor(10, 10.0).take(3);
    retry_etcd_with_strategy(etcd, reusable_args, retry_strategy, f).await
}

pub async fn retry_etcd_with_strategy<A, T, F, Fut>(
    etcd: etcd_client::Client,
    reusable_args: A,
    retry_strategy: impl IntoIterator<Item = Duration>,
    mut f: F,
) -> Result<T, etcd_client::Error>
where
    A: Clone + Send + 'static,
    Fut: Future<Output = Result<T, etcd_client::Error>> + Send + 'static,
    F: FnMut(etcd_client::Client, A) -> Fut,
    T: Send + 'static,
{
    let mut retry_strategy = retry_strategy.into_iter();
    loop {
        match f(etcd.clone(), reusable_args.clone()).await {
            Ok(o) => return Ok(o),
            Err(e) => {
                if is_transient(&e) {
                    warn!("failed due to transient state {:?}", e);
                    match retry_strategy.next() {
                        Some(duration) => {
                            tokio::time::sleep(duration).await;
                        }
                        None => return Err(e),
                    }
                } else {
                    error!("failed due to non-transient state: {:?}", e);
                    return Err(e);
                }
            }
        }
    }
}

pub(crate) async fn retry_etcd_legacy<T, F, Fut>(
    retry_strategy: impl IntoIterator<Item = Duration>,
    mut f: F,
) -> Result<T, etcd_client::Error>
where
    Fut: Future<Output = Result<T, etcd_client::Error>> + Send + 'static,
    F: FnMut() -> Fut,
    T: Send + 'static,
{
    let mut retry_strategy = retry_strategy.into_iter();
    loop {
        match f().await {
            Ok(o) => return Ok(o),
            Err(e) => {
                if is_transient(&e) {
                    warn!("failed due to transient state {:?}", e);
                    match retry_strategy.next() {
                        Some(duration) => {
                            tokio::time::sleep(duration).await;
                        }
                        None => return Err(e),
                    }
                } else {
                    error!("failed due to non-transient state: {:?}", e);
                    return Err(e);
                }
            }
        }
    }
}
