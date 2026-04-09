use bytes::Bytes;
use http_body_util::Empty;
use hyper::{Request, Response, body::Incoming, server::conn::http2, service::service_fn};
use hyper_util::rt::{TokioExecutor, TokioIo};
use std::io::{self, IsTerminal};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio::sync::oneshot;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

use rust_etcd_utils::channel::connect_with_reliable_balanced_channel;

#[allow(dead_code)]
pub async fn get_etcd_client() -> etcd_client::Client {
    let url = option_env!("ETCD_TEST_URL").unwrap_or("http://localhost:2379");
    connect_with_reliable_balanced_channel([url], None)
        .await
        .expect("failed to connect to etcd")
}

#[allow(dead_code)]
pub fn random_str(len: usize) -> String {
    use rand::{Rng, distributions::Alphanumeric, thread_rng};
    let mut rng = thread_rng();
    (&mut rng)
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}

#[allow(dead_code)]
pub fn setup_tracing() {
    let env_filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::TRACE.into())
        .with_default_directive("rust_etcd_utils=trace".parse().unwrap())
        .from_env_lossy();
    let subscriber = tracing_subscriber::registry().with(env_filter);

    let is_atty = io::stdout().is_terminal() && io::stderr().is_terminal();
    let io_layer = tracing_subscriber::fmt::layer()
        .with_line_number(true)
        .with_ansi(is_atty);

    subscriber
        .with(io_layer)
        .try_init()
        .expect("failed to setup tracing");
}

#[allow(dead_code)]
pub fn reserve_localhost_addr() -> SocketAddr {
    let listener =
        std::net::TcpListener::bind("127.0.0.1:0").expect("failed to reserve localhost port");
    let addr = listener
        .local_addr()
        .expect("failed to read reserved localhost addr");
    drop(listener);
    addr
}

#[allow(dead_code)]
pub fn endpoint_url(addr: SocketAddr) -> String {
    format!("http://{}", addr)
}

#[allow(dead_code)]
pub struct FakeTcpServer {
    shutdown_tx: Option<oneshot::Sender<()>>,
    join: tokio::task::JoinHandle<()>,
}

#[allow(dead_code)]
pub async fn spawn_fake_tcp_server(addr: SocketAddr) -> FakeTcpServer {
    let listener = TcpListener::bind(addr)
        .await
        .expect("failed to bind fake tcp server");
    let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();

    let join = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = &mut shutdown_rx => break,
                accept_res = listener.accept() => {
                    match accept_res {
                        Ok((stream, _)) => {
                            drop(stream);
                        }
                        Err(_) => break,
                    }
                }
            }
        }
    });

    FakeTcpServer {
        shutdown_tx: Some(shutdown_tx),
        join,
    }
}

impl FakeTcpServer {
    #[allow(dead_code)]
    pub async fn shutdown(mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        let _ = self.join.await;
    }
}

#[allow(dead_code)]
pub struct MockH2Server {
    shutdown_tx: Option<oneshot::Sender<()>>,
    join: tokio::task::JoinHandle<()>,
    conn_tasks: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
}

#[allow(dead_code)]
pub async fn spawn_mock_h2_server(addr: SocketAddr) -> MockH2Server {
    let listener = TcpListener::bind(addr)
        .await
        .expect("failed to bind mock h2 server");
    let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();
    let conn_tasks: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>> = Arc::new(Mutex::new(Vec::new()));
    let conn_tasks_bg = Arc::clone(&conn_tasks);

    let join = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = &mut shutdown_rx => break,
                accept_res = listener.accept() => {
                    let Ok((stream, _)) = accept_res else {
                        break;
                    };
                    let task = tokio::spawn(async move {
                        let io = TokioIo::new(stream);
                        let service = service_fn(|_req: Request<Incoming>| async move {
                            Ok::<_, std::convert::Infallible>(Response::new(Empty::<Bytes>::new()))
                        });
                        let _ = http2::Builder::new(TokioExecutor::new())
                            .serve_connection(io, service)
                            .await;
                    });
                    conn_tasks_bg.lock().await.push(task);
                }
            }
        }
    });

    MockH2Server {
        shutdown_tx: Some(shutdown_tx),
        join,
        conn_tasks,
    }
}

impl MockH2Server {
    #[allow(dead_code)]
    pub async fn shutdown(mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }

        {
            let mut tasks = self.conn_tasks.lock().await;
            for task in tasks.drain(..) {
                task.abort();
            }
        }

        let _ = self.join.await;
    }
}
