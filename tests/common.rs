use std::io::{self, IsTerminal};
use tracing::level_filters::LevelFilter;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

pub async fn get_etcd_client() -> etcd_client::Client {
    let url = option_env!("ETCD_TEST_URL").unwrap_or("http://localhost:2379");
    etcd_client::Client::connect([url], None)
        .await
        .expect("failed to connect to etcd")
}

#[allow(dead_code)]
pub fn random_str(len: usize) -> String {
    use rand::{distributions::Alphanumeric, thread_rng, Rng};
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
