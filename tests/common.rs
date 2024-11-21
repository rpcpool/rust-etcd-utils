pub async fn get_etcd_client() -> etcd_client::Client {

    let url = option_env!("ETCD_TEST_URL").unwrap_or("http://localhost:2379");
    etcd_client::Client::connect([url], None)
        .await
        .expect("failed to connect to etcd")
}

pub fn random_str(len: usize) -> String {
    use rand::{distributions::Alphanumeric, thread_rng, Rng};
    let mut rng = thread_rng();
    (&mut rng)
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}
