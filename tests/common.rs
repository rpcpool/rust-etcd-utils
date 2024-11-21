use etcd_client::ConnectOptions;



pub async fn get_etcd_client() -> etcd_client::Client {

    etcd_client::Client::connect(["http://localhost:2379"], None)
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
