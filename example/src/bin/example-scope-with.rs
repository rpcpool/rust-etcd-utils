use core::time::Duration;

use rust_etcd_utils::{lease::ManagedLeaseFactory, lock::spawn_lock_manager};

#[tokio::main]
async fn main() {
    let etcd = etcd_client::Client::connect(["http://localhost:2379"], None)
        .await
        .expect("failed to connect to etcd");

    let lease_duration = Duration::from_secs(2);
    let managed_lease_factory = ManagedLeaseFactory::new(etcd.clone());
    let (_lock_man_handle, lock_manager) = spawn_lock_manager(etcd.clone(), managed_lease_factory);

    let lock_name = "example-lock";

    let my_managed_lock = lock_manager
        .lock(lock_name, lease_duration)
        .await
        .expect("failed to lock");

    let lock_key = my_managed_lock.get_key();
    let h = tokio::spawn(async move {
        println!("Lock acquired in subtask!");
        // If you need to run something as long as the lock is held, you can use `scope_with` method.
        my_managed_lock
            .scope_with(|_guard| async move {
                let now = tokio::time::Instant::now();
                loop {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    println!("Subtask is still running after {:?}", now.elapsed());
                }
            })
            .await
    });

    println!("Press Ctrl-C to delete the lock lease and abort `scope_with` loop");
    let _ = tokio::signal::ctrl_c().await;

    // You don't want to drop the key manually like this, this is just for demonstration purposes.
    let _ = etcd
        .kv_client()
        .delete(lock_key, None)
        .await
        .expect("failed to delete lock key");

    let _ = h.await;

    println!("Finished!");
}
