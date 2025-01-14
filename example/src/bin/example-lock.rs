use core::time::Duration;

use futures::lock;
use rust_etcd_utils::{lease::ManagedLeaseFactory, lock::spawn_lock_manager};

#[tokio::main]
async fn main() {
    let etcd = etcd_client::Client::connect(["http://localhost:2379"], None)
        .await
        .expect("failed to connect to etcd");

    let lease_duration = Duration::from_secs(2);
    let managed_lease_factory = ManagedLeaseFactory::new(etcd.clone());
    let (lock_man_handle, lock_manager) = spawn_lock_manager(etcd.clone(), managed_lease_factory);

    let lock_name = "example-lock";

    // Managed lock are RAII guards that automatically release the lock when they are dropped.
    let my_managed_lock = lock_manager
        .lock(lock_name, lease_duration)
        .await
        .expect("failed to lock");

    let lock_manager2 = lock_manager.clone();
    let h = tokio::spawn(async move {
        lock_manager2
            .lock("example-lock", lease_duration)
            .await
            .expect("failed to lock");
        println!("Lock acquired in task 2!");
    });

    println!("Lock acquired in main task!");

    println!("Sleeping for 5 second...");
    for i in 1..=5 {
        println!("{}...", i);
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    println!("Dropping managed lock!");
    drop(my_managed_lock);

    println!("Waiting for task 2 to acquire lock...");
    h.await.expect("task 2 failed to acquire lock");

    drop(lock_manager);
    lock_man_handle.await.expect("lock manager failed");
    println!("Finished!");
}
