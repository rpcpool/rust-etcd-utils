use core::time::Duration;

use rust_etcd_utils::{
    lease::ManagedLeaseFactory,
    lock::spawn_lock_manager,
    log::{ExclusiveLogUpdater, LogWatcher},
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DummyValue {
    value: i32,
}

///
/// This example shows how to create a "protected" or "exclusive-writer" log in etcd where you update a key using put operations only.
///
/// The log is protected by a lock, which is acquired by the writer before updating the log.
/// The type system ensures that the lock is held when updating the log by forcing the user to use the `scope_with` method.
///
#[tokio::main]
async fn main() {
    let etcd = etcd_client::Client::connect(["http://localhost:2379"], None)
        .await
        .expect("failed to connect to etcd");

    let lease_duration = Duration::from_secs(2);
    let managed_lease_factory = ManagedLeaseFactory::new(etcd.clone());
    let (_lock_man_handle, lock_manager) = spawn_lock_manager(etcd.clone(), managed_lease_factory);

    let lock_name = "example-lock";

    // Managed lock are RAII guards that automatically release the lock when they are dropped.
    let my_managed_lock = lock_manager
        .lock(lock_name, lease_duration)
        .await
        .expect("failed to lock");

    let lock_key = my_managed_lock.get_key();
    let writer_handle = tokio::spawn(async move {
        println!("Lock acquired in subtask!");
        // As
        my_managed_lock
            .scope_with(|guard| async move {
                let mut updater =
                    ExclusiveLogUpdater::<DummyValue>::with_scope("example-log", guard)
                        .await
                        .expect("failed to create log updater");
                let mut i = 0;
                loop {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    updater
                        .write(DummyValue { value: i })
                        .await
                        .expect("failed to update log");
                    i += 1;
                }
            })
            .await
    });
    let mut log_watcher = LogWatcher::<DummyValue>::new(etcd.clone(), "example-log")
        .await
        .expect("failed to create log watcher");
    let _reader_handle = tokio::spawn(async move {
        loop {
            let val = log_watcher.observe().await.expect("failed to read log");
            println!("Received value: {:?}", val);
        }
    });

    println!("Press Ctrl-C to delete the lock lease and abort `scope_with` loop");
    let _ = tokio::signal::ctrl_c().await;

    // You don't want to drop the key manually like this, this is just for demonstration purposes.
    let _ = etcd
        .kv_client()
        .delete(lock_key, None)
        .await
        .expect("failed to delete lock key");

    let _ = writer_handle.await;

    println!("Finished!");
}
