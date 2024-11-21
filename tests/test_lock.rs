use std::time::Duration;

use common::random_str;
use rust_etcd_utils::{lease::ManagedLeaseFactory, lock::spawn_lock_manager, lock::TryLockError};

mod common;


#[tokio::test]
async fn test_locking() {
    let etcd = common::get_etcd_client().await;
    let managed_lease_factory = ManagedLeaseFactory::new(etcd.clone());
    let (lock_man_handle, lock_man) = spawn_lock_manager(etcd.clone(), managed_lease_factory.clone());
    let lock_name = random_str(10);
    lock_man.try_lock(lock_name, Duration::from_secs(10)).await.expect("failed to lock");
    drop(lock_man);
    lock_man_handle.await.expect("failed to release lock manager handle");
}


#[tokio::test]
async fn it_should_failed_to_lock_already_taken_lock() {
    let etcd = common::get_etcd_client().await;
    let managed_lease_factory = ManagedLeaseFactory::new(etcd.clone());
    let (_lock_man_handle, lock_man) = spawn_lock_manager(etcd.clone(), managed_lease_factory);

    let lock_name = random_str(10);
    let _managed_lock1 = lock_man.try_lock(lock_name.as_str(), Duration::from_secs(10)).await.expect("failed to lock");
    let result = lock_man.try_lock(lock_name, Duration::from_secs(10)).await;

    assert!(matches!(result, Err(TryLockError::AlreadyTaken)));
}

#[tokio::test]
async fn dropping_managed_lock_should_revoke_etcd_lock() {
    let etcd = common::get_etcd_client().await;
    let managed_lease_factory = ManagedLeaseFactory::new(etcd.clone());
    let (_, lock_man) = spawn_lock_manager(etcd.clone(), managed_lease_factory);
    let lock_name = random_str(10);

    let (managed_lock1, delete_cb) = lock_man
        .try_lock_with_delete_callback(lock_name.as_str(), Duration::from_secs(10))
        .await
        .expect("failed to lock");

    drop(managed_lock1);
    let _ = delete_cb.await;

    let _managed_lock2 = lock_man.try_lock(lock_name, Duration::from_secs(10)).await.expect("failed to re-acquire revoked lock");
}


#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lock_lease_should_be_automatically_refreshed() {
    let etcd = common::get_etcd_client().await;
    let managed_lease_factory = ManagedLeaseFactory::new(etcd.clone());
    let (_, lock_man) = spawn_lock_manager(etcd.clone(), managed_lease_factory);

    let lock_name = random_str(10);

    let managed_lock1 = lock_man
        .try_lock(lock_name, Duration::from_secs(2))
        .await
        .expect("failed to lock");

    tokio::time::sleep(Duration::from_secs(3)).await;

    assert!(managed_lock1.is_alive().await);
}