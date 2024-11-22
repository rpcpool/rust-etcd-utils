use std::time::Duration;

use common::random_str;
use rust_etcd_utils::{
    lease::ManagedLeaseFactory,
    lock::{spawn_lock_manager, TryLockError},
};
mod common;

#[tokio::test]
async fn test_locking() {
    let etcd = common::get_etcd_client().await;
    let managed_lease_factory = ManagedLeaseFactory::new(etcd.clone());
    let (lock_man_handle, lock_man) =
        spawn_lock_manager(etcd.clone(), managed_lease_factory.clone());
    let lock_name = random_str(10);
    lock_man
        .try_lock(lock_name, Duration::from_secs(10))
        .await
        .expect("failed to lock");
    drop(lock_man);
    lock_man_handle
        .await
        .expect("failed to release lock manager handle");
}

#[tokio::test]
async fn it_should_failed_to_lock_already_taken_lock() {
    let etcd = common::get_etcd_client().await;
    let managed_lease_factory = ManagedLeaseFactory::new(etcd.clone());
    let (_lock_man_handle, lock_man) = spawn_lock_manager(etcd.clone(), managed_lease_factory);

    let lock_name = random_str(10);
    let _managed_lock1 = lock_man
        .try_lock(lock_name.as_str(), Duration::from_secs(10))
        .await
        .expect("failed to lock");
    let result = lock_man.try_lock(lock_name, Duration::from_secs(10)).await;

    assert!(matches!(result, Err(TryLockError::AlreadyTaken)));
}

#[tokio::test]
async fn dropping_managed_lock_should_revoke_etcd_lock() {
    let etcd = common::get_etcd_client().await;
    let managed_lease_factory = ManagedLeaseFactory::new(etcd.clone());
    let (_, lock_man) = spawn_lock_manager(etcd.clone(), managed_lease_factory);
    let lock_name = random_str(10);

    let managed_lock1 = lock_man
        .try_lock(lock_name.as_str(), Duration::from_secs(10))
        .await
        .expect("failed to lock");

    let revoke_notify = managed_lock1.get_revoke_notify();
    drop(managed_lock1);
    let _ = revoke_notify.wait_for_revoke().await;

    let _managed_lock2 = lock_man
        .try_lock(lock_name, Duration::from_secs(10))
        .await
        .expect("failed to re-acquire revoked lock");
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_managed_lock_scope() {
    let etcd = common::get_etcd_client().await;
    let managed_lease_factory = ManagedLeaseFactory::new(etcd.clone());
    let (_, lock_man) = spawn_lock_manager(etcd.clone(), managed_lease_factory);

    let lock_name = random_str(10);

    let managed_lock1 = lock_man
        .try_lock(lock_name, Duration::from_secs(10))
        .await
        .expect("failed to lock");

    let (tx, rx) = tokio::sync::oneshot::channel();
    let lock_key = managed_lock1.get_key();

    let h = tokio::spawn(async move {
        managed_lock1
            .scope(async move {
                tokio::time::sleep(Duration::from_secs(20)).await;
                // This should never be reached
                tx.send(()).unwrap();
            })
            .await
    });

    etcd.kv_client()
        .delete(lock_key, None)
        .await
        .expect("failed to delete key");

    let _ = h.await;
    let result = rx.await;
    // If the callback rx received a msg it means the scope didn't cancel the future as it should.
    assert!(result.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_lock() {
    let etcd = common::get_etcd_client().await;
    let managed_lease_factory = ManagedLeaseFactory::new(etcd.clone());
    let (_, lock_man) = spawn_lock_manager(etcd.clone(), managed_lease_factory);
    let lock_name = random_str(10);

    let lock = lock_man
        .lock(&lock_name, Duration::from_secs(2))
        .await
        .expect("failed to lock 1");

    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(6)).await;
        println!("will revoke lock");
        drop(lock)
    });

    lock_man
        .lock(lock_name, Duration::from_secs(2))
        .await
        .expect("failed to lock 2");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_managed_lock_revoke_notify_clonability() {
    let etcd = common::get_etcd_client().await;
    let managed_lease_factory = ManagedLeaseFactory::new(etcd.clone());
    let (_, lock_man) = spawn_lock_manager(etcd.clone(), managed_lease_factory);

    let lock_name = random_str(10);

    let managed_lock1 = lock_man
        .try_lock(lock_name, Duration::from_secs(10))
        .await
        .expect("failed to lock");

    let (tx, rx) = tokio::sync::oneshot::channel();
    let lock_key = managed_lock1.get_key();
    let revoke_notify1 = managed_lock1.get_revoke_notify();
    let revoke_notify2 = revoke_notify1.clone();

    let h = tokio::spawn(async move {
        managed_lock1
            .scope(async move {
                tokio::time::sleep(Duration::from_secs(20)).await;
                // This should never be reached
                tx.send(()).unwrap();
            })
            .await
    });

    etcd.kv_client()
        .delete(lock_key, None)
        .await
        .expect("failed to delete key");

    let _ = h.await;
    let result = rx.await;
    // If the callback rx received a msg it means the scope didn't cancel the future as it should.

    assert!(result.is_err());

    let _ = revoke_notify1.wait_for_revoke().await;
    let _ = revoke_notify2.wait_for_revoke().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lock_revoke_notify_should_notify_even_if_lock_is_revoke_before_constructor() {
    let etcd = common::get_etcd_client().await;
    let managed_lease_factory = ManagedLeaseFactory::new(etcd.clone());
    let (_, lock_man) = spawn_lock_manager(etcd.clone(), managed_lease_factory);

    let lock_name = random_str(10);

    let managed_lock1 = lock_man
        .try_lock(lock_name, Duration::from_secs(10))
        .await
        .expect("failed to lock");

    let lock_key = managed_lock1.get_key();
    // Revoke the lock before the managed lock is constructed
    etcd.kv_client()
        .delete(lock_key, None)
        .await
        .expect("failed to delete key");

    // Construction is here
    let revoke_notify1 = managed_lock1.get_revoke_notify();
    let revoke_notify2 = managed_lock1.get_revoke_notify();

    // The lock is already revoked, so the future should return immediately
    let _ = revoke_notify1.wait_for_revoke().await;
    let _ = revoke_notify2.wait_for_revoke().await;
}
