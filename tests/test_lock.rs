use std::time::Duration;

use common::random_str;
use rust_etcd_utils::{
    lease::ManagedLeaseFactory,
    lock::{spawn_lock_manager, spawn_lock_manager_with_lease_factory, TryLockError},
};
mod common;

#[tokio::test]
async fn it_should_failed_to_lock_already_taken_lock() {
    let etcd = common::get_etcd_client().await;
    let (_lock_man_handle, lock_man) = spawn_lock_manager(etcd.clone());

    let lock_name = random_str(10);
    let _managed_lock1 = lock_man
        .try_lock(lock_name.as_str(), Duration::from_secs(10))
        .await
        .expect("failed to lock");
    let result = lock_man.try_lock(lock_name, Duration::from_secs(10)).await;

    assert!(matches!(result, Err(TryLockError::AlreadyTaken)));
}

#[tokio::test]
async fn try_lock_should_fail_with_expired_lease() {
    let etcd = common::get_etcd_client().await;
    let (managed_lease_factory, _) = ManagedLeaseFactory::spawn(etcd.clone());
    let managed_lease = managed_lease_factory
        .new_lease(Duration::from_secs(10), None)
        .await
        .expect("failed to create lease");
    let (_lock_man_handle, lock_man) =
        spawn_lock_manager_with_lease_factory(etcd.clone(), managed_lease_factory);
    let lease_id = managed_lease.lease_id();
    let _ = etcd
        .lease_client()
        .revoke(lease_id)
        .await
        .expect("failed to revoke lease");
    let lock_name = random_str(10);
    let result = lock_man
        .try_lock_with_lease(lock_name.as_str(), managed_lease)
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn lock_should_fail_with_expired_lease() {
    let etcd = common::get_etcd_client().await;
    let (managed_lease_factory, _) = ManagedLeaseFactory::spawn(etcd.clone());
    let managed_lease = managed_lease_factory
        .new_lease(Duration::from_secs(10), None)
        .await
        .expect("failed to create lease");
    let (_lock_man_handle, lock_man) =
        spawn_lock_manager_with_lease_factory(etcd.clone(), managed_lease_factory);
    let lease_id = managed_lease.lease_id();
    let _ = etcd
        .lease_client()
        .revoke(lease_id)
        .await
        .expect("failed to revoke lease");
    let lock_name = random_str(10);
    let result = lock_man
        .lock_with_lease(lock_name.as_str(), managed_lease)
        .await;

    assert!(result.is_err());
}

#[tokio::test]
async fn dropping_managed_lock_should_revoke_etcd_lock() {
    let etcd = common::get_etcd_client().await;
    let (_, lock_man) = spawn_lock_manager(etcd.clone());
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
    let (managed_lease_factory, _) = ManagedLeaseFactory::spawn(etcd.clone());
    let (_, lock_man) = spawn_lock_manager_with_lease_factory(etcd.clone(), managed_lease_factory);

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
    let (managed_lease_factory, _) = ManagedLeaseFactory::spawn(etcd.clone());
    let (_, lock_man) = spawn_lock_manager_with_lease_factory(etcd.clone(), managed_lease_factory);

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
async fn test_managed_lock_scope_with() {
    let etcd = common::get_etcd_client().await;
    let (managed_lease_factory, _) = ManagedLeaseFactory::spawn(etcd.clone());
    let (_, lock_man) = spawn_lock_manager_with_lease_factory(etcd.clone(), managed_lease_factory);

    let lock_name = random_str(10);

    let managed_lock1 = lock_man
        .try_lock(lock_name, Duration::from_secs(10))
        .await
        .expect("failed to lock");

    let (tx, rx) = tokio::sync::oneshot::channel();
    let lock_key = managed_lock1.get_key();

    let h = tokio::spawn(async move {
        managed_lock1
            .scope_with(|_| async move {
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
    let (managed_lease_factory, _) = ManagedLeaseFactory::spawn(etcd.clone());
    let (_, lock_man) = spawn_lock_manager_with_lease_factory(etcd.clone(), managed_lease_factory);
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
    let (managed_lease_factory, _) = ManagedLeaseFactory::spawn(etcd.clone());
    let (_, lock_man) = spawn_lock_manager_with_lease_factory(etcd.clone(), managed_lease_factory);

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
    let (managed_lease_factory, _) = ManagedLeaseFactory::spawn(etcd.clone());
    let (_, lock_man) = spawn_lock_manager_with_lease_factory(etcd.clone(), managed_lease_factory);

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

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn revoke_notify_should_be_completly_independent() {
    let etcd = common::get_etcd_client().await;
    let (managed_lease_factory, _) = ManagedLeaseFactory::spawn(etcd.clone());
    let (_, lock_man) = spawn_lock_manager_with_lease_factory(etcd.clone(), managed_lease_factory);

    let lock_name = random_str(10);

    let managed_lock1 = lock_man
        .try_lock(lock_name, Duration::from_secs(10))
        .await
        .expect("failed to lock");

    // Construction is here
    let revoke_notify1 = managed_lock1.get_revoke_notify();
    let revoke_notify2 = managed_lock1.get_revoke_notify();

    let h = tokio::spawn(async move {
        println!("Waiting for revoke");
        revoke_notify1.wait_for_revoke().await;
        println!("Revoke 1 is done");
    });

    let mut i = 0;
    loop {
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(1)) => {
                i += 1;
                if i == 1 {
                    h.abort();
                }
                if i == 3 {
                    break;
                }
                assert!(managed_lock1.is_alive().await);
                println!("Lock is still alive - {i}");
            }
            _ = revoke_notify2.clone().wait_for_revoke() => {
                println!("Lock is revoked");
                break;
            }
        }
    }

    println!("Awaiting for h");
    let result = h.await;
    assert!(result.unwrap_err().is_cancelled());

    let lock_key = managed_lock1.get_key();
    etcd.kv_client()
        .delete(lock_key, None)
        .await
        .expect("failed to delete key");
    // The lock is already revoked, so the future should return immediately
    let _ = revoke_notify2.wait_for_revoke().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn it_should_notify_revoke_when_underlying_lease_expire() {
    let etcd = common::get_etcd_client().await;
    let (managed_lease_factory, _) = ManagedLeaseFactory::spawn(etcd.clone());
    let (_, lock_man) = spawn_lock_manager_with_lease_factory(etcd.clone(), managed_lease_factory);

    let lock_name = random_str(10);

    let managed_lock1 = lock_man
        .try_lock(lock_name, Duration::from_secs(10))
        .await
        .expect("failed to lock");

    let revoked_notify = managed_lock1.get_revoke_notify();
    let lease_id = managed_lock1.get_managed_lease_weak_ref().lease_id();
    etcd.lease_client()
        .revoke(lease_id)
        .await
        .expect("failed to revoke lease");

    tokio::time::timeout(Duration::from_secs(5), revoked_notify.wait_for_revoke())
        .await
        .expect("failed to wait for revoke");
}
