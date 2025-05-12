use std::time::Duration;

use rust_etcd_utils::lease::ManagedLeaseFactory;

mod common;

#[tokio::test]
async fn it_should_automatically_refresh_lease() {
    let etcd = common::get_etcd_client().await;
    let (managed_lease_factory, _) = ManagedLeaseFactory::spawn(etcd.clone());

    let managed_lease = managed_lease_factory
        .new_lease(Duration::from_secs(2), None)
        .await
        .expect("failed to create lease");
    let lease_expire_notify = managed_lease.get_lease_expire_notify();

    tokio::select! {
        _ = lease_expire_notify.recv() => {
            panic!("lease expired before the lease duration");
        }
        _ = tokio::time::sleep(Duration::from_secs(6)) => {}
    }
    assert!(managed_lease.is_alive().await.expect("lease is not alive"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn it_should_limit_the_amount_of_lease_refresh() {
    let etcd = common::get_etcd_client().await;
    let (managed_lease_factory, _) = ManagedLeaseFactory::spawn(etcd.clone());

    let lease_created_at = std::time::Instant::now();
    let ttl = Duration::from_secs(2);
    let refresh_interval = Duration::from_secs(1);
    let refresh_limit = 3;
    let managed_lease = managed_lease_factory
        .new_lease_with_auto_refresh_limit(ttl, Some(refresh_interval), Some(refresh_limit))
        .await
        .expect("failed to create lease");

    let estimated_lease_duration = refresh_interval * (refresh_limit as u32);

    let lease_expired_notify = managed_lease.get_lease_expire_notify();
    tokio::select! {
        _ = lease_expired_notify.recv() => {
            let elapsed = lease_created_at.elapsed();
            assert!(elapsed >= estimated_lease_duration, "lease expired before the auto refresh limit");
        }
        _ = tokio::time::sleep(estimated_lease_duration * 2) => {
            assert!(!managed_lease.is_alive().await.expect("failed to get lease alive status"));

        }
    }

    assert!(managed_lease.is_alive().await.expect("lease is not alive"));
}

#[tokio::test]
async fn test_lease_expire_notify_on_drop() {
    let etcd = common::get_etcd_client().await;
    let (managed_lease_factory, _) = ManagedLeaseFactory::spawn(etcd.clone());

    let managed_lease = managed_lease_factory
        .new_lease(Duration::from_secs(2), None)
        .await
        .expect("failed to create lease");
    let lease_expire_notify = managed_lease.get_lease_expire_notify();
    let lease_expire_notify_clone = lease_expire_notify.clone();
    let weak_lease_expire_notify = managed_lease.get_weak().get_lease_expire_notify();

    drop(managed_lease);

    let _ = weak_lease_expire_notify.recv().await;
    let _ = lease_expire_notify_clone.recv().await;
    let _ = lease_expire_notify.recv().await;
}

#[tokio::test]
async fn test_lease_expire_notify_when_etcd_revoke() {
    let etcd = common::get_etcd_client().await;
    let (managed_lease_factory, _) = ManagedLeaseFactory::spawn(etcd.clone());

    let managed_lease = managed_lease_factory
        .new_lease(Duration::from_secs(2), None)
        .await
        .expect("failed to create lease");
    let lease_expire_notify = managed_lease.get_lease_expire_notify();
    let lease_expire_notify_clone = lease_expire_notify.clone();
    let weak_lease_expire_notify = managed_lease.get_weak().get_lease_expire_notify();

    etcd.lease_client()
        .revoke(managed_lease.lease_id())
        .await
        .expect("failed to revoke lease");

    assert!(!managed_lease.is_alive().await.expect("lease is not alive"));
    let _ = weak_lease_expire_notify.recv().await;
    let _ = lease_expire_notify_clone.recv().await;
    let _ = lease_expire_notify.recv().await;
}

#[tokio::test]
async fn lease_revoke_notify_should_return_immediately_if_created_after_delete() {
    let etcd = common::get_etcd_client().await;
    let (managed_lease_factory, _) = ManagedLeaseFactory::spawn(etcd.clone());

    let managed_lease = managed_lease_factory
        .new_lease(Duration::from_secs(2), None)
        .await
        .expect("failed to create lease");

    // Revoke before the lease notify is created
    etcd.lease_client()
        .revoke(managed_lease.lease_id())
        .await
        .expect("failed to revoke lease");

    let lease_expire_notify = managed_lease.get_lease_expire_notify();

    assert!(!managed_lease.is_alive().await.expect("lease is not alive"));
    let _ = lease_expire_notify.recv().await;
}
