use std::time::Duration;

use common::random_str;
use rust_etcd_utils::{
    lease::ManagedLeaseFactory,
    lock::{spawn_lock_manager, ManagedLockGuard},
    log::{ExclusiveLogUpdater, LogWatcher, WriteError},
};
use serde::{Deserialize, Serialize};

mod common;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct DummyValue {
    value: i64,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_should_work_within_lock_scope_lifetime() {
    let etcd = common::get_etcd_client().await;
    let managed_lease_factory = ManagedLeaseFactory::new(etcd.clone());
    let (_, lock_man) = spawn_lock_manager(etcd.clone(), managed_lease_factory);
    let lock_name = random_str(10);
    let log_name = random_str(10);
    let lock = lock_man
        .lock(lock_name, Duration::from_secs(2))
        .await
        .expect("failed to lock 2");

    let mut log_watcher = LogWatcher::<DummyValue>::new(etcd.clone(), log_name.clone())
        .await
        .expect("failed to create log watcher");

    let write_result = lock
        .scope_with(|scope| async move {
            let mut writer = ExclusiveLogUpdater::with_scope(log_name, scope)
                .await
                .expect("failed to create log writer");

            writer.write(DummyValue { value: 1 }).await
        })
        .await;

    assert!(!write_result.is_err());
    let log_version = write_result
        .expect("lock revoked")
        .expect("failed to write");
    assert!(log_version == 1);
    println!("before observe");
    let actual = log_watcher.observe().await.expect("failed to read key");
    assert_eq!(actual, DummyValue { value: 1 });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn releasing_lock_should_release_the_log_too() {
    let etcd = common::get_etcd_client().await;
    let managed_lease_factory = ManagedLeaseFactory::new(etcd.clone());
    let (_, lock_man) = spawn_lock_manager(etcd.clone(), managed_lease_factory);
    let lock_name1 = random_str(10);
    let lock_name2 = random_str(10);
    let log_name = random_str(10);
    let lock = lock_man
        .lock(lock_name1, Duration::from_secs(2))
        .await
        .expect("failed to lock 2");

    let log_name2 = log_name.clone();
    let write_result = lock
        .scope_with(|scope| async move {
            let mut writer = ExclusiveLogUpdater::with_scope(log_name2, scope)
                .await
                .expect("failed to create log writer");

            writer.write(DummyValue { value: 1 }).await
        })
        .await;

    assert!(!write_result.is_err());
    let log_version = write_result
        .expect("lock revoked")
        .expect("failed to write");
    assert!(log_version == 1);

    // Drop the lock
    drop(lock);

    let lock = lock_man
        .lock(&lock_name2, Duration::from_secs(2))
        .await
        .expect("failed to lock 1");

    let mut log_watcher = LogWatcher::<DummyValue>::new(etcd.clone(), log_name.clone())
        .await
        .expect("failed to create log watcher");
    // Try to write to the log
    let write_result = lock
        .scope_with(|scope| async move {
            let mut writer = ExclusiveLogUpdater::with_scope(log_name, scope)
                .await
                .expect("failed to create log writer");

            writer.write(DummyValue { value: 100 }).await
        })
        .await;

    assert!(!write_result.is_err());
    let log_version = write_result
        .expect("lock revoked")
        .expect("failed to write");
    assert!(log_version == 2);
    let actual = log_watcher.observe().await.expect("failed to read key");
    assert_eq!(actual, DummyValue { value: 100 });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_should_fail_if_lock_already_revoked() {
    let mut etcd = common::get_etcd_client().await;
    let managed_lease_factory = ManagedLeaseFactory::new(etcd.clone());
    let (_, lock_man) = spawn_lock_manager(etcd.clone(), managed_lease_factory);
    let lock_name1 = random_str(10);
    let log_name = random_str(10);
    let lock = lock_man
        .lock(lock_name1, Duration::from_secs(2))
        .await
        .expect("failed to lock 2");

    let log_name2 = log_name.clone();
    let static_scope: ManagedLockGuard<'static> = lock
        .scope_with(|scope| async move { unsafe { std::mem::transmute(scope) } })
        .await
        .expect("lock revoked");

    // Drop the lock by using raw etcd client
    let raw_lock_key = lock.get_key();
    let _ = etcd
        .delete(raw_lock_key, None)
        .await
        .expect("failed to delete lock key");

    let mut log_writer = ExclusiveLogUpdater::<DummyValue>::with_scope(log_name2, static_scope)
        .await
        .expect("failed to create log writer");

    let result = log_writer.write(DummyValue { value: 1 }).await;

    assert!(matches!(result, Err(WriteError::LostExclusivity)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn log_watch_should_work_even_if_the_underlying_log_gets_deleted() {
    let mut etcd = common::get_etcd_client().await;
    let managed_lease_factory = ManagedLeaseFactory::new(etcd.clone());
    let (_, lock_man) = spawn_lock_manager(etcd.clone(), managed_lease_factory);
    let lock_name1 = random_str(10);
    let lock_name2 = random_str(10);
    let log_name = random_str(10);
    let lock = lock_man
        .lock(lock_name1, Duration::from_secs(2))
        .await
        .expect("failed to lock 2");
    let mut log_watcher = LogWatcher::<DummyValue>::new(etcd.clone(), log_name.clone())
        .await
        .expect("failed to create log watcher");
    let log_name2 = log_name.clone();
    let write_result = lock
        .scope_with(|scope| async move {
            let mut writer = ExclusiveLogUpdater::with_scope(log_name2, scope)
                .await
                .expect("failed to create log writer");

            writer.write(DummyValue { value: 1 }).await
        })
        .await;

    assert!(!write_result.is_err());
    let log_version = write_result
        .expect("lock revoked")
        .expect("failed to write");
    assert!(log_version == 1);
    let actual = log_watcher.observe().await.expect("failed to read key");
    assert_eq!(actual, DummyValue { value: 1 });
    // Drop the lock
    drop(lock);

    etcd.delete(log_name.as_str(), None)
        .await
        .expect("failed to delete log key");

    let lock = lock_man
        .lock(&lock_name2, Duration::from_secs(2))
        .await
        .expect("failed to lock 1");

    // Try to write to the log
    let log_name2 = log_name.clone();
    let write_result = lock
        .scope_with(|scope| async move {
            let mut writer = ExclusiveLogUpdater::with_scope(log_name2, scope)
                .await
                .expect("failed to create log writer");

            writer.write(DummyValue { value: 100 }).await
        })
        .await;

    assert!(!write_result.is_err());
    let log_version = write_result
        .expect("lock revoked")
        .expect("failed to write");
    assert!(log_version == 1);
    let actual = log_watcher.observe().await.expect("failed to read key");
    assert_eq!(actual, DummyValue { value: 100 });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn log_watch_should_return_current_etcd_value_if_exists_initially() {
    let etcd = common::get_etcd_client().await;
    let log_name = random_str(10);
    let initial_value = DummyValue { value: 100 };
    etcd.kv_client()
        .put(
            log_name.as_str(),
            serde_json::to_string(&initial_value).expect("json"),
            None,
        )
        .await
        .expect("failed to put initial value");

    let mut log_watcher = LogWatcher::<DummyValue>::new(etcd.clone(), log_name.clone())
        .await
        .expect("failed to create log watcher");

    let actual = log_watcher.observe().await.expect("failed to read key");
    assert_eq!(actual, initial_value);
}
