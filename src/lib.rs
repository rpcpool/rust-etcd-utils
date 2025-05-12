//!
//! A set of utilities for working with etcd client Rust client.
//!
//! The library provides API to create "managed" etcd resources, such as leases, locks, and logs.
//!
//! Most of the API is built on top of the etcd client library, and provides a higher-level abstraction.
//!
//! It is designed to be used in asynchronous Rust applications, and provides a set of utilities for working with etcd in a concurrent environment.
//!
//! As you start your journey with etcd you will find that the etcd client library is a bit low-level and requires a lot of boilerplate code to work with.
//! You have to implement your own lease renewal, early lock release and you typically wants to run future against a lock lifetime.
//!
//! Moreover, properly managing watcher can be tricky as you have to deal with many edge cases.
//!
//! Also, every object has retry logic to handle transient errors when communicating with etcd.
//! If you are using etcd in a distributed system, you will encounter transient errors such as network partition or server overload.
//! This library provides a set of utilities to handle these errors and retry the operation.
//!
//! Lastly, all modules in this crate uses RAII pattern to manage resources.
//! When you drop a resource, it will automatically release the lock or lease.
//!
//! # Examples
//!
//! For a complete example, see the [`tests`] directory.
//!
//! ```rust
//! use etcd_client::Client;
//! use rust_etcd_utils::{
//!    lock::spawn_lock_manager,
//! }
//!
//! #[tokio::main]
//! async fn main() {
//!
//!     let etcd = Client::connect(["http://localhost:2379"], None)
//!         .await
//!         .expect("failed to connect to etcd");
//!     let (lock_manager, _lock_manager_handle) = spawn_lock_manager(etcd);
//!     
//!     let lock = lock_manager
//!         .lock("my_lock", std::time::Duration::from_secs(10))
//!         .await
//!         .expect("failed to lock");
//!     
//!     lock.scope_with(|scope| async move {
//!         // Do something WHILE YOU HOLD THE LOCK
//!         // This is a good place to do some work that requires exclusive access to a resource.
//!         // For example, you can write to a log or update a value in etcd.
//!         // If the lock is lost while you are doing this work, the future will be cancelled.
//!         ...
//!     }).await.expect("lock lost");
//!     
//!     let revoke_notif = lock.get_revoke_notify();
//!
//!     drop(lock));    // Release the lock early
//!
//!     // Wait for the lock to be released
//!     let _ = revoke_notif.recv().await; // The lock is released
//! }
//! ```
//!
//!
//! [`tests`]: https://github.com/rpcpool/rust-etcd-utils/tree/main/tests
//!

///
/// Provides an API over "managed" lease
///
pub mod lease;

///
/// Provides an API over "managed" lock
///  
pub mod lock;

///
/// Utility function to manage various transient errors.
pub mod retry;

///
/// Robust API for watching etcd changes
pub mod watcher;

///
/// Alias for etcd revision
pub type Revision = i64;

///
/// Provides an API over a lock-protected log
///
/// A log is an append-only style data structure stored on etcd, where the writer is protected by a lock.
///
pub mod log;

///
/// Utiltities for inter-task communication
///
pub mod sync;
