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
