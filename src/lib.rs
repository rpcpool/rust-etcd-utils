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
