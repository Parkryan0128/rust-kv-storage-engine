#![forbid(unsafe_code)]
//! Embedded LSM key-value store.
//!
//! ```
//! use rust_kv_storage_engine::Engine;
//! let engine = Engine::new();
//! engine.put(b"hello", b"world")?;
//! assert_eq!(engine.get(b"hello")?.as_deref(), Some(&b"world"[..]));
//! engine.delete(b"hello")?;
//! # Ok::<(), rust_kv_storage_engine::EngineError>(())
//! ```
// No timing overhead in normal builds.
macro_rules! write_timed {
    ($phase:ident, $body:expr) => {{
        #[cfg(feature = "write-profiling")]
        let _timer = crate::write_profile::Timer::new(crate::write_profile::Phase::$phase);
        $body
    }};
}
mod batch;
mod block;
mod bloom;
mod cache;
mod codec;
mod compaction;
mod engine;
mod error;
mod fault;
mod inspection;
mod manifest;
mod memtable;
#[cfg(test)]
mod read_fixture;
#[cfg(test)]
mod read_path_tests;
mod scan;
mod sstable;
#[cfg(test)]
mod test_hooks;
mod wal;
mod wal_preallocation;
#[cfg(feature = "write-profiling")]
#[doc(hidden)]
pub mod write_profile;
pub use batch::WriteBatch;
pub use compaction::CompactionStyle;
pub use engine::{Engine, KvEngine, Options, Result, Stats};
pub use error::EngineError;
pub use inspection::{Inspection, MemtableInfo, RecordPreview, TableInfo};
pub use scan::Scan;
