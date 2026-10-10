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
mod sstable;
#[cfg(test)]
mod test_hooks;
mod wal;
pub use compaction::CompactionStyle;
pub use engine::{Engine, KvEngine, Options, Result, Stats};
pub use error::EngineError;
pub use inspection::{Inspection, MemtableInfo, RecordPreview, TableInfo};
