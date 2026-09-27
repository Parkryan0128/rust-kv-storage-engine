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
mod bloom;
mod cache;
mod codec;
mod compaction;
mod engine;
mod error;
mod fault;
mod manifest;
mod memtable;
mod sstable;
mod wal;
pub use engine::{Engine, KvEngine, Options, Result, Stats};
pub use error::EngineError;
