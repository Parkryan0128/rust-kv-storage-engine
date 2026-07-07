//! Embedded LSM-tree key-value storage engine.

mod engine;
mod error;
mod memtable;

pub use engine::{Engine, KvEngine, Result};
pub use error::EngineError;
