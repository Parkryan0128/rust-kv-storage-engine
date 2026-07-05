//! Embedded LSM-tree key-value storage engine.

mod engine;
mod error;

pub use engine::{Engine, KvEngine, Result};
pub use error::EngineError;
