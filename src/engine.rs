use bytes::Bytes;

use crate::error::EngineError;

pub type Result<T> = std::result::Result<T, EngineError>;

/// Core key-value storage API.
pub trait KvEngine {
    fn put(&self, key: &[u8], value: &[u8]) -> Result<()>;
    fn get(&self, key: &[u8]) -> Result<Option<Bytes>>;
    fn delete(&self, key: &[u8]) -> Result<()>;
}

pub struct Engine;

impl Engine {
    pub fn new() -> Self {
        Self
    }
}

impl KvEngine for Engine {
    fn put(&self, _key: &[u8], _value: &[u8]) -> Result<()> {
        todo!()
    }

    fn get(&self, _key: &[u8]) -> Result<Option<Bytes>> {
        todo!()
    }

    fn delete(&self, _key: &[u8]) -> Result<()> {
        todo!()
    }
}
