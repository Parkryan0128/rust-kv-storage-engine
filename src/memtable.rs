use std::collections::BTreeMap;

use bytes::Bytes;

/// A single key's state inside the memtable.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Entry {
    Value(Bytes),
    Tombstone,
}

/// In-memory sorted buffer for recent writes. Single-threaded for now.
pub struct MemTable {
    data: BTreeMap<Vec<u8>, Entry>,
}

impl MemTable {
    pub fn new() -> Self {
        Self {
            data: BTreeMap::new(),
        }
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) {
        self.data
            .insert(key.to_vec(), Entry::Value(Bytes::copy_from_slice(value)));
    }

    pub fn get(&self, key: &[u8]) -> Option<Bytes> {
        match self.data.get(key)? {
            Entry::Value(value) => Some(value.clone()),
            Entry::Tombstone => None,
        }
    }

    pub fn delete(&mut self, key: &[u8]) {
        self.data.insert(key.to_vec(), Entry::Tombstone);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_then_get() {
        let mut table = MemTable::new();
        table.put(b"key", b"value");
        assert_eq!(table.get(b"key"), Some(Bytes::from_static(b"value")));
    }

    #[test]
    fn get_missing_key() {
        let table = MemTable::new();
        assert_eq!(table.get(b"missing"), None);
    }

    #[test]
    fn delete_then_get() {
        let mut table = MemTable::new();
        table.put(b"key", b"value");
        table.delete(b"key");
        assert_eq!(table.get(b"key"), None);
    }

    #[test]
    fn put_overwrites_previous_value() {
        let mut table = MemTable::new();
        table.put(b"key", b"first");
        table.put(b"key", b"second");
        assert_eq!(table.get(b"key"), Some(Bytes::from_static(b"second")));
    }
}
