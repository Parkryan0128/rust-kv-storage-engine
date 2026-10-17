use bytes::Bytes;
#[path = "mem_index.rs"]
mod mem_index;
pub(crate) use mem_index::{MemIndex, MemScan};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Record {
    pub seq: u64,
    pub value: Option<Bytes>,
}
impl Record {
    pub fn memory_size(&self, key: &[u8]) -> usize {
        48 + key.len() + self.value.as_ref().map_or(0, Bytes::len)
    }
    pub fn encoded_len(&self, key: &[u8]) -> usize {
        17 + key.len() + self.value.as_ref().map_or(0, Bytes::len)
    }
}
#[derive(Default)]
pub(crate) struct MemTable {
    pub data: MemIndex,
    pub bytes: usize,
}
impl MemTable {
    pub fn apply(
        &mut self,
        entries: impl IntoIterator<Item = (Vec<u8>, Record)>,
        remove_deletes: bool,
    ) {
        self.data.apply(entries, remove_deletes, &mut self.bytes);
    }
    pub fn allocated_bytes(&self) -> usize {
        self.data.allocated_bytes()
    }
    pub fn insert(&mut self, key: Vec<u8>, record: Record) {
        self.bytes += record.memory_size(&key);
        let key_len = key.len();
        if let Some(old) = self.data.insert(key, record) {
            self.bytes -= 48 + key_len + old.value.as_ref().map_or(0, Bytes::len);
        }
    }
    pub fn remove(&mut self, key: &[u8]) {
        if let Some(old) = self.data.remove(key) {
            self.bytes -= old.memory_size(key);
        }
    }
    #[inline]
    pub fn get(&self, key: &[u8]) -> Option<Record> {
        self.data.get(key).cloned()
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn accounting_and_tombstones() {
        let mut m = MemTable::default();
        m.insert(
            b"k".to_vec(),
            Record {
                seq: 1,
                value: Some(Bytes::from_static(b"123")),
            },
        );
        assert_eq!(m.bytes, 52);
        m.insert(
            b"k".to_vec(),
            Record {
                seq: 2,
                value: None,
            },
        );
        assert_eq!(m.bytes, 49);
        assert_eq!(m.get(b"k").unwrap().value, None);
        assert_eq!(m.get(b"absent"), None);
    }

    #[test]
    fn overwrites_and_removals_account_for_changed_value_sizes() {
        let mut m = MemTable::default();
        for (seq, len) in [(1, 100), (2, 1), (3, 500), (4, 0)] {
            m.insert(
                b"key".to_vec(),
                Record {
                    seq,
                    value: Some(Bytes::from(vec![42; len])),
                },
            );
            assert_eq!(m.bytes, 51 + len);
            assert_eq!(m.data.len(), 1);
        }
        m.insert(
            b"other".to_vec(),
            Record {
                seq: 5,
                value: None,
            },
        );
        assert_eq!(m.bytes, 104);
        m.remove(b"missing");
        assert_eq!(m.bytes, 104);
        m.remove(b"key");
        assert_eq!(m.bytes, 53);
        m.remove(b"other");
        assert_eq!(m.bytes, 0);
        assert!(m.data.is_empty());
    }
}
