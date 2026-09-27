use bytes::Bytes;
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Record {
    pub seq: u64,
    pub value: Option<Bytes>,
}
impl Record {
    pub fn size(&self, key: &[u8]) -> usize {
        48 + key.len() + self.value.as_ref().map_or(0, Bytes::len)
    }
}
#[derive(Default)]
pub(crate) struct MemTable {
    pub data: BTreeMap<Vec<u8>, Record>,
    pub bytes: usize,
}
impl MemTable {
    pub fn insert(&mut self, key: Vec<u8>, record: Record) {
        self.bytes += record.size(&key);
        if let Some(old) = self.data.insert(key.clone(), record) {
            self.bytes -= old.size(&key);
        }
    }
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
}
