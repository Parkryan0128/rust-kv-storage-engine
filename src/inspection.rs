use crate::memtable::Record;

#[derive(Debug, Clone)]
pub struct RecordPreview {
    pub key: Vec<u8>,
    pub key_len: usize,
    pub value: Option<Vec<u8>>,
    pub value_len: usize,
    pub sequence: u64,
}
impl RecordPreview {
    pub(crate) fn new(key: &[u8], record: &Record) -> Self {
        Self {
            key: key[..key.len().min(128)].to_vec(),
            key_len: key.len(),
            value: record
                .value
                .as_ref()
                .map(|v| v[..v.len().min(128)].to_vec()),
            value_len: record.value.as_ref().map_or(0, |v| v.len()),
            sequence: record.seq,
        }
    }
}
#[derive(Debug, Clone)]
pub struct MemtableInfo {
    pub wal_id: Option<u64>,
    pub bytes: usize,
    pub records: usize,
    pub preview: Vec<RecordPreview>,
}
#[derive(Debug, Clone)]
pub struct TableInfo {
    pub id: u64,
    pub bytes: u64,
    pub records: u64,
    pub max_sequence: u64,
}
#[derive(Debug, Clone)]
pub struct Inspection {
    pub sequence: u64,
    pub active: MemtableInfo,
    pub frozen: Vec<MemtableInfo>,
    pub tables: Vec<TableInfo>,
}
