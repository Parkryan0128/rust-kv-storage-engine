use crate::{
    codec::*,
    error::corrupt,
    fault,
    memtable::{MemTable, Record},
    Result, WriteBatch,
};
use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
};
const MAGIC: &[u8; 8] = b"RKVWAL01";
// Sequence zero is invalid in legacy records. Older readers reject this envelope
// rather than replaying a prefix of its operations as independent writes.
const BATCH_VERSION: u32 = 1;
#[cfg(test)]
#[path = "wal_tests.rs"]
mod tests;
pub(crate) struct Wal {
    pub file: File,
    pub bytes: u64,
    pub id: u64,
}
impl Wal {
    pub fn create(path: &Path, id: u64) -> Result<Self> {
        let mut file = OpenOptions::new()
            .write(true)
            .read(true)
            .create_new(true)
            .open(path)?;
        file.write_all(MAGIC)?;
        file.sync_all()?;
        Ok(Self { file, bytes: 8, id })
    }
    pub fn recover(
        path: &Path,
        id: u64,
        allow_tail: bool,
        last_seq: &mut u64,
    ) -> Result<(Self, MemTable)> {
        let mut file = OpenOptions::new().write(true).read(true).open(path)?;
        // Incomplete newest header: no writes could have been acknowledged yet.
        if allow_tail && file.metadata()?.len() < 8 {
            file.set_len(0)?;
            file.write_all(MAGIC)?;
            file.sync_all()?;
            return Ok((Self { file, bytes: 8, id }, MemTable::default()));
        }
        let mut magic = [0; 8];
        file.read_exact(&mut magic)
            .map_err(|_| corrupt("truncated WAL magic"))?;
        if &magic != MAGIC {
            return Err(corrupt("WAL format/version"));
        }
        let mut table = MemTable::default();
        let mut valid = 8;
        while let Some(b) = read_frame(&mut file, allow_tail)? {
            replay(&b, &mut table, last_seq)?;
            valid = file.stream_position()?;
        }
        if file.metadata()?.len() != valid {
            file.set_len(valid)?;
            file.sync_all()?;
        }
        file.seek(SeekFrom::End(0))?;
        Ok((
            Self {
                file,
                bytes: valid,
                id,
            },
            table,
        ))
    }
    pub fn append(&mut self, key: &[u8], r: &Record) -> Result<()> {
        let mut b = Vec::with_capacity(r.encoded_len(key));
        encode_record(key, r, &mut b);
        self.append_payload(&b)
    }

    pub fn append_batch(&mut self, batch: &WriteBatch, first_seq: u64, size: usize) -> Result<()> {
        let mut b = Vec::with_capacity(size);
        put_u64(&mut b, 0);
        put_u32(&mut b, BATCH_VERSION);
        put_u32(&mut b, batch.len() as u32);
        for (i, entry) in batch.entries.iter().enumerate() {
            let record = Record {
                seq: first_seq + i as u64,
                value: entry.value.clone(),
            };
            encode_record(&entry.key, &record, &mut b);
        }
        debug_assert_eq!(b.len(), size);
        self.append_payload(&b)
    }

    fn append_payload(&mut self, b: &[u8]) -> Result<()> {
        fault::hit("wal_before_append")?;
        self.bytes += write_frame(&mut self.file, b)?;
        fault::hit("wal_before_sync")?;
        self.file.sync_all()?;
        fault::hit("wal_after_sync")?;
        Ok(())
    }
}

fn replay(payload: &[u8], table: &mut MemTable, last_seq: &mut u64) -> Result<()> {
    let mut c = Cursor { b: payload };
    if !payload.starts_with(&[0; 8]) {
        let (key, record) = decode_record(&mut c)?;
        c.done()?;
        if record.seq <= *last_seq {
            return Err(corrupt("non-increasing WAL sequence"));
        }
        *last_seq = record.seq;
        table.insert(key, record);
        return Ok(());
    }
    c.u64()?;
    if c.u32()? != BATCH_VERSION {
        return Err(corrupt("WAL batch version"));
    }
    let count = c.u32()? as usize;
    if count == 0 || count > c.b.len() / 17 {
        return Err(corrupt("WAL batch count"));
    }
    // Validate all operations before mutating recovery state. The second pass
    // avoids staging another owned copy of every key/value in the batch.
    let records = c.b;
    let mut end_seq = *last_seq;
    for i in 0..count {
        let record = decode_record_ref(&mut c)?;
        if record.seq <= end_seq || (i != 0 && end_seq.checked_add(1) != Some(record.seq)) {
            return Err(corrupt("WAL batch sequence"));
        }
        end_seq = record.seq;
    }
    c.done()?;
    let mut c = Cursor { b: records };
    for _ in 0..count {
        let (key, record) = decode_record(&mut c)?;
        table.insert(key, record);
    }
    *last_seq = end_seq;
    Ok(())
}
