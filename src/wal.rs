use crate::wal_preallocation::Preallocation;
use crate::{
    codec::*,
    error::corrupt,
    fault,
    memtable::{MemTable, Record},
    Result, WriteBatch,
};
use std::{
    fs::{File, OpenOptions},
    io::{BufReader, Read, Seek, SeekFrom, Write},
    path::Path,
};
const MAGIC: &[u8; 8] = b"RKVWAL01";
// Sequence zero is invalid in legacy records. Older readers reject this envelope
// rather than replaying a prefix of its operations as independent writes.
const BATCH_VERSION: u32 = 1;
// Reuse ordinary commit buffers without retaining a rare multi-megabyte batch.
const MAX_RETAINED_BUFFER: usize = 64 * 1024;
#[cfg(test)]
#[path = "wal_tests.rs"]
mod tests;
pub(crate) struct Wal {
    pub file: File,
    pub bytes: u64,
    pub id: u64,
    buffer: Vec<u8>,
    preallocation: Preallocation,
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
        Ok(Self {
            file,
            bytes: 8,
            id,
            buffer: Vec::new(),
            preallocation: Preallocation::new(8),
        })
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
            return Ok((
                Self {
                    file,
                    bytes: 8,
                    id,
                    buffer: Vec::new(),
                    preallocation: Preallocation::new(8),
                },
                MemTable::default(),
            ));
        }
        let mut magic = [0; 8];
        file.read_exact(&mut magic)
            .map_err(|_| corrupt("truncated WAL magic"))?;
        if &magic != MAGIC {
            return Err(corrupt("WAL format/version"));
        }
        let mut table = MemTable::default();
        let mut valid = 8;
        let mut buffer = Vec::new();
        let mut pending = Vec::new();
        let mut pending_bytes = 0;
        {
            let mut reader = BufReader::with_capacity(MAX_RETAINED_BUFFER, &mut file);
            while read_frame_into(&mut reader, allow_tail, &mut buffer)? {
                if !buffer.starts_with(&[0; 8]) {
                    let entry = decode_single(&buffer, last_seq)?;
                    let bytes = entry.1.memory_size(&entry.0);
                    if pending_bytes + bytes > MAX_RETAINED_BUFFER {
                        table.apply(pending.drain(..), false);
                        pending_bytes = 0;
                    }
                    pending.push(entry);
                    pending_bytes += bytes;
                    if pending.len() == 256 || pending_bytes >= MAX_RETAINED_BUFFER {
                        table.apply(pending.drain(..), false);
                        pending_bytes = 0;
                    }
                } else {
                    table.apply(pending.drain(..), false);
                    pending_bytes = 0;
                    replay_batch(&buffer, &mut table, last_seq)?;
                }
                // Commit the logical offset only after CRC and replay validation.
                // BufReader may have already read past this frame into a torn tail.
                valid += (HEADER + buffer.len()) as u64;
                if buffer.capacity() > MAX_RETAINED_BUFFER {
                    buffer = Vec::new();
                }
            }
        }
        table.apply(pending, false);
        // Discard read-ahead before truncation and seek to the verified end.
        if buffer.capacity() > MAX_RETAINED_BUFFER {
            buffer = Vec::new();
        } else {
            buffer.clear();
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
                buffer,
                preallocation: Preallocation::new(valid),
            },
            table,
        ))
    }
    pub fn append(&mut self, key: &[u8], r: &Record) -> Result<()> {
        self.begin_frame(r.encoded_len(key));
        encode_record(key, r, &mut self.buffer);
        self.append_frame()
    }

    pub fn append_batch(&mut self, batch: &WriteBatch, first_seq: u64, size: usize) -> Result<()> {
        write_timed!(WalEncode, {
            self.begin_frame(size);
            put_u64(&mut self.buffer, 0);
            put_u32(&mut self.buffer, BATCH_VERSION);
            put_u32(&mut self.buffer, batch.len() as u32);
            for (i, entry) in batch.entries.iter().enumerate() {
                encode_record_parts(
                    &entry.key,
                    first_seq + i as u64,
                    entry.value.as_deref(),
                    &mut self.buffer,
                );
            }
            debug_assert_eq!(self.buffer.len(), HEADER + size);
        });
        self.append_frame()
    }

    fn begin_frame(&mut self, payload_len: usize) {
        self.buffer.clear();
        self.buffer.reserve(HEADER + payload_len);
        self.buffer.resize(HEADER, 0);
    }

    fn append_frame(&mut self) -> Result<()> {
        let result = (|| {
            let header = write_timed!(WalChecksum, frame_header(&self.buffer[HEADER..]))?;
            self.buffer[..HEADER].copy_from_slice(&header);
            fault::hit("wal_before_append")?;
            write_timed!(
                WalAllocate,
                self.preallocation.reserve(
                    &self.file,
                    self.bytes.saturating_add(self.buffer.len() as u64),
                )
            );
            fault::hit("wal_after_reserve")?;
            // Header and payload share one allocation and one write_all call.
            // write_all still retries interrupted/short writes before syncing.
            write_timed!(WalWrite, self.file.write_all(&self.buffer))?;
            self.bytes += self.buffer.len() as u64;
            fault::hit("wal_before_sync")?;
            write_timed!(WalSync, self.file.sync_all())?;
            fault::hit("wal_after_sync")?;
            Ok(())
        })();
        if self.buffer.capacity() > MAX_RETAINED_BUFFER {
            self.buffer = Vec::new();
        } else {
            self.buffer.clear();
        }
        result
    }
}

fn decode_single(payload: &[u8], last_seq: &mut u64) -> Result<(Vec<u8>, Record)> {
    let mut c = Cursor { b: payload };
    let entry = decode_record(&mut c)?;
    c.done()?;
    if entry.1.seq <= *last_seq {
        return Err(corrupt("non-increasing WAL sequence"));
    }
    *last_seq = entry.1.seq;
    Ok(entry)
}

fn replay_batch(payload: &[u8], table: &mut MemTable, last_seq: &mut u64) -> Result<()> {
    let mut c = Cursor { b: payload };
    if c.u64()? != 0 {
        return Err(corrupt("WAL batch envelope"));
    }
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
    // The immutable payload was fully decoded and checked above. Replay uses
    // the same page-local batch path without another owned staging buffer.
    table.apply(
        (0..count).map(|_| decode_record(&mut c).expect("validated WAL batch")),
        false,
    );
    *last_seq = end_seq;
    Ok(())
}
