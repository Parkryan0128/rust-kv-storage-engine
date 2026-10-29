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
        let mut pending = ReplayBuffer::default();
        {
            let mut reader = BufReader::with_capacity(MAX_RETAINED_BUFFER, &mut file);
            while read_frame_into(&mut reader, allow_tail, &mut buffer)? {
                if !buffer.starts_with(&[0; 8]) {
                    let entry = decode_single(&buffer, last_seq)?;
                    pending.push(entry, &mut table);
                } else {
                    // Validate the complete atomic frame before staging any of
                    // its operations. Small frames share bounded replay chunks.
                    // A short disorder sample selects chunk sorting for large
                    // frames; the streaming fallback accepts any key order.
                    let (count, records, end_seq) = validate_batch(&buffer, *last_seq)?;
                    let mut cursor = Cursor { b: records };
                    let entries = (0..count)
                        .map(|_| decode_record(&mut cursor).expect("validated WAL batch"));
                    if count > REPLAY_STREAM_THRESHOLD && !disordered_replay_prefix(records, count)
                    {
                        pending.flush(&mut table);
                        table.apply(entries, false);
                    } else {
                        for entry in entries {
                            pending.push(entry, &mut table);
                        }
                    }
                    *last_seq = end_seq;
                }
                // Commit the logical offset only after CRC and replay validation.
                // BufReader may have already read past this frame into a torn tail.
                valid += (HEADER + buffer.len()) as u64;
                if buffer.capacity() > MAX_RETAINED_BUFFER {
                    buffer = Vec::new();
                }
            }
        }
        pending.flush(&mut table);
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

fn validate_batch(payload: &[u8], last_seq: u64) -> Result<(usize, &[u8], u64)> {
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
    let mut end_seq = last_seq;
    for i in 0..count {
        let record = decode_record_ref(&mut c)?;
        if record.seq <= end_seq || (i != 0 && end_seq.checked_add(1) != Some(record.seq)) {
            return Err(corrupt("WAL batch sequence"));
        }
        end_seq = record.seq;
    }
    c.done()?;
    Ok((count, records, end_seq))
}

// This bounded sample is only a performance hint, never proof of full ordering.
// Missing later disorder simply retains the general streaming apply path.
// Keep sampled decoding out of the hot frame loop, including small frames
// that never invoke this hint.
#[inline(never)]
fn disordered_replay_prefix(records: &[u8], count: usize) -> bool {
    let mut cursor = Cursor { b: records };
    let mut previous = decode_record_ref(&mut cursor)
        .expect("validated WAL batch")
        .key;
    for _ in 1..count.min(8) {
        let key = decode_record_ref(&mut cursor)
            .expect("validated WAL batch")
            .key;
        if previous > key {
            return true;
        }
        previous = key;
    }
    false
}

#[cfg(test)]
fn replay_batch(payload: &[u8], table: &mut MemTable, last_seq: &mut u64) -> Result<()> {
    let (count, records, end_seq) = validate_batch(payload, *last_seq)?;
    let mut c = Cursor { b: records };
    table.apply(
        (0..count).map(|_| decode_record(&mut c).expect("validated WAL batch")),
        false,
    );
    *last_seq = end_seq;
    Ok(())
}

// The large-frame streaming threshold is independent of staging capacity.
const REPLAY_STREAM_THRESHOLD: usize = 256;
const REPLAY_RECORDS: usize = 1024;
// Temporary replay payload is bounded separately from retained WAL I/O buffers.
const REPLAY_BYTES: usize = 256 * 1024;
struct ReplayBuffer {
    entries: Vec<(Vec<u8>, Record)>,
    bytes: usize,
    sorted: bool,
}
impl Default for ReplayBuffer {
    fn default() -> Self {
        Self {
            entries: Vec::new(),
            bytes: 0,
            sorted: true,
        }
    }
}
impl ReplayBuffer {
    fn push(&mut self, entry: (Vec<u8>, Record), table: &mut MemTable) {
        let bytes = entry.1.memory_size(&entry.0);
        if self.bytes + bytes > REPLAY_BYTES {
            self.flush(table);
        }
        if self.sorted && self.entries.last().is_some_and(|last| last.0 > entry.0) {
            self.sorted = false;
        }
        self.entries.push(entry);
        self.bytes += bytes;
        if self.entries.len() == REPLAY_RECORDS
            || self.bytes >= REPLAY_BYTES
            || (self.entries.len() == REPLAY_STREAM_THRESHOLD && !self.needs_wider_window(table))
        {
            self.flush(table);
        }
    }
    // A larger window only helps scattered writes that cannot share pages in
    // the small chunk. Ordered and concentrated writes keep the original bound.
    // This is a locality hint, not a promise about future keys or sequence order.
    fn needs_wider_window(&self, table: &MemTable) -> bool {
        let threshold = REPLAY_STREAM_THRESHOLD / 2;
        if self.sorted || table.data.page_count() <= threshold {
            return false;
        }
        let first = self.entries[0].0.as_slice();
        let (mut lower, mut upper) = (first, first);
        for (key, _) in &self.entries[1..] {
            lower = lower.min(key.as_slice());
            upper = upper.max(key.as_slice());
        }
        table.data.page_span(lower, upper) > threshold
    }
    fn flush(&mut self, table: &mut MemTable) {
        // Sorting sparse chunks across a large index rarely reuses a page.
        // Require at least two staged records per existing page on average;
        // otherwise retain WAL order and avoid paying for a full chunk sort.
        if !self.sorted && self.entries.len() / 2 >= table.data.page_count() {
            // Recovery state is unpublished. Group nearby keys to amortize
            // page ownership and shifts; sequence breaks ties so the newest
            // duplicate/tombstone still wins. Chunks are applied in WAL order.
            self.entries
                .sort_unstable_by(|(a, ar), (b, br)| a.cmp(b).then_with(|| ar.seq.cmp(&br.seq)));
        }
        table.apply(self.entries.drain(..), false);
        self.bytes = 0;
        self.sorted = true;
    }
}

#[cfg(test)]
#[path = "wal_profile.rs"]
mod profile;
