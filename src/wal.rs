use crate::{
    codec::*,
    error::corrupt,
    fault,
    memtable::{MemTable, Record},
    Result,
};
use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
};
const MAGIC: &[u8; 8] = b"RKVWAL01";
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
            let mut c = Cursor { b: &b };
            let (key, r) = decode_record(&mut c)?;
            c.done()?;
            if r.seq <= *last_seq {
                return Err(corrupt("non-increasing WAL sequence"));
            }
            *last_seq = r.seq;
            table.insert(key, r);
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
        fault::hit("wal_before_append")?;
        self.bytes += write_frame(&mut self.file, &b)?;
        fault::hit("wal_before_sync")?;
        self.file.sync_all()?;
        fault::hit("wal_after_sync")?;
        Ok(())
    }
}
