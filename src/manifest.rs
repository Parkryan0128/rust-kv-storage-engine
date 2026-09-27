use crate::{codec::*, error::corrupt, fault, sstable::sync_dir, Result};
use std::{
    collections::HashSet,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::Path,
};
const MAGIC: &[u8; 8] = b"RKVMAN01";
#[derive(Clone, Default)]
pub(crate) struct Manifest {
    pub wal_floor: u64,
    pub max_seq: u64,
    pub tables: Vec<u64>,
}
impl Manifest {
    pub fn load(dir: &Path) -> Result<Self> {
        let mut f = File::open(dir.join("MANIFEST"))?;
        let mut magic = [0; 8];
        f.read_exact(&mut magic)?;
        if &magic != MAGIC {
            return Err(corrupt("manifest version"));
        }
        let b = read_frame(&mut f, false)?.ok_or_else(|| corrupt("missing manifest"))?;
        let mut rest = [0];
        if f.read(&mut rest)? != 0 {
            return Err(corrupt("trailing manifest bytes"));
        }
        let mut c = Cursor { b: &b };
        let wal_floor = c.u64()?;
        let max_seq = c.u64()?;
        let n = c.u32()? as usize;
        if n > c.b.len() / 8 {
            return Err(corrupt("manifest table count"));
        }
        let mut tables = Vec::with_capacity(n);
        let mut seen = HashSet::new();
        for _ in 0..n {
            let id = c.u64()?;
            if id == 0 || !seen.insert(id) {
                return Err(corrupt("manifest table ID"));
            }
            tables.push(id);
        }
        c.done()?;
        Ok(Self {
            wal_floor,
            max_seq,
            tables,
        })
    }
    pub fn save(&self, dir: &Path) -> Result<()> {
        let tmp = dir.join("MANIFEST.tmp");
        let mut f = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&tmp)?;
        f.write_all(MAGIC)?;
        let mut b = vec![];
        put_u64(&mut b, self.wal_floor);
        put_u64(&mut b, self.max_seq);
        put_u32(&mut b, self.tables.len() as u32);
        for id in &self.tables {
            put_u64(&mut b, *id);
        }
        write_frame(&mut f, &b)?;
        fault::hit("manifest_before_sync")?;
        f.sync_all()?;
        fault::hit("manifest_after_sync")?;
        fs::rename(tmp, dir.join("MANIFEST"))?;
        fault::hit("manifest_after_rename")?;
        sync_dir(dir)?;
        fault::hit("manifest_after_dir_sync")?;
        Ok(())
    }
}
