use crate::{
    cache::{Cache, Counters},
    codec::MAX_RECORD,
    compaction::Merge,
    error::{corrupt, EngineError},
    fault,
    manifest::Manifest,
    memtable::{MemTable, Record},
    sstable::{sync_dir, Table},
    wal::Wal,
};
use bytes::Bytes;
use crossbeam::channel::{bounded, Sender};
use fs2::FileExt;
use parking_lot::{Condvar, Mutex, RwLock};
use std::{
    collections::{HashSet, VecDeque},
    fs::{self, File, OpenOptions},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread::{self, JoinHandle},
};
pub type Result<T> = std::result::Result<T, EngineError>;

pub trait KvEngine {
    fn put(&self, key: &[u8], value: &[u8]) -> Result<()>;
    fn get(&self, key: &[u8]) -> Result<Option<Bytes>>;
    fn delete(&self, key: &[u8]) -> Result<()>;
}
#[derive(Debug, Clone)]
pub struct Options {
    /// Rotation threshold for memory or WAL bytes.
    pub memtable_size_limit: usize,
    /// Writers wait when this queue is full.
    pub max_immutable_memtables: usize,
    pub block_size: usize,
    pub block_cache_capacity: usize,
    pub bloom_filter_bits_per_key: usize,
    /// SST count that triggers compaction; minimum 2.
    pub compaction_file_threshold: usize,
    pub max_key_size: usize,
    pub max_value_size: usize,
}
impl Default for Options {
    fn default() -> Self {
        Self {
            memtable_size_limit: 4 * 1024 * 1024,
            max_immutable_memtables: 2,
            block_size: 16 * 1024,
            block_cache_capacity: 8 * 1024 * 1024,
            bloom_filter_bits_per_key: 10,
            compaction_file_threshold: 4,
            max_key_size: 1024 * 1024,
            max_value_size: 16 * 1024 * 1024,
        }
    }
}
impl Options {
    fn validate(&self) -> Result<()> {
        if self.memtable_size_limit == 0
            || self.max_immutable_memtables == 0
            || self.block_size < 64
            || self.block_size > MAX_RECORD
            || !(1..=30).contains(&self.bloom_filter_bits_per_key)
            || self.compaction_file_threshold < 2
            || self
                .max_key_size
                .saturating_add(self.max_value_size)
                .saturating_add(17)
                > MAX_RECORD
        {
            return Err(EngineError::InvalidConfig(
                "invalid limits, block size, Bloom bits or compaction threshold".into(),
            ));
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Default)]
pub struct Stats {
    pub sequence: u64,
    pub active_memtable_bytes: usize,
    pub immutable_memtable_bytes: usize,
    pub immutable_memtables: usize,
    pub sst_files: usize,
    pub sst_records: u64,
    pub block_reads: u64,
    pub cache_hits: u64,
    pub bloom_negatives: u64,
    pub cache_bytes: usize,
}
struct Frozen {
    id: u64,
    mem: MemTable,
}
struct State {
    mem: MemTable,
    immutable: VecDeque<Arc<Frozen>>,
    tables: Vec<Arc<Table>>,
    manifest: Manifest,
    sequence: u64,
    next_id: u64,
    fatal: Option<String>,
}
struct Disk {
    dir: PathBuf,
    _lock: File,
}
impl Drop for Disk {
    fn drop(&mut self) {
        // Explicit unlock avoids retaining the lock in a child between fork and exec.
        let _ = FileExt::unlock(&self._lock);
    }
}
struct Core {
    options: Options,
    disk: Option<Disk>,
    state: RwLock<State>,
    writer: Mutex<Option<Wal>>,
    maintenance: Mutex<()>,
    cache: Mutex<Cache>,
    counters: Counters,
    notify: Sender<()>,
    stop: AtomicBool,
    progress: Mutex<u64>,
    changed: Condvar,
}
struct Handle {
    core: Arc<Core>,
    worker: Mutex<Option<JoinHandle<()>>>,
}
#[derive(Clone)]
pub struct Engine {
    inner: Arc<Handle>,
}
impl Default for Engine {
    fn default() -> Self {
        Self::new()
    }
}
impl Engine {
    /// In-memory only; use `open` to persist data.
    pub fn new() -> Self {
        Self::build(
            Options::default(),
            None,
            None,
            State {
                mem: MemTable::default(),
                immutable: VecDeque::new(),
                tables: vec![],
                manifest: Manifest::default(),
                sequence: 0,
                next_id: 1,
                fatal: None,
            },
        )
        .expect("in-memory engine has no fallible startup")
    }
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_options(path, Options::default())
    }
    pub fn open_with_options(path: impl AsRef<Path>, options: Options) -> Result<Self> {
        options.validate()?;
        let dir = path.as_ref();
        fs::create_dir_all(dir)?;
        let dir = fs::canonicalize(dir)?;
        // Sync parents too, since create_dir_all may have created them.
        for ancestor in dir.ancestors() {
            sync_dir(ancestor)?;
        }
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(dir.join("LOCK"))?;
        match FileExt::try_lock_exclusive(&lock) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                return Err(EngineError::Locked)
            }
            Err(e) => return Err(e.into()),
        }
        fs::create_dir_all(dir.join("wal"))?;
        fs::create_dir_all(dir.join("sst"))?;
        sync_dir(&dir)?;
        let mut wal_ids = ids(&dir.join("wal"), "wal")?;
        let sst_ids = ids(&dir.join("sst"), "sst")?;
        let manifest = if dir.join("MANIFEST").exists() {
            Manifest::load(&dir)?
        } else {
            if !wal_ids.is_empty() || !sst_ids.is_empty() {
                return Err(corrupt("missing manifest with existing data"));
            }
            let m = Manifest::default();
            m.save(&dir)?;
            m
        };
        let mut next_id = wal_ids
            .iter()
            .chain(sst_ids.iter())
            .chain(manifest.tables.iter())
            .copied()
            .max()
            .unwrap_or(0)
            .max(manifest.wal_floor)
            .checked_add(1)
            .ok_or(EngineError::SequenceExhausted)?;
        let mut tables = vec![];
        for id in &manifest.tables {
            let table = Table::open(&sst_path(&dir, *id), *id)?;
            if table.max_seq > manifest.max_seq {
                return Err(corrupt("SST newer than manifest"));
            }
            tables.push(Arc::new(table));
        }
        wal_ids.retain(|id| *id > manifest.wal_floor);
        wal_ids.sort_unstable();
        let mut sequence = manifest.max_seq;
        let mut immutable = VecDeque::new();
        let mut mem = MemTable::default();
        let mut active = None;
        for (i, id) in wal_ids.iter().enumerate() {
            let last = i + 1 == wal_ids.len();
            let (wal, table) = Wal::recover(&wal_path(&dir, *id), *id, last, &mut sequence)?;
            if last {
                mem = table;
                active = Some(wal);
            } else {
                immutable.push_back(Arc::new(Frozen {
                    id: *id,
                    mem: table,
                }));
            }
        }
        if active.is_none() {
            let id = next_id;
            next_id = next_id
                .checked_add(1)
                .ok_or(EngineError::SequenceExhausted)?;
            active = Some(Wal::create(&wal_path(&dir, id), id)?);
            sync_dir(&dir.join("wal"))?;
        }
        // Validate the manifest before deleting files.
        let live: HashSet<_> = manifest.tables.iter().copied().collect();
        for id in sst_ids {
            if !live.contains(&id) {
                fs::remove_file(sst_path(&dir, id))?;
            }
        }
        for id in ids(&dir.join("wal"), "wal")? {
            if id <= manifest.wal_floor {
                fs::remove_file(wal_path(&dir, id))?;
            }
        }
        for sub in ["wal", "sst"] {
            for entry in fs::read_dir(dir.join(sub))? {
                let p = entry?.path();
                if p.extension().is_some_and(|x| x == "tmp") {
                    fs::remove_file(p)?;
                }
            }
            sync_dir(&dir.join(sub))?;
        }
        let state = State {
            mem,
            immutable,
            tables,
            manifest,
            sequence,
            next_id,
            fatal: None,
        };
        Self::build(options, Some(Disk { dir, _lock: lock }), active, state)
    }
    fn build(options: Options, disk: Option<Disk>, wal: Option<Wal>, state: State) -> Result<Self> {
        let (tx, rx) = bounded(1);
        let capacity = options.block_cache_capacity;
        let persistent = disk.is_some();
        let core = Arc::new(Core {
            options,
            disk,
            state: RwLock::new(state),
            writer: Mutex::new(wal),
            maintenance: Mutex::new(()),
            cache: Mutex::new(Cache::new(capacity)),
            counters: Counters::default(),
            notify: tx,
            stop: AtomicBool::new(false),
            progress: Mutex::new(0),
            changed: Condvar::new(),
        });
        let worker = if persistent {
            let c = core.clone();
            Some(
                thread::Builder::new()
                    .name("kv-maintenance".into())
                    .spawn(move || {
                        while rx.recv().is_ok() {
                            if c.stop.load(Ordering::Acquire) {
                                break;
                            }
                            let _guard = c.maintenance.lock();
                            loop {
                                if c.stop.load(Ordering::Acquire) {
                                    break;
                                }
                                if c.state.read().fatal.is_some() {
                                    break;
                                }
                                let work = if c.state.read().tables.len()
                                    >= c.options.compaction_file_threshold
                                {
                                    c.compact_all()
                                } else if !c.state.read().immutable.is_empty() {
                                    c.flush_one()
                                } else {
                                    break;
                                };
                                if let Err(e) = work {
                                    c.poison(&e);
                                    break;
                                }
                            }
                        }
                    })?,
            )
        } else {
            None
        };
        let engine = Self {
            inner: Arc::new(Handle {
                core,
                worker: Mutex::new(worker),
            }),
        };
        engine.inner.core.wake();
        Ok(engine)
    }
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.core.mutate(key, Some(value))
    }
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.core.mutate(key, None)
    }
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let c = &self.inner.core;
        let s = c.state.read();
        check(&s)?;
        if let Some(r) = s.mem.get(key) {
            return Ok(r.value);
        }
        for m in s.immutable.iter().rev() {
            if let Some(r) = m.mem.get(key) {
                return Ok(r.value);
            }
        }
        let tables = s.tables.clone();
        drop(s);
        let mut newest: Option<Record> = None;
        for table in tables {
            if let Some(r) = table.get(key, &c.cache, &c.counters)? {
                if newest.as_ref().is_none_or(|old| r.seq > old.seq) {
                    newest = Some(r);
                }
            }
        }
        Ok(newest.and_then(|r| r.value))
    }
    /// Flush all writes before this call's writer barrier to SSTs.
    pub fn flush(&self) -> Result<()> {
        let c = &self.inner.core;
        if c.disk.is_none() {
            return check(&c.state.read());
        }
        let goal = {
            let mut w = c.writer.lock();
            c.wait_room()?;
            if let Err(e) = c.freeze(&mut w) {
                c.poison(&e);
                return Err(e);
            }
            let s = c.state.read();
            s.immutable.back().map_or(s.manifest.wal_floor, |f| f.id)
        };
        c.wake();
        let _guard = c.maintenance.lock();
        let result = (|| loop {
            let s = c.state.read();
            check(&s)?;
            if s.manifest.wal_floor >= goal {
                return Ok(());
            }
            drop(s);
            if c.state.read().tables.len() >= c.options.compaction_file_threshold {
                c.compact_all()?;
            }
            c.flush_one()?;
        })();
        if let Err(e) = &result {
            c.poison(e);
        }
        result
    }
    /// Flush, then merge all live SSTs.
    pub fn compact(&self) -> Result<()> {
        self.flush()?;
        let c = &self.inner.core;
        if c.disk.is_none() {
            return Ok(());
        }
        let _guard = c.maintenance.lock();
        check(&c.state.read())?;
        let result = c.compact_all();
        if let Err(e) = &result {
            c.poison(e);
        }
        result
    }
    pub fn stats(&self) -> Stats {
        let c = &self.inner.core;
        let s = c.state.read();
        Stats {
            sequence: s.sequence,
            active_memtable_bytes: s.mem.bytes,
            immutable_memtable_bytes: s.immutable.iter().map(|m| m.mem.bytes).sum(),
            immutable_memtables: s.immutable.len(),
            sst_files: s.tables.len(),
            sst_records: s.tables.iter().map(|t| t.count).sum(),
            block_reads: c.counters.reads.load(Ordering::Relaxed),
            cache_hits: c.counters.hits.load(Ordering::Relaxed),
            bloom_negatives: c.counters.bloom_negatives.load(Ordering::Relaxed),
            cache_bytes: c.cache.lock().bytes(),
        }
    }
}
impl KvEngine for Engine {
    fn put(&self, k: &[u8], v: &[u8]) -> Result<()> {
        Engine::put(self, k, v)
    }
    fn get(&self, k: &[u8]) -> Result<Option<Bytes>> {
        Engine::get(self, k)
    }
    fn delete(&self, k: &[u8]) -> Result<()> {
        Engine::delete(self, k)
    }
}
impl Drop for Handle {
    fn drop(&mut self) {
        self.core.stop.store(true, Ordering::Release);
        self.core.wake();
        if let Some(w) = self.worker.get_mut().take() {
            let _ = w.join();
        }
    }
}
fn check(s: &State) -> Result<()> {
    match &s.fatal {
        Some(e) => Err(EngineError::Background(e.clone())),
        None => Ok(()),
    }
}
fn wal_path(dir: &Path, id: u64) -> PathBuf {
    dir.join("wal").join(format!("{id:020}.wal"))
}
fn sst_path(dir: &Path, id: u64) -> PathBuf {
    dir.join("sst").join(format!("{id:020}.sst"))
}
fn ids(dir: &Path, ext: &str) -> Result<Vec<u64>> {
    let mut out = vec![];
    for entry in fs::read_dir(dir)? {
        let p = entry?.path();
        if p.extension().is_some_and(|e| e == ext) {
            let id = p
                .file_stem()
                .and_then(|s| s.to_str())
                .and_then(|s| s.parse::<u64>().ok())
                .filter(|i| *i > 0)
                .ok_or_else(|| corrupt("invalid storage filename"))?;
            if p.file_name().and_then(|n| n.to_str()) != Some(format!("{id:020}.{ext}").as_str()) {
                return Err(corrupt("noncanonical storage filename"));
            }
            out.push(id);
        }
    }
    out.sort_unstable();
    Ok(out)
}
impl Core {
    fn wake(&self) {
        let _ = self.notify.try_send(());
    }
    fn progress(&self) {
        let mut p = self.progress.lock();
        *p = p.wrapping_add(1);
        self.changed.notify_all();
    }
    fn poison(&self, e: &EngineError) {
        self.state.write().fatal = Some(e.to_string());
        self.progress();
    }
    fn allocate(&self) -> Result<u64> {
        let mut s = self.state.write();
        let id = s.next_id;
        s.next_id = id.checked_add(1).ok_or(EngineError::SequenceExhausted)?;
        Ok(id)
    }
    fn wait_room(&self) -> Result<()> {
        let mut p = self.progress.lock();
        loop {
            let s = self.state.read();
            check(&s)?;
            if s.immutable.len() < self.options.max_immutable_memtables {
                return Ok(());
            }
            drop(s);
            self.wake();
            self.changed.wait(&mut p);
        }
    }
    fn mutate(&self, key: &[u8], value: Option<&[u8]>) -> Result<()> {
        if key.len() > self.options.max_key_size
            || value.is_some_and(|v| v.len() > self.options.max_value_size)
        {
            return Err(EngineError::InvalidConfig(
                "key or value exceeds configured size limit".into(),
            ));
        }
        let mut w = self.writer.lock();
        self.wait_room()?;
        // Rotate first so a rotation error cannot fail an already-applied write.
        if self.disk.is_some()
            && (self.state.read().mem.bytes >= self.options.memtable_size_limit
                || w.as_ref()
                    .is_some_and(|w| w.bytes >= self.options.memtable_size_limit as u64))
        {
            if let Err(e) = self.freeze(&mut w) {
                self.poison(&e);
                return Err(e);
            }
            self.wake();
        }
        let seq = self
            .state
            .read()
            .sequence
            .checked_add(1)
            .ok_or(EngineError::SequenceExhausted)?;
        let record = Record {
            seq,
            value: value.map(Bytes::copy_from_slice),
        };
        if let Some(wal) = w.as_mut() {
            if let Err(e) = wal.append(key, &record) {
                self.poison(&e);
                return Err(e);
            }
        }
        let mut s = self.state.write();
        s.mem.insert(key.to_vec(), record);
        s.sequence = seq;
        Ok(())
    }
    fn freeze(&self, w: &mut Option<Wal>) -> Result<()> {
        {
            let s = self.state.read();
            check(&s)?;
            if s.mem.data.is_empty() {
                return Ok(());
            }
        }
        let disk = self.disk.as_ref().unwrap();
        let id = self.allocate()?;
        let next = Wal::create(&wal_path(&disk.dir, id), id)?;
        sync_dir(&disk.dir.join("wal"))?;
        fault::hit("wal_after_rotation")?;
        let old = w.replace(next).unwrap();
        let mut s = self.state.write();
        let mem = std::mem::take(&mut s.mem);
        s.immutable.push_back(Arc::new(Frozen { id: old.id, mem }));
        Ok(())
    }
    fn flush_one(&self) -> Result<()> {
        let frozen = match self.state.read().immutable.front() {
            Some(f) => f.clone(),
            None => return Ok(()),
        };
        let dir = &self.disk.as_ref().unwrap().dir;
        let id = self.allocate()?;
        let table = Arc::new(Table::write(
            &sst_path(dir, id),
            id,
            frozen
                .mem
                .data
                .iter()
                .map(|(k, r)| Ok((k.clone(), r.clone()))),
            frozen.mem.data.len() as u64,
            self.options.block_size,
            self.options.bloom_filter_bits_per_key,
        )?);
        let mut manifest = self.state.read().manifest.clone();
        manifest.wal_floor = frozen.id;
        manifest.max_seq = manifest.max_seq.max(table.max_seq);
        manifest.tables.push(id);
        manifest.save(dir)?;
        {
            let mut s = self.state.write();
            s.manifest = manifest;
            s.tables.push(table);
            s.immutable.pop_front();
        }
        self.progress();
        fault::hit("flush_before_wal_delete")?;
        fs::remove_file(wal_path(dir, frozen.id))?;
        sync_dir(&dir.join("wal"))?;
        Ok(())
    }
    fn compact_all(&self) -> Result<()> {
        let tables = self.state.read().tables.clone();
        if tables.is_empty() {
            return Ok(());
        }
        let dir = &self.disk.as_ref().unwrap().dir;
        let id = self.allocate()?;
        let estimate = tables.iter().map(|t| t.count).sum();
        let merged = Arc::new(Table::write(
            &sst_path(dir, id),
            id,
            Merge::new(&tables)?,
            estimate,
            self.options.block_size,
            self.options.bloom_filter_bits_per_key,
        )?);
        let mut manifest = self.state.read().manifest.clone();
        manifest.tables = vec![id];
        manifest.save(dir)?;
        {
            let mut s = self.state.write();
            s.manifest = manifest;
            s.tables = vec![merged];
        }
        fault::hit("compaction_before_old_delete")?;
        for old in tables {
            fs::remove_file(sst_path(dir, old.id))?;
        }
        sync_dir(&dir.join("sst"))?;
        Ok(())
    }
}
