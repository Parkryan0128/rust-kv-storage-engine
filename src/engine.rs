use crate::{
    cache::{BlockCache, Counters},
    codec::MAX_RECORD,
    compaction::{self, CompactionStyle, Merge},
    error::{corrupt, EngineError},
    fault,
    inspection::{Inspection, MemtableInfo, RecordPreview, TableInfo},
    manifest::Manifest,
    memtable::{MemTable, Record},
    scan::{KeyRange, Scan},
    sstable::{sync_dir, Table, TableCursor},
    wal::Wal,
    WriteBatch,
};
use bytes::Bytes;
use crossbeam_channel::{bounded, Sender};
use fs2::FileExt;
use parking_lot::{Condvar, Mutex, RwLock};
use std::{
    cmp::Reverse,
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    fs::{self, File, OpenOptions},
    ops::RangeBounds,
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
    /// Writers wait for room in this queue when rotating a full active buffer.
    pub max_immutable_memtables: usize,
    pub block_size: usize,
    pub block_cache_capacity: usize,
    pub bloom_filter_bits_per_key: usize,
    pub compaction_style: CompactionStyle,
    /// Files per size bucket (or all files for Full); minimum 2.
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
            compaction_style: CompactionStyle::default(),
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
    pub sst_bytes: u64,
    /// Bytes of completed SST publications since this engine was opened.
    pub flush_bytes: u64,
    pub compaction_input_bytes: u64,
    pub compaction_output_bytes: u64,
    pub compactions: u64,
    /// Largest number of input SSTs in a single merge since open (at most sixteen).
    pub max_compaction_inputs: u64,
}
struct Frozen {
    id: u64,
    mem: MemTable,
}
struct State {
    mem: MemTable,
    immutable: VecDeque<Arc<Frozen>>,
    tables: Arc<Vec<Arc<Table>>>,
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
struct Maintenance {
    prefer_flush: bool,
}
struct Core {
    options: Options,
    disk: Option<Disk>,
    state: RwLock<State>,
    writer: Mutex<Option<Wal>>,
    maintenance: Mutex<Maintenance>,
    cache: BlockCache,
    counters: Counters,
    notify: Sender<()>,
    stop: AtomicBool,
    progress: Mutex<()>,
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
                tables: Arc::new(vec![]),
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
        let dir = create_db_dir(path.as_ref())?;
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
        tables.sort_unstable_by_key(|table| Reverse(table.max_seq));
        wal_ids.retain(|id| *id > manifest.wal_floor);
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
            tables: Arc::new(tables),
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
        let cache = BlockCache::with_block_size(capacity, options.block_size);
        let persistent = disk.is_some();
        let core = Arc::new(Core {
            options,
            disk,
            state: RwLock::new(state),
            writer: Mutex::new(wal),
            maintenance: Mutex::new(Maintenance { prefer_flush: true }),
            cache,
            counters: Counters::default(),
            notify: tx,
            stop: AtomicBool::new(false),
            progress: Mutex::new(()),
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
                            loop {
                                let mut guard = c.maintenance.lock();
                                if c.stop.load(Ordering::Acquire) {
                                    break;
                                }
                                if c.state.read().fatal.is_some() {
                                    break;
                                }
                                match c.maintenance_step(true, &mut guard) {
                                    Ok(true) => {}
                                    Ok(false) => break,
                                    Err(e) => {
                                        c.poison(&e);
                                        break;
                                    }
                                }
                                // Hand off between jobs so a flush with a finite
                                // WAL goal cannot wait for all future writes.
                                parking_lot::MutexGuard::unlock_fair(guard);
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
    /// Atomically apply an ordered batch of puts and deletes.
    ///
    /// Persistent batches use one checksummed WAL frame and one `sync_all()`.
    /// Readers see the state before or after the whole batch; separate `get`
    /// calls do not constitute a snapshot. The last operation on a key wins.
    /// Empty batches check engine health and otherwise do nothing.
    ///
    /// Every entry must fit the configured key/value limits. The encoded batch
    /// (16-byte envelope plus 17-byte headers and key/value bytes) must fit in
    /// 64 MiB. Validation errors apply nothing and leave the engine usable.
    /// A batch may exceed the memtable target; it is never split across WALs.
    /// After an I/O error, reopen the engine: recovery may contain the entire
    /// failed batch, but never a committed prefix of it.
    ///
    /// ```
    /// use rust_kv_storage_engine::{Engine, WriteBatch};
    /// let db = Engine::new();
    /// let mut batch = WriteBatch::new();
    /// batch.put(b"name", b"Ryan").delete(b"old-name");
    /// db.write_batch(batch)?;
    /// assert_eq!(db.get(b"name")?.as_deref(), Some(&b"Ryan"[..]));
    /// # Ok::<(), rust_kv_storage_engine::EngineError>(())
    /// ```
    pub fn write_batch(&self, batch: WriteBatch) -> Result<()> {
        self.inner.core.write_batch(batch)
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
        for table in tables.iter() {
            // Tables are published in descending max-sequence order. A hit
            // (including a tombstone) can make every remaining file obsolete.
            if newest.as_ref().is_some_and(|r| r.seq >= table.max_seq) {
                break;
            }
            if let Some(r) = table.get(key, &c.cache, &c.counters)? {
                if newest.as_ref().is_none_or(|old| r.seq > old.seq) {
                    newest = Some(r);
                }
            }
        }
        Ok(newest.and_then(|r| r.value))
    }
    /// Scan a point-in-time range in ascending bytewise key order.
    ///
    /// Bounds are byte slices: `start..end`, `start..=end`, `start..`, `..end`,
    /// `..`, or a pair of `std::ops::Bound` values. Reversed bounds return
    /// `InvalidConfig`; equal bounds are empty unless both ends are included.
    ///
    /// Creation copies matching active keys and shares their immutable value
    /// bytes under the state lock. Frozen buffers are captured there and merged
    /// after releasing it. SST data is read lazily without that lock
    /// and without polluting the point-read cache. Handle each iterator item's
    /// `Result`: a disk error ends the scan, potentially after earlier results.
    /// Drop or exhaust the scan to release pinned files and the database lock.
    ///
    /// ```
    /// use rust_kv_storage_engine::{Engine, Result};
    /// let db = Engine::new();
    /// db.put(b"user:1", b"Ryan")?;
    /// db.put(b"user:2", b"Sam")?;
    /// let rows = db.scan(b"user:1".as_slice()..b"user:3".as_slice())?
    ///     .collect::<Result<Vec<_>>>()?;
    /// assert_eq!(rows.len(), 2);
    /// let all = db.scan(..)?.take(10).collect::<Result<Vec<_>>>()?;
    /// # Ok::<(), rust_kv_storage_engine::EngineError>(())
    /// ```
    pub fn scan<'a>(&self, range: impl RangeBounds<&'a [u8]>) -> Result<Scan> {
        let range = KeyRange::new(range)?;
        let s = self.inner.core.state.read();
        check(&s)?;
        if range.is_empty() {
            return Ok(Scan::empty());
        }
        let mut memory: BTreeMap<Vec<u8>, Record> = BTreeMap::new();
        for (key, record) in s.mem.data.range::<[u8], _>(range.bounds()) {
            memory.insert(key.clone(), record.clone());
        }
        let frozen: Vec<_> = s.immutable.iter().cloned().collect();
        let tables = s.tables.clone();
        drop(s);
        // Frozen buffers never change. Pin the same snapshot as active memory
        // and SSTs, but do their potentially large merge without blocking writes.
        #[cfg(test)]
        crate::test_hooks::pause("scan_before_frozen_merge");
        for frozen in frozen {
            for (key, record) in frozen.mem.data.range::<[u8], _>(range.bounds()) {
                memory
                    .entry(key.clone())
                    .and_modify(|old| {
                        if record.seq > old.seq {
                            *old = record.clone();
                        }
                    })
                    .or_insert_with(|| record.clone());
            }
        }
        Ok(Scan::new(self.clone(), memory, &tables, range))
    }
    pub(crate) fn scan_counters(&self) -> &Counters {
        &self.inner.core.counters
    }
    /// Flush writes before the writer barrier, then drain eligible compactions.
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
        let mut guard = c.maintenance.lock();
        let result = (|| loop {
            let s = c.state.read();
            check(&s)?;
            let flushed = s.manifest.wal_floor >= goal;
            drop(s);
            if !c.maintenance_step(!flushed, &mut guard)? {
                return Ok(());
            }
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
    /// Consistent metadata snapshot; previews contain at most 32 records and 128 bytes per field.
    pub fn inspect(&self) -> Result<Inspection> {
        let s = self.inner.core.state.read();
        check(&s)?;
        // Preserve publication order in the inspection API independently of
        // the sequence-ordered snapshot used by point reads.
        let by_id: HashMap<_, _> = s.tables.iter().map(|t| (t.id, t)).collect();
        let memory = |mem: &MemTable, wal_id| MemtableInfo {
            wal_id,
            bytes: mem.bytes,
            records: mem.data.len(),
            preview: mem
                .data
                .iter()
                .take(32)
                .map(|(k, r)| RecordPreview::new(k, r))
                .collect(),
        };
        Ok(Inspection {
            sequence: s.sequence,
            active: memory(&s.mem, None),
            frozen: s
                .immutable
                .iter()
                .map(|f| memory(&f.mem, Some(f.id)))
                .collect(),
            tables: s
                .manifest
                .tables
                .iter()
                .map(|id| {
                    let t = by_id[id];
                    TableInfo {
                        id: t.id,
                        bytes: t.file_bytes,
                        records: t.count,
                        max_sequence: t.max_seq,
                    }
                })
                .collect(),
        })
    }
    /// Samples an immutable table without holding the state lock during file reads.
    pub fn inspect_table(&self, id: u64) -> Result<Vec<RecordPreview>> {
        let table = {
            let s = self.inner.core.state.read();
            check(&s)?;
            s.tables
                .iter()
                .find(|t| t.id == id)
                .cloned()
                .ok_or_else(|| EngineError::InvalidConfig("table is no longer live".into()))?
        };
        let mut cursor = TableCursor::new(table)?;
        let mut preview = Vec::new();
        while let Some(record) = cursor.current()? {
            preview.push(RecordPreview::from_parts(
                record.key,
                record.seq,
                record.value,
            ));
            if preview.len() == 32 {
                break;
            }
            cursor.advance()?;
        }
        Ok(preview)
    }
    pub fn stats(&self) -> Stats {
        let c = &self.inner.core;
        let s = c.state.read();
        let (cache_bytes, cache_hits) = c.cache.stats();
        Stats {
            sequence: s.sequence,
            active_memtable_bytes: s.mem.bytes,
            immutable_memtable_bytes: s.immutable.iter().map(|m| m.mem.bytes).sum(),
            immutable_memtables: s.immutable.len(),
            sst_files: s.tables.len(),
            sst_records: s.tables.iter().map(|t| t.count).sum(),
            block_reads: c.counters.reads.load(Ordering::Relaxed),
            cache_hits,
            bloom_negatives: c.counters.bloom_negatives.load(Ordering::Relaxed),
            cache_bytes,
            sst_bytes: s.tables.iter().map(|t| t.file_bytes).sum(),
            flush_bytes: c.counters.flush_bytes.load(Ordering::Relaxed),
            compaction_input_bytes: c.counters.compaction_input_bytes.load(Ordering::Relaxed),
            compaction_output_bytes: c.counters.compaction_output_bytes.load(Ordering::Relaxed),
            compactions: c.counters.compactions.load(Ordering::Relaxed),
            max_compaction_inputs: c.counters.max_compaction_inputs.load(Ordering::Relaxed),
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
fn create_db_dir(path: &Path) -> Result<PathBuf> {
    fs::create_dir_all(path)?;
    let dir = fs::canonicalize(path)?;
    // A published manifest means initialization already synced the ancestors.
    // Without one, include ancestors left by an interrupted create_dir_all:
    // merely syncing directories created by this call would miss those entries.
    if !dir.join("MANIFEST").try_exists()? {
        for ancestor in dir.ancestors() {
            sync_dir(ancestor)?;
        }
    }
    Ok(dir)
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
        let _guard = self.progress.lock();
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
            #[cfg(test)]
            crate::test_hooks::pause("write_backpressure");
            self.wake();
            self.changed.wait(&mut p);
        }
    }
    fn validate_entry(&self, key: &[u8], value: Option<&[u8]>) -> Result<()> {
        if key.len() > self.options.max_key_size
            || value.is_some_and(|v| v.len() > self.options.max_value_size)
        {
            return Err(EngineError::InvalidConfig(
                "key or value exceeds configured size limit".into(),
            ));
        }
        Ok(())
    }
    // The caller holds the writer barrier, so no other write can consume these
    // sequence numbers. Reject exhaustion before waiting or rotating the WAL.
    fn prepare_write(&self, w: &mut Option<Wal>, operations: u64) -> Result<u64> {
        let end = {
            let s = self.state.read();
            check(&s)?;
            s.sequence
                .checked_add(operations)
                .ok_or(EngineError::SequenceExhausted)?
        };
        // Rotate first so a rotation error cannot fail an already-applied write.
        if self.disk.is_some()
            && (self.state.read().mem.bytes >= self.options.memtable_size_limit
                || w.as_ref()
                    .is_some_and(|w| w.bytes >= self.options.memtable_size_limit as u64))
        {
            // A full frozen queue does not prevent filling the current buffer.
            // Only rotation needs another slot; the writer barrier serializes it.
            self.wait_room()?;
            if let Err(e) = self.freeze(w) {
                self.poison(&e);
                return Err(e);
            }
            self.wake();
        }
        Ok(end)
    }
    fn write_batch(&self, batch: WriteBatch) -> Result<()> {
        #[cfg(feature = "write-profiling")]
        crate::write_profile::reset();
        if batch.is_empty() {
            return check(&self.state.read());
        }
        let size = write_timed!(Validate, {
            for entry in &batch.entries {
                self.validate_entry(&entry.key, entry.value.as_deref())?;
            }
            batch.encoded_len()?
        });
        let mut w = write_timed!(WriterLock, self.writer.lock());
        let end = write_timed!(Prepare, self.prepare_write(&mut w, batch.len() as u64))?;
        let start = end - batch.len() as u64;
        if let Some(wal) = w.as_mut() {
            if let Err(e) = wal.append_batch(&batch, start + 1, size) {
                self.poison(&e);
                return Err(e);
            }
        }
        // The writer barrier also excludes flush/rotation; a single state lock
        // publishes all operations, including repeated keys, as one change.
        #[cfg(test)]
        crate::test_hooks::pause("write_before_publish");
        let mut s = write_timed!(StateLock, self.state.write());
        write_timed!(Apply, {
            // Maintenance can fail while WAL I/O runs without the state lock.
            // Keep that failure visible; the synced batch may reappear on reopen.
            check(&s)?;
            for (i, entry) in batch.entries.into_iter().enumerate() {
                let record = Record {
                    seq: start + 1 + i as u64,
                    value: entry.value,
                };
                if self.disk.is_none() && record.value.is_none() {
                    s.mem.remove(&entry.key);
                } else {
                    s.mem.insert(entry.key, record);
                }
                if i == 0 {
                    #[cfg(test)]
                    crate::test_hooks::pause("batch_after_first_apply");
                    if let Err(e) = fault::hit("batch_after_first_apply") {
                        let e = EngineError::from(e);
                        s.fatal = Some(e.to_string());
                        drop(s);
                        self.progress();
                        return Err(e);
                    }
                }
            }
            s.sequence = end;
        });
        Ok(())
    }
    fn mutate(&self, key: &[u8], value: Option<&[u8]>) -> Result<()> {
        self.validate_entry(key, value)?;
        let mut w = self.writer.lock();
        let seq = self.prepare_write(&mut w, 1)?;
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
        #[cfg(test)]
        crate::test_hooks::pause("write_before_publish");
        let mut s = self.state.write();
        check(&s)?;
        if self.disk.is_none() && record.value.is_none() {
            // No older on-disk values exist to hide in a volatile engine.
            s.mem.remove(key);
        } else {
            s.mem.insert(key.to_vec(), record);
        }
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
            frozen.mem.data.iter().map(Ok),
            frozen.mem.data.len() as u64,
            self.options.block_size,
            self.options.bloom_filter_bits_per_key,
        )?);
        let mut manifest = self.state.read().manifest.clone();
        manifest.wal_floor = frozen.id;
        manifest.max_seq = manifest.max_seq.max(table.max_seq);
        manifest.tables.push(id);
        manifest.save(dir)?;
        self.counters
            .flush_bytes
            .fetch_add(table.file_bytes, Ordering::Relaxed);
        {
            let mut s = self.state.write();
            s.manifest = manifest;
            let tables = Arc::make_mut(&mut s.tables);
            tables.push(table);
            tables.sort_unstable_by_key(|table| Reverse(table.max_seq));
            s.immutable.pop_front();
        }
        self.progress();
        fault::hit("flush_before_wal_delete")?;
        fs::remove_file(wal_path(dir, frozen.id))?;
        sync_dir(&dir.join("wal"))?;
        Ok(())
    }
    // The caller holds maintenance. Alternate urgent flushes with eligible
    // compactions so continuous writers cannot starve either kind of work.
    // Explicit flush passes false after reaching its finite WAL goal, so later
    // writers cannot make it drain an indefinitely replenished frozen queue.
    fn maintenance_step(&self, allow_flush: bool, schedule: &mut Maintenance) -> Result<bool> {
        let queued = self.state.read().immutable.len();
        if allow_flush && queued >= self.options.max_immutable_memtables && schedule.prefer_flush {
            self.flush_one()?;
            schedule.prefer_flush = false;
        } else {
            let inputs = self.compaction_inputs();
            if !inputs.is_empty() {
                self.compact_tables(inputs)?;
                schedule.prefer_flush = true;
            } else if allow_flush && queued != 0 {
                self.flush_one()?;
                schedule.prefer_flush = false;
            } else {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn compaction_inputs(&self) -> Vec<Arc<Table>> {
        compaction::pick(
            &self.state.read().tables,
            self.options.compaction_file_threshold,
            self.options.compaction_style,
        )
    }
    fn compact_all(&self) -> Result<()> {
        let tables = self.state.read().tables.as_ref().clone();
        self.compact_tables(tables)
    }
    fn compact_tables(&self, tables: Vec<Arc<Table>>) -> Result<()> {
        if tables.is_empty() {
            return Ok(());
        }
        // Publish bounded partial merges. A crash between rounds leaves a valid
        // manifest, and tombstones survive until all remaining SSTs are selected.
        let mut pending: VecDeque<_> = tables.into();
        loop {
            let count = compaction::input_count(pending.iter().map(|t| t.compaction_buffer_bytes));
            let inputs = pending.drain(..count).collect();
            let merged = self.compact_group(inputs)?;
            if pending.is_empty() {
                return Ok(());
            }
            pending.push_back(merged);
            #[cfg(test)]
            crate::test_hooks::pause("compaction_between_groups");
        }
    }
    fn compact_group(&self, tables: Vec<Arc<Table>>) -> Result<Arc<Table>> {
        debug_assert!((1..=compaction::MAX_INPUTS).contains(&tables.len()));
        self.counters
            .max_compaction_inputs
            .fetch_max(tables.len() as u64, Ordering::Relaxed);
        let dir = &self.disk.as_ref().unwrap().dir;
        let id = self.allocate()?;
        let estimate = tables.iter().map(|t| t.count).sum();
        let selected: HashSet<_> = tables.iter().map(|t| t.id).collect();
        let drop_tombstones = selected.len() == self.state.read().tables.len();
        let input_bytes = tables.iter().map(|t| t.file_bytes).sum();
        let merged = Arc::new(Table::write_with(
            &sst_path(dir, id),
            id,
            estimate,
            self.options.block_size,
            self.options.bloom_filter_bits_per_key,
            |writer| Merge::new(&tables, drop_tombstones)?.write_into(writer),
        )?);
        let mut manifest = self.state.read().manifest.clone();
        manifest.tables.retain(|id| !selected.contains(id));
        manifest.tables.push(id);
        manifest.save(dir)?;
        self.counters
            .compaction_input_bytes
            .fetch_add(input_bytes, Ordering::Relaxed);
        self.counters
            .compaction_output_bytes
            .fetch_add(merged.file_bytes, Ordering::Relaxed);
        self.counters.compactions.fetch_add(1, Ordering::Relaxed);
        {
            let mut s = self.state.write();
            s.manifest = manifest;
            let live = Arc::make_mut(&mut s.tables);
            live.retain(|t| !selected.contains(&t.id));
            live.push(merged.clone());
            live.sort_unstable_by_key(|table| Reverse(table.max_seq));
        }
        // Retire before purging each shard. Readers may finish an admission that
        // started earlier, but cannot refill a shard after its purge completes.
        // Do not hold the state lock needed by unrelated memtable reads.
        #[cfg(test)]
        crate::test_hooks::pause("before_cache_purge");
        for table in &tables {
            table.retire();
        }
        self.cache.remove_tables(&selected);
        fault::hit("compaction_before_old_delete")?;
        for old in tables {
            fs::remove_file(sst_path(dir, old.id))?;
            fault::hit("compaction_after_old_delete")?;
        }
        sync_dir(&dir.join("sst"))?;
        fault::hit("compaction_after_dir_sync")?;
        Ok(merged)
    }
}

#[cfg(test)]
#[path = "batch_tests.rs"]
mod batch_tests;

#[cfg(test)]
#[path = "write_tests.rs"]
mod write_tests;

#[cfg(test)]
#[path = "scan_tests.rs"]
mod scan_tests;

#[cfg(test)]
mod audit_tests {
    use super::*;
    use std::{sync::mpsc::sync_channel, time::Duration};

    #[test]
    fn manual_compaction_releases_inputs_between_groups_unless_a_scan_pins_them() {
        for pin_with_scan in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let db = Engine::open_with_options(
                dir.path(),
                Options {
                    compaction_file_threshold: 1000,
                    ..Options::default()
                },
            )
            .unwrap();
            for i in 0..(compaction::MAX_INPUTS + 1) as u8 {
                db.put(&[i], &[i]).unwrap();
                db.flush().unwrap();
            }
            let inputs: Vec<_> = db
                .inner
                .core
                .state
                .read()
                .tables
                .iter()
                .take(compaction::MAX_INPUTS)
                .map(Arc::downgrade)
                .collect();
            let scan = pin_with_scan.then(|| db.scan(..).unwrap());
            let worker_db = db.clone();
            let (entered_tx, entered_rx) = sync_channel(1);
            let (resume_tx, resume_rx) = sync_channel(1);
            let worker = thread::spawn(move || {
                crate::test_hooks::arm("compaction_between_groups", entered_tx, resume_rx);
                worker_db.compact()
            });
            entered_rx.recv_timeout(Duration::from_secs(10)).unwrap();
            let retained = inputs.iter().all(|t| t.upgrade().is_some());
            drop(scan);
            let released = inputs.iter().all(|t| t.upgrade().is_none());
            resume_tx.send(()).unwrap();
            worker.join().unwrap().unwrap();
            assert_eq!(retained, pin_with_scan);
            assert!(
                released,
                "obsolete input handles survived the first merge round"
            );
            assert_eq!(
                db.scan(..)
                    .unwrap()
                    .collect::<Result<Vec<_>>>()
                    .unwrap()
                    .len(),
                compaction::MAX_INPUTS + 1
            );
        }
    }

    #[test]
    fn cache_purge_does_not_hold_the_state_write_lock() {
        let dir = tempfile::tempdir().unwrap();
        let db = Engine::open_with_options(
            dir.path(),
            Options {
                compaction_file_threshold: 1000,
                ..Options::default()
            },
        )
        .unwrap();
        db.put(b"disk", b"old").unwrap();
        db.flush().unwrap();
        let worker_db = db.clone();
        let (entered_tx, entered_rx) = sync_channel(1);
        let (resume_tx, resume_rx) = sync_channel(1);
        let worker = thread::spawn(move || {
            crate::test_hooks::arm("before_cache_purge", entered_tx, resume_rx);
            worker_db.compact()
        });
        entered_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        // try_read fails immediately if publication still holds the write lock.
        assert!(db.inner.core.state.try_read().is_some());
        db.put(b"memory", b"unblocked").unwrap();
        assert_eq!(db.get(b"memory").unwrap().unwrap(), "unblocked");
        resume_tx.send(()).unwrap();
        worker.join().unwrap().unwrap();
    }
}
