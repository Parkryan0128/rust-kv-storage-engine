use crate::{Config, Result};
use serde_json::{json, Value};
use std::path::Path;
pub type Row = (Vec<u8>, bytes::Bytes);
pub type Rows<'a> = Box<dyn Iterator<Item = Result<Row>> + 'a>;

#[cfg(not(feature = "rocks"))]
pub struct Backend(rust_kv_storage_engine::Engine);
#[cfg(not(feature = "rocks"))]
impl Backend {
    pub fn open(path: &Path, c: &Config) -> Result<Self> {
        Ok(Self(rust_kv_storage_engine::Engine::open_with_options(
            path,
            rust_kv_storage_engine::Options {
                block_size: c.block,
                block_cache_capacity: c.cache,
                memtable_size_limit: c.memtable,
                // Fixed-layout read fixtures disable automatic merging. Mutating
                // workloads use the engine's normal background compaction policy.
                compaction_file_threshold: if c.fixed { usize::MAX } else { 4 },
                ..Default::default()
            },
        )?))
    }
    pub fn get(&self, key: &[u8]) -> Result<Option<bytes::Bytes>> {
        Ok(self.0.get(key)?)
    }
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        Ok(self.0.put(key, value)?)
    }
    pub fn batch(&self, entries: &[crate::Entry]) -> Result<()> {
        let mut b = rust_kv_storage_engine::WriteBatch::new();
        for (k, v) in entries {
            if let Some(v) = v {
                b.put(k, v);
            } else {
                b.delete(k);
            }
        }
        Ok(self.0.write_batch(b)?)
    }
    pub fn scan(&self, lo: &[u8], hi: &[u8]) -> Result<Rows<'_>> {
        Ok(Box::new(
            self.0.scan(lo..hi)?.map(|r| r.map_err(Into::into)),
        ))
    }
    pub fn flush(&self) -> Result<()> {
        Ok(self.0.flush()?)
    }
    pub fn compact(&self) -> Result<()> {
        Ok(self.0.compact()?)
    }
    pub fn stats(&self) -> Result<Value> {
        let s = self.0.stats();
        Ok(
            json!({"sst_files":s.sst_files,"sst_bytes":s.sst_bytes,"cache_bytes":s.cache_bytes,"block_reads":s.block_reads,"cache_hits":s.cache_hits,"memtable_bytes":s.active_memtable_bytes+s.immutable_memtable_bytes,"flush_bytes":s.flush_bytes,"compaction_output_bytes":s.compaction_output_bytes}),
        )
    }
}

#[cfg(feature = "rocks")]
pub struct Backend {
    db: rocksdb::DB,
    write: rocksdb::WriteOptions,
}
#[cfg(feature = "rocks")]
impl Backend {
    pub fn open(path: &Path, c: &Config) -> Result<Self> {
        use rocksdb::{BlockBasedOptions, Cache, DBCompressionType, Options, WriteOptions, DB};
        let mut o = Options::default();
        o.create_if_missing(true);
        o.set_compression_type(DBCompressionType::None);
        o.set_bottommost_compression_type(DBCompressionType::None);
        o.set_write_buffer_size(c.memtable);
        o.set_max_write_buffer_number(3);
        o.set_use_fsync(true);
        o.set_disable_auto_compactions(c.fixed);
        if c.fixed {
            o.set_level_zero_slowdown_writes_trigger(1000);
            o.set_level_zero_stop_writes_trigger(1000);
        }
        let mut table = BlockBasedOptions::default();
        if c.cache == 0 {
            table.disable_cache();
        } else {
            table.set_block_cache(&Cache::new_lru_cache(c.cache));
        }
        table.set_block_size(c.block);
        table.set_bloom_filter(10.0, false);
        table.set_cache_index_and_filter_blocks(false);
        o.set_block_based_table_factory(&table);
        let mut write = WriteOptions::default();
        write.set_sync(true);
        write.disable_wal(false);
        Ok(Self {
            db: DB::open(&o, path)?,
            write,
        })
    }
    // Owned values on both sides; do not compare engine copies against pinned
    // RocksDB cache slices with a different ownership/lifetime contract.
    pub fn get(&self, key: &[u8]) -> Result<Option<bytes::Bytes>> {
        Ok(self.db.get(key)?.map(bytes::Bytes::from))
    }
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        Ok(self.db.put_opt(key, value, &self.write)?)
    }
    pub fn batch(&self, entries: &[crate::Entry]) -> Result<()> {
        let mut b = rocksdb::WriteBatch::default();
        for (k, v) in entries {
            if let Some(v) = v {
                b.put(k, v);
            } else {
                b.delete(k);
            }
        }
        Ok(self.db.write_opt(b, &self.write)?)
    }
    pub fn scan(&self, lo: &[u8], hi: &[u8]) -> Result<Rows<'_>> {
        use rocksdb::{Direction, IteratorMode, ReadOptions};
        let mut r = ReadOptions::default();
        r.fill_cache(false);
        r.set_verify_checksums(true);
        r.set_iterate_upper_bound(hi.to_vec());
        Ok(Box::new(
            self.db
                .iterator_opt(IteratorMode::From(lo, Direction::Forward), r)
                .map(|r| {
                    r.map(|(k, v)| (k.into_vec(), bytes::Bytes::from(v.into_vec())))
                        .map_err(Into::into)
                }),
        ))
    }
    pub fn flush(&self) -> Result<()> {
        let mut o = rocksdb::FlushOptions::default();
        o.set_wait(true);
        Ok(self.db.flush_opt(&o)?)
    }
    pub fn compact(&self) -> Result<()> {
        self.flush()?;
        self.db.compact_range(None::<&[u8]>, None::<&[u8]>);
        Ok(())
    }
    pub fn stats(&self) -> Result<Value> {
        Ok(
            json!({"sst_files":self.db.live_files()?.len(),"sst_bytes":self.db.property_int_value("rocksdb.total-sst-files-size")?,"cache_bytes":self.db.property_int_value("rocksdb.block-cache-usage")?,"memtable_bytes":self.db.property_int_value("rocksdb.cur-size-all-mem-tables")?,"table_reader_bytes":self.db.property_int_value("rocksdb.estimate-table-readers-mem")?}),
        )
    }
}
