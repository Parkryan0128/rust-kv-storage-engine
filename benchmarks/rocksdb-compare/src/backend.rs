use crate::Result;
use serde_json::{json, Value};
use std::path::Path;

#[cfg(not(feature = "rocks"))]
pub struct Backend(rust_kv_storage_engine::Engine);

#[cfg(not(feature = "rocks"))]
impl Backend {
    pub fn open(path: &Path) -> Result<Self> {
        Ok(Self(rust_kv_storage_engine::Engine::open(path)?))
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        Ok(self.0.put(key, value)?)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        Ok(self.0.delete(key)?)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<bytes::Bytes>> {
        Ok(self.0.get(key)?)
    }

    pub fn flush(&self) -> Result<()> {
        Ok(self.0.flush()?)
    }

    pub fn compact(&self) -> Result<()> {
        Ok(self.0.compact()?)
    }

    pub fn stats(&self) -> Result<Value> {
        let s = self.0.stats();
        Ok(json!({
            "sst_files":s.sst_files,"sst_bytes":s.sst_bytes,
            "memtable_bytes":s.active_memtable_bytes+s.immutable_memtable_bytes,
            "cache_bytes":s.cache_bytes,"block_reads":s.block_reads,"cache_hits":s.cache_hits,
            "flush_bytes":s.flush_bytes,"compaction_output_bytes":s.compaction_output_bytes
        }))
    }
}

#[cfg(feature = "rocks")]
pub struct Backend {
    db: rocksdb::DB,
    write: rocksdb::WriteOptions,
    read: rocksdb::ReadOptions,
}

#[cfg(feature = "rocks")]
impl Backend {
    pub fn open(path: &Path) -> Result<Self> {
        use rocksdb::{BlockBasedOptions, Cache, DBCompressionType, Options, WriteOptions, DB};
        let mut options = Options::default();
        options.create_if_missing(true);
        options.set_write_buffer_size(4 * 1024 * 1024);
        options.set_max_write_buffer_number(3);
        options.set_compression_type(DBCompressionType::None);
        options.set_bottommost_compression_type(DBCompressionType::None);
        options.set_use_fsync(true);
        let mut table = BlockBasedOptions::default();
        table.set_block_cache(&Cache::new_lru_cache(8 * 1024 * 1024));
        table.set_block_size(16 * 1024);
        table.set_bloom_filter(10.0, false);
        // Both engines keep table metadata outside their block cache budget.
        table.set_cache_index_and_filter_blocks(false);
        options.set_block_based_table_factory(&table);
        let mut write = WriteOptions::default();
        write.set_sync(true);
        write.disable_wal(false);
        Ok(Self {
            db: DB::open(&options, path)?,
            write,
            read: rocksdb::ReadOptions::default(),
        })
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        Ok(self.db.put_opt(key, value, &self.write)?)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        Ok(self.db.delete_opt(key, &self.write)?)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<rocksdb::DBPinnableSlice<'_>>> {
        Ok(self.db.get_pinned_opt(key, &self.read)?)
    }

    pub fn flush(&self) -> Result<()> {
        let mut options = rocksdb::FlushOptions::default();
        options.set_wait(true);
        self.db.flush_opt(&options)?;
        self.db
            .wait_for_compact(&rocksdb::WaitForCompactOptions::default())?;
        Ok(())
    }

    pub fn compact(&self) -> Result<()> {
        self.flush()?;
        let mut options = rocksdb::CompactOptions::default();
        options.set_bottommost_level_compaction(rocksdb::BottommostLevelCompaction::Force);
        self.db
            .compact_range_opt::<&[u8], &[u8]>(None, None, &options);
        self.db
            .wait_for_compact(&rocksdb::WaitForCompactOptions::default())?;
        assert_eq!(
            self.db.property_int_value("rocksdb.background-errors")?,
            Some(0)
        );
        Ok(())
    }

    pub fn stats(&self) -> Result<Value> {
        let property = |name: &str| self.db.property_int_value(name);
        Ok(json!({
            "sst_bytes":property("rocksdb.total-sst-files-size")?,
            "memtable_bytes":property("rocksdb.size-all-mem-tables")?,
            "cache_bytes":property("rocksdb.block-cache-usage")?,
            "table_reader_bytes":property("rocksdb.estimate-table-readers-mem")?
        }))
    }
}
