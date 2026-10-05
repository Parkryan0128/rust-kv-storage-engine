use crate::Result;
use serde_json::{json, Value};
use std::path::Path;

#[cfg(not(feature = "rocks"))]
pub struct Backend(rust_kv_storage_engine::Engine);

#[cfg(not(feature = "rocks"))]
impl Backend {
    pub fn open(path: &Path) -> Result<Self> {
        use rust_kv_storage_engine::{Engine, Options};
        Ok(Self(Engine::open_with_options(
            path,
            Options {
                block_size: crate::block_bytes(),
                compaction_file_threshold: 64,
                ..Options::default()
            },
        )?))
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<bytes::Bytes>> {
        Ok(self.0.get(key)?)
    }

    pub fn stats(&self) -> Result<Value> {
        let s = self.0.stats();
        Ok(json!({
            "sst_files":s.sst_files,"sst_bytes":s.sst_bytes,
            "cache_bytes":s.cache_bytes,"block_reads":s.block_reads,
            "cache_hits":s.cache_hits,"bloom_negatives":s.bloom_negatives
        }))
    }
}

#[cfg(feature = "rocks")]
pub struct Backend {
    db: rocksdb::DB,
    read: rocksdb::ReadOptions,
}

#[cfg(feature = "rocks")]
fn options() -> rocksdb::Options {
    use rocksdb::{BlockBasedOptions, Cache, DBCompressionType, Options};
    let mut options = Options::default();
    options.create_if_missing(true);
    options.set_write_buffer_size(4 * 1024 * 1024);
    options.set_max_write_buffer_number(3);
    options.set_compression_type(DBCompressionType::None);
    options.set_bottommost_compression_type(DBCompressionType::None);
    options.set_use_fsync(true);
    options.set_disable_auto_compactions(true);
    let mut table = BlockBasedOptions::default();
    table.set_block_cache(&Cache::new_lru_cache(8 * 1024 * 1024));
    table.set_block_size(crate::block_bytes());
    table.set_bloom_filter(10.0, false);
    table.set_cache_index_and_filter_blocks(false);
    options.set_block_based_table_factory(&table);
    options
}

#[cfg(feature = "rocks")]
impl Backend {
    pub fn open(path: &Path) -> Result<Self> {
        Ok(Self {
            db: rocksdb::DB::open_for_read_only(&options(), path, false)?,
            read: rocksdb::ReadOptions::default(),
        })
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<rocksdb::DBPinnableSlice<'_>>> {
        Ok(self.db.get_pinned_opt(key, &self.read)?)
    }

    pub fn stats(&self) -> Result<Value> {
        let property = |name: &str| self.db.property_int_value(name);
        Ok(json!({
            "sst_files":self.db.live_files()?.len(),
            "sst_bytes":property("rocksdb.total-sst-files-size")?,
            "cache_bytes":property("rocksdb.block-cache-usage")?,
            "table_reader_bytes":property("rocksdb.estimate-table-readers-mem")?
        }))
    }

    // Fixture setup only: produce sorted, disjoint SSTs without per-key WAL/fsync.
    pub fn prepare(path: &Path, keys: u64, value_bytes: usize, tables: u64) -> Result<()> {
        use rocksdb::{IngestExternalFileOptions, SstFileWriter, DB};
        assert!(!path.exists(), "fixture directory must be new");
        let options = options();
        let db = DB::open(&options, path)?;
        let mut paths = Vec::new();
        for part in 0..tables {
            let file = path.with_extension(format!("external-{part}.sst"));
            let mut writer = SstFileWriter::create(&options);
            writer.open(&file)?;
            let mut value = vec![42; value_bytes];
            for key in keys * part / tables..keys * (part + 1) / tables {
                value[..8].copy_from_slice(&key.to_be_bytes());
                value[8] = 0;
                writer.put(key.to_be_bytes(), &value)?;
            }
            writer.finish()?;
            paths.push(file);
            println!("READ_FIXTURE rocksdb table={}/{tables}", part + 1);
        }
        let mut ingest = IngestExternalFileOptions::default();
        ingest.set_move_files(true);
        db.ingest_external_file_opts(&ingest, paths)?;
        assert_eq!(db.live_files()?.len() as u64, tables);
        Ok(())
    }
}
