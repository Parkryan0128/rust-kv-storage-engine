use rust_kv_storage_engine::{Engine, EngineError};

#[test]
fn empty_keys_values_and_deletes_survive_reopen_and_compaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = Engine::open(dir.path()).unwrap();
    db.put(b"", b"").unwrap();
    db.put(b"other", b"").unwrap();
    db.flush().unwrap();
    db.delete(b"").unwrap();
    drop(db);

    let db = Engine::open(dir.path()).unwrap();
    assert_eq!(db.get(b"").unwrap(), None);
    assert_eq!(db.get(b"other").unwrap().as_deref(), Some(&b""[..]));
    db.compact().unwrap();
    assert_eq!(db.stats().sst_records, 1);
    db.put(b"", b"").unwrap();
    db.delete(b"other").unwrap();
    db.compact().unwrap();
    drop(db);

    let db = Engine::open(dir.path()).unwrap();
    assert_eq!(db.get(b"").unwrap().as_deref(), Some(&b""[..]));
    assert_eq!(db.get(b"other").unwrap(), None);
    assert_eq!(db.stats().sequence, 5);
    db.delete(b"").unwrap();
    db.compact().unwrap();
    assert_eq!(db.stats().sst_records, 0);
    drop(db);

    let db = Engine::open(dir.path()).unwrap();
    assert_eq!(db.get(b"").unwrap(), None);
    assert_eq!(db.stats().sequence, 6);
    db.put(b"after-empty", b"value").unwrap();
    drop(db);
    let db = Engine::open(dir.path()).unwrap();
    assert_eq!(db.stats().sequence, 7);
    assert_eq!(db.get(b"after-empty").unwrap().unwrap(), "value");
}

#[test]
fn directory_aliases_share_the_same_lock() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let alias = dir.path().join("alias");
    let db = Engine::open(&path).unwrap();
    db.put(b"key", b"value").unwrap();
    std::os::unix::fs::symlink(&path, &alias).unwrap();
    assert!(matches!(Engine::open(&alias), Err(EngineError::Locked)));
    drop(db);
    let db = Engine::open(&alias).unwrap();
    assert_eq!(db.get(b"key").unwrap().unwrap(), "value");
    assert!(matches!(Engine::open(&path), Err(EngineError::Locked)));
}
