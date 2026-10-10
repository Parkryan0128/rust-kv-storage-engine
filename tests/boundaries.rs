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

#[test]
fn existing_database_opens_below_a_search_only_ancestor() {
    use std::{fs, os::unix::fs::PermissionsExt, path::PathBuf};

    struct Restore(PathBuf, fs::Permissions);
    impl Drop for Restore {
        fn drop(&mut self) {
            fs::set_permissions(&self.0, self.1.clone()).unwrap();
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let ancestor = dir.path().join("search-only");
    let path = ancestor.join("db");
    let db = Engine::open(&path).unwrap();
    db.put(b"key", b"before").unwrap();
    drop(db);
    let _restore = Restore(
        ancestor.clone(),
        fs::metadata(&ancestor).unwrap().permissions(),
    );
    fs::set_permissions(&ancestor, fs::Permissions::from_mode(0o111)).unwrap();
    if fs::File::open(&ancestor).is_ok() {
        // Root/capabilities can bypass the condition being tested.
        return;
    }
    let db = Engine::open(&path).unwrap();
    assert_eq!(db.get(b"key").unwrap().unwrap(), "before");
    db.put(b"key", b"after").unwrap();
    drop(db);
    let db = Engine::open(&path).unwrap();
    assert_eq!(db.get(b"key").unwrap().unwrap(), "after");
}

#[test]
fn nested_directory_creation_still_supports_recovery_and_locking() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("one/two/three/db");
    let db = Engine::open(&path).unwrap();
    db.put(b"key", b"value").unwrap();
    assert!(matches!(Engine::open(&path), Err(EngineError::Locked)));
    drop(db);
    let db = Engine::open(&path).unwrap();
    assert_eq!(db.get(b"key").unwrap().unwrap(), "value");
}
