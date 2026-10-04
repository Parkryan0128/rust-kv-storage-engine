# Local storage demo

Run from the repository root on Linux or macOS:

```bash
cargo run --locked --release --example demo
```

Open **http://127.0.0.1:8080** in a browser. To use another port:

```bash
cargo run --locked --release --example demo -- 8088
```

One Rust process serves the browser UI and runs the storage engine. The demo needs no frontend build or external database. It binds to loopback and is intended for local use.

## Walkthrough

1. Save `user:1 = Ryan`, then read it. Watch the WAL size, sequence, and memory preview change.
2. Click **Flush to disk**. Select the new SST to inspect its first records.
3. Delete the key. A tombstone appears in memory, while the older SST can still contain the previous value.
4. Click **Close & reopen database**, then read the key again. This closes the engine and opens the same files, replaying the WAL where needed.
5. Add sample batches and flush between them to create several files. **Full compact** merges all live SSTs and can remove obsolete records and tombstones.
6. Open **Compaction comparison** and run the workload. Both policies receive the same changes, and their values are checked after every batch. You can pause or resume between batches.

The explorer refreshes after operations complete. Reopening is a clean shutdown and recovery; process-kill tests are covered in the [test notes](validation.md). Runtime byte counters reset on reopen, while the files and logical sequence are preserved.

## Compaction comparison

Both policies start with 1,024 keys, using eight-byte keys and 128-byte values. A full merge creates the initial dataset. Then 24 batches of 64 writes update a set of 128 keys, with a flush after each batch.

The graph counts SST output bytes since open, including the initial dataset. It excludes WAL and manifest writes and does not measure physical device writes. The interface also shows retained disk space and file count, since size-tiered compaction can leave more old versions on disk.

In a recorded local run, Full wrote 1,837,954 SST bytes and SizeTiered wrote 701,734, a 61.8% reduction. This interactive workload is smaller than the workloads in the [compaction benchmark](compaction.md); the UI calculates its percentage from the current run.

## Sessions and limits

Each launch creates a temporary sandbox. **Close & reopen database** keeps it; **Start a new sandbox** clears it and resets the comparison. The UI cannot resume a previous session after the server stops. Abrupt termination can leave temporary directories behind; the playground path is printed at startup.

The demo's limits are:

| Setting | Limit |
|---|---|
| Key | 128 bytes of UTF-8 text |
| Value | 1,024 bytes of UTF-8 text |
| Memory and table previews | First 32 records; fields truncated to 128 bytes |
| Playground mutations | 10,000 per session |
| Memtable threshold | 64 KiB |
| Block cache | 4 MiB per engine |
| Block size | 4 KiB |
| Operation log | Latest 40 events |

The engine supports binary keys and values. The demo decodes previews lossily for display; exact byte lengths are available through the inspection API. Table previews show stored versions, which can differ from the value returned by `get()`. The buffer limits above do not cap total process memory.

Comparison runs have a fixed size and use two separate temporary databases. Requests are serialized, and each comparison request advances one batch.

## Implementation notes

`tiny_http` and `serde_json` are development dependencies used by the example. The core engine does not depend on them.

`Engine::inspect()` returns a consistent metadata snapshot with bounded record previews. `inspect_table(id)` holds a table handle during disk reads and returns an error if the table is no longer live when requested. `stats()` is sampled separately, so it may reflect a different point in time.

[Test commands and recorded runs](validation.md)
