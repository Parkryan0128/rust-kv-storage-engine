//! Isolate WAL replay costs without changing the production recovery path.
use super::*;
use std::{hint::black_box, time::Instant};

fn fixture(keys: u64, batch: usize, random: bool) -> (Vec<u8>, Vec<Vec<u8>>) {
    let mut file = MAGIC.to_vec();
    let mut payloads = Vec::new();
    for first in (0..keys).step_by(batch) {
        let count = (keys - first).min(batch as u64);
        let mut payload = Vec::new();
        put_u64(&mut payload, 0);
        put_u32(&mut payload, BATCH_VERSION);
        put_u32(&mut payload, count as u32);
        for offset in 0..count {
            let sequence = first + offset;
            let key = if random {
                (sequence * 7919) % keys
            } else {
                sequence
            };
            encode_record_parts(
                &key.to_be_bytes(),
                sequence + 1,
                Some(&[42; 128]),
                &mut payload,
            );
        }
        write_frame(&mut file, &payload).unwrap();
        payloads.push(payload);
    }
    (file, payloads)
}

fn decode(payloads: &[Vec<u8>]) -> Vec<Vec<(Vec<u8>, Record)>> {
    payloads
        .iter()
        .map(|payload| {
            let mut cursor = Cursor { b: &payload[16..] };
            let mut entries = Vec::new();
            while !cursor.b.is_empty() {
                entries.push(decode_record(&mut cursor).unwrap());
            }
            entries
        })
        .collect()
}

fn check(table: &MemTable, keys: u64) {
    assert_eq!(table.data.len(), keys as usize);
    assert_eq!(
        table.bytes,
        keys as usize * (crate::memtable::RECORD_CHARGE + 8 + 128)
    );
    for key in [0, keys / 2, keys - 1] {
        assert_eq!(
            table.get(&key.to_be_bytes()).unwrap().value.as_deref(),
            Some([42; 128].as_slice())
        );
    }
}

#[test]
#[ignore = "bounded WAL phase diagnostic; run explicitly in release mode"]
fn profile_recovery_stages() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("profile.wal");
    let passes = 8;
    for (batch, random) in [(256, false), (256, true), (32, true), (1, false)] {
        let keys = if batch == 1 { 10_000 } else { 20_000 };
        let (bytes, payloads) = fixture(keys, batch, random);
        std::fs::write(&path, &bytes).unwrap();
        File::open(&path).unwrap().sync_all().unwrap();
        // Warm the file and allocator before collecting timings.
        let (_, warm) = Wal::recover(&path, 1, false, &mut 0).unwrap();
        check(&warm, keys);
        drop(warm);
        let mut times = [0.0; 5];
        for _ in 0..passes {
            let start = Instant::now();
            let (wal, table) = Wal::recover(&path, 1, false, &mut 0).unwrap();
            times[0] += start.elapsed().as_secs_f64();
            assert_eq!(wal.bytes, bytes.len() as u64);
            check(&table, keys);
            drop((wal, table));

            let start = Instant::now();
            let mut reader = BufReader::with_capacity(MAX_RETAINED_BUFFER, &bytes[8..]);
            let mut buffer = Vec::new();
            let mut frames = 0;
            while read_frame_into(&mut reader, false, &mut buffer).unwrap() {
                black_box(&buffer);
                frames += 1;
            }
            times[1] += start.elapsed().as_secs_f64();
            assert_eq!(frames, payloads.len());

            let mut table = MemTable::default();
            let mut sequence = 0;
            let start = Instant::now();
            for payload in &payloads {
                replay_batch(black_box(payload), &mut table, &mut sequence).unwrap();
            }
            times[2] += start.elapsed().as_secs_f64();
            assert_eq!(sequence, keys);
            check(&table, keys);
            drop(table);

            let start = Instant::now();
            let owned = decode(black_box(&payloads));
            times[3] += start.elapsed().as_secs_f64();
            let mut table = MemTable::default();
            let start = Instant::now();
            for entries in owned {
                table.apply(entries, false);
            }
            times[4] += start.elapsed().as_secs_f64();
            check(&table, keys);
        }
        for (stage, seconds) in [
            "recover-file",
            "read-frames-memory",
            "replay-batches",
            "decode-owned",
            "apply-owned",
        ]
        .into_iter()
        .zip(times)
        {
            println!(
                "RECOVERY_PROFILE {}",
                serde_json::json!({
                    "batch":batch,"random":random,"keys":keys,"passes":passes,"stage":stage,
                    "operations":keys * passes,"seconds":seconds,
                    "notes":"Release libtest diagnostic. File replay uses warm OS cache; frame-only reads use memory. Phase paths allocate differently, overlap, and must not be added or treated as production percentages. Table verification/drop is outside timers. Small-batch replay helper excludes production coalescing."
                })
            );
        }
    }
}
