use super::{hash, PrefixHasher};
use std::{hint::black_box, time::Instant};

// CPU-only diagnostic for repeated-prefix wins and unrelated-key overhead.
// End-to-end SST publication is measured separately by merge_matrix.py.
#[test]
#[ignore = "bounded hash diagnostic; run explicitly in release"]
fn prefix_hash_profile() {
    for len in [8usize, 32, 136, 1024] {
        for common in [false, true] {
            let mut state = 7u64;
            let mut keys: Vec<Vec<u8>> = (0u64..8192)
                .map(|n| {
                    let mut key = vec![42; len];
                    if !common {
                        for byte in &mut key {
                            state ^= state << 13;
                            state ^= state >> 7;
                            state ^= state << 17;
                            *byte = state as u8;
                        }
                    }
                    key[len - 8..].copy_from_slice(&n.to_be_bytes());
                    key
                })
                .collect();
            keys.sort_unstable();
            let expected: u64 = keys.iter().map(|key| hash(key)).fold(0, u64::wrapping_add);
            for trial in 0..6 {
                for cached in [trial % 2 == 0, trial % 2 != 0] {
                    let mut hasher = PrefixHasher::default();
                    let start = Instant::now();
                    let mut sum = 0u64;
                    for _ in 0..16 {
                        for key in &keys {
                            let key = black_box(key.as_slice());
                            sum =
                                sum.wrapping_add(if cached { hasher.hash(key) } else { hash(key) });
                        }
                    }
                    let seconds = start.elapsed().as_secs_f64();
                    assert_eq!(black_box(sum), expected.wrapping_mul(16));
                    println!(
                        "PREFIX_HASH_PROFILE {}",
                        serde_json::json!({
                            "key_bytes":len, "common_prefix":common, "cached":cached,
                            "trial":trial+1, "seconds":seconds, "operations":keys.len()*16
                        })
                    );
                }
            }
        }
    }
}
