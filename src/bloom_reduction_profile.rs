//! Same-process arithmetic diagnostic; not end-to-end Get throughput.
use super::*;
use std::{hint::black_box, time::Instant};

fn modulo_contains(bloom: &Bloom, mut hash: u64) -> bool {
    let step = delta(hash);
    let bits = bloom.bits.len() as u64 * 8;
    for _ in 0..bloom.probes {
        let bit = (hash % bits) as usize;
        if bloom.bits[bit / 8] & (1 << (bit % 8)) == 0 {
            return false;
        }
        hash = hash.wrapping_add(step);
    }
    true
}

fn reciprocal_contains(bloom: &Bloom, mut hash: u64) -> bool {
    let step = delta(hash);
    for _ in 0..bloom.probes {
        let bit = bloom.bit_position(hash);
        if bloom.bits[bit / 8] & (1 << (hash & 7)) == 0 {
            return false;
        }
        hash = hash.wrapping_add(step);
    }
    true
}

#[test]
#[ignore = "same-process Bloom reduction diagnostic; run explicitly in release mode"]
fn profile_bloom_reduction() {
    type Probe = fn(&Bloom, u64) -> bool;
    for count in [4096u64, 100_000, 200_000] {
        let mut bloom = Bloom::new(black_box(count), 10);
        for key in 0..count {
            bloom.insert_hash(hash(&key.to_be_bytes()));
        }
        for present in [true, false] {
            let mut rng = 7919u64;
            let queries: Vec<_> = (0..8192)
                .map(|_| {
                    rng ^= rng << 13;
                    rng ^= rng >> 7;
                    rng ^= rng << 17;
                    let key = rng % count + if present { 0 } else { count };
                    hash(&key.to_be_bytes())
                })
                .collect();
            let expected: u64 = queries
                .iter()
                .map(|&hash| {
                    let reference = modulo_contains(&bloom, hash);
                    assert_eq!(bloom.contains_hash(hash), reference);
                    assert_eq!(reciprocal_contains(&bloom, hash), reference);
                    if present {
                        assert!(reference);
                    }
                    u64::from(reference)
                })
                .sum();
            let methods: [(&str, Probe); 2] = [
                ("reciprocal", reciprocal_contains),
                ("modulo", modulo_contains),
            ];
            for trial in 1..=6 {
                for position in 0..2 {
                    let (name, probe) = methods[(trial + position) % 2];
                    // Identical opaque function-pointer calls on both sides.
                    // Hashing, corpus construction and verification are outside.
                    let probe = black_box(probe);
                    for &hash in &queries {
                        black_box(probe(black_box(&bloom), hash));
                    }
                    let repeats = 128;
                    let mut positives = 0u64;
                    let start = Instant::now();
                    for _ in 0..repeats {
                        for &hash in &queries {
                            positives += u64::from(probe(black_box(&bloom), hash));
                        }
                    }
                    let seconds = start.elapsed().as_secs_f64();
                    assert_eq!(black_box(positives), expected * repeats);
                    println!(
                        "BLOOM_PROFILE {}",
                        serde_json::json!({"count":count,"present":present,
                        "method":name,"trial":trial,"order":position+1,
                        "metadata_bytes":std::mem::size_of::<Bloom>(),
                        "operations":queries.len() as u64*repeats,"seconds":seconds,
                        "notes":"Warm prehashed corpus; same Bloom bytes and struct layout; opaque function-call overhead on both methods. Not total Get throughput or a RocksDB comparison."})
                    );
                }
            }
        }
    }
}
