//! Opt-in, thread-local batch-write diagnostics. Not part of the stable API.
//! Default builds compile out all clocks and counters. Call `take` immediately
//! after a batch on the same thread; failed batches contain partial timings.
use std::{cell::Cell, time::Instant};

pub const PHASES: [&str; 9] = [
    "validate",
    "writer_lock",
    "prepare",
    "wal_encode",
    "wal_checksum",
    "wal_write",
    "wal_sync",
    "state_lock",
    "apply",
];
#[derive(Clone, Copy)]
pub(crate) enum Phase {
    Validate,
    WriterLock,
    Prepare,
    WalEncode,
    WalChecksum,
    WalWrite,
    WalSync,
    StateLock,
    Apply,
}
thread_local! { static TIMES: Cell<[u64; 9]> = const { Cell::new([0; 9]) }; }
pub(crate) fn reset() {
    TIMES.with(|t| t.set([0; 9]));
}
/// Consume nanoseconds spent in each phase of the most recent batch.
pub fn take() -> [u64; 9] {
    TIMES.with(|t| t.replace([0; 9]))
}
pub(crate) struct Timer {
    phase: Phase,
    start: Instant,
}
impl Timer {
    pub fn new(phase: Phase) -> Self {
        Self {
            phase,
            start: Instant::now(),
        }
    }
}
impl Drop for Timer {
    fn drop(&mut self) {
        let ns = self.start.elapsed().as_nanos().min(u64::MAX as u128) as u64;
        TIMES.with(|t| {
            let mut times = t.get();
            times[self.phase as usize] = times[self.phase as usize].saturating_add(ns);
            t.set(times);
        });
    }
}
