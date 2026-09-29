//! Lock-free latency histograms, summarized to the log once per minute.
//!
//! Recording is a couple of relaxed atomic adds, so it is safe to call on
//! the listener hot path and from every client task. Buckets are
//! log-linear (8 per power of two), so reported percentiles are within
//! ~12% of the true value; `max` is exact.

use log::info;
use std::{
    sync::atomic::{AtomicU64, Ordering::Relaxed},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

const SUB_BITS: u32 = 3;
const SUB: u64 = 1 << SUB_BITS;
const N_BUCKETS: usize = ((64 - SUB_BITS as usize) + 1) * SUB as usize;

pub(crate) struct Histogram {
    name: &'static str,
    buckets: [AtomicU64; N_BUCKETS],
    max: AtomicU64,
}

fn bucket_of(v: u64) -> usize {
    if v < SUB {
        return v as usize;
    }
    let exp = 63 - v.leading_zeros();
    let sub = (v >> (exp - SUB_BITS)) & (SUB - 1);
    ((exp - SUB_BITS + 1) as u64 * SUB + sub) as usize
}

/// Smallest value that falls in bucket `i`.
fn bucket_floor(i: usize) -> u64 {
    let i = i as u64;
    if i < SUB {
        return i;
    }
    let exp = i / SUB + u64::from(SUB_BITS) - 1;
    (SUB + i % SUB) << (exp - u64::from(SUB_BITS))
}

impl Histogram {
    const fn new(name: &'static str) -> Self {
        Self { name, buckets: [const { AtomicU64::new(0) }; N_BUCKETS], max: AtomicU64::new(0) }
    }

    pub(crate) fn record(&self, v: u64) {
        self.buckets[bucket_of(v)].fetch_add(1, Relaxed);
        self.max.fetch_max(v, Relaxed);
    }

    pub(crate) fn record_duration_us(&self, d: Duration) {
        self.record(u64::try_from(d.as_micros()).unwrap_or(u64::MAX));
    }

    /// Records wall-clock now minus `since_us` (0 if `since_us` is in the future).
    /// `since_us == 0` means unknown (e.g. a snapshot broadcast before the first
    /// applied block) and is skipped.
    pub(crate) fn record_age_us(&self, since_us: u64) {
        if since_us == 0 {
            return;
        }
        self.record(now_us().saturating_sub(since_us));
    }

    /// Drains the histogram and returns a one-line summary, or None if empty.
    fn take_summary(&self) -> Option<String> {
        let counts: Vec<u64> = self.buckets.iter().map(|b| b.swap(0, Relaxed)).collect();
        let max = self.max.swap(0, Relaxed);
        let n: u64 = counts.iter().sum();
        if n == 0 {
            return None;
        }
        let pct = |p: f64| {
            let rank = ((n as f64) * p).ceil().max(1.0) as u64;
            let mut seen = 0;
            for (i, c) in counts.iter().enumerate() {
                seen += c;
                if seen >= rank {
                    return bucket_floor(i).min(max);
                }
            }
            max
        };
        Some(format!("{}: n={n} p50={} p99={} p999={} max={max}", self.name, pct(0.5), pct(0.99), pct(0.999)))
    }
}

pub(crate) fn now_us() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| u64::try_from(d.as_micros()).unwrap_or(u64::MAX))
}

// Reference points: block_time is consensus time; local_time is when hl-node
// wrote the block to its output file (~150-200 ms later). Everything after
// local_time is latency added on this host by obs.

/// hl-node local_time minus block_time (upstream: consensus -> hl-node file write).
pub(crate) static HL_WRITE_LAG_MS: Histogram = Histogram::new("hl_write_lag_ms");
/// Wall clock minus hl-node local_time when the listener applies a block's L4 updates.
pub(crate) static APPLY_AFTER_WRITE_US: Histogram = Histogram::new("apply_after_write_us");
/// Time hl_listen waits to acquire the listener mutex for one fs event.
pub(crate) static LOCK_WAIT_US: Histogram = Histogram::new("listener_lock_wait_us");
/// Time hl_listen holds the listener mutex for one fs event.
pub(crate) static LOCK_HOLD_US: Histogram = Histogram::new("listener_lock_hold_us");
/// Snapshot-validation state clone, once per 60 s fetch (runs under the listener mutex).
pub(crate) static SNAPSHOT_CLONE_US: Histogram = Histogram::new("snapshot_clone_us");
/// Snapshot-validation catch-up and comparison, once per 60 s fetch (blocking pool, no mutex).
pub(crate) static SNAPSHOT_VALIDATE_US: Histogram = Histogram::new("snapshot_validate_us");
/// JSON parse of one order-statuses / order-diffs line (runs under the listener mutex).
pub(crate) static STATUSES_PARSE_US: Histogram = Histogram::new("statuses_parse_us");
pub(crate) static DIFFS_PARSE_US: Histogram = Histogram::new("diffs_parse_us");
/// apply_updates of one block (runs under the listener mutex).
pub(crate) static APPLY_US: Histogram = Histogram::new("apply_us");
/// compute_l2_snapshots duration (runs under the listener mutex).
pub(crate) static L2_COMPUTE_US: Histogram = Histogram::new("l2_compute_us");
/// Coins some client wants a sig-fig L2 variant of, per L2 compute.
pub(crate) static L2_AGGREGATED_COINS: Histogram = Histogram::new("l2_aggregated_coins");
/// L2 book apply of one block's diffs, before its statuses arrive (under the listener mutex).
pub(crate) static L2_APPLY_US: Histogram = Histogram::new("l2_apply_us");
/// Check of the L2 book against the L4 book after an L4 apply (under the listener mutex).
pub(crate) static L2_CHECK_US: Histogram = Histogram::new("l2_check_us");
/// Coins whose L2 book differed from the L4 book, per check that found any (should never be recorded).
pub(crate) static L2_DIVERGENT_COINS: Histogram = Histogram::new("l2_divergent_coins");
/// Rebuild of the whole L2 book from the L4 book (after init or an L2 apply error).
pub(crate) static L2_REBUILD_US: Histogram = Histogram::new("l2_rebuild_us");
/// Wall clock minus hl-node local_time when a client with a matching
/// subscription has finished sending that block's message, per stream.
pub(crate) static CLIENT_L2_AFTER_WRITE_US: Histogram = Histogram::new("client_l2_after_write_us");
pub(crate) static CLIENT_L4_AFTER_WRITE_US: Histogram = Histogram::new("client_l4_after_write_us");
pub(crate) static CLIENT_ORDER_UPDATES_AFTER_WRITE_US: Histogram =
    Histogram::new("client_order_updates_after_write_us");
pub(crate) static CLIENT_FILLS_AFTER_WRITE_US: Histogram = Histogram::new("client_fills_after_write_us");
/// Time a client task spends handling one broadcast message (filtering, serializing, sending).
pub(crate) static CLIENT_HANDLE_US: Histogram = Histogram::new("client_handle_us");
/// Messages still queued for a client when it receives one; it is disconnected once this passes 100.
pub(crate) static CLIENT_QUEUE_LEN: Histogram = Histogram::new("client_queue_len");

static ALL: [&Histogram; 21] = [
    &HL_WRITE_LAG_MS,
    &APPLY_AFTER_WRITE_US,
    &LOCK_WAIT_US,
    &LOCK_HOLD_US,
    &SNAPSHOT_CLONE_US,
    &SNAPSHOT_VALIDATE_US,
    &STATUSES_PARSE_US,
    &DIFFS_PARSE_US,
    &APPLY_US,
    &L2_COMPUTE_US,
    &L2_AGGREGATED_COINS,
    &L2_APPLY_US,
    &L2_CHECK_US,
    &L2_DIVERGENT_COINS,
    &L2_REBUILD_US,
    &CLIENT_L2_AFTER_WRITE_US,
    &CLIENT_L4_AFTER_WRITE_US,
    &CLIENT_ORDER_UPDATES_AFTER_WRITE_US,
    &CLIENT_FILLS_AFTER_WRITE_US,
    &CLIENT_HANDLE_US,
    &CLIENT_QUEUE_LEN,
];

pub(crate) async fn report_loop(period: Duration) {
    let mut ticker = tokio::time::interval(period);
    ticker.tick().await;
    loop {
        ticker.tick().await;
        let parts: Vec<String> = ALL.iter().filter_map(|h| h.take_summary()).collect();
        if !parts.is_empty() {
            info!("[latency] {}s window: {}", period.as_secs(), parts.join(" | "));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bucket_floor_is_inverse_lower_bound() {
        for v in (0..100_000u64).chain([u64::MAX / 3, u64::MAX - 1, u64::MAX]) {
            let i = bucket_of(v);
            assert!(i < N_BUCKETS, "{v}");
            assert!(bucket_floor(i) <= v, "{v}");
            if i + 1 < N_BUCKETS {
                assert!(bucket_floor(i + 1) > v, "{v}");
            }
        }
    }

    #[test]
    fn percentiles_are_close_and_drain() {
        let h = Histogram::new("t");
        for v in 1..=1000 {
            h.record(v);
        }
        let s = h.take_summary().unwrap();
        assert!(s.contains("n=1000"), "{s}");
        assert!(s.contains("max=1000"), "{s}");
        let p50: u64 = s.split("p50=").nth(1).unwrap().split(' ').next().unwrap().parse().unwrap();
        assert!((440..=500).contains(&p50), "{s}");
        assert!(h.take_summary().is_none());
    }
}
