use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use alloy::primitives::Address;
use chrono::NaiveDateTime;
use rayon::iter::{IndexedParallelIterator, IntoParallelRefIterator, ParallelIterator};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::value::RawValue;

use crate::{
    order_book::{Oid, Side},
    types::{Fill, L4Order, OrderDiff},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct NodeDataOrderDiff {
    user: Address,
    oid: u64,
    px: String,
    coin: String,
    /// Lets the L2 book apply diffs without the order statuses. Not part of
    /// the L4 book updates clients get, which never had it.
    #[serde(skip_serializing)]
    side: Side,
    pub(crate) raw_book_diff: OrderDiff,
}

impl NodeDataOrderDiff {
    pub(crate) fn diff(&self) -> OrderDiff {
        self.raw_book_diff.clone()
    }
    pub(crate) const fn oid(&self) -> Oid {
        Oid::new(self.oid)
    }

    #[cfg(test)]
    pub(crate) fn coin(&self) -> crate::order_book::Coin {
        crate::order_book::Coin::new(&self.coin)
    }

    pub(crate) fn coin_str(&self) -> &str {
        &self.coin
    }

    pub(crate) fn px(&self) -> &str {
        &self.px
    }

    pub(crate) const fn side(&self) -> Side {
        self.side
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct NodeDataFill(pub Address, pub Fill);

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct NodeDataOrderStatus {
    pub time: NaiveDateTime,
    pub user: Address,
    pub status: String,
    pub order: L4Order,
}

impl NodeDataOrderStatus {
    pub(crate) fn is_inserted_into_book(&self) -> bool {
        (self.status == "open" && !self.order.is_trigger && (self.order.tif != Some("Ioc".to_string())))
            || (self.order.is_trigger && self.status == "triggered")
    }
}

#[derive(Clone, Copy, strum_macros::Display)]
pub(crate) enum EventSource {
    Fills,
    OrderStatuses,
    OrderDiffs,
}

impl EventSource {
    #[must_use]
    pub(crate) fn event_source_dir(self, dir: &Path) -> PathBuf {
        match self {
            Self::Fills => dir.join("hl/data/node_fills_by_block"),
            Self::OrderStatuses => dir.join("hl/data/node_order_statuses_by_block"),
            Self::OrderDiffs => dir.join("hl/data/node_raw_book_diffs_by_block"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Batch<E> {
    local_time: NaiveDateTime,
    block_time: NaiveDateTime,
    block_number: u64,
    /// Shared, so a clone is cheap: the listener and the L2 book thread both keep a block's diffs.
    events: Arc<Vec<E>>,
}

impl<E> Batch<E> {
    #[allow(clippy::unwrap_used)]
    pub(crate) fn block_time(&self) -> u64 {
        self.block_time.and_utc().timestamp_millis().try_into().unwrap()
    }

    /// When hl-node wrote this batch (UTC, µs since epoch).
    pub(crate) fn local_time_us(&self) -> u64 {
        self.local_time.and_utc().timestamp_micros().try_into().unwrap_or(0)
    }

    pub(crate) const fn block_number(&self) -> u64 {
        self.block_number
    }

    pub(crate) fn events(self) -> Vec<E>
    where
        E: Clone,
    {
        Arc::unwrap_or_clone(self.events)
    }

    pub(crate) fn events_ref(&self) -> &[E] {
        &self.events
    }
}

/// Lines shorter than this are parsed in one go. `from_str_par` first scans
/// the line for event boundaries (about a third of a full parse), which only
/// pays off on large lines.
const PAR_PARSE_MIN_LINE_LEN: usize = 128 * 1024;
/// Events per rayon task in `from_str_par`.
const PAR_PARSE_MIN_EVENTS: usize = 16;

/// A batch line with its events left unparsed.
#[derive(Deserialize)]
struct RawBatch<'a> {
    local_time: NaiveDateTime,
    block_time: NaiveDateTime,
    block_number: u64,
    #[serde(borrow)]
    events: Vec<&'a RawValue>,
}

impl<E: DeserializeOwned + Send> Batch<E> {
    /// Same result as `serde_json::from_str`, with the events of a large line
    /// parsed in parallel. Order-status lines are ~1 MB at the US open, and
    /// parsing them serially was 3.3 ms p50 / 13 ms p99 of the listener's
    /// time per block (2026-09-28). A torn (incomplete) line still fails.
    pub(crate) fn from_str_par(line: &str) -> serde_json::Result<Self> {
        Self::from_str_par_above(line, PAR_PARSE_MIN_LINE_LEN)
    }

    fn from_str_par_above(line: &str, min_line_len: usize) -> serde_json::Result<Self> {
        if line.len() < min_line_len {
            return serde_json::from_str(line);
        }
        let RawBatch { local_time, block_time, block_number, events } = serde_json::from_str(line)?;
        let events = events
            .par_iter()
            .with_min_len(PAR_PARSE_MIN_EVENTS)
            .map(|event| serde_json::from_str(event.get()))
            .collect::<serde_json::Result<Vec<E>>>()?;
        Ok(Self { local_time, block_time, block_number, events: Arc::new(events) })
    }
}

#[cfg(test)]
mod par_parse_test {
    use super::*;
    use std::{fs, time::Instant};

    /// Unfiltered hl-node order-status / order-diff lines (100 blocks each); test-only, not committed.
    const RAW_STATUSES: &str = "tmp/fixture/obs_raw_statuses.jsonl";
    const RAW_DIFFS: &str = "tmp/fixture/obs_raw_diffs.jsonl";

    fn check<E: DeserializeOwned + Send + Serialize>(path: &str) {
        let Ok(lines) = fs::read_to_string(path) else {
            eprintln!("skipping: {path} not present");
            return;
        };
        let (mut serial_time, mut par_time, mut n) = (std::time::Duration::ZERO, std::time::Duration::ZERO, 0);
        for line in lines.lines() {
            let start = Instant::now();
            let serial: Batch<E> = serde_json::from_str(line).unwrap();
            serial_time += start.elapsed();
            let start = Instant::now();
            let par = Batch::<E>::from_str_par(line).unwrap();
            par_time += start.elapsed();
            let forced = Batch::<E>::from_str_par_above(line, 0).unwrap();
            let expected = serde_json::to_string(&serial).unwrap();
            assert_eq!(serde_json::to_string(&par).unwrap(), expected);
            assert_eq!(serde_json::to_string(&forced).unwrap(), expected);
            // hl-node may not have finished writing the line yet.
            for cut in [1, line.len() / 3, line.len() / 2, line.len() - 1] {
                assert!(Batch::<E>::from_str_par_above(&line[..cut], 0).is_err(), "{path}: cut at {cut}");
            }
            n += 1;
        }
        assert!(n > 0, "{path}: no lines");
        eprintln!("{path}: {n} lines, serial {:?}/line, from_str_par {:?}/line", serial_time / n, par_time / n);
    }

    #[test]
    fn par_parse_matches_serde_on_raw_lines() {
        check::<NodeDataOrderStatus>(RAW_STATUSES);
        check::<NodeDataOrderDiff>(RAW_DIFFS);
    }
}
