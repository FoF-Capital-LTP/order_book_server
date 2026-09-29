use crate::{
    listeners::order_book::{CoinL2Snapshots, L2SnapshotParams},
    order_book::{
        Coin, OrderBook, Snapshot,
        multi_book::Snapshots,
        types::InnerOrder,
    },
    prelude::*,
    types::{
        node_data::{Batch, EventSource, NodeDataFill, NodeDataOrderDiff, NodeDataOrderStatus},
        subscription::MAX_LEVELS,
    },
};
use log::warn;
use reqwest::Client;
use serde_json::json;
use std::collections::VecDeque;
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::time::sleep;

/// Timeout for the snapshot fetch POST to localhost:3001/info. The hl-node
/// info server normally responds in <100 ms; a 30 s ceiling is generous
/// enough to absorb a slow disk during snapshot serialization but tight
/// enough that we don't let `fetched_snapshot_cache` grow unbounded if the
/// info server hangs (which would silently re-trigger the original
/// 23 GB-RSS class of bug).
const SNAPSHOT_FETCH_TIMEOUT: Duration = Duration::from_secs(30);

/// Bounded retries for transient network errors (connection refused,
/// timeout, premature EOF) when POSTing to localhost:3001/info. The
/// info server is co-located on this box, so the failure mode we want to
/// absorb is hl-visor restarts: when hl-visor cycles, its info server
/// disappears for ~5-30s before becoming reachable again. Without this
/// retry, every hl-visor restart fataled order-book-server (validated
/// 2026-05-31: 19 cascaded `Abci state reading error` fatals downstream
/// of 4 hl-visor restarts).
///
/// 5 attempts with exponential backoff (1s, 2s, 4s, 8s, 16s) ≈ 31s total
/// elapsed retry budget, which exceeds typical hl-visor cold-start
/// latency. We do NOT retry on HTTP 4xx/5xx — those signal a semantic
/// problem (request rejected, info server reporting an error) that more
/// requests will not fix, and we want to fail fast so systemd cold-starts
/// us into a known-good state.
const SNAPSHOT_FETCH_MAX_ATTEMPTS: u32 = 5;
const SNAPSHOT_FETCH_INITIAL_BACKOFF: Duration = Duration::from_secs(1);

pub(super) async fn process_rmp_file(dir: &Path) -> Result<PathBuf> {
    let output_path = dir.join("out.json");
    let payload = json!({
        "type": "fileSnapshot",
        "request": {
            "type": "l4Snapshots",
            "includeUsers": true,
            "includeTriggerOrders": false
        },
        "outPath": output_path,
        "includeHeightInOutput": true
    });

    let client = Client::builder().timeout(SNAPSHOT_FETCH_TIMEOUT).build()?;
    let mut backoff = SNAPSHOT_FETCH_INITIAL_BACKOFF;
    let mut last_err: Option<reqwest::Error> = None;
    for attempt in 1..=SNAPSHOT_FETCH_MAX_ATTEMPTS {
        match client
            .post("http://localhost:3001/info")
            .header("Content-Type", "application/json")
            .json(&payload)
            .send()
            .await
        {
            Ok(resp) => match resp.error_for_status() {
                // HTTP 2xx — done.
                Ok(_) => return Ok(output_path),
                // HTTP 4xx/5xx — semantic failure; do NOT retry.
                Err(err) => return Err(err.into()),
            },
            Err(err) => {
                // Only retry transient transport errors. `is_status` would be
                // an HTTP error and is handled above (we never get here for
                // it), so the remaining variants — connect, timeout, body
                // read — are all "info server is bouncing right now" and
                // worth one more attempt.
                let transient = err.is_connect() || err.is_timeout() || err.is_request();
                if !transient || attempt == SNAPSHOT_FETCH_MAX_ATTEMPTS {
                    return Err(err.into());
                }
                warn!(
                    "snapshot fetch attempt {attempt}/{SNAPSHOT_FETCH_MAX_ATTEMPTS} failed ({err}); retrying in {:?}",
                    backoff
                );
                last_err = Some(err);
                sleep(backoff).await;
                backoff = backoff.saturating_mul(2);
            }
        }
    }
    // Loop only exits via return; this is unreachable but keeps the type
    // checker happy without an explicit unreachable!() panic on a hot path.
    Err(last_err
        .map(|e| e.into())
        .unwrap_or_else(|| "snapshot fetch exhausted retries with no recorded error".into()))
}

/// Validate that the local order book state matches the authoritative snapshot.
///
/// Returns the set of coins present in `expected` but absent from local
/// `snapshot` (the "extra" books). The caller can graft these into local
/// state to absorb newly-listed coins without restarting the listener.
/// Per-order divergences and missing-on-server cases are still hard errors,
/// because they signal real state corruption rather than a benign coin add.
pub(super) fn validate_snapshot_consistency<O: Clone + PartialEq + Debug + InnerOrder>(
    snapshot: &Snapshots<O>,
    expected: Snapshots<O>,
    ignore_spot: bool,
) -> Result<HashMap<Coin, Snapshot<O>>> {
    let mut snapshot_map: HashMap<_, _> =
        expected.value().into_iter().filter(|(c, _)| !c.is_spot() || !ignore_spot).collect();

    let mut misordered = Vec::new();
    for (coin, book) in snapshot.as_ref() {
        if ignore_spot && coin.is_spot() {
            continue;
        }
        let book1 = book.as_ref();
        if let Some(book2) = snapshot_map.remove(coin) {
            // Compare by oid first; queue order within a level is only reported
            // below. Both snapshots are at the same height, so once insertBefore
            // is honored a queue-order difference means the local book diverged.
            for (orders1, orders2) in book1.as_ref().iter().zip(book2.as_ref()) {
                let expected_by_oid: HashMap<_, _> = orders2.iter().map(|o| (o.oid(), o)).collect();
                for order1 in orders1 {
                    match expected_by_oid.get(&order1.oid()) {
                        Some(order2) if *order1 == **order2 => {}
                        Some(order2) => {
                            return Err(format!(
                                "Order {:?} does not match, expected: {:?} received: {:?}",
                                order1.oid(),
                                *order2,
                                order1
                            )
                            .into());
                        }
                        None => {
                            return Err(format!(
                                "Order {:?} present locally but missing from fetched {} snapshot: {:?}",
                                order1.oid(),
                                coin.value(),
                                order1
                            )
                            .into());
                        }
                    }
                }
                if orders1.len() != orders2.len() {
                    return Err(format!(
                        "{} book side size mismatch: local {} orders, fetched {} orders",
                        coin.value(),
                        orders1.len(),
                        orders2.len()
                    )
                    .into());
                }
                if !orders1.iter().map(InnerOrder::oid).eq(orders2.iter().map(InnerOrder::oid)) {
                    misordered.push(coin.value());
                }
            }
        } else if !book1[0].is_empty() || !book1[1].is_empty() {
            return Err(format!("Missing {} book", coin.value()).into());
        }
    }
    if !misordered.is_empty() {
        misordered.sort();
        warn!(
            "[snapshot-queue-order] {} book side(s) hold the same orders in a different queue order: {:?}",
            misordered.len(),
            misordered.iter().take(20).collect::<Vec<_>>()
        );
    }
    // Remaining entries in snapshot_map are "extra" books in the authoritative
    // snapshot — typically newly-listed coins. Return them so the caller can
    // graft them in instead of dying.
    Ok(snapshot_map)
}

impl L2SnapshotParams {
    pub(crate) const fn new(n_sig_figs: Option<u32>, mantissa: Option<u64>) -> Self {
        Self { n_sig_figs, mantissa }
    }
}

/// All L2 variants (full depth, 5 sig figs with mantissa None/2/5, then 4, 3, 2 sig figs) of one book.
/// Clients get at most `MAX_LEVELS` levels, so only that many are stored per
/// variant. Each variant is still derived from the same untruncated parent as
/// before: a coarser bucket spans many finer ones, and re-bucketing an
/// already-bucketed price can differ at a digit boundary (e.g. ask 99999.5).
pub(super) fn compute_coin_l2_snapshots<O: InnerOrder>(order_book: &OrderBook<O>) -> CoinL2Snapshots {
    let max = Some(MAX_LEVELS);
    let params = |n_sig_figs, mantissa| L2SnapshotParams { n_sig_figs, mantissa };
    // Feeding the book's price levels into the bucketer equals bucketing the full-depth L2 snapshot.
    let sf5 = order_book.to_l2_snapshot(None, Some(5), None);
    // Some(2) is NOT a superset of this info!
    let sf5_m5 = sf5.to_l2_snapshot(None, Some(5), Some(5));
    let sf4 = sf5_m5.to_l2_snapshot(None, Some(4), None);
    let sf3 = sf4.to_l2_snapshot(None, Some(3), None);
    HashMap::from([
        (params(None, None), order_book.to_l2_snapshot(max, None, None)),
        (params(Some(5), Some(2)), sf5.to_l2_snapshot(max, Some(5), Some(2))),
        (params(Some(2), None), sf3.to_l2_snapshot(max, Some(2), None)),
        (params(Some(5), None), sf5.truncate(MAX_LEVELS)),
        (params(Some(5), Some(5)), sf5_m5.truncate(MAX_LEVELS)),
        (params(Some(4), None), sf4.truncate(MAX_LEVELS)),
        (params(Some(3), None), sf3.truncate(MAX_LEVELS)),
    ])
}

/// Only the raw variant (`n_sig_figs` and `mantissa` None) of one book, for
/// coins nobody wants aggregated; see `l2_demand`.
pub(super) fn compute_coin_raw_l2_snapshot<O: InnerOrder>(order_book: &OrderBook<O>) -> CoinL2Snapshots {
    HashMap::from([(L2SnapshotParams::new(None, None), order_book.to_l2_snapshot(Some(MAX_LEVELS), None, None))])
}

/// The pre-2026-09-28 `compute_coin_l2_snapshots`: every variant at full depth.
#[cfg(test)]
pub(crate) fn compute_coin_l2_snapshots_full_depth<O: InnerOrder>(order_book: &OrderBook<O>) -> CoinL2Snapshots {
    let mut entries = Vec::new();
    let snapshot = order_book.to_l2_snapshot(None, None, None);
    entries.push((L2SnapshotParams { n_sig_figs: None, mantissa: None }, snapshot));
    let mut add_new_snapshot = |n_sig_figs: Option<u32>, mantissa: Option<u64>, idx: usize| {
        if let Some((_, last_snapshot)) = &entries.get(entries.len() - idx) {
            let snapshot = last_snapshot.to_l2_snapshot(None, n_sig_figs, mantissa);
            entries.push((L2SnapshotParams { n_sig_figs, mantissa }, snapshot));
        }
    };
    for n_sig_figs in (2..=5).rev() {
        if n_sig_figs == 5 {
            for mantissa in [None, Some(2), Some(5)] {
                if mantissa == Some(5) {
                    add_new_snapshot(Some(n_sig_figs), mantissa, 2);
                } else {
                    add_new_snapshot(Some(n_sig_figs), mantissa, 1);
                }
            }
        } else {
            add_new_snapshot(Some(n_sig_figs), None, 1);
        }
    }
    entries.into_iter().collect()
}

/// Full recompute of every book; the reference the incremental cache in
/// `OrderBookState::l2_snapshots` is tested against.
#[cfg(test)]
pub(crate) fn compute_l2_snapshots<O: InnerOrder + Send + Sync>(
    order_books: &crate::order_book::multi_book::OrderBooks<O>,
) -> super::L2Snapshots {
    use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
    use std::sync::Arc;
    super::L2Snapshots(
        order_books
            .as_ref()
            .par_iter()
            .map(|(coin, order_book)| (coin.clone(), Arc::new(compute_coin_l2_snapshots(order_book))))
            .collect(),
    )
}

pub(super) enum EventBatch {
    Orders(Batch<NodeDataOrderStatus>),
    BookDiffs(Batch<NodeDataOrderDiff>),
    Fills(Batch<NodeDataFill>),
}

/// Parses one line of an `event_source` file into (block height, batch). An
/// incomplete line (hl-node still writing it) is an error.
pub(super) fn parse_event_line(event_source: EventSource, line: &str) -> serde_json::Result<(u64, EventBatch)> {
    let start = std::time::Instant::now();
    match event_source {
        EventSource::Fills => {
            serde_json::from_str(line).map(|batch: Batch<NodeDataFill>| (batch.block_number(), EventBatch::Fills(batch)))
        }
        EventSource::OrderStatuses => {
            let res = Batch::from_str_par(line)
                .map(|batch: Batch<NodeDataOrderStatus>| (batch.block_number(), EventBatch::Orders(batch)));
            crate::latency::STATUSES_PARSE_US.record_duration_us(start.elapsed());
            res
        }
        EventSource::OrderDiffs => {
            let res = Batch::from_str_par(line)
                .map(|batch: Batch<NodeDataOrderDiff>| (batch.block_number(), EventBatch::BookDiffs(batch)));
            crate::latency::DIFFS_PARSE_US.record_duration_us(start.elapsed());
            res
        }
    }
}

/// Maximum number of unprocessed Batches a single BatchQueue may hold before
/// the listener treats the backlog as runaway lag and bails. At ~14.5
/// blocks/s this is roughly ~70 minutes of buffered events per stream — more
/// than enough to absorb a snapshot fetch + peer failover but tight enough
/// that we can't silently grow to 23 GB RSS again.
pub(super) const BATCH_QUEUE_CAP: usize = 60_000;

pub(super) struct BatchQueue<T> {
    deque: VecDeque<Batch<T>>,
    last_ts: Option<u64>,
}

impl<T> BatchQueue<T> {
    pub(super) const fn new() -> Self {
        Self { deque: VecDeque::new(), last_ts: None }
    }

    /// Push a batch, returning `Ok(true)` if it was inserted, `Ok(false)` if
    /// it was a stale/duplicate height (silently dropped), or `Err` if the
    /// queue exceeds `BATCH_QUEUE_CAP`. Callers should escalate the Err so
    /// systemd can restart and the lag-watchdog can take over.
    pub(super) fn push(&mut self, block: Batch<T>) -> Result<bool> {
        if let Some(last_ts) = self.last_ts {
            if last_ts >= block.block_number() {
                return Ok(false);
            }
        }
        if self.deque.len() >= BATCH_QUEUE_CAP {
            return Err(format!(
                "BatchQueue overflow: {} unprocessed batches (cap {}). Listener consumer is starved.",
                self.deque.len(),
                BATCH_QUEUE_CAP
            )
            .into());
        }
        self.last_ts = Some(block.block_number());
        self.deque.push_back(block);
        Ok(true)
    }

    pub(super) fn pop_front(&mut self) -> Option<Batch<T>> {
        self.deque.pop_front()
    }

    pub(super) fn front(&self) -> Option<&Batch<T>> {
        self.deque.front()
    }

    pub(super) fn clear(&mut self) {
        self.deque.clear();
        self.last_ts = None;
    }
}
