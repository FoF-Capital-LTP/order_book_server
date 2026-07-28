use crate::{
    listeners::order_book::{L2SnapshotParams, L2Snapshots},
    order_book::{
        Coin, Snapshot,
        multi_book::{OrderBooks, Snapshots},
        types::InnerOrder,
    },
    prelude::*,
    types::{
        inner::InnerLevel,
        node_data::{Batch, NodeDataFill, NodeDataOrderDiff, NodeDataOrderStatus},
    },
};
use log::warn;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
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
pub(super) fn validate_snapshot_consistency<O: Clone + PartialEq + Debug>(
    snapshot: &Snapshots<O>,
    expected: Snapshots<O>,
    ignore_spot: bool,
) -> Result<HashMap<Coin, Snapshot<O>>> {
    let mut snapshot_map: HashMap<_, _> =
        expected.value().into_iter().filter(|(c, _)| !c.is_spot() || !ignore_spot).collect();

    for (coin, book) in snapshot.as_ref() {
        if ignore_spot && coin.is_spot() {
            continue;
        }
        let book1 = book.as_ref();
        if let Some(book2) = snapshot_map.remove(coin) {
            for (orders1, orders2) in book1.as_ref().iter().zip(book2.as_ref()) {
                for (order1, order2) in orders1.iter().zip(orders2.iter()) {
                    if *order1 != *order2 {
                        return Err(
                            format!("Orders do not match, expected: {:?} received: {:?}", *order2, *order1).into()
                        );
                    }
                }
            }
        } else if !book1[0].is_empty() || !book1[1].is_empty() {
            return Err(format!("Missing {} book", coin.value()).into());
        }
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

pub(super) fn compute_l2_snapshots<O: InnerOrder + Send + Sync>(order_books: &OrderBooks<O>) -> L2Snapshots {
    L2Snapshots(
        order_books
            .as_ref()
            .par_iter()
            .map(|(coin, order_book)| {
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
                                // Some(2) is NOT a superset of this info!
                                add_new_snapshot(Some(n_sig_figs), mantissa, 2);
                            } else {
                                add_new_snapshot(Some(n_sig_figs), mantissa, 1);
                            }
                        }
                    } else {
                        add_new_snapshot(Some(n_sig_figs), None, 1);
                    }
                }
                (coin.clone(), entries.into_iter().collect::<HashMap<L2SnapshotParams, Snapshot<InnerLevel>>>())
            })
            .collect(),
    )
}

pub(super) enum EventBatch {
    Orders(Batch<NodeDataOrderStatus>),
    BookDiffs(Batch<NodeDataOrderDiff>),
    Fills(Batch<NodeDataFill>),
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
