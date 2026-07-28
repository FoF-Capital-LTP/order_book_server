use crate::{
    listeners::{directory::DirectoryListener, order_book::state::OrderBookState},
    order_book::{
        Coin, Snapshot,
        multi_book::{Snapshots, load_snapshots_from_json},
    },
    prelude::*,
    types::{
        L4Order,
        inner::{InnerL4Order, InnerLevel},
        node_data::{Batch, EventSource, NodeDataFill, NodeDataOrderDiff, NodeDataOrderStatus},
    },
};
use alloy::primitives::Address;
use fs::File;
use log::{debug, error, info, warn};
use notify::{Event, RecursiveMode, Watcher, recommended_watcher};
use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet, VecDeque},
    io::{BufRead, BufReader, Seek, SeekFrom},
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering as AtomicOrdering},
    },
    time::Duration,
};
use tokio::{
    sync::{
        Mutex,
        broadcast::Sender,
        mpsc::{UnboundedSender, channel, unbounded_channel},
    },
    time::{Instant, interval_at, sleep},
};

/// Maximum fs events buffered between the notify watcher and the listener
/// loop. The watcher fires three streams (status/diffs/fills) at roughly
/// 14.5 blocks/s ⇒ ~45 events/s steady state, so 10k caps ~3.5 minutes of
/// in-flight events. If the listener falls farther behind than that, we
/// prefer to drop+fatal so systemd can restart cleanly rather than let
/// memory and the channel grow without bound.
const FS_EVENT_CHANNEL_CAP: usize = 10_000;

/// Max permitted block_time vs wall-clock lag before the lag watchdog fires
/// a fatal. Tuned to be generous enough to absorb a snapshot fetch +
/// peer failover (~30-60s) but tight enough to catch the runaway case
/// where the consumer falls so far behind that an apply_updates panic is
/// imminent.
const MAX_BLOCK_TIME_LAG_MS: i64 = 120_000;

/// Grace period (seconds) after `init_from_snapshot` during which the lag
/// watchdog is suppressed. After a cold start, the consumer must process
/// a backlog of blocks from the hourly file (which may span several minutes
/// of chain time). During this window, block_time naturally lags wall-clock.
/// 300s covers the worst observed case (183s after hl-node auto-update
/// restart on 2026-06-10, where the grace of 180s expired 3s too early).
const LAG_GRACE_AFTER_INIT_SECS: u64 = 300;

/// Plan E: how many times in a row the same byte offset may fail to parse
/// while still emitting an `ERROR` log on each attempt. The first few
/// failures are normal torn-write windows (microseconds-to-tens-of-ms while
/// hl-node finishes flushing the line); they should be visible. Beyond
/// this count we stay silent and rate-limit a periodic WARN so the journal
/// does not fill up and (more importantly) so the per-event log/work is
/// small enough not to back up the bounded fs_event channel.
const PARSE_FAIL_LOUD_RETRIES: u32 = 3;

/// Plan E: minimum interval between rate-limited "still stuck" WARN lines
/// once a single offset has failed beyond `PARSE_FAIL_LOUD_RETRIES`.
const PARSE_FAIL_WARN_INTERVAL: Duration = Duration::from_secs(60);

/// How often the lag watchdog runs. Independent of fs activity.
const WATCHDOG_INTERVAL_SECS: u64 = 15;
use utils::{BatchQueue, EventBatch, process_rmp_file, validate_snapshot_consistency};

mod state;
mod utils;

// WARNING - this code assumes no other file system operations are occurring in the watched directories
// if there are scripts running, this may not work as intended
pub(crate) async fn hl_listen(listener: Arc<Mutex<OrderBookListener>>, dir: PathBuf) -> Result<()> {
    let order_statuses_dir = EventSource::OrderStatuses.event_source_dir(&dir).canonicalize()?;
    let fills_dir = EventSource::Fills.event_source_dir(&dir).canonicalize()?;
    let order_diffs_dir = EventSource::OrderDiffs.event_source_dir(&dir).canonicalize()?;
    info!("Monitoring order status directory: {}", order_statuses_dir.display());
    info!("Monitoring order diffs directory: {}", order_diffs_dir.display());
    info!("Monitoring fills directory: {}", fills_dir.display());

    // Bounded channel between notify watcher and listener loop. We use
    // try_send from the notify thread so that if we ever go full, we get
    // an error log + a clearly-attributable fatal (overflow_flag + 30s/60s
    // watchdog) rather than silently growing memory like the previous
    // unbounded version did during the May 2026 lag-storm incident.
    let (fs_event_tx, mut fs_event_rx) = channel(FS_EVENT_CHANNEL_CAP);
    let fs_event_overflow = Arc::new(AtomicBool::new(false));
    let overflow_for_watcher = fs_event_overflow.clone();
    let mut watcher = recommended_watcher(move |res| {
        let fs_event_tx = fs_event_tx.clone();
        match fs_event_tx.try_send(res) {
            Ok(()) => {}
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                overflow_for_watcher.store(true, std::sync::atomic::Ordering::SeqCst);
                error!(
                    "fs event channel FULL (cap {FS_EVENT_CHANNEL_CAP}); dropping event. Listener is starved — fatal."
                );
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                error!("fs event channel closed; watcher exiting");
            }
        }
    })?;

    let ignore_spot = {
        let listener = listener.lock().await;
        listener.ignore_spot
    };

    // every so often, we fetch a new snapshot and the snapshot_fetch_task starts running.
    // Result is sent back along this channel (if error, we want to return to top level)
    let (snapshot_fetch_task_tx, mut snapshot_fetch_task_rx) = unbounded_channel::<Result<()>>();

    watcher.watch(&order_statuses_dir, RecursiveMode::Recursive)?;
    watcher.watch(&fills_dir, RecursiveMode::Recursive)?;
    watcher.watch(&order_diffs_dir, RecursiveMode::Recursive)?;
    let start = Instant::now() + Duration::from_secs(5);
    let mut ticker = interval_at(start, Duration::from_secs(60));
    // Guard against concurrent snapshot fetches. The ticker fires every 60s
    // unconditionally, but if a previous fetch_snapshot task is still running
    // (HTTP + file parse + validation can exceed 60s on a busy book), we skip
    // the tick. Without this, overlapping fetches call begin_caching() which
    // resets the cache and causes "Not enough cached updates" → fatal cascade.
    let snapshot_in_flight = Arc::new(AtomicBool::new(false));
    // Independent periodic ticker for the lag watchdog. Uses interval_at so
    // we don't share fate with `sleep` (which gets reset by every fs event).
    let lag_start = Instant::now() + Duration::from_secs(WATCHDOG_INTERVAL_SECS);
    let mut lag_ticker = interval_at(lag_start, Duration::from_secs(WATCHDOG_INTERVAL_SECS));
    loop {
        tokio::select! {
            event = fs_event_rx.recv() =>  match event {
                Some(Ok(event)) => {
                    if event.kind.is_create() || event.kind.is_modify() {
                        let new_path = &event.paths[0];
                        if new_path.starts_with(&order_statuses_dir) && new_path.is_file() {
                            listener
                                .lock()
                                .await
                                .process_update(&event, new_path, EventSource::OrderStatuses)
                                .map_err(|err| format!("Order status processing error: {err}"))?;
                        } else if new_path.starts_with(&fills_dir) && new_path.is_file() {
                            listener
                                .lock()
                                .await
                                .process_update(&event, new_path, EventSource::Fills)
                                .map_err(|err| format!("Fill update processing error: {err}"))?;
                        } else if new_path.starts_with(&order_diffs_dir) && new_path.is_file() {
                            listener
                                .lock()
                                .await
                                .process_update(&event, new_path, EventSource::OrderDiffs)
                                .map_err(|err| format!("Book diff processing error: {err}"))?;
                        }
                    }
                }
                Some(Err(err)) => {
                    error!("Watcher error: {err}");
                    return Err(format!("Watcher error: {err}").into());
                }
                None => {
                    error!("Channel closed. Listener exiting");
                    return Err("Channel closed.".into());
                }
            },
            snapshot_fetch_res = snapshot_fetch_task_rx.recv() => {
                match snapshot_fetch_res {
                    None => {
                        return Err("Snapshot fetch task sender dropped".into());
                    }
                    Some(Err(err)) => {
                        return Err(format!("Abci state reading error: {err}").into());
                    }
                    Some(Ok(())) => {}
                }
            }
            _ = ticker.tick() => {
                if !snapshot_in_flight.load(AtomicOrdering::SeqCst) {
                    snapshot_in_flight.store(true, AtomicOrdering::SeqCst);
                    let listener = listener.clone();
                    let snapshot_fetch_task_tx = snapshot_fetch_task_tx.clone();
                    let in_flight = snapshot_in_flight.clone();
                    fetch_snapshot(dir.clone(), listener, snapshot_fetch_task_tx, ignore_spot, in_flight);
                }
            }
            // 30s rather than 5s: hl-visor occasionally swaps upstream peers
            // (early-eof from peer, bootstrap, reconnect) which produces a
            // 5-15s gap with no new blocks and therefore no fs events. A 5s
            // threshold treats those routine failovers as fatals; 30s tolerates
            // them while still catching genuine "watcher went deaf" cases.
            //
            // We also check the fs_event_overflow flag here (set by the
            // bounded watcher channel when try_send fails). If we ever drop a
            // watcher event we cannot trust local state, so we exit and let
            // systemd cold-restart the listener.
            () = sleep(Duration::from_secs(30)) => {
                if fs_event_overflow.load(std::sync::atomic::Ordering::SeqCst) {
                    return Err("fs event channel overflowed — listener consumer was starved".into());
                }
                let listener = listener.lock().await;
                if listener.is_ready() {
                    return Err("No file events for 30s — watcher may have stopped or hl-node fell badly behind".into());
                }
            }
            // Lag watchdog: fires every WATCHDOG_INTERVAL_SECS regardless of
            // fs activity. Catches the "events still arriving but consumer
            // can't keep up" case — the 30s no-events branch above only
            // catches a *dead* watcher and silently lets a backed-up watcher
            // accumulate until it crashes at apply_updates with
            // "Expecting block X got Y". This was the root cause of all the
            // daily 01:00 UTC fatals.
            _ = lag_ticker.tick() => {
                // fs_event_overflow is independent of fetch_snapshot timing
                // (it signals inotify-channel saturation, which a slow
                // snapshot validation does not cause), so check it
                // unconditionally.
                if fs_event_overflow.load(std::sync::atomic::Ordering::SeqCst) {
                    return Err("fs event channel overflowed — listener consumer was starved".into());
                }
                let listener = listener.lock().await;
                // Skip the wall-clock block_time lag check while a snapshot
                // fetch is in flight. We use the AtomicBool `snapshot_in_flight`
                // rather than `is_fetching_snapshot()` because the AtomicBool
                // covers the FULL lifecycle including validate_snapshot_consistency
                // (which runs after the cache is taken, outside the mutex, and
                // can exceed 120s on a busy book). The previous is_fetching_snapshot()
                // check was too narrow — it cleared as soon as take_cache() ran,
                // leaving validation unprotected.
                //
                // Also skip when an active torn-write stall is detected
                // (Plan E tracker shows >30s stuck at the same offset).
                // The consumer isn't starved — it's waiting for hl-node to
                // flush an incomplete line. Once flushed, lag recovers
                // instantly. Without this, a torn-write lasting >120s
                // triggers a needless fatal+restart cycle.
                if listener.is_ready()
                    && !snapshot_in_flight.load(AtomicOrdering::SeqCst)
                    && !listener.has_active_parse_stall()
                    && !listener.in_init_grace_period()
                {
                    if let Some(lag_ms) = listener.block_time_lag_ms() {
                        if lag_ms > MAX_BLOCK_TIME_LAG_MS {
                            return Err(format!(
                                "Listener block_time lag {lag_ms} ms exceeds {MAX_BLOCK_TIME_LAG_MS} ms — consumer is starved"
                            ).into());
                        }
                    }
                }
            }
        }
    }
}

fn fetch_snapshot(
    dir: PathBuf,
    listener: Arc<Mutex<OrderBookListener>>,
    tx: UnboundedSender<Result<()>>,
    ignore_spot: bool,
    in_flight: Arc<AtomicBool>,
) {
    let tx = tx.clone();
    tokio::spawn(async move {
        // Inner block so every exit path — including the early `return`s below —
        // falls through to the `in_flight` reset. Returning straight out of the
        // task would leave the flag stuck at `true`, permanently disabling the
        // 60s snapshot ticker and starving the listener until BatchQueue overflows.
        let res = async {
            match process_rmp_file(&dir).await {
            Ok(output_fln) => {
                let state = {
                    let mut listener = listener.lock().await;
                    listener.begin_caching();
                    listener.clone_state()
                };
                let snapshot = load_snapshots_from_json::<InnerL4Order, (Address, L4Order)>(&output_fln).await;
                info!("Snapshot fetched");
                // sleep to let some updates build up.
                sleep(Duration::from_secs(1)).await;
                let mut cache = {
                    let mut listener = listener.lock().await;
                    listener.take_cache()
                };
                info!("Cache has {} elements", cache.len());
                match snapshot {
                    Ok((height, expected_snapshot)) => {
                        if let Some(mut state) = state {
                            let mut catch_up_failed = false;
                            while state.height() < height {
                                if let Some((order_statuses, order_diffs)) = cache.pop_front() {
                                    if let Err(err) = state.apply_updates(order_statuses, order_diffs) {
                                        // Gap or other error during validation
                                        // catch-up — the main loop already
                                        // handled this (e.g. gap-grace-resync
                                        // invalidated state). Abandon this
                                        // validation pass; next tick will
                                        // re-fetch cleanly.
                                        warn!(
                                            "[snapshot-catchup] apply_updates failed during validation catch-up (non-fatal): {err}"
                                        );
                                        catch_up_failed = true;
                                        break;
                                    }
                                } else {
                                    // Not enough cached updates to reach snapshot
                                    // height. This is transient (snapshot is newer
                                    // than our cache). Skip validation this round.
                                    warn!(
                                        "[snapshot-catchup] not enough cached updates (state.height={}, snapshot height={height}); skipping validation",
                                        state.height()
                                    );
                                    catch_up_failed = true;
                                    break;
                                }
                            }
                            if catch_up_failed {
                                return Ok::<(), Error>(());
                            }
                            if state.height() > height {
                                // Fetched snapshot is older than local state. Skip
                                // validation — next tick will fetch a fresher one.
                                warn!(
                                    "[snapshot-catchup] fetched snapshot height ({height}) lagging stored state ({}); skipping validation",
                                    state.height()
                                );
                                return Ok::<(), Error>(());
                            }
                            let stored_snapshot = state.compute_snapshot().snapshot;
                            info!("Validating snapshot");
                            match validate_snapshot_consistency(&stored_snapshot, expected_snapshot, ignore_spot) {
                                Ok(extras) if extras.is_empty() => Ok(()),
                                Ok(extras) => {
                                    // Newly-listed (or previously-ignored) coins appeared in the
                                    // authoritative snapshot but not in our local state. Graft them
                                    // in so the listener does not have to restart.
                                    let coins: Vec<_> =
                                        extras.keys().map(|c| c.value().to_string()).collect();
                                    warn!(
                                        "Absorbing {} extra orderbook(s) from fetched snapshot: {:?}",
                                        extras.len(),
                                        coins
                                    );
                                    let mut listener = listener.lock().await;
                                    if let Some(state) = listener.order_book_state.as_mut() {
                                        state.absorb_extra_books(extras, true);
                                    }
                                    Ok(())
                                }
                                Err(err) => {
                                    // Validation mismatch is a timing race: between
                                    // the snapshot fetch and the comparison, orders
                                    // at the same price level get replaced by
                                    // different orders. The local state (built from
                                    // the authoritative diff stream) is correct;
                                    // the fetched snapshot simply aged. Log and
                                    // continue rather than crashing the process.
                                    warn!(
                                        "[snapshot-validation-race] mismatch during consistency check (non-fatal): {err}"
                                    );
                                    Ok(())
                                }
                            }
                        } else {
                            listener.lock().await.init_from_snapshot(expected_snapshot, height);
                            Ok(())
                        }
                    }
                    Err(err) => Err(err),
                }
            }
            Err(err) => Err(err),
        }
        }
        .await;
        in_flight.store(false, AtomicOrdering::SeqCst);
        let _unused = tx.send(res);
        Ok::<(), Error>(())
    });
}

pub(crate) struct OrderBookListener {
    ignore_spot: bool,
    fill_status_file: Option<File>,
    order_status_file: Option<File>,
    order_diff_file: Option<File>,
    // None if we haven't seen a valid snapshot yet
    order_book_state: Option<OrderBookState>,
    last_fill: Option<u64>,
    order_diff_cache: BatchQueue<NodeDataOrderDiff>,
    order_status_cache: BatchQueue<NodeDataOrderStatus>,
    // Only Some when we want it to collect updates
    fetched_snapshot_cache: Option<VecDeque<(Batch<NodeDataOrderStatus>, Batch<NodeDataOrderDiff>)>>,
    internal_message_tx: Option<Sender<Arc<InternalMessage>>>,
    /// Most recent block_time we've observed in any incoming batch (ms since
    /// epoch). Drives the lag watchdog so we can detect "events still
    /// arriving but consumer can't keep up" without waiting for an
    /// apply_updates fatal.
    last_block_time_ms: Option<u64>,
    /// Wall-clock instant at which the listener became ready (first
    /// successful `init_from_snapshot`). The lag watchdog suppresses
    /// block_time checks for `LAG_GRACE_AFTER_INIT` seconds after this
    /// point, giving the consumer time to catch up through the backlog
    /// of blocks that accumulated in the hourly file before the snapshot
    /// was taken.
    ready_at: Option<Instant>,
    /// Diagnostic-only: when Some(path), the next successfully parsed batch
    /// from that source logs its block_number under `[hour-rollover-diag]`.
    /// Set by `on_file_creation` when opening a new hourly file, consumed by
    /// `process_data` on first successful parse. No control-flow effect.
    pending_first_batch_log_fills: Option<PathBuf>,
    pending_first_batch_log_order_statuses: Option<PathBuf>,
    pending_first_batch_log_order_diffs: Option<PathBuf>,
    /// Plan E: per-source tracker for repeated parse failures at the same
    /// byte offset. The seek-rewind-break path used to re-`error!` on every
    /// fs modify event for the same stuck line, which back-pressured the
    /// bounded fs_event channel hard enough to trip the
    /// `fs event channel overflowed` fatal (observed 2026-06-01 15:14:29Z).
    /// We now silence the log after `PARSE_FAIL_LOUD_RETRIES` and emit a
    /// rate-limited WARN every `PARSE_FAIL_WARN_INTERVAL`.
    parse_fail_fills: ParseFailureTracker,
    parse_fail_order_statuses: ParseFailureTracker,
    parse_fail_order_diffs: ParseFailureTracker,
}

/// Plan E: tracks "stuck on the same byte offset" state for a single
/// `EventSource`. `Default::default()` is the cleared (no failures pending)
/// state.
#[derive(Default)]
struct ParseFailureTracker {
    /// Absolute byte offset within the currently-tracked file at which the
    /// last unsuccessful parse occurred. None when the previous parse on
    /// this source succeeded (or no parse has happened yet).
    last_fail_offset: Option<u64>,
    /// Number of consecutive failures at `last_fail_offset`. Reset to 0 on
    /// any successful parse OR when the offset changes.
    fail_count: u32,
    /// Wall-clock when the current run of failures began. Used by the
    /// rate-limited WARN to report cumulative stuck duration.
    first_fail_at: Option<Instant>,
    /// Last time we emitted a rate-limited WARN for this stuck offset.
    /// None until the first WARN fires.
    last_warn_at: Option<Instant>,
}

impl ParseFailureTracker {
    /// Reset the tracker — a parse just succeeded at (or past) this source's
    /// previous stuck offset, so any prior failure is no longer pending.
    fn clear(&mut self) {
        *self = Self::default();
    }
}

impl OrderBookListener {
    pub(crate) fn new(internal_message_tx: Option<Sender<Arc<InternalMessage>>>, ignore_spot: bool) -> Self {
        Self {
            ignore_spot,
            fill_status_file: None,
            order_status_file: None,
            order_diff_file: None,
            order_book_state: None,
            last_fill: None,
            fetched_snapshot_cache: None,
            internal_message_tx,
            order_diff_cache: BatchQueue::new(),
            order_status_cache: BatchQueue::new(),
            last_block_time_ms: None,
            ready_at: None,
            pending_first_batch_log_fills: None,
            pending_first_batch_log_order_statuses: None,
            pending_first_batch_log_order_diffs: None,
            parse_fail_fills: ParseFailureTracker::default(),
            parse_fail_order_statuses: ParseFailureTracker::default(),
            parse_fail_order_diffs: ParseFailureTracker::default(),
        }
    }

    /// Plan E: borrow the per-source parse-failure tracker.
    fn parse_fail_tracker_mut(&mut self, event_source: EventSource) -> &mut ParseFailureTracker {
        match event_source {
            EventSource::Fills => &mut self.parse_fail_fills,
            EventSource::OrderStatuses => &mut self.parse_fail_order_statuses,
            EventSource::OrderDiffs => &mut self.parse_fail_order_diffs,
        }
    }

    /// Diagnostic helper: take the pending-first-batch-log marker for a source.
    fn take_pending_first_batch_log(&mut self, event_source: EventSource) -> Option<PathBuf> {
        match event_source {
            EventSource::Fills => self.pending_first_batch_log_fills.take(),
            EventSource::OrderStatuses => self.pending_first_batch_log_order_statuses.take(),
            EventSource::OrderDiffs => self.pending_first_batch_log_order_diffs.take(),
        }
    }

    /// Diagnostic helper: arm the pending-first-batch-log marker for a source.
    fn set_pending_first_batch_log(&mut self, event_source: EventSource, path: PathBuf) {
        match event_source {
            EventSource::Fills => self.pending_first_batch_log_fills = Some(path),
            EventSource::OrderStatuses => self.pending_first_batch_log_order_statuses = Some(path),
            EventSource::OrderDiffs => self.pending_first_batch_log_order_diffs = Some(path),
        }
    }

    /// Returns wall-clock-vs-block_time lag in ms. None if no batch seen yet
    /// or if block_time is in the future (clock skew). Used by the lag
    /// watchdog.
    fn block_time_lag_ms(&self) -> Option<i64> {
        let last = self.last_block_time_ms?;
        let now_ms: i64 = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .ok()?
            .as_millis()
            .try_into()
            .ok()?;
        let last_i64: i64 = last.try_into().ok()?;
        Some(now_ms.saturating_sub(last_i64))
    }

    /// Returns true if the listener is still within the post-init grace
    /// period. During this window, the consumer is catching up through the
    /// hourly file backlog and block_time will naturally lag wall-clock.
    fn in_init_grace_period(&self) -> bool {
        self.ready_at.map_or(false, |at| {
            at.elapsed() < Duration::from_secs(LAG_GRACE_AFTER_INIT_SECS)
        })
    }

    /// Returns true if any source has an active torn-write stall that has
    /// lasted more than 30 seconds. Used by the lag watchdog to suppress
    /// false-positive fatals during torn-write windows: the consumer is not
    /// truly "starved" — it's blocked on an incomplete line that hl-node
    /// hasn't flushed yet. Once the line completes, parsing resumes and the
    /// lag drops instantly.
    fn has_active_parse_stall(&self) -> bool {
        const STALL_GRACE: Duration = Duration::from_secs(30);
        let now = Instant::now();
        [&self.parse_fail_fills, &self.parse_fail_order_statuses, &self.parse_fail_order_diffs]
            .into_iter()
            .any(|t| {
                t.fail_count > PARSE_FAIL_LOUD_RETRIES
                    && t.first_fail_at
                        .map_or(false, |start| now.duration_since(start) > STALL_GRACE)
            })
    }

    fn clone_state(&self) -> Option<OrderBookState> {
        self.order_book_state.clone()
    }

    pub(crate) const fn is_ready(&self) -> bool {
        self.order_book_state.is_some()
    }

    pub(crate) fn universe(&self) -> HashSet<Coin> {
        self.order_book_state.as_ref().map_or_else(HashSet::new, OrderBookState::compute_universe)
    }

    #[allow(clippy::type_complexity)]
    // pops earliest pair of cached updates that have the same timestamp if possible
    fn pop_cache(&mut self) -> Option<(Batch<NodeDataOrderStatus>, Batch<NodeDataOrderDiff>)> {
        // synchronize to same block
        while let Some(t) = self.order_diff_cache.front() {
            if let Some(s) = self.order_status_cache.front() {
                match t.block_number().cmp(&s.block_number()) {
                    Ordering::Less => {
                        self.order_diff_cache.pop_front();
                    }
                    Ordering::Equal => {
                        return self
                            .order_status_cache
                            .pop_front()
                            .and_then(|t| self.order_diff_cache.pop_front().map(|s| (t, s)));
                    }
                    Ordering::Greater => {
                        self.order_status_cache.pop_front();
                    }
                }
            } else {
                break;
            }
        }
        None
    }

    fn receive_batch(&mut self, updates: EventBatch) -> Result<()> {
        match updates {
            EventBatch::Orders(batch) => {
                self.last_block_time_ms = Some(self.last_block_time_ms.map_or(batch.block_time(), |prev| prev.max(batch.block_time())));
                self.order_status_cache.push(batch)?;
            }
            EventBatch::BookDiffs(batch) => {
                self.last_block_time_ms = Some(self.last_block_time_ms.map_or(batch.block_time(), |prev| prev.max(batch.block_time())));
                self.order_diff_cache.push(batch)?;
            }
            EventBatch::Fills(batch) => {
                self.last_block_time_ms = Some(self.last_block_time_ms.map_or(batch.block_time(), |prev| prev.max(batch.block_time())));
                if self.last_fill.is_none_or(|height| height < batch.block_number()) {
                    // send fill updates if we received a new update
                    if let Some(tx) = &self.internal_message_tx {
                        let tx = tx.clone();
                        tokio::spawn(async move {
                            let snapshot = Arc::new(InternalMessage::Fills { batch });
                            let _unused = tx.send(snapshot);
                        });
                    }
                }
            }
        }
        if self.is_ready() {
            if let Some((order_statuses, order_diffs)) = self.pop_cache() {
                self.order_book_state
                    .as_mut()
                    .map(|book| book.apply_updates(order_statuses.clone(), order_diffs.clone()))
                    .transpose()?;
                if let Some(cache) = &mut self.fetched_snapshot_cache {
                    cache.push_back((order_statuses.clone(), order_diffs.clone()));
                }
                if let Some(tx) = &self.internal_message_tx {
                    let tx = tx.clone();
                    tokio::spawn(async move {
                        let updates = Arc::new(InternalMessage::L4BookUpdates {
                            diff_batch: order_diffs,
                            status_batch: order_statuses,
                        });
                        let _unused = tx.send(updates);
                    });
                }
            }
        }
        Ok(())
    }

    fn begin_caching(&mut self) {
        self.fetched_snapshot_cache = Some(VecDeque::new());
    }

    // tkae the cached updates and stop collecting updates
    fn take_cache(&mut self) -> VecDeque<(Batch<NodeDataOrderStatus>, Batch<NodeDataOrderDiff>)> {
        self.fetched_snapshot_cache.take().unwrap_or_default()
    }

    fn init_from_snapshot(&mut self, snapshot: Snapshots<InnerL4Order>, height: u64) {
        info!("No existing snapshot");
        let mut new_order_book = OrderBookState::from_snapshot(snapshot, height, 0, true, self.ignore_spot);
        let mut retry = false;
        while let Some((order_statuses, order_diffs)) = self.pop_cache() {
            if new_order_book.apply_updates(order_statuses, order_diffs).is_err() {
                info!(
                    "Failed to apply updates to this book (likely missing older updates). Waiting for next snapshot."
                );
                retry = true;
                break;
            }
        }
        if !retry {
            self.order_book_state = Some(new_order_book);
            // Seed last_block_time_ms to wall-clock now. The snapshot gives us
            // authoritative state at this moment — there is no "real" lag yet.
            // Without this, the first parsed batch (which may have an old
            // block_time from before the restart gap) would set last_block_time_ms
            // to a stale value, causing the lag watchdog to immediately fire.
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);
            self.last_block_time_ms = Some(now_ms);
            // Record when we became ready. The lag watchdog grants a grace
            // period after init so the consumer can catch up through the
            // backlog without being killed.
            self.ready_at = Some(Instant::now());
            info!("Order book ready");
        }
    }

    // forcibly grab current snapshot
    pub(crate) fn compute_snapshot(&mut self) -> Option<TimedSnapshots> {
        self.order_book_state.as_mut().map(|o| o.compute_snapshot())
    }

    // prevent snapshotting mutiple times at the same height
    fn l2_snapshots(&mut self, prevent_future_snaps: bool) -> Option<(u64, L2Snapshots)> {
        self.order_book_state.as_mut().and_then(|o| o.l2_snapshots(prevent_future_snaps))
    }
}

impl OrderBookListener {
    fn process_update(&mut self, event: &Event, new_path: &PathBuf, event_source: EventSource) -> Result<()> {
        if event.kind.is_create() {
            info!("-- Event: {} created --", new_path.display());
            self.on_file_creation(new_path.clone(), event_source)?;
        }
        // Check for `Modify` event (only if the file is already initialized)
        else {
            // If we are not tracking anything right now, we treat a file update as declaring that it has been created.
            // Unfortunately, we miss the update that occurs at this time step.
            // We go to the end of the file to read for updates after that.
            if self.is_reading(event_source) {
                self.on_file_modification(event_source)?;
            } else {
                info!("-- Event: {} modified, tracking it now --", new_path.display());
                let file = self.file_mut(event_source);
                let mut new_file = File::open(new_path)?;
                new_file.seek(SeekFrom::End(0))?;
                *file = Some(new_file);
            }
        }
        Ok(())
    }
}

impl DirectoryListener for OrderBookListener {
    fn is_reading(&self, event_source: EventSource) -> bool {
        match event_source {
            EventSource::Fills => self.fill_status_file.is_some(),
            EventSource::OrderStatuses => self.order_status_file.is_some(),
            EventSource::OrderDiffs => self.order_diff_file.is_some(),
        }
    }

    fn file_mut(&mut self, event_source: EventSource) -> &mut Option<File> {
        match event_source {
            EventSource::Fills => &mut self.fill_status_file,
            EventSource::OrderStatuses => &mut self.order_status_file,
            EventSource::OrderDiffs => &mut self.order_diff_file,
        }
    }

    fn on_file_creation(&mut self, new_file: PathBuf, event_source: EventSource) -> Result<()> {
        // Drain whatever is left in the previous-hour file *line by line*
        // rather than buffering the whole thing into a String. Hour-rollover
        // files routinely reach 5–25 GB; the previous read_to_string blew
        // up RSS to 23 GB and held the listener mutex for many seconds,
        // which was the trigger for the daily 01:00 UTC fatal cascade.
        //
        // The previous file has already been closed by hl-node (rotation
        // already happened), so there is no risk of an EOF-mid-line — we
        // can stream until BufRead returns 0.
        let height_at_entry = self.order_book_state.as_ref().map(OrderBookState::height);
        let had_prev_file = self.file_mut(event_source).is_some();
        info!(
            "[hour-rollover-diag] on_file_creation enter: source={event_source} new_file={} state.height={:?} had_prev_file={had_prev_file}",
            new_file.display(),
            height_at_entry,
        );
        if had_prev_file {
            #[allow(clippy::unwrap_used)]
            let file = self.file_mut(event_source).take().unwrap();
            self.stream_lines(file, event_source)?;
        }
        *self.file_mut(event_source) = Some(File::open(&new_file)?);
        // Mark next successful parse on this source as the new-file first batch.
        self.set_pending_first_batch_log(event_source, new_file.clone());
        let height_at_exit = self.order_book_state.as_ref().map(OrderBookState::height);
        info!(
            "[hour-rollover-diag] on_file_creation exit: source={event_source} new_file={} state.height={:?}",
            new_file.display(),
            height_at_exit,
        );
        Ok(())
    }

    fn process_data(&mut self, data: String, event_source: EventSource) -> Result<()> {
        let total_len = data.len();
        let lines = data.lines();
        for line in lines {
            if line.is_empty() {
                continue;
            }
            let res = match event_source {
                EventSource::Fills => serde_json::from_str::<Batch<NodeDataFill>>(line).map(|batch| {
                    let height = batch.block_number();
                    (height, EventBatch::Fills(batch))
                }),
                EventSource::OrderStatuses => serde_json::from_str(line)
                    .map(|batch: Batch<NodeDataOrderStatus>| (batch.block_number(), EventBatch::Orders(batch))),
                EventSource::OrderDiffs => serde_json::from_str(line)
                    .map(|batch: Batch<NodeDataOrderDiff>| (batch.block_number(), EventBatch::BookDiffs(batch))),
            };
            let (height, event_batch) = match res {
                Ok(data) => data,
                Err(err) => {
                    // If we run into a serialization error (hitting EOF), just return to last line.
                    //
                    // Plan E: the seek-rewind-break path used to `error!` on every fs modify
                    // event for the same stuck offset. A real torn-write at 2026-06-01 15:09:44Z
                    // produced ~150 retries × 3 sources in 5 minutes; the resulting log/work
                    // back-pressured the bounded fs_event channel and tripped the
                    // `fs event channel overflowed` fatal at 15:14:29Z. We now dedup by
                    // post-rewind file offset: keep the first few attempts loud (covers normal
                    // torn-write windows), then go silent with a rate-limited WARN.
                    let line_start_offset = line.as_ptr() as usize - data.as_ptr() as usize;
                    let bytes_to_rewind = total_len - line_start_offset;
                    #[allow(clippy::unwrap_used)]
                    let rewind_len: i64 = bytes_to_rewind.try_into().unwrap();
                    let post_rewind_offset = self
                        .file_mut(event_source)
                        .as_mut()
                        .and_then(|f| {
                            f.seek_relative(-rewind_len).ok()?;
                            f.stream_position().ok()
                        });
                    let now = Instant::now();
                    let height_for_log = self.order_book_state.as_ref().map(OrderBookState::height);
                    let line_excerpt: &str = &line[..line.len().min(100)];
                    let tracker = self.parse_fail_tracker_mut(event_source);
                    let same_offset_as_last = post_rewind_offset.is_some()
                        && tracker.last_fail_offset == post_rewind_offset;
                    if same_offset_as_last {
                        tracker.fail_count = tracker.fail_count.saturating_add(1);
                    } else {
                        tracker.last_fail_offset = post_rewind_offset;
                        tracker.fail_count = 1;
                        tracker.first_fail_at = Some(now);
                        tracker.last_warn_at = None;
                    }
                    if tracker.fail_count <= PARSE_FAIL_LOUD_RETRIES {
                        error!(
                            "{event_source} serialization error {err}, height: {height_for_log:?}, line: {line_excerpt:?}",
                        );
                    } else {
                        let should_warn = tracker
                            .last_warn_at
                            .map_or(true, |t| now.duration_since(t) >= PARSE_FAIL_WARN_INTERVAL);
                        if should_warn {
                            let stuck_for = tracker
                                .first_fail_at
                                .map(|t| now.duration_since(t))
                                .unwrap_or_default();
                            warn!(
                                "[plan-e-dedup] {event_source} still stuck at offset {:?} \
                                 after {} attempts ({}s); height: {height_for_log:?}, line: {line_excerpt:?}, \
                                 last err: {err}",
                                tracker.last_fail_offset,
                                tracker.fail_count,
                                stuck_for.as_secs(),
                            );
                            tracker.last_warn_at = Some(now);
                        }
                    }
                    break;
                }
            };
            // Plan E: a parse just succeeded — any prior stuck-offset state for this source
            // is no longer pending (either the torn-write was filled in, or hl-node skipped
            // past it on rotation). Clear the tracker so the next genuine failure is loud.
            self.parse_fail_tracker_mut(event_source).clear();
            if height % 1000 == 0 {
                // Demoted from info! and rate from /100 to /1000 blocks: at
                // ~14.5 blocks/s the old cadence produced three INFO lines
                // every ~7 s per stream (Fills+OrderStatuses+OrderDiffs).
                // /1000 ≈ once per stream per ~70 s, and debug! keeps it
                // out of the journal under RUST_LOG=warn,server=info.
                debug!("{event_source} block: {height}");
            }
            if let Some(path) = self.take_pending_first_batch_log(event_source) {
                info!(
                    "[hour-rollover-diag] live-file first batch: source={event_source} new_file={} first_block={height} state.height={:?}",
                    path.display(),
                    self.order_book_state.as_ref().map(OrderBookState::height),
                );
            }
            if let Err(err) = self.receive_batch(event_batch) {
                if err.to_string().contains("[gap-grace-resync]") {
                    // Gap detected on fresh start — state is now invalid because
                    // we missed blocks containing New order diffs. Invalidate
                    // state so the next periodic snapshot fetch re-initializes
                    // cleanly. This is NOT a fatal — the system self-heals
                    // within 60s (the snapshot fetch interval).
                    warn!("[gap-grace-resync] invalidating state; will re-init on next snapshot fetch");
                    self.order_book_state = None;
                    self.last_block_time_ms = None;
                    self.ready_at = None;
                    // Clear batch queues to prevent BatchQueue overflow while
                    // waiting for the next snapshot fetch to re-initialize state.
                    self.order_status_cache.clear();
                    self.order_diff_cache.clear();
                    return Ok(());
                }
                self.order_book_state = None;
                return Err(err);
            }
        }
        let snapshot = self.l2_snapshots(true);
        if let Some(snapshot) = snapshot {
            if let Some(tx) = &self.internal_message_tx {
                let tx = tx.clone();
                tokio::spawn(async move {
                    let snapshot = Arc::new(InternalMessage::Snapshot { l2_snapshots: snapshot.1, time: snapshot.0 });
                    let _unused = tx.send(snapshot);
                });
            }
        }
        Ok(())
    }
}

impl OrderBookListener {
    /// Streaming variant of process_data used on hour-rollover for the
    /// already-closed previous-hour file. Reads one line at a time via a
    /// 1 MiB BufReader instead of slurping the whole file into a String.
    /// Bounds RSS regardless of hourly file size.
    fn stream_lines(&mut self, file: File, event_source: EventSource) -> Result<()> {
        let height_at_entry = self.order_book_state.as_ref().map(OrderBookState::height);
        info!(
            "[hour-rollover-diag] stream_lines enter: source={event_source} state.height={height_at_entry:?}"
        );
        let mut lines_drained: u64 = 0;
        let mut first_height_seen: Option<u64> = None;
        let mut last_height_seen: Option<u64> = None;
        let reader = BufReader::with_capacity(1024 * 1024, file);
        for line_res in reader.lines() {
            let line = match line_res {
                Ok(l) => l,
                Err(err) => {
                    error!("{event_source} stream read error: {err}");
                    info!(
                        "[hour-rollover-diag] stream_lines read-error exit: source={event_source} lines_drained={lines_drained} first_height_seen={first_height_seen:?} last_height_seen={last_height_seen:?}"
                    );
                    return Err(err.into());
                }
            };
            if line.is_empty() {
                continue;
            }
            let res = match event_source {
                EventSource::Fills => serde_json::from_str::<Batch<NodeDataFill>>(&line).map(|batch| {
                    let height = batch.block_number();
                    (height, EventBatch::Fills(batch))
                }),
                EventSource::OrderStatuses => serde_json::from_str(&line)
                    .map(|batch: Batch<NodeDataOrderStatus>| (batch.block_number(), EventBatch::Orders(batch))),
                EventSource::OrderDiffs => serde_json::from_str(&line)
                    .map(|batch: Batch<NodeDataOrderDiff>| (batch.block_number(), EventBatch::BookDiffs(batch))),
            };
            match res {
                Ok((height, event_batch)) => {
                    if height % 1000 == 0 {
                        // Same cadence/level rationale as the live-file path
                        // above. Keep this in sync with that other site.
                        debug!("{event_source} block: {height}");
                    }
                    if first_height_seen.is_none() {
                        first_height_seen = Some(height);
                    }
                    last_height_seen = Some(height);
                    lines_drained += 1;
                    if let Err(err) = self.receive_batch(event_batch) {
                        if err.to_string().contains("[gap-grace-resync]") {
                            warn!("[gap-grace-resync] invalidating state during stream_lines; will re-init on next snapshot fetch");
                            self.order_book_state = None;
                            self.last_block_time_ms = None;
                            self.ready_at = None;
                            self.order_status_cache.clear();
                            self.order_diff_cache.clear();
                            // Stop draining — state is gone, remaining lines are useless
                            break;
                        }
                        self.order_book_state = None;
                        info!(
                            "[hour-rollover-diag] stream_lines receive_batch-error exit: source={event_source} lines_drained={lines_drained} first_height_seen={first_height_seen:?} last_height_seen={last_height_seen:?}"
                        );
                        return Err(err);
                    }
                }
                Err(err) => {
                    // The previous-hour file is closed by hl-node, so a
                    // parse error here is *not* an EOF-mid-line (those only
                    // happen on the live current-hour file). It's truly
                    // malformed data — propagating subsequent lines into
                    // receive_batch could push corrupt state forward and
                    // cause an "Expecting block X got Y" fatal far later
                    // that's hard to diagnose. Better to fail loudly here
                    // and let systemd restart with a fresh snapshot fetch.
                    error!(
                        "{event_source} stream parse error on closed previous-hour file: {err}, line: {:?}",
                        &line[..line.len().min(100)]
                    );
                    info!(
                        "[hour-rollover-diag] stream_lines parse-error exit: source={event_source} lines_drained={lines_drained} first_height_seen={first_height_seen:?} last_height_seen={last_height_seen:?}"
                    );
                    return Err(format!(
                        "Stream parse error in closed {event_source} file: {err}"
                    ).into());
                }
            }
        }
        let height_at_exit = self.order_book_state.as_ref().map(OrderBookState::height);
        info!(
            "[hour-rollover-diag] stream_lines ok exit: source={event_source} lines_drained={lines_drained} first_height_seen={first_height_seen:?} last_height_seen={last_height_seen:?} state.height={height_at_exit:?}"
        );
        let snapshot = self.l2_snapshots(true);
        if let Some(snapshot) = snapshot {
            if let Some(tx) = &self.internal_message_tx {
                let tx = tx.clone();
                tokio::spawn(async move {
                    let snapshot =
                        Arc::new(InternalMessage::Snapshot { l2_snapshots: snapshot.1, time: snapshot.0 });
                    let _unused = tx.send(snapshot);
                });
            }
        }
        Ok(())
    }
}

pub(crate) struct L2Snapshots(HashMap<Coin, HashMap<L2SnapshotParams, Snapshot<InnerLevel>>>);

impl L2Snapshots {
    pub(crate) const fn as_ref(&self) -> &HashMap<Coin, HashMap<L2SnapshotParams, Snapshot<InnerLevel>>> {
        &self.0
    }
}

pub(crate) struct TimedSnapshots {
    pub(crate) time: u64,
    pub(crate) height: u64,
    pub(crate) snapshot: Snapshots<InnerL4Order>,
}

// Messages sent from node data listener to websocket dispatch to support streaming
pub(crate) enum InternalMessage {
    Snapshot { l2_snapshots: L2Snapshots, time: u64 },
    Fills { batch: Batch<NodeDataFill> },
    L4BookUpdates { diff_batch: Batch<NodeDataOrderDiff>, status_batch: Batch<NodeDataOrderStatus> },
}

#[derive(Eq, PartialEq, Hash)]
pub(crate) struct L2SnapshotParams {
    n_sig_figs: Option<u32>,
    mantissa: Option<u64>,
}
