use crate::{
    servers::websocket_server::SnapshotShared,
    latency,
    listeners::{
        directory::DirectoryListener,
        order_book::{
            l2_book::{CheckPlan, L4View},
            l2_thread::{L2Input, L2Thread},
            state::OrderBookState,
        },
    },
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

use utils::{BatchQueue, EventBatch, parse_event_line, process_rmp_file, validate_snapshot_consistency};
pub(crate) use early_order_updates::{ClientUserDemand, UserDemand};
pub(crate) use l2_demand::{ClientL2Demand, L2Demand};
#[cfg(test)]
pub(crate) use utils::compute_l2_snapshots;

mod early_order_updates;
mod l2_book;
mod l2_demand;
mod l2_thread;
mod state;
mod utils;

// WARNING - this code assumes no other file system operations are occurring in the watched directories
// if there are scripts running, this may not work as intended
/// Handles one fs event under the listener mutex, recording lock wait and hold times.
async fn process_update_timed(
    listener: &Mutex<OrderBookListener>,
    event: &Event,
    new_path: &PathBuf,
    event_source: EventSource,
) -> Result<()> {
    let wait_start = Instant::now();
    let mut listener = listener.lock().await;
    let hold_start = Instant::now();
    latency::LOCK_WAIT_US.record_duration_us(hold_start - wait_start);
    // Parsing and applying are synchronous and take
    // milliseconds; block_in_place hands this worker's queued tasks (incl. the
    // LIFO slot, which other workers cannot steal) to another thread meanwhile,
    // so client tasks woken by the broadcasts are not stuck behind us.
    let res = tokio::task::block_in_place(|| listener.process_update(event, new_path, event_source));
    latency::LOCK_HOLD_US.record_duration_us(hold_start.elapsed());
    res
}

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
                            process_update_timed(&listener, &event, new_path, EventSource::OrderStatuses)
                                .await
                                .map_err(|err| format!("Order status processing error: {err}"))?;
                        } else if new_path.starts_with(&fills_dir) && new_path.is_file() {
                            process_update_timed(&listener, &event, new_path, EventSource::Fills)
                                .await
                                .map_err(|err| format!("Fill update processing error: {err}"))?;
                        } else if new_path.starts_with(&order_diffs_dir) && new_path.is_file() {
                            process_update_timed(&listener, &event, new_path, EventSource::OrderDiffs)
                                .await
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
                let checkable = listener.is_ready()
                    && !snapshot_in_flight.load(AtomicOrdering::SeqCst)
                    && !listener.has_active_parse_stall()
                    && !listener.in_init_grace_period();
                if checkable {
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
                    let start = Instant::now();
                    let state = tokio::task::block_in_place(|| listener.clone_state());
                    latency::SNAPSHOT_CLONE_US.record_duration_us(start.elapsed());
                    state
                };
                let snapshot = load_snapshots_from_json::<InnerL4Order, (Address, L4Order)>(&output_fln).await;
                info!("Snapshot fetched");
                // sleep to let some updates build up.
                sleep(Duration::from_secs(1)).await;
                let cache = {
                    let mut listener = listener.lock().await;
                    listener.take_cache()
                };
                info!("Cache has {} elements", cache.len());
                match snapshot {
                    Ok((height, expected_snapshot)) => {
                        if let Some(state) = state {
                            // ~0.5 s of CPU on the full book. Run on an async worker, it
                            // stalled the listener: take_cache's unlock can wake the waiting
                            // listener into this worker's LIFO slot, which other workers
                            // cannot steal, and it stayed there until this work finished.
                            let start = Instant::now();
                            let validation = tokio::task::spawn_blocking(move || {
                                catch_up_and_validate(state, cache, height, expected_snapshot, ignore_spot)
                            })
                            .await;
                            latency::SNAPSHOT_VALIDATE_US.record_duration_us(start.elapsed());
                            // It only reads a copy of the state, so a panic in it is not fatal either.
                            let validation = validation.unwrap_or_else(|err| {
                                warn!("[snapshot-catchup] validation task failed (non-fatal): {err}");
                                Validation::Skipped
                            });
                            if let Validation::Consistent(extras) = validation {
                                if !extras.is_empty() {
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
                                    listener.absorb_extra_books(extras);
                                }
                            }
                            Ok(())
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

#[derive(Debug)]
enum Validation {
    /// The cached updates could not bring the local state to the snapshot height.
    Skipped,
    /// Same orders on both sides; holds the books only the fetched snapshot has.
    Consistent(HashMap<Coin, Snapshot<InnerL4Order>>),
    Mismatch,
}

/// Brings `state` (cloned when the snapshot was requested) up to the fetched
/// snapshot's height with the updates cached meanwhile, then compares the two.
/// Synchronous and CPU-heavy; the caller runs it on the blocking pool, which
/// also frees the large state and snapshots there.
fn catch_up_and_validate(
    mut state: OrderBookState,
    mut cache: VecDeque<(Batch<NodeDataOrderStatus>, Batch<NodeDataOrderDiff>)>,
    height: u64,
    expected_snapshot: Snapshots<InnerL4Order>,
    ignore_spot: bool,
) -> Validation {
    while state.height() < height {
        if let Some((order_statuses, order_diffs)) = cache.pop_front() {
            if let Err(err) = state.apply_updates(&order_statuses, &order_diffs) {
                // Gap or other error during validation
                // catch-up — the main loop already
                // handled this (e.g. gap-grace-resync
                // invalidated state). Abandon this
                // validation pass; next tick will
                // re-fetch cleanly.
                warn!("[snapshot-catchup] apply_updates failed during validation catch-up (non-fatal): {err}");
                return Validation::Skipped;
            }
        } else {
            // Not enough cached updates to reach snapshot
            // height. This is transient (snapshot is newer
            // than our cache). Skip validation this round.
            warn!(
                "[snapshot-catchup] not enough cached updates (state.height={}, snapshot height={height}); skipping validation",
                state.height()
            );
            return Validation::Skipped;
        }
    }
    if state.height() > height {
        // Fetched snapshot is older than local state. Skip
        // validation — next tick will fetch a fresher one.
        warn!(
            "[snapshot-catchup] fetched snapshot height ({height}) lagging stored state ({}); skipping validation",
            state.height()
        );
        return Validation::Skipped;
    }
    let stored_snapshot = state.compute_snapshot();
    info!("Validating snapshot");
    match validate_snapshot_consistency(&stored_snapshot, expected_snapshot, ignore_spot) {
        Ok(extras) => Validation::Consistent(extras),
        Err(err) => {
            // Validation mismatch is a timing race: between
            // the snapshot fetch and the comparison, orders
            // at the same price level get replaced by
            // different orders. The local state (built from
            // the authoritative diff stream) is correct;
            // the fetched snapshot simply aged. Log and
            // continue rather than crashing the process.
            warn!("[snapshot-validation-race] mismatch during consistency check (non-fatal): {err}");
            Validation::Mismatch
        }
    }
}

pub(crate) struct OrderBookListener {
    ignore_spot: bool,
    fill_status_file: Option<File>,
    order_status_file: Option<File>,
    order_diff_file: Option<File>,
    // None if we haven't seen a valid snapshot yet
    order_book_state: Option<OrderBookState>,
    /// Keeps the L2 book from the diffs alone, so L2 snapshots go out before
    /// the block's statuses arrive, and sends them (see `sync_l2`).
    l2: L2Thread,
    /// What the L2 book is checked against after each L4 block.
    l2_plan: CheckPlan,
    /// Of the L2 book once it has the diffs sent to it so far.
    l2_height: u64,
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
    /// hl-node local_time (µs) of the last block applied to the L4 book.
    last_applied_local_time_us: u64,
    /// Coins (and their L2 variants) some client subscribes to.
    l2_demand: Arc<L2Demand>,
    /// Users some client subscribes to orderUpdates of.
    user_demand: Arc<UserDemand>,
    /// Block of the last `InternalMessage::OrderUpdates` sent.
    last_early_order_updates: u64,
    /// The last `InternalMessage::OrderUpdates` sent, until checked against the full parse of its line.
    unchecked_early_order_updates: Option<Arc<InternalMessage>>,
    /// Set once early orderUpdates missed a status: none are sent after.
    early_order_updates_off: bool,
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
        let l2_demand = Arc::<L2Demand>::default();
        Self {
            ignore_spot,
            fill_status_file: None,
            order_status_file: None,
            order_diff_file: None,
            order_book_state: None,
            l2: L2Thread::spawn(internal_message_tx.clone(), l2_demand.clone()),
            l2_plan: CheckPlan::default(),
            l2_height: 0,
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
            last_applied_local_time_us: 0,
            l2_demand,
            user_demand: Arc::default(),
            last_early_order_updates: 0,
            unchecked_early_order_updates: None,
            early_order_updates_off: false,
        }
    }

    pub(crate) fn l2_demand(&self) -> Arc<L2Demand> {
        self.l2_demand.clone()
    }

    pub(crate) fn user_demand(&self) -> Arc<UserDemand> {
        self.user_demand.clone()
    }

    /// Sends the orderUpdates of a complete statuses line's block before the
    /// line is parsed (see `early_order_updates`), once per block and only for
    /// blocks whose L4 updates would be sent.
    fn send_early_order_updates(&mut self, line: &str) {
        let (Some(tx), Some(state)) = (&self.internal_message_tx, &self.order_book_state) else { return };
        let users = self.user_demand.users();
        if users.is_empty() || self.early_order_updates_off {
            return;
        }
        let start = Instant::now();
        let Some((block_number, local_time_us)) = early_order_updates::header(line) else { return };
        if block_number <= state.height().max(self.last_early_order_updates) || state.is_gap(block_number) {
            return;
        }
        self.last_early_order_updates = block_number;
        let Some(statuses) = early_order_updates::statuses_of(line, &users) else { return };
        latency::EARLY_ORDER_UPDATES_US.record_duration_us(start.elapsed());
        let msg = Arc::new(InternalMessage::OrderUpdates { block_number, local_time_us, users, statuses });
        let _unused = tx.send(msg.clone());
        self.unchecked_early_order_updates = Some(msg);
    }

    /// Checks the early orderUpdates of `batch`'s block against the batch, before
    /// its L4 updates go out. Clients leave the users they cover out of those, so
    /// statuses they missed (if hl-node changed how it writes the line) are sent
    /// now, and no more are sent early.
    fn check_early_order_updates(&mut self, batch: &Batch<NodeDataOrderStatus>) {
        let block = batch.block_number();
        let Some(msg) = self.unchecked_early_order_updates.take_if(
            |msg| matches!(msg.as_ref(), InternalMessage::OrderUpdates { block_number, .. } if *block_number == block),
        ) else {
            return;
        };
        let InternalMessage::OrderUpdates { local_time_us, users, statuses, .. } = msg.as_ref() else { return };
        let mut sent = statuses.iter().peekable();
        let mut missed = Vec::new();
        for status in batch.events_ref().iter().filter(|status| users.contains(&status.user)) {
            if sent.peek() == Some(&status) {
                sent.next();
            } else {
                missed.push(status.clone());
            }
        }
        if missed.is_empty() && sent.peek().is_none() {
            return;
        }
        error!(
            "[early-order-updates] block {block}: {} statuses sent early, {} missed, {} not matched; no longer sending them early",
            statuses.len(),
            missed.len(),
            sent.count(),
        );
        self.early_order_updates_off = true;
        if let (false, Some(tx)) = (missed.is_empty(), &self.internal_message_tx) {
            let (local_time_us, users) = (*local_time_us, users.clone());
            let _unused = tx.send(Arc::new(InternalMessage::OrderUpdates { block_number: block, local_time_us, users, statuses: missed }));
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

    /// Copy of the state for snapshot validation. Runs under the listener
    /// mutex every 60 s: a deep clone of all books stalled the listener ~40 ms
    /// even in parallel (2026-09-29); books are now shared copy-on-write.
    fn clone_state(&self) -> Option<OrderBookState> {
        self.order_book_state.clone()
    }

    pub(crate) const fn is_ready(&self) -> bool {
        self.order_book_state.is_some()
    }

    /// Coins clients may subscribe to.
    pub(crate) fn universe(&self) -> Arc<HashSet<String>> {
        if let Some(universe) = self.l2.universe() {
            return universe;
        }
        let Some(state) = &self.order_book_state else { return Arc::default() };
        let coins = state.books().as_ref().keys().filter(|coin| !(self.ignore_spot && coin.is_spot()));
        Arc::new(coins.map(Coin::value).collect())
    }

    /// Drops both books (they are rebuilt from the next snapshot).
    fn clear_state(&mut self) {
        self.order_book_state = None;
        self.l2.clear();
    }

    /// Sends the L2 book the L4 book's books to check it against after the
    /// latter changed, or all of them if it has to be rebuilt.
    fn sync_l2(&mut self) {
        let Some(l4) = self.order_book_state.as_mut() else { return };
        let changed = l4.take_changed();
        let l4 = &*l4;
        if self.l2.take_needs_rebuild() {
            self.l2_plan.reset();
            self.l2_height = self.l2_height.max(l4.height());
            self.l2.send(L2Input::Rebuild(L4View::full(l4), self.last_applied_local_time_us));
            return;
        }
        let universe = self.l2.universe().unwrap_or_default();
        if let Some(view) = self.l2_plan.next(l4, changed, self.l2_height, &universe) {
            self.l2.send(L2Input::Check(view));
        }
    }

    /// Grafts books of coins only the fetched snapshot has into both books.
    fn absorb_extra_books(&mut self, extras: HashMap<Coin, Snapshot<InnerL4Order>>) {
        let Some(l4) = self.order_book_state.as_mut() else { return };
        let coins: HashSet<Coin> = extras.keys().cloned().collect();
        l4.absorb_extra_books(extras, true);
        self.l2.send(L2Input::Graft(L4View::of(l4, coins)));
        self.sync_l2();
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
                self.l2_height = self.l2_height.max(batch.block_number());
                self.l2.send(L2Input::Diffs(batch.clone()));
                self.order_diff_cache.push(batch)?;
            }
            EventBatch::Fills(batch) => {
                self.last_block_time_ms = Some(self.last_block_time_ms.map_or(batch.block_time(), |prev| prev.max(batch.block_time())));
                if self.last_fill.is_none_or(|height| height < batch.block_number()) {
                    // send fill updates if we received a new update
                    if let Some(tx) = &self.internal_message_tx {
                        let _unused = tx.send(Arc::new(InternalMessage::Fills { batch }));
                    }
                }
            }
        }
        if self.is_ready() {
            if let Some((order_statuses, order_diffs)) = self.pop_cache() {
                let msg = Arc::new(InternalMessage::L4BookUpdates { diff_batch: order_diffs, status_batch: order_statuses });
                let InternalMessage::L4BookUpdates { diff_batch: order_diffs, status_batch: order_statuses } = msg.as_ref()
                else {
                    unreachable!()
                };
                let Some(state) = self.order_book_state.as_mut() else { return Ok(()) };
                // Clients build L4 and orderUpdates messages from the batches alone, so send
                // before applying: apply_updates took 3.8 ms p50 / 18 ms p99 (2026-09-28).
                // Still under the listener mutex, so an L4 subscribe snapshot cannot fall
                // between the send and the apply. A gap resyncs the state: nothing to send.
                // Broadcast sends are sync and never block, so send in place rather than
                // tokio::spawn: a task spawned from here sat in this worker's unstealable
                // LIFO slot until the listener released the worker (then after
                // compute_l2_snapshots), adding ~15 ms to L4 delivery (measured 2026-09-27).
                // Also keeps message order.
                if !state.is_gap(order_statuses.block_number()) {
                    if let Some(tx) = &self.internal_message_tx {
                        let _unused = tx.send(msg.clone());
                    }
                }
                let apply_start = std::time::Instant::now();
                state.apply_updates(order_statuses, order_diffs)?;
                latency::APPLY_US.record_duration_us(apply_start.elapsed());
                // The block is only usable once both files have it, so the later write counts.
                let local_time_us = order_statuses.local_time_us().max(order_diffs.local_time_us());
                latency::APPLY_AFTER_WRITE_US.record_age_us(local_time_us);
                latency::HL_WRITE_LAG_MS.record((local_time_us / 1000).saturating_sub(order_diffs.block_time()));
                self.last_applied_local_time_us = local_time_us;
                if let Some(cache) = &mut self.fetched_snapshot_cache {
                    cache.push_back((order_statuses.clone(), order_diffs.clone()));
                }
                self.sync_l2();
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
            if new_order_book.apply_updates(&order_statuses, &order_diffs).is_err() {
                info!(
                    "Failed to apply updates to this book (likely missing older updates). Waiting for next snapshot."
                );
                retry = true;
                break;
            }
        }
        if !retry {
            self.order_book_state = Some(new_order_book);
            self.sync_l2();
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

    /// (time, height, snapshot) of one coin's book; None if not ready or coin untracked.
    pub(crate) fn compute_coin_snapshot(&self, coin: &Coin) -> Option<(u64, u64, Snapshot<InnerL4Order>)> {
        self.order_book_state.as_ref().and_then(|o| o.compute_coin_snapshot(coin))
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
            // hl-node ends each line with a newline: a line followed by one is complete.
            let line_end = line.as_ptr() as usize - data.as_ptr() as usize + line.len();
            if matches!(event_source, EventSource::OrderStatuses) && data.as_bytes().get(line_end) == Some(&b'\n') {
                self.send_early_order_updates(line);
            }
            let res = parse_event_line(event_source, line);
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
            if let EventBatch::Orders(batch) = &event_batch {
                self.check_early_order_updates(batch);
            }
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
                    self.clear_state();
                    self.last_block_time_ms = None;
                    self.ready_at = None;
                    // Clear batch queues to prevent BatchQueue overflow while
                    // waiting for the next snapshot fetch to re-initialize state.
                    self.order_status_cache.clear();
                    self.order_diff_cache.clear();
                    return Ok(());
                }
                self.clear_state();
                return Err(err);
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
            let res = parse_event_line(event_source, &line);
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
                            self.clear_state();
                            self.last_block_time_ms = None;
                            self.ready_at = None;
                            self.order_status_cache.clear();
                            self.order_diff_cache.clear();
                            // Stop draining — state is gone, remaining lines are useless
                            break;
                        }
                        self.clear_state();
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
        Ok(())
    }
}

/// Every L2 variant of one coin's book. Shared (Arc) between consecutive
/// snapshot messages until the coin's book changes.
pub(crate) type CoinL2Snapshots = HashMap<L2SnapshotParams, Snapshot<InnerLevel>>;

pub(crate) struct L2Snapshots(HashMap<Coin, Arc<CoinL2Snapshots>>);

impl L2Snapshots {
    pub(crate) const fn as_ref(&self) -> &HashMap<Coin, Arc<CoinL2Snapshots>> {
        &self.0
    }
}

// Messages sent from node data listener to websocket dispatch to support streaming
pub(crate) enum InternalMessage {
    /// `local_time_us`: hl-node write time of the block this snapshot reflects.
    /// `universe`: coins clients may subscribe to.
    Snapshot {
        l2_snapshots: L2Snapshots,
        time: u64,
        local_time_us: u64,
        shared: Arc<SnapshotShared>,
        universe: Arc<HashSet<String>>,
    },
    Fills { batch: Batch<NodeDataFill> },
    L4BookUpdates { diff_batch: Batch<NodeDataOrderDiff>, status_batch: Batch<NodeDataOrderStatus> },
    /// The statuses of `users` in a block, sent before its `L4BookUpdates`, which
    /// clients leave these users' orderUpdates out of. `local_time_us`: hl-node
    /// write time of the statuses.
    OrderUpdates {
        block_number: u64,
        local_time_us: u64,
        users: Arc<HashSet<Address>>,
        statuses: Vec<NodeDataOrderStatus>,
    },
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub(crate) struct L2SnapshotParams {
    n_sig_figs: Option<u32>,
    mantissa: Option<u64>,
}


#[cfg(test)]
mod validation_test {
    use super::*;
    use crate::order_book::multi_book::load_snapshots_from_str;

    const FIXTURE: &str = "tmp/fixture/out.snap.json";
    const REPLAY_BLOCKS: &str = "tmp/fixture/replay_blocks.jsonl";

    #[test]
    fn catch_up_and_validate_on_real_blocks() {
        let (Ok(json), Ok(blocks)) = (fs::read_to_string(FIXTURE), fs::read_to_string(REPLAY_BLOCKS)) else {
            eprintln!("skipping: {FIXTURE} or {REPLAY_BLOCKS} not present");
            return;
        };
        let (height, snapshot) = load_snapshots_from_str::<InnerL4Order, (Address, L4Order)>(&json).unwrap();
        drop(json);
        let state = OrderBookState::from_snapshot(snapshot, height, 0, true, true);
        let lines = blocks.lines().collect::<Vec<_>>();
        let cache = |n: usize| {
            lines
                .chunks(2)
                .take(n)
                .map(|pair| (serde_json::from_str(pair[0]).unwrap(), serde_json::from_str(pair[1]).unwrap()))
                .collect::<VecDeque<_>>()
        };
        let mut later = state.clone();
        for (statuses, diffs) in cache(20) {
            later.apply_updates(&statuses, &diffs).unwrap();
        }
        let target = height + 20;
        assert_eq!(later.height(), target);

        // Extra cached blocks past the snapshot height are left unapplied.
        let validation = catch_up_and_validate(state.clone(), cache(30), target, later.compute_snapshot(), true);
        assert!(matches!(&validation, Validation::Consistent(extras) if extras.is_empty()), "{validation:?}");

        // A book only the fetched snapshot has is handed back for grafting.
        let mut books = later.compute_snapshot().value();
        books.insert(Coin::new("NEWCOIN"), books[&Coin::new("BTC")].clone());
        let validation = catch_up_and_validate(state.clone(), cache(20), target, Snapshots::new(books), true);
        assert!(
            matches!(&validation, Validation::Consistent(extras) if extras.keys().eq([&Coin::new("NEWCOIN")])),
            "{validation:?}"
        );

        // Too few cached blocks to reach the snapshot, or a snapshot older than the state.
        let validation = catch_up_and_validate(state.clone(), cache(10), target, later.compute_snapshot(), true);
        assert!(matches!(validation, Validation::Skipped), "{validation:?}");
        let validation = catch_up_and_validate(later.clone(), cache(0), height, state.compute_snapshot(), true);
        assert!(matches!(validation, Validation::Skipped), "{validation:?}");

        // A snapshot that does not match the caught-up state.
        let validation = catch_up_and_validate(state.clone(), cache(20), target, state.compute_snapshot(), true);
        assert!(matches!(validation, Validation::Mismatch), "{validation:?}");
    }

    /// L4 updates go out for every applied block (they are sent before the apply), never for a gap.
    #[test]
    fn l4_updates_sent_for_applied_blocks_only() {
        let (Ok(json), Ok(blocks)) = (fs::read_to_string(FIXTURE), fs::read_to_string(REPLAY_BLOCKS)) else {
            eprintln!("skipping: {FIXTURE} or {REPLAY_BLOCKS} not present");
            return;
        };
        let (height, snapshot) = load_snapshots_from_str::<InnerL4Order, (Address, L4Order)>(&json).unwrap();
        drop(json);
        let (tx, mut rx) = tokio::sync::broadcast::channel(16);
        let mut listener = OrderBookListener::new(Some(tx), true);
        listener.order_book_state = Some(OrderBookState::from_snapshot(snapshot, height, 0, true, true));
        let lines = blocks.lines().collect::<Vec<_>>();
        let feed = |listener: &mut OrderBookListener, block: usize| {
            listener.receive_batch(EventBatch::Orders(serde_json::from_str(lines[2 * block]).unwrap())).unwrap();
            listener.receive_batch(EventBatch::BookDiffs(serde_json::from_str(lines[2 * block + 1]).unwrap()))
        };
        // L2 snapshots come from the L2 thread in between.
        let mut l4_updates = |listener: &OrderBookListener| {
            listener.l2.sync();
            std::iter::from_fn(|| rx.try_recv().ok())
                .filter_map(|msg| match msg.as_ref() {
                    InternalMessage::L4BookUpdates { status_batch, diff_batch } => {
                        Some((status_batch.block_number(), diff_batch.block_number()))
                    }
                    _ => None,
                })
                .collect::<Vec<_>>()
        };
        for block in 0..3 {
            feed(&mut listener, block).unwrap();
            let height = height + 1 + block as u64;
            assert_eq!(l4_updates(&listener), [(height, height)]);
        }
        // Block height+5 after height+3: a gap resyncs instead of sending.
        let err = feed(&mut listener, 4).unwrap_err();
        assert!(err.to_string().contains("[gap-grace-resync]"), "{err}");
        assert_eq!(l4_updates(&listener), []);
        assert_eq!(listener.order_book_state.as_ref().unwrap().height(), height + 3);
    }

    /// A block's L2 snapshot goes out once its diffs are in, before its statuses;
    /// only demanded coins have one.
    #[test]
    fn l2_snapshot_sent_before_statuses() {
        let (Ok(json), Ok(blocks)) = (fs::read_to_string(FIXTURE), fs::read_to_string(REPLAY_BLOCKS)) else {
            eprintln!("skipping: {FIXTURE} or {REPLAY_BLOCKS} not present");
            return;
        };
        let (height, snapshot) = load_snapshots_from_str::<InnerL4Order, (Address, L4Order)>(&json).unwrap();
        drop(json);
        let (tx, mut rx) = tokio::sync::broadcast::channel(16);
        let mut listener = OrderBookListener::new(Some(tx), true);
        let n_coins = snapshot.as_ref().keys().filter(|coin| !coin.is_spot()).count();
        let mut demand = ClientL2Demand::new(listener.l2_demand());
        demand.sync(&[crate::types::subscription::Subscription::L2Book {
            coin: "BTC".into(),
            n_sig_figs: None,
            n_levels: None,
            mantissa: None,
        }]
        .into());
        assert!(listener.l2.universe().is_none());
        listener.init_from_snapshot(snapshot, height);
        listener.l2.sync();
        assert_eq!(listener.l2.universe().unwrap().len(), n_coins);
        let lines = blocks.lines().collect::<Vec<_>>();
        let last_msg = std::cell::RefCell::new(None);
        let mut snapshot_after = |listener: &mut OrderBookListener, line: &str, diffs: bool| {
            let batch = if diffs {
                EventBatch::BookDiffs(serde_json::from_str(line).unwrap())
            } else {
                EventBatch::Orders(serde_json::from_str(line).unwrap())
            };
            listener.receive_batch(batch).unwrap();
            listener.l2.sync();
            let mut snapshot = None;
            while let Ok(msg) = rx.try_recv() {
                if let InternalMessage::Snapshot { l2_snapshots, time, universe, .. } = msg.as_ref() {
                    assert_eq!(universe.len(), n_coins);
                    let coins: Vec<_> = l2_snapshots.as_ref().keys().cloned().collect();
                    snapshot = Some((*time, coins));
                    *last_msg.borrow_mut() = Some(msg.clone());
                }
            }
            snapshot
        };
        // Right after init: the snapshot's height (block time unknown).
        assert_eq!(snapshot_after(&mut listener, lines[0], false), Some((0, vec![Coin::new("BTC")])));
        for block in 0..3 {
            let diffs: Batch<NodeDataOrderDiff> = serde_json::from_str(lines[2 * block + 1]).unwrap();
            let got = snapshot_after(&mut listener, lines[2 * block + 1], true);
            assert_eq!(got, Some((diffs.block_time(), vec![Coin::new("BTC")])), "block {block}");
            assert_eq!(listener.order_book_state.as_ref().unwrap().height(), height + 1 + block as u64);
            if block < 2 {
                // The statuses bring the L4 book level with the L2 book: nothing new to send.
                assert_eq!(snapshot_after(&mut listener, lines[2 * block + 2], false), None);
            }
        }
        // Statuses of block 3 are late: the L2 book runs ahead.
        let diffs: Batch<NodeDataOrderDiff> = serde_json::from_str(lines[7]).unwrap();
        assert_eq!(snapshot_after(&mut listener, lines[7], true).map(|s| s.0), Some(diffs.block_time()));
        assert_eq!(listener.order_book_state.as_ref().unwrap().height(), height + 3);
        assert_eq!(snapshot_after(&mut listener, lines[6], false), None);
        assert_eq!(listener.order_book_state.as_ref().unwrap().height(), height + 4);

        // The rest, the statuses of every 7th pair of blocks late: the last snapshot is the L4 book's.
        for block in (4..lines.len() / 2).step_by(2) {
            let [s0, d0, s1, d1] = [0, 1, 2, 3].map(|i| lines[2 * block + i]);
            let order = if block % 7 == 0 {
                [(d0, true), (d1, true), (s0, false), (s1, false)]
            } else {
                [(s0, false), (d0, true), (s1, false), (d1, true)]
            };
            for (line, diffs) in order {
                snapshot_after(&mut listener, line, diffs);
            }
        }
        let l4 = listener.order_book_state.as_ref().unwrap();
        assert_eq!(l4.height(), height + (lines.len() / 2) as u64);
        let msg = last_msg.borrow().clone().unwrap();
        let InternalMessage::Snapshot { l2_snapshots, time, .. } = msg.as_ref() else { unreachable!() };
        assert_eq!(*time, l4.time());
        let btc = Coin::new("BTC");
        let full = compute_l2_snapshots(l4.books());
        let expected = &full.as_ref()[&btc];
        let got = &l2_snapshots.as_ref()[&btc];
        // Only the raw variant is wanted.
        assert_eq!(got.len(), 1);
        for (params, got) in got.iter() {
            assert_eq!(format!("{got:?}"), format!("{:?}", expected[params]), "{params:?}");
        }
        assert_eq!(l2_thread::L2_DIVERGENT_TOTAL.load(AtomicOrdering::Relaxed), 0);
    }

    /// A block's early orderUpdates come before its L4 updates, with the same
    /// statuses of the subscribed users, once per block and only from complete lines.
    #[test]
    fn early_order_updates_before_l4_updates() {
        let (Ok(json), Ok(blocks)) = (fs::read_to_string(FIXTURE), fs::read_to_string(REPLAY_BLOCKS)) else {
            eprintln!("skipping: {FIXTURE} or {REPLAY_BLOCKS} not present");
            return;
        };
        let (height, snapshot) = load_snapshots_from_str::<InnerL4Order, (Address, L4Order)>(&json).unwrap();
        drop(json);
        let (tx, mut rx) = tokio::sync::broadcast::channel(64);
        let mut listener = OrderBookListener::new(Some(tx), true);
        listener.order_book_state = Some(OrderBookState::from_snapshot(snapshot, height, 0, true, true));
        let lines = blocks.lines().collect::<Vec<_>>();
        let statuses: Vec<Batch<NodeDataOrderStatus>> =
            lines.iter().step_by(2).map(|line| serde_json::from_str(line).unwrap()).collect();
        let mut counts: HashMap<Address, usize> = HashMap::new();
        for status in statuses.iter().flat_map(Batch::events_ref) {
            *counts.entry(status.user).or_default() += 1;
        }
        let mut users: Vec<_> = counts.into_iter().collect();
        users.sort_by_key(|&(user, n)| (std::cmp::Reverse(n), user));
        let users: Vec<Address> = users.into_iter().take(5).map(|(user, _)| user).collect();
        let mut demand = ClientUserDemand::new(listener.user_demand());
        demand.sync(&users.iter().map(|&user| crate::types::subscription::Subscription::OrderUpdates { user }).collect());

        let mut messages = |listener: &mut OrderBookListener, data: &str, source: EventSource| {
            listener.process_data(data.to_string(), source).unwrap();
            std::iter::from_fn(|| rx.try_recv().ok()).filter(|msg| !matches!(msg.as_ref(), InternalMessage::Snapshot { .. })).collect::<Vec<_>>()
        };
        // Torn: no newline yet, so neither message goes out.
        assert!(messages(&mut listener, lines[0], EventSource::OrderStatuses).is_empty());
        let mut n_statuses = 0;
        let mut pairs: Vec<&[&str]> = lines.chunks(2).collect();
        let last_two: [&[&str]; 2] = pairs.split_off(pairs.len() - 2).try_into().unwrap();
        for (block, pair) in pairs.into_iter().enumerate() {
            let height = height + 1 + block as u64;
            let early = messages(&mut listener, &format!("{}\n", pair[0]), EventSource::OrderStatuses);
            let [early] = early.as_slice() else { panic!("block {height}: {} messages", early.len()) };
            let InternalMessage::OrderUpdates { block_number, users: sent_users, statuses: sent, .. } = early.as_ref() else {
                panic!("block {height}: not orderUpdates")
            };
            assert_eq!((*block_number, sent_users.len()), (height, users.len()));
            let l4 = messages(&mut listener, &format!("{}\n", pair[1]), EventSource::OrderDiffs);
            let [l4] = l4.as_slice() else { panic!("block {height}: {} messages", l4.len()) };
            let InternalMessage::L4BookUpdates { status_batch, .. } = l4.as_ref() else { panic!("block {height}: not L4") };
            let expected: Vec<_> = status_batch.events_ref().iter().filter(|s| users.contains(&s.user)).cloned().collect();
            assert_eq!(*sent, expected, "block {height}");
            n_statuses += sent.len();
            // A block already applied, or seen early, is not sent again.
            assert!(messages(&mut listener, &format!("{}\n", pair[0]), EventSource::OrderStatuses).is_empty());
        }
        assert!(n_statuses > 100, "{n_statuses}");

        // hl-node writes a user's event differently: the scan misses it, the check sends it.
        let [late, last] = last_two;
        let user = format!(r#""user":"{:#x}""#, users[0]);
        assert!(late[0].contains(&user), "no status of the busiest user in the next to last block");
        let tampered = late[0].replacen(&user, &user.replace(':', ": "), 1);
        let msgs = messages(&mut listener, &format!("{tampered}\n"), EventSource::OrderStatuses);
        let [early, missed] = msgs.as_slice() else { panic!("{} messages", msgs.len()) };
        let (InternalMessage::OrderUpdates { statuses: early, .. }, InternalMessage::OrderUpdates { statuses: missed, .. }) =
            (early.as_ref(), missed.as_ref())
        else {
            panic!("not orderUpdates")
        };
        assert_eq!((missed.len(), missed[0].user), (1, users[0]));
        let batch: Batch<NodeDataOrderStatus> = serde_json::from_str(late[0]).unwrap();
        let expected = batch.events_ref().iter().filter(|s| users.contains(&s.user)).count();
        assert_eq!(early.len() + 1, expected);
        messages(&mut listener, &format!("{}\n", late[1]), EventSource::OrderDiffs);
        // No more early ones after that.
        let msgs = messages(&mut listener, &format!("{}\n", last[0]), EventSource::OrderStatuses);
        assert!(msgs.is_empty(), "{} messages", msgs.len());
        let msgs = messages(&mut listener, &format!("{}\n", last[1]), EventSource::OrderDiffs);
        assert!(matches!(msgs.as_slice(), [msg] if matches!(msg.as_ref(), InternalMessage::L4BookUpdates { .. })));
        drop(demand);
    }
}
