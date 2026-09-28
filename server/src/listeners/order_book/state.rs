use crate::{
    listeners::order_book::{CoinL2Snapshots, L2Snapshots, utils::compute_coin_l2_snapshots},
    order_book::{
        Coin, InnerOrder, Oid, Px, Snapshot,
        multi_book::{OrderBooks, Snapshots},
    },
    prelude::*,
    types::{
        inner::{InnerL4Order, InnerOrderDiff},
        node_data::{Batch, NodeDataOrderDiff, NodeDataOrderStatus},
    },
};
use log::warn;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

/// Don't re-warn about the same not-yet-grafted coin more often than this
/// many blocks. ~14.5 blocks/s ⇒ 200 blocks ≈ 14 seconds. Keeps the log
/// readable when a high-activity new coin appears between snapshot fetches
/// (default fetch interval is 60 s).
const NOT_YET_GRAFTED_WARN_THROTTLE_BLOCKS: u64 = 200;

/// New diffs whose insertBefore anchor was not resting at the order's level.
static INSERT_BEFORE_MISSES: AtomicU64 = AtomicU64::new(0);

#[derive(Clone)]
pub(super) struct OrderBookState {
    order_book: OrderBooks<InnerL4Order>,
    height: u64,
    time: u64,
    snapped: bool,
    ignore_spot: bool,
    /// When true, the next forward block gap is tolerated (height jumps
    /// forward) instead of triggering a fatal. Set to true after snapshot
    /// initialization because the consumer starts reading from the live file
    /// at EOF — blocks between the snapshot height and the current write
    /// position are missed. The first gap is expected and harmless (the
    /// snapshot provides authoritative state); subsequent gaps indicate real
    /// data loss and must still fatal.
    allow_initial_gap: bool,
    /// Throttle map for "skipping <Op> for not-yet-grafted coin" warnings.
    /// Value is the block height at which we last warned for that coin.
    not_yet_grafted_last_warn: HashMap<Coin, u64>,
    /// L2 variants per coin as of the last `l2_snapshots`; only coins that
    /// `order_book.take_changed()` reports are recomputed.
    l2_cache: HashMap<Coin, Arc<CoinL2Snapshots>>,
}

impl OrderBookState {
    pub(super) fn from_snapshot(
        snapshot: Snapshots<InnerL4Order>,
        height: u64,
        time: u64,
        ignore_triggers: bool,
        ignore_spot: bool,
    ) -> Self {
        Self {
            ignore_spot,
            time,
            height,
            order_book: OrderBooks::from_snapshots(snapshot, ignore_triggers),
            snapped: false,
            allow_initial_gap: true,
            not_yet_grafted_last_warn: HashMap::new(),
            l2_cache: HashMap::new(),
        }
    }

    /// Returns true if the caller should emit a warn for `coin` at `height`,
    /// false if the previous warn for that coin was within
    /// `NOT_YET_GRAFTED_WARN_THROTTLE_BLOCKS`. Updates the throttle map on
    /// emit. Cleared opportunistically when a coin gets grafted in
    /// `absorb_extra_books`.
    fn should_warn_not_yet_grafted(&mut self, coin: &Coin, height: u64) -> bool {
        match self.not_yet_grafted_last_warn.get(coin) {
            Some(&last) if height.saturating_sub(last) < NOT_YET_GRAFTED_WARN_THROTTLE_BLOCKS => false,
            _ => {
                self.not_yet_grafted_last_warn.insert(coin.clone(), height);
                true
            }
        }
    }

    pub(super) const fn height(&self) -> u64 {
        self.height
    }

    // forcibly take snapshot of all books
    pub(super) fn compute_snapshot(&self) -> Snapshots<InnerL4Order> {
        self.order_book.to_snapshots_par()
    }

    pub(super) fn compute_coin_snapshot(&self, coin: &Coin) -> Option<(u64, u64, Snapshot<InnerL4Order>)> {
        self.order_book.as_ref().get(coin).map(|book| (self.time, self.height, book.to_snapshot()))
    }

    /// Same as `clone()`, but clones the per-coin books in parallel.
    pub(super) fn par_clone(&self) -> Self {
        Self {
            order_book: self.order_book.par_clone(),
            height: self.height,
            time: self.time,
            snapped: self.snapped,
            ignore_spot: self.ignore_spot,
            allow_initial_gap: self.allow_initial_gap,
            not_yet_grafted_last_warn: self.not_yet_grafted_last_warn.clone(),
            l2_cache: self.l2_cache.clone(),
        }
    }

    // (time, snapshot)
    pub(super) fn l2_snapshots(&mut self, prevent_future_snaps: bool) -> Option<(u64, L2Snapshots)> {
        if self.snapped {
            None
        } else {
            self.snapped = prevent_future_snaps || self.snapped;
            let start = std::time::Instant::now();
            let snapshots = self.update_l2_cache();
            crate::latency::L2_COMPUTE_US.record_duration_us(start.elapsed());
            Some((self.time, snapshots))
        }
    }

    /// Recomputes the L2 variants of the books changed since the last call
    /// (all books on the first call) and returns the whole cache.
    fn update_l2_cache(&mut self) -> L2Snapshots {
        let changed = self.order_book.take_changed();
        let books = self.order_book.as_ref();
        let stale: Vec<_> = if self.l2_cache.len() == books.len() {
            changed.iter().filter_map(|coin| books.get_key_value(coin)).collect()
        } else {
            books.iter().filter(|(coin, _)| changed.contains(*coin) || !self.l2_cache.contains_key(*coin)).collect()
        };
        let fresh: Vec<_> =
            stale.par_iter().map(|(coin, book)| ((*coin).clone(), Arc::new(compute_coin_l2_snapshots(book)))).collect();
        self.l2_cache.extend(fresh);
        if self.l2_cache.len() != books.len() {
            // Books are never removed today; keep the cache exact regardless.
            self.l2_cache.retain(|coin, _| books.contains_key(coin));
        }
        L2Snapshots(self.l2_cache.clone())
    }

    pub(super) fn compute_universe(&self) -> HashSet<Coin> {
        self.order_book.as_ref().keys().cloned().collect()
    }

    /// Graft fetched snapshots for previously-untracked coins into local state.
    /// Used to absorb newly-listed assets without restarting the listener.
    pub(super) fn absorb_extra_books(
        &mut self,
        extras: HashMap<Coin, Snapshot<InnerL4Order>>,
        ignore_triggers: bool,
    ) {
        for (coin, snapshot) in extras {
            if self.ignore_spot && coin.is_spot() {
                continue;
            }
            // Once grafted, drop any throttle entry so a future re-deletion
            // (defensive, shouldn't normally happen) gets a fresh warn.
            self.not_yet_grafted_last_warn.remove(&coin);
            self.order_book.insert_book(coin, snapshot, ignore_triggers);
        }
    }

    pub(super) fn apply_updates(
        &mut self,
        order_statuses: &Batch<NodeDataOrderStatus>,
        order_diffs: &Batch<NodeDataOrderDiff>,
    ) -> Result<()> {
        let height = order_statuses.block_number();
        let time = order_statuses.block_time();
        assert_eq!(order_statuses.block_number(), order_diffs.block_number());
        if height > self.height + 1 {
            let gap_blocks = height.saturating_sub(self.height);
            if self.allow_initial_gap {
                // First gap after snapshot init — expected when the consumer
                // starts at EOF of the current hour-file. The skipped blocks
                // may contain New order diffs that we need to process, so we
                // cannot simply jump forward and continue (that causes
                // "Unable to find order on the book" fatals on subsequent
                // Remove diffs). Instead, signal the caller to invalidate
                // state and re-fetch a snapshot at the current height.
                warn!(
                    "[fresh-start] initial gap detected: self.height={} expected={} got={} gap_blocks={gap_blocks} — invalidating state for snapshot re-fetch",
                    self.height,
                    self.height + 1,
                    height,
                );
            } else {
                // Steady-state gap — typically caused by hl-node getting
                // stuck and hl-visor restarting it. The node bootstraps from
                // a recent round and resumes writing at a much higher block
                // height. The local order-book state is now stale; signal the
                // caller to invalidate and re-fetch a snapshot rather than
                // crashing the process.
                warn!(
                    "[gap-resync] apply_updates gap detected: self.height={} expected={} got={} gap_blocks={gap_blocks} block_time_ms={} — invalidating state for snapshot re-fetch",
                    self.height,
                    self.height + 1,
                    height,
                    time,
                );
            }
            self.allow_initial_gap = false;
            return Err("[gap-grace-resync]".into());
        } else if height <= self.height {
            // This is not an error in case we started caching long before a snapshot is fetched
            return Ok(());
        }
        // A sequential block arrived — clear the initial-gap grace period.
        // From now on, any forward gap is a real data integrity issue.
        self.allow_initial_gap = false;
        let mut order_map = order_statuses
            .events_ref()
            .iter()
            .filter_map(|order_status| {
                if order_status.is_inserted_into_book() {
                    Some((Oid::new(order_status.order.oid), order_status))
                } else {
                    None
                }
            })
            .collect::<HashMap<_, _>>();
        for diff in order_diffs.events_ref() {
            let oid = diff.oid();
            let coin = diff.coin();
            if coin.is_spot() && self.ignore_spot {
                continue;
            }
            let inner_diff = diff.diff().try_into()?;
            match inner_diff {
                InnerOrderDiff::New { sz, insert_before } => {
                    if let Some(order) = order_map.remove(&oid) {
                        let time = order.time.and_utc().timestamp_millis();
                        let mut inner_order: InnerL4Order = order.clone().try_into()?;
                        inner_order.modify_sz(sz);
                        // must replace time with time of entering book, which is the timestamp of the order status update
                        #[allow(clippy::unwrap_used)]
                        inner_order.convert_trigger(time.try_into().unwrap());
                        // For stop market/limit triggers, status.order.limitPx is the trigger
                        // condition price, not the resting price on the book. The actual price
                        // the order rests at is on the diff event itself. For ordinary limit
                        // orders the two are equal, so this is a no-op there.
                        inner_order.limit_px = Px::parse_from_str(diff.px())?;
                        // A missing insertBefore anchor only misplaces the order within its
                        // level (sizes and L2 stay right), so warn rather than fail the listener.
                        if !self.order_book.add_order_before(inner_order, insert_before) {
                            let misses = INSERT_BEFORE_MISSES.fetch_add(1, Ordering::Relaxed) + 1;
                            if misses.is_power_of_two() {
                                warn!("insertBefore anchor not on the book, rested at the back of its level ({misses} so far) {diff:?}");
                            }
                        }
                    } else {
                        return Err(format!("Unable to find order opening status {diff:?}").into());
                    }
                }
                InnerOrderDiff::Update { new_sz, .. } => {
                    // If the book is not tracked yet, this is a newly-listed
                    // coin whose snapshot has not been grafted via
                    // absorb_extra_books. Skip — the next fetch_snapshot will
                    // absorb it and bring local state into sync. Hard-erroring
                    // here would crash the listener for a benign add.
                    if !self.order_book.has_book(&coin) {
                        if self.should_warn_not_yet_grafted(&coin, height) {
                            warn!(
                                "Skipping Update for not-yet-grafted coin {} oid {:?} at block {height}; waiting for absorb_extra_books",
                                coin.value(),
                                oid
                            );
                        }
                        continue;
                    }
                    if !self.order_book.modify_sz(oid, coin, new_sz) {
                        return Err(format!("Unable to find order on the book {diff:?}").into());
                    }
                }
                InnerOrderDiff::Remove => {
                    if !self.order_book.has_book(&coin) {
                        if self.should_warn_not_yet_grafted(&coin, height) {
                            warn!(
                                "Skipping Remove for not-yet-grafted coin {} oid {:?} at block {height}; waiting for absorb_extra_books",
                                coin.value(),
                                oid
                            );
                        }
                        continue;
                    }
                    if !self.order_book.cancel_order(oid, coin) {
                        return Err(format!("Unable to find order on the book {diff:?}").into());
                    }
                }
            }
        }
        self.height += 1;
        self.time = time;
        self.snapped = false;
        Ok(())
    }
}

#[cfg(test)]
mod real_snapshot_test {
    use super::*;
    use crate::{
        listeners::order_book::utils::{compute_coin_l2_snapshots_full_depth, compute_l2_snapshots},
        order_book::multi_book::load_snapshots_from_str,
        types::{L4Order, OrderDiff, inner::InnerLevel, subscription::MAX_LEVELS},
    };
    use alloy::primitives::Address;
    use std::time::{Duration, Instant};

    const FIXTURE: &str = "tmp/fixture/out.snap.json";
    /// The 400 hl-node blocks right after FIXTURE's height: alternating
    /// order-status and book-diff lines (statuses pre-filtered to the ones
    /// `apply_updates` inserts into the book). Test-only, not committed.
    const REPLAY_BLOCKS: &str = "tmp/fixture/replay_blocks.jsonl";

    fn same_l2(a: &Snapshot<InnerLevel>, b: &Snapshot<InnerLevel>) -> bool {
        a.as_ref().iter().zip(b.as_ref()).all(|(a, b)| {
            a.len() == b.len() && a.iter().zip(b).all(|(a, b)| a.px == b.px && a.sz == b.sz && a.n == b.n)
        })
    }

    /// The incremental L2 cache must equal a full recompute after every real block.
    #[test]
    fn incremental_l2_matches_full_recompute_on_real_blocks() {
        let (Ok(json), Ok(blocks)) = (fs::read_to_string(FIXTURE), fs::read_to_string(REPLAY_BLOCKS)) else {
            eprintln!("skipping: {FIXTURE} or {REPLAY_BLOCKS} not present");
            return;
        };
        let (height, snapshot) = load_snapshots_from_str::<InnerL4Order, (Address, L4Order)>(&json).unwrap();
        drop(json);
        let mut state = OrderBookState::from_snapshot(snapshot, height, 0, true, true);
        let first = state.l2_snapshots(true).unwrap().1;
        assert!(state.l2_snapshots(true).is_none(), "snapped state must not re-snapshot");
        let mut prev: HashMap<Coin, Arc<CoinL2Snapshots>> = first.as_ref().clone();
        let n_coins = prev.len();
        let (mut n_blocks, mut n_recomputed, mut incr_time, mut full_time) = (0, 0, Duration::ZERO, Duration::ZERO);
        let (mut n_insert_before, mut n_ahead_checked) = (0, 0);
        let mut lines = blocks.lines();
        while let (Some(statuses), Some(diffs)) = (lines.next(), lines.next()) {
            let statuses: Batch<NodeDataOrderStatus> = serde_json::from_str(statuses).unwrap();
            let diffs: Batch<NodeDataOrderDiff> = serde_json::from_str(diffs).unwrap();
            assert_eq!(statuses.block_number(), state.height + 1);
            state.apply_updates(&statuses, &diffs).unwrap();
            n_blocks += 1;

            // An insertBefore order must queue ahead of its anchor while both still rest.
            let mut books = HashMap::new();
            for diff in diffs.events_ref() {
                let OrderDiff::New { insert_before: Some(anchor), .. } = diff.diff() else { continue };
                n_insert_before += 1;
                let coin = diff.coin();
                let snapshot = books.entry(coin.clone()).or_insert_with(|| state.compute_coin_snapshot(&coin).unwrap().2);
                let queue = snapshot.as_ref().iter().flatten().map(InnerOrder::oid).collect::<Vec<_>>();
                let pos = |oid: &Oid| queue.iter().position(|o| o == oid);
                if let (Some(order), Some(anchor)) = (pos(&diff.oid()), pos(&Oid::new(anchor))) {
                    assert!(order < anchor, "block {} {diff:?} queued behind its anchor", state.height);
                    n_ahead_checked += 1;
                }
            }
            if n_blocks == 200 {
                // A newly-listed coin grafted mid-stream, and an existing book replaced.
                let btc = state.compute_coin_snapshot(&Coin::new("BTC")).unwrap().2;
                let sol = state.compute_coin_snapshot(&Coin::new("SOL")).unwrap().2;
                let extras = HashMap::from([(Coin::new("NEWCOIN"), btc), (Coin::new("SOL"), sol)]);
                state.absorb_extra_books(extras, true);
            }

            let start = Instant::now();
            let (_, incremental) = state.l2_snapshots(true).unwrap();
            incr_time += start.elapsed();
            let start = Instant::now();
            let full = compute_l2_snapshots(&state.order_book);
            full_time += start.elapsed();

            let (incremental, full) = (incremental.as_ref(), full.as_ref());
            assert_eq!(incremental.len(), full.len(), "block {}", state.height);
            for (coin, expected) in full {
                let got = &incremental[coin];
                assert_eq!(got.len(), expected.len(), "{coin:?}");
                for (params, expected) in expected.iter() {
                    assert!(same_l2(&got[params], expected), "block {} {coin:?} differs", state.height);
                }
                match prev.get(coin) {
                    Some(old) if Arc::ptr_eq(old, got) => {}
                    _ => n_recomputed += 1,
                }
            }
            prev = incremental.clone();

            for book in state.order_book.as_ref().values() {
                book.assert_level_sizes();
            }
            if n_blocks % 10 == 1 {
                // Every variant equals the old full-depth chain cut to what clients can request.
                for (coin, book) in state.order_book.as_ref() {
                    let got = &incremental[coin];
                    let expected = compute_coin_l2_snapshots_full_depth(book);
                    assert_eq!(got.len(), expected.len(), "{coin:?}");
                    for (params, expected) in &expected {
                        let (got, expected) = (&got[params], expected.truncate(MAX_LEVELS));
                        assert!(got.as_ref().iter().all(|side| side.len() <= MAX_LEVELS));
                        assert!(same_l2(got, &expected), "block {} {coin:?} {params:?} differs", state.height);
                    }
                }
            }
        }
        assert_eq!(n_blocks, 400, "fixture should hold 400 blocks");
        assert_eq!(INSERT_BEFORE_MISSES.load(Ordering::Relaxed), 0, "every insertBefore anchor should be on the book");
        assert!(n_ahead_checked > 0);
        eprintln!("{n_insert_before} insertBefore diffs, {n_ahead_checked} checked queued ahead of their anchor");
        eprintln!(
            "{n_blocks} blocks, {n_coins} coins: {:.1} coins recomputed per block; l2 incremental {:?}/block vs full {:?}/block",
            n_recomputed as f64 / f64::from(n_blocks),
            incr_time / n_blocks,
            full_time / n_blocks,
        );
    }

    /// Validation reports queue-order differences, so a book built from a node snapshot must
    /// hand back every level in the node's queue order.
    #[test]
    fn node_snapshot_queue_order_round_trips() {
        let Ok(json) = fs::read_to_string(FIXTURE) else {
            eprintln!("skipping: {FIXTURE} not present");
            return;
        };
        let load = || load_snapshots_from_str::<InnerL4Order, (Address, L4Order)>(&json).unwrap();
        let ((height, snapshot), (_, expected)) = (load(), load());
        drop(json);
        let state = OrderBookState::from_snapshot(snapshot, height, 0, true, false);
        let local = state.compute_snapshot();
        let (mut n_sides, mut n_misordered) = (0, 0);
        for (coin, mut expected) in expected.value() {
            expected.remove_triggers();
            for (got, expected) in local.as_ref()[&coin].as_ref().iter().zip(expected.as_ref()) {
                n_sides += 1;
                if !got.iter().map(InnerOrder::oid).eq(expected.iter().map(InnerOrder::oid)) {
                    n_misordered += 1;
                }
            }
        }
        assert!(n_sides > 200);
        assert_eq!(n_misordered, 0, "of {n_sides} book sides");
    }

    #[test]
    fn coin_snapshot_and_par_clone_match_full_state() {
        let Ok(json) = fs::read_to_string(FIXTURE) else {
            eprintln!("skipping: {FIXTURE} not present");
            return;
        };
        let (height, snapshot) = load_snapshots_from_str::<InnerL4Order, (Address, L4Order)>(&json).unwrap();
        drop(json);
        let mut state = OrderBookState::from_snapshot(snapshot, height, 1234, true, true);
        state.snapped = true;
        state.not_yet_grafted_last_warn.insert(Coin::new("NEWCOIN"), 7);
        let full = state.compute_snapshot();
        assert!(full.as_ref().len() > 100);

        // Old L4 subscribe path: filter the full snapshot for one coin.
        for (coin, expected) in full.as_ref() {
            let (time, h, got) = state.compute_coin_snapshot(coin).unwrap();
            assert_eq!((time, h), (1234, height));
            assert_eq!(got.as_ref(), expected.as_ref(), "{coin:?}");
        }
        assert!(state.compute_coin_snapshot(&Coin::new("NO_SUCH_COIN")).is_none());

        let (serial, par) = (state.clone(), state.par_clone());
        for copy in [&serial, &par] {
            assert_eq!(
                (copy.height, copy.time, copy.snapped, copy.ignore_spot, copy.allow_initial_gap),
                (state.height, state.time, state.snapped, state.ignore_spot, state.allow_initial_gap)
            );
            assert_eq!(copy.not_yet_grafted_last_warn, state.not_yet_grafted_last_warn);
        }
        let (serial, par) = (serial.compute_snapshot(), par.compute_snapshot());
        assert_eq!(serial.as_ref().len(), par.as_ref().len());
        for (coin, expected) in serial.as_ref() {
            assert_eq!(par.as_ref()[coin].as_ref(), expected.as_ref(), "{coin:?}");
        }
    }
}
