//! The book L2 snapshots are computed from, kept from the book diffs alone.
//!
//! A diff carries its order's coin, side, price and size, which is all an L2
//! level needs, so this book applies a block as soon as its diffs line is
//! parsed. The L4 book also needs the block's order statuses, which hl-node
//! writes ~1.7 ms (p50; 11 ms p99) after the diffs and which take ~1.4 ms more
//! to parse (2026-09-29): L2 no longer waits for them.
//!
//! Both books apply the same diffs, so they cannot drift apart unless a diff
//! is applied differently (no New diff's side differed from its status in
//! 1.8M checked, 2026-09-29). Still, after every L4 block the books of the
//! coins it changed, and of a few more in turn, are compared by their order
//! checksums (see `OrderBook::checksum`), and a differing coin is rebuilt from
//! the L4 book and counted (`l2_divergent_coins`). Comparing the changed coins'
//! levels instead took ~3 ms per block (2026-09-29).
//!
//! The book lives on a thread of its own (see `l2_thread`) and is checked
//! against `L4View`s: the L4 book's books of the coins to check, shared
//! copy-on-write. The listener picks the coins (`CheckPlan`); the L2 thread
//! drops a view once checked, as the L4 book's next write to a book still
//! shared copies it.

use crate::{
    latency,
    listeners::order_book::{
        CoinL2Snapshots, L2Snapshots,
        state::{OrderBookState, apply_block, apply_coin_updates, group_diffs_by_coin},
        utils::{compute_coin_l2_snapshots, compute_coin_raw_l2_snapshot},
    },
    order_book::{Coin, InnerOrder, Oid, OrderBook, Px, Side, Sz, multi_book::OrderBooks},
    prelude::*,
    types::{inner::InnerL4Order, node_data::{Batch, NodeDataOrderDiff}},
};
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Instant,
};

/// Blocks this book may stay ahead of the L4 book before the coins the L4
/// book changed meanwhile are checked the slow way (rebuilt from the L4 book
/// and the diffs since). Normally the next L4 block catches up and they are
/// compared directly.
const MAX_UNCHECKED_BLOCKS: u64 = 100;

/// Coins compared per check besides the changed ones, in turn: every coin
/// (~1400) about every 5 s at ~14.5 blocks/s. 100 added ~90 µs per check to
/// the ~100 µs of the changed ones (2026-09-29).
const SWEEP_COINS: usize = 20;

/// The L4 book's books of some coins at its current block.
pub(super) struct L4View {
    height: u64,
    time: u64,
    ignore_spot: bool,
    /// The coins covered (no spot if `ignore_spot`); those `books` lacks have no L4 book.
    coins: HashSet<Coin>,
    books: OrderBooks<InnerL4Order>,
}

impl L4View {
    /// Every book of `l4`.
    pub(super) fn full(l4: &OrderBookState) -> Self {
        Self::of(l4, l4.books().as_ref().keys().cloned().collect())
    }

    /// The books of `coins` in `l4`.
    pub(super) fn of(l4: &OrderBookState, mut coins: HashSet<Coin>) -> Self {
        let ignore_spot = l4.ignore_spot();
        coins.retain(|coin| !(ignore_spot && coin.is_spot()));
        Self { height: l4.height(), time: l4.time(), ignore_spot, books: l4.books().subset(&coins), coins }
    }

    pub(super) const fn height(&self) -> u64 {
        self.height
    }
}

/// Which of the L4 book's books the L2 book is checked against, and when.
#[derive(Default)]
pub(super) struct CheckPlan {
    /// Coins the L4 book changed since the L2 book was last checked against it.
    unchecked: HashSet<Coin>,
    unchecked_blocks: u64,
    /// Coins still to compare this round (see `SWEEP_COINS`).
    sweep: Vec<Coin>,
}

impl CheckPlan {
    /// The view to check the L2 book against after `l4` changed the books of
    /// `changed`, or None to wait for `l4` to catch up with it. `l2_height`: of
    /// the L2 book once it has the diffs sent so far; `l2_universe`: its coins.
    pub(super) fn next(
        &mut self,
        l4: &OrderBookState,
        changed: HashSet<Coin>,
        l2_height: u64,
        l2_universe: &HashSet<String>,
    ) -> Option<L4View> {
        let ignore_spot = l4.ignore_spot();
        self.unchecked.extend(changed.into_iter().filter(|coin| !(ignore_spot && coin.is_spot())));
        let coins = if l2_height <= l4.height() {
            let mut coins = std::mem::take(&mut self.unchecked);
            // A few more every check, in case a book changed unnoticed.
            if self.sweep.is_empty() {
                let l4_coins = l4.books().as_ref().keys().cloned();
                let all: HashSet<_> = l4_coins.chain(l2_universe.iter().map(|coin| Coin::new(coin))).collect();
                self.sweep = all.into_iter().filter(|coin| !(ignore_spot && coin.is_spot())).collect();
            }
            let n = self.sweep.len().min(SWEEP_COINS);
            coins.extend(self.sweep.drain(..n));
            coins
        } else {
            // Normally the next L4 block catches up with the L2 book.
            self.unchecked_blocks += 1;
            if self.unchecked_blocks < MAX_UNCHECKED_BLOCKS {
                return None;
            }
            std::mem::take(&mut self.unchecked)
        };
        self.unchecked_blocks = 0;
        Some(L4View::of(l4, coins))
    }

    /// For an L2 book rebuilt from every L4 book.
    pub(super) fn reset(&mut self) {
        self.unchecked.clear();
        self.unchecked_blocks = 0;
    }
}

/// What an L2 level needs of a resting order.
#[derive(Clone, Debug)]
pub(super) struct L2Order {
    oid: Oid,
    side: Side,
    px: Px,
    sz: Sz,
}

impl InnerOrder for L2Order {
    fn oid(&self) -> Oid {
        self.oid.clone()
    }

    fn side(&self) -> Side {
        self.side
    }

    fn limit_px(&self) -> Px {
        self.px
    }

    fn sz(&self) -> Sz {
        self.sz
    }

    fn decrement_sz(&mut self, dec: Sz) {
        self.sz.decrement_sz(dec.value());
    }

    fn fill(&mut self, maker_order: &mut Self) -> Sz {
        let match_sz = self.sz().min(maker_order.sz());
        self.decrement_sz(match_sz);
        maker_order.decrement_sz(match_sz);
        match_sz
    }

    fn modify_sz(&mut self, sz: Sz) {
        self.sz = sz;
    }

    fn convert_trigger(&mut self, _: u64) {}
}

impl From<&InnerL4Order> for L2Order {
    fn from(order: &InnerL4Order) -> Self {
        Self { oid: order.oid(), side: order.side, px: order.limit_px, sz: order.sz }
    }
}

pub(super) struct L2BookState {
    books: OrderBooks<L2Order>,
    height: u64,
    /// block_time and hl-node local_time (µs) of the last applied block.
    time: u64,
    local_time_us: u64,
    ignore_spot: bool,
    snapped: bool,
    /// L2 variants per coin some client subscribes to, as of the last
    /// `l2_snapshots`; only coins that `books.take_changed()` reports are recomputed.
    l2_cache: HashMap<Coin, Arc<CoinL2Snapshots>>,
    /// Coins clients may subscribe to (spot left out if `ignore_spot`).
    universe: Arc<HashSet<String>>,
}

// Anonymous lifetimes in `impl Trait` arguments are not stable yet.
#[allow(single_use_lifetimes)]
impl L2BookState {
    /// The L2 book of `l4` (a full view), brought forward with `pending`: the
    /// diffs of the blocks after `l4`'s, in order. `local_time_us`: of `l4`'s last block.
    pub(super) fn from_l4<'a>(
        l4: &L4View,
        pending: impl IntoIterator<Item = &'a Batch<NodeDataOrderDiff>>,
        local_time_us: u64,
    ) -> Result<Self> {
        let mut state = Self {
            books: l2_books(l4, |_| true),
            height: l4.height,
            time: l4.time,
            local_time_us,
            ignore_spot: l4.ignore_spot,
            snapped: false,
            l2_cache: HashMap::new(),
            universe: Arc::default(),
        };
        state.refresh_universe();
        for diffs in pending {
            if diffs.block_number() > state.height {
                state.apply(diffs)?;
            }
        }
        Ok(state)
    }

    #[cfg(test)]
    pub(super) const fn height(&self) -> u64 {
        self.height
    }

    pub(super) fn universe(&self) -> Arc<HashSet<String>> {
        self.universe.clone()
    }

    fn refresh_universe(&mut self) {
        if self.universe.len() != self.books.as_ref().len() {
            self.universe = Arc::new(self.books.as_ref().keys().map(Coin::value).collect());
        }
    }

    /// Applies the block after `height()`. Returns false for an earlier block
    /// (ignored), and an error for a later one or if a diff does not apply; the
    /// book is then unusable.
    pub(super) fn apply(&mut self, diffs: &Batch<NodeDataOrderDiff>) -> Result<bool> {
        let height = diffs.block_number();
        if height <= self.height {
            return Ok(false);
        }
        if height != self.height + 1 {
            return Err(format!("L2 book at block {} got block {height}", self.height).into());
        }
        apply_l2_diffs(&mut self.books, diffs, self.ignore_spot, None)?;
        self.refresh_universe();
        self.height = height;
        self.time = diffs.block_time();
        self.local_time_us = diffs.local_time_us();
        self.snapped = false;
        Ok(true)
    }

    /// Checks the coins of `l4` (see `CheckPlan`) against this book, which
    /// must not be behind it. Coins whose books differ are rebuilt from `l4`
    /// and returned. `pending`: the diffs of the blocks after `l4`'s, in order,
    /// needed if this book is ahead.
    pub(super) fn check<'a>(
        &mut self,
        l4: &L4View,
        pending: impl IntoIterator<Item = &'a Batch<NodeDataOrderDiff>>,
    ) -> Result<Vec<Coin>> {
        if self.height < l4.height {
            return Err(format!("L2 book at block {} is behind the L4 book at {}", self.height, l4.height).into());
        }
        let (divergent, rebuilt) = if self.height == l4.height {
            let divergent = differing(&l4.coins, &self.books, &l4.books);
            let rebuilt = self.rebuild(l4, &divergent, [])?;
            (divergent, rebuilt)
        } else {
            let rebuilt = self.rebuild(l4, &l4.coins, pending)?;
            (differing(&l4.coins, &self.books, &rebuilt), rebuilt)
        };
        self.replace_books(&divergent, &rebuilt);
        Ok(divergent.into_iter().collect())
    }

    /// Replaces the books of `l4`'s coins with ones rebuilt from it (see
    /// `check`), e.g. after they were grafted into the L4 book from a fetched snapshot.
    pub(super) fn graft<'a>(
        &mut self,
        l4: &L4View,
        pending: impl IntoIterator<Item = &'a Batch<NodeDataOrderDiff>>,
    ) -> Result<()> {
        let rebuilt = self.rebuild(l4, &l4.coins, pending)?;
        self.replace_books(&l4.coins, &rebuilt);
        Ok(())
    }

    /// The L2 books of `coins` (all covered by `l4`) at this book's height:
    /// `l4`'s, brought forward with `pending` (see `check`).
    fn rebuild<'a>(
        &self,
        l4: &L4View,
        coins: &HashSet<Coin>,
        pending: impl IntoIterator<Item = &'a Batch<NodeDataOrderDiff>>,
    ) -> Result<OrderBooks<L2Order>> {
        if coins.is_empty() {
            return Ok(OrderBooks::default());
        }
        let mut rebuilt = l2_books(l4, |coin| coins.contains(coin));
        let mut next = l4.height + 1;
        for diffs in pending {
            let height = diffs.block_number();
            if height < next || height > self.height {
                continue;
            }
            if height != next {
                break;
            }
            apply_l2_diffs(&mut rebuilt, diffs, self.ignore_spot, Some(coins))?;
            next += 1;
        }
        if next != self.height + 1 {
            return Err(format!("no diffs of block {next} to bring L2 books to block {}", self.height).into());
        }
        Ok(rebuilt)
    }

    fn replace_books(&mut self, coins: &HashSet<Coin>, rebuilt: &OrderBooks<L2Order>) {
        if coins.is_empty() {
            return;
        }
        for coin in coins {
            self.books.replace_book(coin, rebuilt.as_ref().get(coin).cloned());
        }
        self.universe = Arc::default();
        self.refresh_universe();
        self.snapped = false;
    }

    /// (block time, hl-node write time, snapshots, universe) of the last block,
    /// unless already returned. Only coins in `demand` have snapshots; those
    /// mapped to true have every variant, the rest only the raw one.
    pub(super) fn l2_snapshots(
        &mut self,
        demand: &HashMap<Coin, bool>,
    ) -> Option<(u64, u64, L2Snapshots, Arc<HashSet<String>>)> {
        if self.snapped {
            return None;
        }
        self.snapped = true;
        let start = Instant::now();
        let snapshots = self.update_l2_cache(demand);
        latency::L2_COMPUTE_US.record_duration_us(start.elapsed());
        latency::L2_AGGREGATED_COINS.record(demand.values().filter(|aggregated| **aggregated).count() as u64);
        Some((self.time, self.local_time_us, snapshots, self.universe()))
    }

    /// Recomputes the entries of demanded coins whose book changed since the
    /// last call, or that have none or lack the variants now wanted, drops
    /// entries nobody wants, and returns the cache. A coin whose aggregated
    /// variants are no longer wanted keeps them (still correct) until its
    /// book next changes.
    fn update_l2_cache(&mut self, demand: &HashMap<Coin, bool>) -> L2Snapshots {
        let changed = self.books.take_changed();
        self.l2_cache.retain(|coin, entry| {
            !changed.contains(coin) && demand.get(coin).is_some_and(|&aggregated| !aggregated || entry.len() > 1)
        });
        let books = self.books.as_ref();
        let stale: Vec<_> = demand
            .iter()
            .filter(|(coin, _)| !self.l2_cache.contains_key(*coin))
            .filter_map(|(coin, &aggregated)| books.get_key_value(coin).map(|(coin, book)| (coin, book, aggregated)))
            .collect();
        let fresh: Vec<_> = stale
            .par_iter()
            .map(|(coin, book, aggregated)| {
                let snapshots =
                    if *aggregated { compute_coin_l2_snapshots(book) } else { compute_coin_raw_l2_snapshot(book) };
                ((*coin).clone(), Arc::new(snapshots))
            })
            .collect();
        self.l2_cache.extend(fresh);
        L2Snapshots(self.l2_cache.clone())
    }
}

/// L2 copies of the books of `l4` whose coin `keep` accepts (never spot if `l4` ignores it).
fn l2_books(l4: &L4View, keep: impl Fn(&Coin) -> bool + Sync) -> OrderBooks<L2Order> {
    let ignore_spot = l4.ignore_spot;
    l4.books.par_map(|coin| keep(coin) && !(ignore_spot && coin.is_spot()), |order| L2Order::from(order))
}

/// Applies a block's diffs (only those of `only`'s coins if Some) to `books`.
fn apply_l2_diffs(
    books: &mut OrderBooks<L2Order>,
    diffs: &Batch<NodeDataOrderDiff>,
    ignore_spot: bool,
    only: Option<&HashSet<Coin>>,
) -> Result<()> {
    let mut work = group_diffs_by_coin(diffs, ignore_spot);
    if let Some(only) = only {
        work.retain(|coin, _| only.contains(&Coin::new(coin)));
    }
    // Coins without a book skip Update/Remove diffs, as the L4 book does (which also warns).
    apply_block(books, work, |book, diffs| apply_coin_updates(book, &diffs, rest_l2_order))?;
    Ok(())
}

fn rest_l2_order(book: &mut OrderBook<L2Order>, diff: &NodeDataOrderDiff, sz: Sz, insert_before: Option<Oid>) -> Result<()> {
    let order = L2Order { oid: diff.oid(), side: diff.side(), px: Px::parse_from_str(diff.px())?, sz };
    // Same queue position as in the L4 book, which reports anchor misses.
    book.add_order_before(order, insert_before);
    Ok(())
}

/// The coins of `coins` whose books hold different orders; a missing book is
/// taken as empty.
fn differing<O: InnerOrder, P: InnerOrder>(coins: &HashSet<Coin>, a: &OrderBooks<O>, b: &OrderBooks<P>) -> HashSet<Coin> {
    coins.iter().filter(|coin| checksum(a, coin) != checksum(b, coin)).cloned().collect()
}

fn checksum<O: InnerOrder>(books: &OrderBooks<O>, coin: &Coin) -> u64 {
    books.as_ref().get(coin).map_or(0, |book| book.checksum())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        listeners::order_book::utils::{compute_coin_l2_snapshots_full_depth, compute_l2_snapshots},
        order_book::{Snapshot, multi_book::load_snapshots_from_str},
        types::{L4Order, inner::InnerLevel, node_data::NodeDataOrderStatus, subscription::MAX_LEVELS},
    };
    use alloy::primitives::Address;
    use std::{collections::VecDeque, fs, time::Duration};

    const FIXTURE: &str = "tmp/fixture/out.snap.json";
    /// See `state::real_snapshot_test::REPLAY_BLOCKS`.
    const REPLAY_BLOCKS: &str = "tmp/fixture/replay_blocks.jsonl";

    type Block = (Batch<NodeDataOrderStatus>, Batch<NodeDataOrderDiff>);

    fn load() -> Option<(OrderBookState, Vec<Block>)> {
        let (Ok(json), Ok(blocks)) = (fs::read_to_string(FIXTURE), fs::read_to_string(REPLAY_BLOCKS)) else {
            eprintln!("skipping: {FIXTURE} or {REPLAY_BLOCKS} not present");
            return None;
        };
        let (height, snapshot) = load_snapshots_from_str::<InnerL4Order, (Address, L4Order)>(&json).unwrap();
        let lines: Vec<_> = blocks.lines().collect();
        let blocks = lines
            .chunks(2)
            .map(|pair| (serde_json::from_str(pair[0]).unwrap(), serde_json::from_str(pair[1]).unwrap()))
            .collect();
        Some((OrderBookState::from_snapshot(snapshot, height, 0, true, true), blocks))
    }

    /// Checks `l2` the way the listener has it checked after `l4` changed `changed`.
    #[allow(single_use_lifetimes)]
    fn check<'a>(
        l2: &mut L2BookState,
        plan: &mut CheckPlan,
        l4: &OrderBookState,
        changed: HashSet<Coin>,
        pending: impl IntoIterator<Item = &'a Batch<NodeDataOrderDiff>>,
    ) -> Result<Vec<Coin>> {
        match plan.next(l4, changed, l2.height(), &l2.universe()) {
            Some(view) => l2.check(&view, pending),
            None => Ok(Vec::new()),
        }
    }

    fn same_l2(a: &Snapshot<InnerLevel>, b: &Snapshot<InnerLevel>) -> bool {
        a.as_ref().iter().zip(b.as_ref()).all(|(a, b)| {
            a.len() == b.len() && a.iter().zip(b).all(|(a, b)| a.px == b.px && a.sz == b.sz && a.n == b.n)
        })
    }

    fn assert_same_books(l2: &L2BookState, l4: &OrderBookState) {
        let non_spot = |books: Vec<&Coin>| books.into_iter().filter(|coin| !coin.is_spot()).cloned().collect::<Vec<_>>();
        assert_eq!(
            non_spot(l2.books.as_ref().keys().collect()),
            non_spot(l4.books().as_ref().keys().collect()),
            "block {}",
            l4.height()
        );
        for (coin, book) in l2.books.as_ref() {
            let l4_book = &l4.books().as_ref()[coin];
            assert!(book.same_levels(l4_book), "block {} {coin:?}", l4.height());
            assert_eq!(book.checksum(), l4_book.checksum(), "block {} {coin:?}", l4.height());
            book.assert_level_sizes();
        }
    }

    /// Replays real blocks the way the listener gets them: each block's diffs go
    /// to the L2 book first, the L4 book follows, at times lagging several blocks.
    /// The books never differ, and the L2 snapshots of demanded coins equal a full
    /// recompute from the L4 book.
    #[test]
    fn l2_book_matches_l4_book_on_real_blocks() {
        let Some((mut l4, blocks)) = load() else { return };
        let start = Instant::now();
        let mut l2 = L2BookState::from_l4(&L4View::full(&l4), [], 0).unwrap();
        let rebuild_time = start.elapsed();
        l4.take_changed();
        assert!(l2.books.as_ref().keys().all(|coin| !coin.is_spot()));
        assert_same_books(&l2, &l4);
        let coin = |name: &str| Coin::new(name);
        // Demand changes mid-stream: SOL (aggregated) and a quiet coin (raw) join at block 100, ETH goes raw at 300.
        let mut demand = HashMap::from([(coin("BTC"), true), (coin("ETH"), true), (coin("HYPE"), false)]);
        let (mut n_blocks, mut n_checks, mut n_ahead, mut n_recomputed) = (0, 0, 0, 0);
        let (mut apply_time, mut check_time, mut l2_time) = (Duration::ZERO, Duration::ZERO, Duration::ZERO);
        let mut prev: HashMap<Coin, Arc<CoinL2Snapshots>> = HashMap::new();
        let mut lagging: VecDeque<&Block> = VecDeque::new();
        let mut plan = CheckPlan::default();
        for block in &blocks {
            let start = Instant::now();
            assert!(l2.apply(&block.1).unwrap());
            apply_time += start.elapsed();
            assert!(!l2.apply(&block.1).unwrap(), "a block applies once");
            n_blocks += 1;
            lagging.push_back(block);
            // The statuses of blocks 10-14 of every 50 come late: the L4 book lags up to 5 blocks.
            if (10..15).contains(&(n_blocks % 50)) {
                n_ahead += 1;
                continue;
            }
            while let Some((statuses, diffs)) = lagging.pop_front() {
                l4.apply_updates(statuses, diffs).unwrap();
                let changed = l4.take_changed();
                let start = Instant::now();
                let divergent = check(&mut l2, &mut plan, &l4, changed, lagging.iter().map(|block| &block.1)).unwrap();
                check_time += start.elapsed();
                assert!(divergent.is_empty(), "block {}: {divergent:?}", l4.height());
                n_checks += 1;
            }
            assert_eq!(l2.height(), l4.height());

            if n_blocks == 100 {
                let quiet = l2.books.as_ref().iter().min_by_key(|(_, book)| book.to_snapshot().as_ref().iter().map(Vec::len).sum::<usize>()).unwrap().0;
                demand.extend([(coin("SOL"), true), (quiet.clone(), false)]);
            }
            if n_blocks == 200 {
                // A newly-listed coin grafted into the L4 book from a fetched snapshot, and an existing book replaced.
                let btc = l4.compute_coin_snapshot(&coin("BTC")).unwrap().2;
                let sol = l4.compute_coin_snapshot(&coin("SOL")).unwrap().2;
                let extras = HashMap::from([(coin("NEWCOIN"), btc), (coin("SOL"), sol)]);
                let coins = extras.keys().cloned().collect();
                l4.absorb_extra_books(extras, true);
                l2.graft(&L4View::of(&l4, coins), []).unwrap();
                assert!(l2.universe().contains("NEWCOIN"));
                demand.insert(coin("NEWCOIN"), false);
            }
            if n_blocks == 300 {
                demand.insert(coin("ETH"), false);
            }

            let start = Instant::now();
            let (time, local_time_us, snapshots, universe) = l2.l2_snapshots(&demand).unwrap();
            l2_time += start.elapsed();
            assert!(l2.l2_snapshots(&demand).is_none(), "snapped state must not re-snapshot");
            assert_eq!((time, local_time_us), (block.1.block_time(), block.1.local_time_us()));
            assert_eq!(universe.len(), l2.books.as_ref().len());
            let full = compute_l2_snapshots(l4.books());
            let snapshots = snapshots.as_ref();
            assert_eq!(snapshots.keys().collect::<HashSet<_>>(), demand.keys().collect::<HashSet<_>>());
            for (coin, got) in snapshots {
                let expected = &full.as_ref()[coin];
                // Coins wanted aggregated have every variant; others the raw one, or every variant
                // (correct for the current book) until the book next changes.
                assert!(got.len() == expected.len() || (got.len() == 1 && !demand[coin]), "{coin:?}");
                for (params, got) in got.iter() {
                    assert!(same_l2(got, &expected[params]), "block {} {coin:?} {params:?} differs", l4.height());
                }
                match prev.get(coin) {
                    Some(old) if Arc::ptr_eq(old, got) => {}
                    _ => n_recomputed += 1,
                }
            }
            prev = snapshots.clone();

            if n_blocks % 10 == 1 {
                assert_same_books(&l2, &l4);
                // Every variant equals the old full-depth chain cut to what clients can request.
                for (coin, got) in snapshots {
                    let expected = compute_coin_l2_snapshots_full_depth(&l4.books().as_ref()[coin]);
                    for (params, got) in got.iter() {
                        let expected = expected[params].truncate(MAX_LEVELS);
                        assert!(got.as_ref().iter().all(|side| side.len() <= MAX_LEVELS));
                        assert!(same_l2(got, &expected), "block {} {coin:?} {params:?} differs", l4.height());
                    }
                }
            }
        }
        assert_eq!(n_blocks, 400, "fixture should hold 400 blocks");
        assert_same_books(&l2, &l4);
        assert_eq!(l2.l2_cache[&coin("ETH")].len(), 1, "ETH went raw and changed since");
        assert!(l2.l2_cache[&coin("SOL")].len() > 1);
        eprintln!(
            "{n_blocks} blocks ({n_ahead} with the L2 book ahead), {n_checks} checks: rebuild {rebuild_time:?}; \
             apply {:?}/block, check {:?}/check, l2 {:?}/block, {:.1} coins recomputed/block",
            apply_time / n_blocks,
            check_time / n_checks,
            l2_time / n_blocks,
            f64::from(n_recomputed) / f64::from(n_blocks),
        );
    }

    /// A differing coin is found and rebuilt, whether the books are at the same
    /// block or the L2 book is ahead (then only after `MAX_UNCHECKED_BLOCKS`).
    #[test]
    fn divergent_coins_are_found_and_rebuilt() {
        let Some((mut l4, blocks)) = load() else { return };
        let mut l2 = L2BookState::from_l4(&L4View::full(&l4), [], 0).unwrap();
        let coin = |name: &str| Coin::new(name);
        // BTC loses a bid, ETH its book; a coin L4 does not have appears.
        let corrupt = |l2: &mut L2BookState| {
            let mut btc = OrderBook::clone(&l2.books.as_ref()[&coin("BTC")]);
            let bid = btc.to_snapshot().as_ref()[0][0].oid();
            assert!(btc.cancel_order(bid));
            l2.books.replace_book(&coin("BTC"), Some(Arc::new(btc)));
            l2.books.replace_book(&coin("ETH"), None);
        };
        corrupt(&mut l2);
        l2.books.replace_book(&coin("BOGUS"), l2.books.as_ref().get(&coin("SOL")).cloned());

        let changed = HashSet::from([coin("BTC"), coin("ETH"), coin("BOGUS"), coin("SOL")]);
        let mut divergent = l2.check(&L4View::of(&l4, changed), []).unwrap();
        divergent.sort();
        assert_eq!(divergent, [coin("BOGUS"), coin("BTC"), coin("ETH")]);
        assert_same_books(&l2, &l4);
        assert!(!l2.universe().contains("BOGUS") && l2.universe().contains("ETH"));

        // The L2 book 3 blocks ahead: checks wait for the L4 book, then check the slow way.
        for (_, diffs) in &blocks[..3] {
            l2.apply(diffs).unwrap();
        }
        corrupt(&mut l2);
        let pending = || blocks[..3].iter().map(|block| &block.1);
        let mut plan = CheckPlan::default();
        for _ in 1..MAX_UNCHECKED_BLOCKS {
            assert!(plan.next(&l4, HashSet::from([coin("BTC")]), l2.height(), &l2.universe()).is_none());
        }
        let view = plan.next(&l4, HashSet::from([coin("ETH")]), l2.height(), &l2.universe()).unwrap();
        let mut divergent = l2.check(&view, pending()).unwrap();
        divergent.sort();
        assert_eq!(divergent, [coin("BTC"), coin("ETH")]);
        for (i, (statuses, diffs)) in blocks[..3].iter().enumerate() {
            l4.apply_updates(statuses, diffs).unwrap();
            let changed = l4.take_changed();
            assert!(check(&mut l2, &mut plan, &l4, changed, pending().skip(i + 1)).unwrap().is_empty());
        }
        assert_same_books(&l2, &l4);

        // Without the diffs of every block in between, the slow check fails.
        for (_, diffs) in &blocks[3..5] {
            l2.apply(diffs).unwrap();
        }
        for _ in 1..MAX_UNCHECKED_BLOCKS {
            check(&mut l2, &mut plan, &l4, HashSet::from([coin("BTC")]), [&blocks[4].1]).unwrap();
        }
        assert!(check(&mut l2, &mut plan, &l4, HashSet::new(), [&blocks[4].1]).is_err());
        // Nor can the L2 book be behind the L4 book.
        let mut l2 = L2BookState::from_l4(&L4View::full(&l4), [], 0).unwrap();
        l4.apply_updates(&blocks[3].0, &blocks[3].1).unwrap();
        let changed = l4.take_changed();
        assert!(l2.check(&L4View::of(&l4, changed), []).is_err());

        // An order level sizes cannot tell from another is found too.
        let mut l2 = L2BookState::from_l4(&L4View::full(&l4), [], 0).unwrap();
        let (statuses, diffs) = &blocks[4];
        l2.apply(diffs).unwrap();
        l4.apply_updates(statuses, diffs).unwrap();
        let mut btc = OrderBook::clone(&l2.books.as_ref()[&coin("BTC")]);
        let mut bid = btc.to_snapshot().as_ref()[0][0].clone();
        assert!(btc.cancel_order(bid.oid()));
        bid.oid = Oid::new(u64::MAX);
        btc.add_order_before(bid, None);
        assert!(btc.same_levels(&*l2.books.as_ref()[&coin("BTC")]));
        l2.books.replace_book(&coin("BTC"), Some(Arc::new(btc)));
        let changed = l4.take_changed();
        assert!(changed.contains(&coin("BTC")));
        assert_eq!(l2.check(&L4View::of(&l4, changed), []).unwrap(), [coin("BTC")]);
        assert_same_books(&l2, &l4);

        // One no diff touches is found by the sweep, within a round.
        let quiet = l2
            .books
            .as_ref()
            .iter()
            .find(|(coin, book)| book.checksum() != 0 && !diffs.events_ref().iter().any(|diff| diff.coin() == **coin))
            .unwrap()
            .0
            .clone();
        l2.books.replace_book(&quiet, None);
        let rounds = l2.books.as_ref().len().div_ceil(SWEEP_COINS) + 1;
        let mut plan = CheckPlan::default();
        let found = (0..rounds)
            .find_map(|_| Some(check(&mut l2, &mut plan, &l4, HashSet::new(), []).unwrap()).filter(|d| !d.is_empty()));
        assert_eq!(found, Some(vec![quiet]));
        assert_same_books(&l2, &l4);
    }

    #[test]
    fn apply_takes_blocks_in_order() {
        let Some((l4, blocks)) = load() else { return };
        // Built with the diffs of later blocks, skipping those it has.
        let mut l2 = L2BookState::from_l4(&L4View::full(&l4), blocks[..3].iter().map(|block| &block.1), 7).unwrap();
        assert_eq!(l2.height(), l4.height() + 3);
        assert!(!l2.apply(&blocks[2].1).unwrap());
        assert!(l2.apply(&blocks[4].1).is_err(), "block 4 before block 3");
        assert!(L2BookState::from_l4(&L4View::full(&l4), [&blocks[1].1], 7).is_err());
        let l2 = L2BookState::from_l4(&L4View::full(&l4), [], 7).unwrap();
        assert_eq!((l2.height(), l2.local_time_us), (l4.height(), 7));
    }
}
