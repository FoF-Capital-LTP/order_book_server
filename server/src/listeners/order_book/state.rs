use crate::{
    listeners::order_book::{L2Snapshots, TimedSnapshots, utils::compute_l2_snapshots},
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
use log::{error, warn};
use std::collections::{HashMap, HashSet, VecDeque};

/// Don't re-warn about the same not-yet-grafted coin more often than this
/// many blocks. ~14.5 blocks/s ⇒ 200 blocks ≈ 14 seconds. Keeps the log
/// readable when a high-activity new coin appears between snapshot fetches
/// (default fetch interval is 60 s).
const NOT_YET_GRAFTED_WARN_THROTTLE_BLOCKS: u64 = 200;

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

    // forcibly take snapshot - (time, height, snapshot)
    pub(super) fn compute_snapshot(&self) -> TimedSnapshots {
        TimedSnapshots { time: self.time, height: self.height, snapshot: self.order_book.to_snapshots_par() }
    }

    // (time, snapshot)
    pub(super) fn l2_snapshots(&mut self, prevent_future_snaps: bool) -> Option<(u64, L2Snapshots)> {
        if self.snapped {
            None
        } else {
            self.snapped = prevent_future_snaps || self.snapped;
            Some((self.time, compute_l2_snapshots(&self.order_book)))
        }
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
        order_statuses: Batch<NodeDataOrderStatus>,
        order_diffs: Batch<NodeDataOrderDiff>,
    ) -> Result<()> {
        let height = order_statuses.block_number();
        let time = order_statuses.block_time();
        assert_eq!(order_statuses.block_number(), order_diffs.block_number());
        if height > self.height + 1 {
            if self.allow_initial_gap {
                // First gap after snapshot init — expected when the consumer
                // starts at EOF of the current hour-file. The snapshot gives
                // us authoritative state at self.height; blocks between
                // self.height+1 and height-1 were written before we started
                // reading and are safe to skip. Jump forward and continue.
                warn!(
                    "[fresh-start] accepting initial gap: self.height={} expected={} got={} gap_blocks={} — snapshot state is authoritative",
                    self.height,
                    self.height + 1,
                    height,
                    height.saturating_sub(self.height),
                );
                self.allow_initial_gap = false;
                self.height = height - 1;
                // Fall through to normal processing below
            } else {
                // Diagnostic for the recurring 01:00 UTC hour-rollover fatal
                // (see [hour-rollover-diag] elsewhere). Logs the gap shape so we
                // can tell whether this is a small drift (off-by-one race during
                // file rotation) or a full ~one-hour jump (drain skipped / new
                // file opens at chain tip). gap_blocks ≈ 52507 ≈ 1h*14.5 bps in
                // the 06-01 occurrence.
                error!(
                    "[hour-rollover-diag] apply_updates gap detected: self.height={} expected={} got={} gap_blocks={} block_time_ms={}",
                    self.height,
                    self.height + 1,
                    height,
                    height.saturating_sub(self.height),
                    time,
                );
                return Err(format!("Expecting block {}, got block {}", self.height + 1, height).into());
            }
        } else if height <= self.height {
            // This is not an error in case we started caching long before a snapshot is fetched
            return Ok(());
        }
        // A sequential block arrived — clear the initial-gap grace period.
        // From now on, any forward gap is a real data integrity issue.
        self.allow_initial_gap = false;
        let mut diffs = order_diffs.events().into_iter().collect::<VecDeque<_>>();
        let mut order_map = order_statuses
            .events()
            .into_iter()
            .filter_map(|order_status| {
                if order_status.is_inserted_into_book() {
                    Some((Oid::new(order_status.order.oid), order_status))
                } else {
                    None
                }
            })
            .collect::<HashMap<_, _>>();
        while let Some(diff) = diffs.pop_front() {
            let oid = diff.oid();
            let coin = diff.coin();
            if coin.is_spot() && self.ignore_spot {
                continue;
            }
            let inner_diff = diff.diff().try_into()?;
            match inner_diff {
                InnerOrderDiff::New { sz } => {
                    if let Some(order) = order_map.remove(&oid) {
                        let time = order.time.and_utc().timestamp_millis();
                        let mut inner_order: InnerL4Order = order.try_into()?;
                        inner_order.modify_sz(sz);
                        // must replace time with time of entering book, which is the timestamp of the order status update
                        #[allow(clippy::unwrap_used)]
                        inner_order.convert_trigger(time.try_into().unwrap());
                        // For stop market/limit triggers, status.order.limitPx is the trigger
                        // condition price, not the resting price on the book. The actual price
                        // the order rests at is on the diff event itself. For ordinary limit
                        // orders the two are equal, so this is a no-op there.
                        inner_order.limit_px = Px::parse_from_str(diff.px())?;
                        self.order_book.add_order(inner_order);
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
