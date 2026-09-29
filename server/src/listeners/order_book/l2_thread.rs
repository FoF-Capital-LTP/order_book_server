//! The L2 book (see `l2_book`) on a thread of its own, which also computes
//! and sends the L2 snapshots.
//!
//! On the listener, a block's L2 work (apply 0.8 ms, compute 1.2 ms p50)
//! delayed the statuses of the same block, which arrive ~1.7 ms after its
//! diffs: orderUpdates went from 3.8 to 5.1 ms p50 (2026-09-29). The listener
//! now only sends this thread the diffs as they are parsed, and after every L4
//! block a view of the books to check (`L4View`, shared copy-on-write).

use crate::{
    latency,
    listeners::order_book::{
        InternalMessage, L2Demand,
        l2_book::{L2BookState, L4View},
    },
    servers::websocket_server::SnapshotShared,
    types::node_data::{Batch, NodeDataOrderDiff},
};
use log::warn;
use std::{
    collections::{HashSet, VecDeque},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc,
    },
    time::Instant,
};
use tokio::sync::broadcast::Sender;

/// Threads of the L2 thread's own rayon pool, which keeps its parallel work
/// from queueing behind the listener's (and the reverse).
const POOL_THREADS: usize = 8;

/// Diffs kept for rebuilds while no L4 view comes (e.g. before the first
/// snapshot): ~70 s of blocks.
const MAX_PENDING: usize = 1000;

/// Coins the L2 book differed from the L4 book in since start.
pub(super) static L2_DIVERGENT_TOTAL: AtomicU64 = AtomicU64::new(0);

pub(super) enum L2Input {
    /// A block's diffs, as parsed.
    Diffs(Batch<NodeDataOrderDiff>),
    /// The L4 book's books to check the L2 book against (see `CheckPlan`).
    Check(L4View),
    /// A full view to rebuild the L2 book from, with the hl-node write time (µs) of its block.
    Rebuild(L4View, u64),
    /// The books of coins grafted into the L4 book.
    Graft(L4View),
    /// The L4 book was dropped.
    Clear,
    /// Answered once everything sent before is handled and its snapshot sent.
    #[cfg(test)]
    Barrier(mpsc::Sender<()>),
}

/// What the listener reads of the L2 thread.
struct Shared {
    /// Coins clients may subscribe to; None without an L2 book.
    universe: Mutex<Option<Arc<HashSet<String>>>>,
    /// Set when the L2 book is dropped, for the listener to send a full view.
    needs_rebuild: AtomicBool,
}

pub(super) struct L2Thread {
    tx: mpsc::Sender<L2Input>,
    shared: Arc<Shared>,
}

impl L2Thread {
    /// Runs until the returned handle is dropped.
    pub(super) fn spawn(internal_message_tx: Option<Sender<Arc<InternalMessage>>>, demand: Arc<L2Demand>) -> Self {
        let (tx, rx) = mpsc::channel();
        let shared = Arc::new(Shared { universe: Mutex::new(None), needs_rebuild: AtomicBool::new(true) });
        let mut worker = Worker {
            l2: None,
            pending: VecDeque::new(),
            shared: shared.clone(),
            internal_message_tx,
            demand,
            last_snapshot_shared: None,
        };
        #[allow(clippy::expect_used)]
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(POOL_THREADS)
            .thread_name(|i| format!("l2-book-{i}"))
            .build()
            .expect("L2 book thread pool");
        #[allow(clippy::expect_used)]
        std::thread::Builder::new()
            .name("l2-book".into())
            .spawn(move || pool.install(move || worker.run(&rx)))
            .expect("L2 book thread");
        Self { tx, shared }
    }

    pub(super) fn send(&self, input: L2Input) {
        // Only fails if the thread panicked, which the panic already reported.
        let _unused = self.tx.send(input);
    }

    /// Coins clients may subscribe to; None without an L2 book.
    #[allow(clippy::unwrap_used)]
    pub(super) fn universe(&self) -> Option<Arc<HashSet<String>>> {
        self.shared.universe.lock().unwrap().clone()
    }

    /// Whether the L2 book needs a full view (see `L2Input::Rebuild`); true once per drop.
    pub(super) fn take_needs_rebuild(&self) -> bool {
        self.shared.needs_rebuild.swap(false, Ordering::Relaxed)
    }

    /// Drops the L2 book, as the L4 book was.
    pub(super) fn clear(&self) {
        self.shared.needs_rebuild.store(true, Ordering::Relaxed);
        self.send(L2Input::Clear);
    }
}

struct Worker {
    l2: Option<L2BookState>,
    /// Diffs of the blocks after the last L4 view's, in order.
    pending: VecDeque<Batch<NodeDataOrderDiff>>,
    shared: Arc<Shared>,
    internal_message_tx: Option<Sender<Arc<InternalMessage>>>,
    demand: Arc<L2Demand>,
    /// Of the last L2 snapshot message: tells the next one which frames clients want.
    last_snapshot_shared: Option<Arc<SnapshotShared>>,
}

impl Worker {
    /// Handles everything queued, then sends the snapshot of the resulting
    /// book, so a backlog is caught up with one snapshot.
    fn run(&mut self, rx: &mpsc::Receiver<L2Input>) {
        #[cfg(test)]
        let mut barriers = Vec::new();
        while let Ok(input) = rx.recv() {
            let mut next = Some(input);
            while let Some(input) = next {
                match input {
                    #[cfg(test)]
                    L2Input::Barrier(done) => barriers.push(done),
                    input => self.handle(input),
                }
                next = rx.try_recv().ok();
            }
            self.publish_universe();
            self.broadcast_snapshot();
            #[cfg(test)]
            for done in barriers.drain(..) {
                let _unused = done.send(());
            }
        }
    }

    fn handle(&mut self, input: L2Input) {
        match input {
            L2Input::Diffs(diffs) => {
                latency::L2_DIFFS_AFTER_WRITE_US.record_age_us(diffs.local_time_us());
                if let Some(l2) = self.l2.as_mut() {
                    let start = Instant::now();
                    match l2.apply(&diffs) {
                        Ok(true) => latency::L2_APPLY_US.record_duration_us(start.elapsed()),
                        Ok(false) => {}
                        Err(err) => self.drop_l2(&format!("dropping the L2 book, to rebuild from the L4 book: {err}")),
                    }
                }
                // Skipped as by the listener's cache.
                if self.pending.back().is_some_and(|last| last.block_number() >= diffs.block_number()) {
                    return;
                }
                if self.pending.len() >= MAX_PENDING {
                    self.pending.pop_front();
                }
                self.pending.push_back(diffs);
            }
            L2Input::Check(l4) => {
                self.prune_pending(l4.height());
                let Some(l2) = self.l2.as_mut() else { return };
                let (start, height) = (Instant::now(), l4.height());
                // Dropped right after: the L4 book copies a book it shares on its next write to it.
                match l2.check(&l4, &self.pending) {
                    Ok(divergent) => {
                        latency::L2_CHECK_US.record_duration_us(start.elapsed());
                        if !divergent.is_empty() {
                            let n = divergent.len() as u64;
                            latency::L2_DIVERGENT_COINS.record(n);
                            let prev = L2_DIVERGENT_TOTAL.fetch_add(n, Ordering::Relaxed);
                            // Each time the total passes a power of two.
                            if prev.checked_ilog2() != (prev + n).checked_ilog2() {
                                warn!(
                                    "[l2-book] L2 book differed from the L4 book at block {height}, rebuilt {divergent:?} ({} coins so far)",
                                    prev + n,
                                );
                            }
                        }
                    }
                    Err(err) => self.drop_l2(&format!("rebuilding the L2 book from the L4 book: {err}")),
                }
            }
            L2Input::Rebuild(l4, local_time_us) => {
                self.prune_pending(l4.height());
                let start = Instant::now();
                let l2 = L2BookState::from_l4(&l4, &self.pending, local_time_us).or_else(|err| {
                    warn!("[l2-book] L2 book rebuilt without the diffs of later blocks: {err}");
                    L2BookState::from_l4(&l4, [], local_time_us)
                });
                latency::L2_REBUILD_US.record_duration_us(start.elapsed());
                match l2 {
                    Ok(l2) => self.l2 = Some(l2),
                    Err(err) => self.drop_l2(&format!("L2 book not rebuilt: {err}")),
                }
            }
            L2Input::Graft(l4) => {
                let Some(l2) = self.l2.as_mut() else { return };
                if let Err(err) = l2.graft(&l4, &self.pending) {
                    self.drop_l2(&format!("dropping the L2 book, to rebuild from the L4 book: {err}"));
                }
            }
            L2Input::Clear => self.l2 = None,
            #[cfg(test)]
            L2Input::Barrier(_) => unreachable!(),
        }
    }

    fn drop_l2(&mut self, why: &str) {
        warn!("[l2-book] {why}");
        self.l2 = None;
        self.shared.needs_rebuild.store(true, Ordering::Relaxed);
    }

    fn prune_pending(&mut self, height: u64) {
        while self.pending.front().is_some_and(|diffs| diffs.block_number() <= height) {
            self.pending.pop_front();
        }
    }

    #[allow(clippy::unwrap_used)]
    fn publish_universe(&self) {
        *self.shared.universe.lock().unwrap() = self.l2.as_ref().map(L2BookState::universe);
    }

    /// Sends the L2 snapshot of the L2 book's height unless already sent.
    fn broadcast_snapshot(&mut self) {
        let Some(l2) = self.l2.as_mut() else { return };
        let Some((time, local_time_us, l2_snapshots, universe)) = l2.l2_snapshots(&self.demand.coins()) else {
            return;
        };
        let Some(tx) = &self.internal_message_tx else { return };
        let shared = Arc::new(SnapshotShared::following(self.last_snapshot_shared.as_deref()));
        self.last_snapshot_shared = Some(shared.clone());
        let _unused =
            tx.send(Arc::new(InternalMessage::Snapshot { l2_snapshots, time, local_time_us, shared, universe }));
    }
}

#[cfg(test)]
impl L2Thread {
    /// Waits until everything sent so far is handled.
    pub(super) fn sync(&self) {
        let (tx, rx) = mpsc::channel();
        self.send(L2Input::Barrier(tx));
        rx.recv().unwrap();
    }
}
