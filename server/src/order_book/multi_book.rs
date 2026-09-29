use crate::{
    order_book::{Coin, InnerOrder, OrderBook, Snapshot},
    prelude::*,
};
use rayon::iter::{IntoParallelIterator, IntoParallelRefIterator, ParallelIterator};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    path::Path,
    sync::Arc,
};
use tokio::fs::read_to_string;

pub(crate) struct Snapshots<O>(HashMap<Coin, Snapshot<O>>);

impl<O> Snapshots<O> {
    pub(crate) const fn new(value: HashMap<Coin, Snapshot<O>>) -> Self {
        Self(value)
    }

    pub(crate) const fn as_ref(&self) -> &HashMap<Coin, Snapshot<O>> {
        &self.0
    }

    pub(crate) fn value(self) -> HashMap<Coin, Snapshot<O>> {
        self.0
    }
}

/// Books are shared copy-on-write like their price levels, so `clone()` is
/// O(coins): the per-minute validation copy used to deep-clone ~550k orders in
/// ~364k levels under the listener mutex (~40 ms even in parallel, 2026-09-29).
/// A book is copied (its oid index and level map, not its orders) on its first
/// write after a clone, in `par_update_books`'s parallel section.
#[derive(Clone)]
pub(crate) struct OrderBooks<O> {
    order_books: BTreeMap<Coin, Arc<OrderBook<O>>>,
    /// Coins whose book may have changed since the last `take_changed`. Every
    /// mutating method below must record its coin here: the listener's
    /// incremental L2 cache only recomputes these.
    changed: HashSet<Coin>,
}

impl<O> Default for OrderBooks<O> {
    fn default() -> Self {
        Self { order_books: BTreeMap::new(), changed: HashSet::new() }
    }
}

impl<O: InnerOrder> OrderBooks<O> {
    pub(crate) const fn as_ref(&self) -> &BTreeMap<Coin, Arc<OrderBook<O>>> {
        &self.order_books
    }
    #[must_use]
    pub(crate) fn from_snapshots(snapshot: Snapshots<O>, ignore_triggers: bool) -> Self {
        Self {
            order_books: snapshot
                .value()
                .into_iter()
                .map(|(coin, book)| (coin, Arc::new(OrderBook::from_snapshot(book, ignore_triggers))))
                .collect(),
            changed: HashSet::new(),
        }
    }

    fn mark_changed(&mut self, coin: &Coin) {
        if !self.changed.contains(coin) {
            self.changed.insert(coin.clone());
        }
    }

    /// Coins whose book may have changed since the previous call.
    pub(crate) fn take_changed(&mut self) -> HashSet<Coin> {
        std::mem::take(&mut self.changed)
    }

    /// Graft a fetched snapshot for a new coin into this multi-book.
    /// Used to absorb coins that appeared in the authoritative snapshot but
    /// were not yet tracked locally (e.g. newly-listed assets), avoiding a
    /// listener restart. Caller is responsible for ensuring `coin` is not
    /// already tracked.
    pub(crate) fn insert_book(&mut self, coin: Coin, snapshot: Snapshot<O>, ignore_triggers: bool) {
        self.mark_changed(&coin);
        self.order_books.insert(coin, Arc::new(OrderBook::from_snapshot(snapshot, ignore_triggers)));
    }

    /// Starts tracking `coin` with `book`, which the caller built up from its diffs.
    pub(crate) fn add_book(&mut self, coin: Coin, book: OrderBook<O>) {
        self.mark_changed(&coin);
        self.order_books.insert(coin, Arc::new(book));
    }

    /// The books of those of `coins` this has, shared; none count as changed.
    // Anonymous lifetimes in `impl Trait` arguments are not stable yet.
    #[allow(single_use_lifetimes)]
    pub(crate) fn subset<'a>(&self, coins: impl IntoIterator<Item = &'a Coin>) -> Self {
        let order_books =
            coins.into_iter().filter_map(|coin| self.order_books.get_key_value(coin)).map(|(c, b)| (c.clone(), b.clone()));
        Self { order_books: order_books.collect(), changed: HashSet::new() }
    }

    /// Replaces (None: stops tracking) the book of `coin`.
    pub(crate) fn replace_book(&mut self, coin: &Coin, book: Option<Arc<OrderBook<O>>>) {
        self.mark_changed(coin);
        match book {
            Some(book) => self.order_books.insert(coin.clone(), book),
            None => self.order_books.remove(coin),
        };
    }
}

impl<O: Send + Sync + InnerOrder> OrderBooks<O> {
    #[must_use]
    pub(crate) fn to_snapshots_par(&self) -> Snapshots<O> {
        let snapshots = self.order_books.par_iter().map(|(c, book)| (c.clone(), book.to_snapshot())).collect();
        Snapshots(snapshots)
    }

    /// Runs `update` on the book of each coin in `work`, in parallel, and marks those
    /// coins changed. Returns the results, and the work for coins without a book.
    pub(crate) fn par_update_books<'w, W: Send, R: Send>(
        &mut self,
        mut work: HashMap<&'w str, W>,
        update: impl Fn(&mut OrderBook<O>, W) -> R + Sync,
    ) -> (Vec<R>, HashMap<&'w str, W>) {
        let mut jobs = Vec::with_capacity(work.len());
        for (coin, book) in &mut self.order_books {
            if work.is_empty() {
                break;
            }
            if let Some(w) = work.remove(coin.as_str()) {
                if !self.changed.contains(coin) {
                    self.changed.insert(coin.clone());
                }
                jobs.push((book, w));
            }
        }
        let results = jobs.into_par_iter().map(|(book, w)| update(Arc::make_mut(book), w)).collect();
        (results, work)
    }

    /// The books of the coins `keep` accepts with every order mapped by `f`,
    /// built in parallel; all of them count as changed.
    pub(crate) fn par_map<P: InnerOrder + Send + Sync>(
        &self,
        keep: impl Fn(&Coin) -> bool + Sync,
        f: impl Fn(&O) -> P + Sync,
    ) -> OrderBooks<P> {
        let order_books: BTreeMap<_, _> = self
            .order_books
            .par_iter()
            .filter(|(coin, _)| keep(coin))
            .map(|(coin, book)| (coin.clone(), Arc::new(book.map_orders(&f))))
            .collect();
        let changed = order_books.keys().cloned().collect();
        OrderBooks { order_books, changed }
    }
}

pub(crate) fn load_snapshots_from_str<O, R>(str: &str) -> Result<(u64, Snapshots<O>)>
where
    O: TryFrom<R, Error = Error>,
    R: Serialize + for<'a> Deserialize<'a>,
{
    #[allow(clippy::type_complexity)]
    let (height, snapshot): (u64, Vec<(String, [Vec<R>; 2])>) = serde_json::from_str(str)?;
    Ok((
        height,
        Snapshots::new(
            snapshot
                .into_iter()
                .map(|(coin, [bids, asks])| {
                    let bids: Vec<O> = bids.into_iter().map(O::try_from).collect::<Result<Vec<O>>>()?;
                    let asks: Vec<O> = asks.into_iter().map(O::try_from).collect::<Result<Vec<O>>>()?;
                    Ok((Coin::new(&coin), Snapshot([bids, asks])))
                })
                .collect::<Result<HashMap<Coin, Snapshot<O>>>>()?,
        ),
    ))
}

pub(crate) async fn load_snapshots_from_json<O, R>(path: &Path) -> Result<(u64, Snapshots<O>)>
where
    O: TryFrom<R, Error = Error> + Send + 'static,
    R: Serialize + for<'a> Deserialize<'a> + 'static,
{
    let file_contents = read_to_string(path).await?;
    // Parsing the full node snapshot (~370 MB) takes about a second; keep it off the async workers.
    tokio::task::spawn_blocking(move || load_snapshots_from_str(&file_contents)).await?
}

#[cfg(test)]
mod tests {
    use crate::{
        order_book::{
            InnerOrder, OrderBook, Px, Side, Snapshot, Sz,
            levels::build_l2_level,
            multi_book::{Coin, Snapshots, load_snapshots_from_json, load_snapshots_from_str},
        },
        prelude::*,
        types::{
            L4Order, Level,
            inner::{InnerL4Order, InnerLevel},
        },
    };
    use alloy::primitives::Address;
    use itertools::Itertools;
    use std::{fs::create_dir_all, path::PathBuf};

    #[must_use]
    fn snapshot_to_l2_snapshot<O: InnerOrder>(
        snapshot: &Snapshot<O>,
        n_levels: Option<usize>,
        n_sig_figs: Option<u32>,
        mantissa: Option<u64>,
    ) -> Snapshot<InnerLevel> {
        let [bids, asks] = &snapshot.0;
        let bids = orders_to_l2_levels(bids, Side::Bid, n_levels, n_sig_figs, mantissa);
        let asks = orders_to_l2_levels(asks, Side::Ask, n_levels, n_sig_figs, mantissa);
        Snapshot([bids, asks])
    }

    #[must_use]
    fn orders_to_l2_levels<O: InnerOrder>(
        orders: &[O],
        side: Side,
        n_levels: Option<usize>,
        n_sig_figs: Option<u32>,
        mantissa: Option<u64>,
    ) -> Vec<InnerLevel> {
        let mut levels = Vec::new();
        if n_levels == Some(0) {
            return levels;
        }
        let mut cur_level: Option<InnerLevel> = None;

        for order in orders {
            if build_l2_level(
                &mut cur_level,
                &mut levels,
                n_levels,
                n_sig_figs,
                mantissa,
                side,
                InnerLevel { px: order.limit_px(), sz: order.sz(), n: 1 },
            ) {
                break;
            }
        }
        levels.extend(cur_level.take());
        levels
    }

    #[derive(Default)]
    struct OrderManager {
        next_oid: u64,
    }

    fn simple_inner_order(oid: u64, side: Side, sz: String, px: String) -> Result<InnerL4Order> {
        let px = Px::parse_from_str(&px)?;
        let sz = Sz::parse_from_str(&sz)?;
        Ok(InnerL4Order {
            user: Address::new([0; 20]),
            coin: Coin::new(""),
            side,
            limit_px: px,
            sz,
            oid,
            timestamp: 0,
            trigger_condition: String::new(),
            is_trigger: false,
            trigger_px: String::new(),
            is_position_tpsl: false,
            reduce_only: false,
            order_type: String::new(),
            tif: None,
            cloid: None,
        })
    }

    impl OrderManager {
        fn order(&mut self, sz: &str, limit_px: &str, side: Side) -> Result<InnerL4Order> {
            let order = simple_inner_order(self.next_oid, side, sz.to_string(), limit_px.to_string())?;
            self.next_oid += 1;
            Ok(order)
        }

        fn batch_order(&mut self, sz: &str, limit_px: &str, side: Side, mult: u64) -> Result<Vec<InnerL4Order>> {
            (0..mult).map(|_| self.order(sz, limit_px, side)).try_collect()
        }
    }

    fn setup_book(book: &mut OrderBook<InnerL4Order>) -> Snapshots<InnerL4Order> {
        let mut o = OrderManager::default();
        let buy_orders1 = o.batch_order("100", "34.01", Side::Bid, 4).unwrap();
        let buy_orders2 = o.batch_order("200", "34.5", Side::Bid, 2).unwrap();
        let buy_orders3 = o.batch_order("300", "34.6", Side::Bid, 1).unwrap();
        let sell_orders1 = o.batch_order("100", "35", Side::Ask, 4).unwrap();
        let sell_orders2 = o.batch_order("200", "35.1", Side::Ask, 2).unwrap();
        let sell_orders3 = o.batch_order("300", "35.5", Side::Ask, 1).unwrap();
        for orders in [buy_orders1, buy_orders2, buy_orders3, sell_orders1, sell_orders2, sell_orders3] {
            for o in orders {
                book.add_order(o);
            }
        }
        Snapshots(vec![(Coin::new(""), book.to_snapshot()); 2].into_iter().collect())
    }

    const SNAPSHOT_JSON: &str = r#"[100, 
    [
        [
            "@1",
            [
                [
                    [
                        "0x0000000000000000000000000000000000000000",
                        {
                            "coin": "@1",
                            "side": "B",
                            "limitPx": "30.444",
                            "sz": "100.0",
                            "oid": 105338503859,
                            "timestamp": 1750660644034,
                            "triggerCondition": "N/A",
                            "isTrigger": false,
                            "triggerPx": "0.0",
                            "children": [],
                            "isPositionTpsl": false,
                            "reduceOnly": false,
                            "orderType": "Limit",
                            "origSz": "100.0",
                            "tif": "Alo",
                            "cloid": null
                        }
                    ],
                    [
                        "0x0000000000000000000000000000000000000000",
                        {
                            "coin": "@1",
                            "side": "B",
                            "limitPx": "30.385",
                            "sz": "5.45",
                            "oid": 105337808436,
                            "timestamp": 1750660453608,
                            "triggerCondition": "N/A",
                            "isTrigger": false,
                            "triggerPx": "0.0",
                            "children": [],
                            "isPositionTpsl": false,
                            "reduceOnly": false,
                            "orderType": "Limit",
                            "origSz": "5.45",
                            "tif": "Gtc",
                            "cloid": null
                        }
                    ]
                ],
                []
            ]
        ]
    ]
]"#;

    #[tokio::test]
    async fn test_deserialization_from_json() -> Result<()> {
        create_dir_all("tmp/deserialization_test")?;
        fs::write("tmp/deserialization_test/out.json", SNAPSHOT_JSON)?;
        load_snapshots_from_json::<InnerL4Order, (Address, L4Order)>(&PathBuf::from(
            "tmp/deserialization_test/out.json",
        ))
        .await?;
        Ok(())
    }

    #[test]
    fn test_deserialization() -> Result<()> {
        load_snapshots_from_str::<InnerL4Order, (Address, L4Order)>(SNAPSHOT_JSON)?;
        Ok(())
    }

    #[test]
    fn test_l4_snapshot_to_l2_snapshot() {
        let mut book = OrderBook::new();
        let coin = Coin::new("");
        let snapshot = setup_book(&mut book);
        let levels = snapshot_to_l2_snapshot(snapshot.0.get(&coin).unwrap(), Some(2), Some(2), Some(1));
        let raw_levels = levels.export_inner_snapshot();
        let ans = [
            vec![Level::new("34".to_string(), "1100".to_string(), 7)],
            vec![
                Level::new("35".to_string(), "400".to_string(), 4),
                Level::new("36".to_string(), "700".to_string(), 3),
            ],
        ];
        assert_eq!(ans, raw_levels);

        let levels = snapshot_to_l2_snapshot(snapshot.0.get(&coin).unwrap(), Some(2), Some(3), Some(5));
        let raw_levels = levels.export_inner_snapshot();
        let ans = [
            vec![
                Level::new("34.5".to_string(), "700".to_string(), 3),
                Level::new("34".to_string(), "400".to_string(), 4),
            ],
            vec![
                Level::new("35".to_string(), "400".to_string(), 4),
                Level::new("35.5".to_string(), "700".to_string(), 3),
            ],
        ];
        assert_eq!(ans, raw_levels);
        let snapshot_from_book = book.to_l2_snapshot(Some(2), Some(3), Some(5));
        let raw_levels_from_book = snapshot_from_book.export_inner_snapshot();
        let snapshot_from_book = book.to_l2_snapshot(None, None, None);
        let snapshot_from_snapshot = snapshot_from_book.to_l2_snapshot(Some(2), Some(3), Some(5));
        let raw_levels_from_snapshot = snapshot_from_snapshot.export_inner_snapshot();
        assert_eq!(raw_levels_from_book, ans);
        assert_eq!(raw_levels_from_snapshot, ans);

        let levels = snapshot_to_l2_snapshot(snapshot.0.get(&coin).unwrap(), Some(2), None, Some(5));
        let raw_levels = levels.export_inner_snapshot();
        let ans = [
            vec![
                Level::new("34.6".to_string(), "300".to_string(), 1),
                Level::new("34.5".to_string(), "400".to_string(), 2),
            ],
            vec![
                Level::new("35".to_string(), "400".to_string(), 4),
                Level::new("35.1".to_string(), "400".to_string(), 2),
            ],
        ];
        assert_eq!(ans, raw_levels);
    }
}

#[cfg(test)]
mod clone_test {
    use super::*;
    use crate::types::{L4Order, inner::InnerL4Order};
    use alloy::primitives::Address;
    use itertools::Itertools;
    use std::{fs, time::Instant};

    /// Real hl-node L4 snapshot (copy of /root/out.json); test-only, not committed.
    const FIXTURE: &str = "tmp/fixture/out.snap.json";

    #[test]
    fn clone_is_shallow_on_real_snapshot() {
        let Ok(json) = fs::read_to_string(FIXTURE) else {
            eprintln!("skipping: {FIXTURE} not present");
            return;
        };
        let (_, snapshot) = load_snapshots_from_str::<InnerL4Order, (Address, L4Order)>(&json).unwrap();
        drop(json);
        let books = OrderBooks::from_snapshots(snapshot, true);
        let n_orders: usize = books.order_books.values().map(|b| b.oid_to_side_px.len()).sum();
        let n_levels: usize = books.order_books.values().map(|b| b.bids.len() + b.asks.len()).sum();
        assert!(books.order_books.len() > 100 && n_orders > 100_000, "fixture too small: {n_orders}");

        let start = Instant::now();
        let copy = books.clone();
        let clone_time = start.elapsed();
        // What the first write to each book after a clone copies: its oid index and level map.
        let start = Instant::now();
        let shallow = books.order_books.values().map(|book| OrderBook::clone(book)).collect_vec();
        let book_copy_time = start.elapsed();
        eprintln!(
            "{} coins, {n_orders} orders in {n_levels} levels: clone {clone_time:?}; copying every book's index and level map {book_copy_time:?}",
            books.order_books.len(),
        );
        assert!(books.order_books.values().zip(copy.order_books.values()).all(|(a, b)| Arc::ptr_eq(a, b)));
        for (book, copy) in books.order_books.values().zip(&shallow) {
            assert!(book.oid_to_side_px == copy.oid_to_side_px);
            assert!(book.bids.iter().zip(&copy.bids).all(|((pa, a), (pb, b))| pa == pb && Arc::ptr_eq(a, b)));
            assert!(book.asks.iter().zip(&copy.asks).all(|((pa, a), (pb, b))| pa == pb && Arc::ptr_eq(a, b)));
        }
    }
}
