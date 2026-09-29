//! Which coins some client wants L2 snapshots of, and which of those it wants
//! an aggregated (sig-fig) variant of.
//!
//! Clients subscribe to L2 of a small share of the ~1400 coins, yet the raw L2
//! of every changed book used to be computed each block, and the six sig-fig
//! variants each walk the whole book. The listener computes only what
//! `L2Demand::coins` asks for.

use crate::{order_book::Coin, types::subscription::Subscription};
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex, MutexGuard, PoisonError},
};

/// Per coin: clients with an L2 subscription to it, and those among them with an aggregated one.
#[derive(Default, Debug, PartialEq, Eq)]
struct Counts {
    clients: usize,
    aggregated: usize,
}

#[derive(Default)]
pub(crate) struct L2Demand(Mutex<HashMap<Coin, Counts>>);

impl L2Demand {
    fn lock(&self) -> MutexGuard<'_, HashMap<Coin, Counts>> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Coins that need L2 snapshots; true if they need the aggregated variants too.
    pub(crate) fn coins(&self) -> HashMap<Coin, bool> {
        self.lock().iter().map(|(coin, counts)| (coin.clone(), counts.aggregated > 0)).collect()
    }
}

/// One client's share of `L2Demand`; released when the client goes away.
pub(crate) struct ClientL2Demand {
    demand: Arc<L2Demand>,
    /// Coins the client subscribes to L2 of; true if to an aggregated variant.
    coins: HashMap<Coin, bool>,
}

impl ClientL2Demand {
    pub(crate) fn new(demand: Arc<L2Demand>) -> Self {
        Self { demand, coins: HashMap::new() }
    }

    /// Brings the shared counts in line with the client's current subscriptions.
    pub(crate) fn sync(&mut self, subscriptions: &HashSet<Subscription>) {
        let mut coins: HashMap<Coin, bool> = HashMap::new();
        for sub in subscriptions {
            if let Subscription::L2Book { coin, n_sig_figs, mantissa, .. } = sub {
                *coins.entry(Coin::new(coin)).or_default() |= n_sig_figs.is_some() || mantissa.is_some();
            }
        }
        if coins == self.coins {
            return;
        }
        let mut counts = self.demand.lock();
        for (coin, &aggregated) in &self.coins {
            release(&mut counts, coin, aggregated);
        }
        for (coin, &aggregated) in &coins {
            let entry = counts.entry(coin.clone()).or_default();
            entry.clients += 1;
            entry.aggregated += usize::from(aggregated);
        }
        drop(counts);
        self.coins = coins;
    }
}

fn release(counts: &mut HashMap<Coin, Counts>, coin: &Coin, aggregated: bool) {
    if let Some(entry) = counts.get_mut(coin) {
        entry.clients -= 1;
        entry.aggregated -= usize::from(aggregated);
        if entry.clients == 0 {
            counts.remove(coin);
        }
    }
}

impl Drop for ClientL2Demand {
    fn drop(&mut self) {
        let mut counts = self.demand.lock();
        for (coin, &aggregated) in &self.coins {
            release(&mut counts, coin, aggregated);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn l2(coin: &str, n_sig_figs: Option<u32>, n_levels: Option<usize>) -> Subscription {
        Subscription::L2Book { coin: coin.into(), n_sig_figs, n_levels, mantissa: None }
    }

    fn sorted(demand: &L2Demand) -> Vec<(String, bool)> {
        let mut coins: Vec<_> = demand.coins().into_iter().map(|(coin, aggregated)| (coin.value(), aggregated)).collect();
        coins.sort();
        coins
    }

    fn want(coins: &[(&str, bool)]) -> Vec<(String, bool)> {
        coins.iter().map(|&(coin, aggregated)| (coin.to_string(), aggregated)).collect()
    }

    #[test]
    fn counts_follow_subscriptions_and_disconnects() {
        let demand = Arc::new(L2Demand::default());
        let mut a = ClientL2Demand::new(demand.clone());
        let mut b = ClientL2Demand::new(demand.clone());
        a.sync(&[l2("BTC", None, None), l2("ETH", None, None)].into());
        assert_eq!(sorted(&demand), want(&[("BTC", false), ("ETH", false)]), "raw L2 needs no aggregated variants");

        a.sync(&[l2("BTC", Some(5), None), l2("BTC", Some(3), Some(10)), l2("ETH", None, None)].into());
        b.sync(&[l2("BTC", Some(4), None), l2("SOL", Some(2), None), Subscription::Trades { coin: "ETH".into() }].into());
        assert_eq!(sorted(&demand), want(&[("BTC", true), ("ETH", false), ("SOL", true)]));

        // One of a's two BTC subscriptions goes; BTC is still wanted by both clients.
        a.sync(&[l2("BTC", Some(3), Some(10))].into());
        assert_eq!(demand.lock()[&Coin::new("BTC")], Counts { clients: 2, aggregated: 2 });
        // a keeps BTC but only raw; b still wants it aggregated.
        a.sync(&[l2("BTC", None, None)].into());
        assert_eq!(demand.lock()[&Coin::new("BTC")], Counts { clients: 2, aggregated: 1 });
        drop(b);
        assert_eq!(sorted(&demand), want(&[("BTC", false)]));
        a.sync(&HashSet::new());
        assert!(sorted(&demand).is_empty());

        a.sync(&[l2("HYPE", Some(5), None)].into());
        drop(a);
        assert!(demand.lock().is_empty());
    }
}
