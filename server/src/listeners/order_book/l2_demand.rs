//! Which coins some client wants an aggregated (sig-fig) L2 variant of.
//!
//! The raw variant is computed for every changed book each block, but the six
//! sig-fig variants each walk the whole book and were most of `l2_compute_us`
//! while almost no client asked for them (2026-09-28). The listener computes
//! them only for the coins in `L2Demand::coins`.

use crate::{order_book::Coin, types::subscription::Subscription};
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex, MutexGuard, PoisonError},
};

/// Number of clients with at least one aggregated L2 subscription, per coin.
#[derive(Default)]
pub(crate) struct L2Demand(Mutex<HashMap<Coin, usize>>);

impl L2Demand {
    fn lock(&self) -> MutexGuard<'_, HashMap<Coin, usize>> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Coins that need their aggregated variants computed.
    pub(crate) fn coins(&self) -> HashSet<Coin> {
        self.lock().keys().cloned().collect()
    }
}

/// One client's share of `L2Demand`; released when the client goes away.
pub(crate) struct ClientL2Demand {
    demand: Arc<L2Demand>,
    coins: HashSet<Coin>,
}

impl ClientL2Demand {
    pub(crate) fn new(demand: Arc<L2Demand>) -> Self {
        Self { demand, coins: HashSet::new() }
    }

    /// Brings the shared counts in line with the client's current subscriptions.
    pub(crate) fn sync(&mut self, subscriptions: &HashSet<Subscription>) {
        let coins: HashSet<Coin> = subscriptions
            .iter()
            .filter_map(|sub| match sub {
                Subscription::L2Book { coin, n_sig_figs, mantissa, .. }
                    if n_sig_figs.is_some() || mantissa.is_some() =>
                {
                    Some(Coin::new(coin))
                }
                _ => None,
            })
            .collect();
        if coins == self.coins {
            return;
        }
        let mut counts = self.demand.lock();
        for coin in coins.difference(&self.coins) {
            *counts.entry(coin.clone()).or_default() += 1;
        }
        for coin in self.coins.difference(&coins) {
            release(&mut counts, coin);
        }
        drop(counts);
        self.coins = coins;
    }
}

fn release(counts: &mut HashMap<Coin, usize>, coin: &Coin) {
    if let Some(n) = counts.get_mut(coin) {
        *n -= 1;
        if *n == 0 {
            counts.remove(coin);
        }
    }
}

impl Drop for ClientL2Demand {
    fn drop(&mut self) {
        let mut counts = self.demand.lock();
        for coin in &self.coins {
            release(&mut counts, coin);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn l2(coin: &str, n_sig_figs: Option<u32>, n_levels: Option<usize>) -> Subscription {
        Subscription::L2Book { coin: coin.into(), n_sig_figs, n_levels, mantissa: None }
    }

    fn sorted(demand: &L2Demand) -> Vec<String> {
        let mut coins: Vec<_> = demand.coins().iter().map(Coin::value).collect();
        coins.sort();
        coins
    }

    #[test]
    fn counts_follow_subscriptions_and_disconnects() {
        let demand = Arc::new(L2Demand::default());
        let mut a = ClientL2Demand::new(demand.clone());
        let mut b = ClientL2Demand::new(demand.clone());
        a.sync(&[l2("BTC", None, None), l2("ETH", None, None)].into());
        assert!(sorted(&demand).is_empty(), "raw L2 needs no aggregated variants");

        a.sync(&[l2("BTC", Some(5), None), l2("BTC", Some(3), Some(10)), l2("ETH", None, None)].into());
        b.sync(&[l2("BTC", Some(4), None), l2("SOL", Some(2), None), Subscription::Trades { coin: "ETH".into() }].into());
        assert_eq!(sorted(&demand), ["BTC", "SOL"]);

        // One of a's two BTC subscriptions goes; BTC is still wanted by both clients.
        a.sync(&[l2("BTC", Some(3), Some(10))].into());
        assert_eq!(demand.lock()[&Coin::new("BTC")], 2);
        drop(b);
        assert_eq!(sorted(&demand), ["BTC"]);
        a.sync(&HashSet::new());
        assert!(sorted(&demand).is_empty());

        a.sync(&[l2("HYPE", Some(5), None)].into());
        drop(a);
        assert!(demand.lock().is_empty());
    }
}
