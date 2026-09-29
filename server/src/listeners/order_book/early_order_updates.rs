//! orderUpdates sent from a block's order-statuses line before the line is parsed.
//!
//! Clients used to get a block's orderUpdates with its L4 updates, once the
//! whole statuses line was parsed (1.3 ms p50 / 4 ms p99 off-peak, lines of
//! ~1 MB at the US open) and the block's diffs were in too. Only the events of
//! users some client subscribes to orderUpdates of are needed, so the listener
//! finds those in the raw line, parses just them and sends them first
//! (`InternalMessage::OrderUpdates`). Clients then leave the users that message
//! covers out of the block's L4 updates.

use crate::types::{node_data::NodeDataOrderStatus, subscription::Subscription};
use alloy::primitives::{Address, hex};
use chrono::NaiveDateTime;
use memchr::{memmem, memrchr};
use serde::Deserialize;
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex, MutexGuard, PoisonError},
};

#[derive(Default)]
struct Users {
    /// Per user: clients subscribed to its orderUpdates.
    clients: HashMap<Address, usize>,
    /// The keys of `clients`, for the listener to take without a copy.
    set: Arc<HashSet<Address>>,
}

/// Users some client subscribes to orderUpdates of.
#[derive(Default)]
pub(crate) struct UserDemand(Mutex<Users>);

impl UserDemand {
    fn lock(&self) -> MutexGuard<'_, Users> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub(crate) fn users(&self) -> Arc<HashSet<Address>> {
        self.lock().set.clone()
    }
}

/// One client's share of `UserDemand`; released when the client goes away.
pub(crate) struct ClientUserDemand {
    demand: Arc<UserDemand>,
    users: HashSet<Address>,
}

impl ClientUserDemand {
    pub(crate) fn new(demand: Arc<UserDemand>) -> Self {
        Self { demand, users: HashSet::new() }
    }

    /// Brings the shared counts in line with the client's current subscriptions.
    pub(crate) fn sync(&mut self, subscriptions: &HashSet<Subscription>) {
        let users: HashSet<Address> = subscriptions
            .iter()
            .filter_map(|sub| match sub {
                Subscription::OrderUpdates { user } => Some(*user),
                _ => None,
            })
            .collect();
        if users != self.users {
            let old = std::mem::replace(&mut self.users, users);
            self.update(&old, &self.users);
        }
    }

    fn update(&self, released: &HashSet<Address>, added: &HashSet<Address>) {
        let mut users = self.demand.lock();
        for user in released {
            if let Some(clients) = users.clients.get_mut(user) {
                *clients -= 1;
                if *clients == 0 {
                    users.clients.remove(user);
                }
            }
        }
        for user in added {
            *users.clients.entry(*user).or_default() += 1;
        }
        users.set = Arc::new(users.clients.keys().copied().collect());
    }
}

impl Drop for ClientUserDemand {
    fn drop(&mut self) {
        if !self.users.is_empty() {
            self.update(&self.users, &HashSet::new());
        }
    }
}

/// The start of a statuses line, up to its events.
#[derive(Deserialize)]
struct Header {
    local_time: NaiveDateTime,
    block_number: u64,
}

const EVENTS_KEY: &[u8] = br#","events":["#;
/// hl-node writes local_time, block_time and block_number first: ~110 bytes.
const MAX_HEADER_LEN: usize = 256;
const USER_KEY: &[u8] = br#""user":"0x"#;
const ADDRESS_HEX_LEN: usize = 40;

/// (block number, hl-node write time in µs since epoch) of a statuses line;
/// None unless it starts as hl-node writes them.
pub(super) fn header(line: &str) -> Option<(u64, u64)> {
    let start = &line.as_bytes()[..line.len().min(MAX_HEADER_LEN)];
    let end = memmem::find(start, EVENTS_KEY)?;
    let header: Header = serde_json::from_str(&format!("{}}}", line.get(..end)?)).ok()?;
    Some((header.block_number, header.local_time.and_utc().timestamp_micros().try_into().ok()?))
}

/// The events of `users` in a complete statuses line, in line order; None if
/// one of them does not parse (the line then only goes the usual way).
pub(super) fn statuses_of(line: &str, users: &HashSet<Address>) -> Option<Vec<NodeDataOrderStatus>> {
    let bytes = line.as_bytes();
    let mut statuses = Vec::new();
    for pos in memmem::find_iter(bytes, USER_KEY) {
        let hex_start = pos + USER_KEY.len();
        let user = Address::from(hex::decode_to_array(bytes.get(hex_start..hex_start + ADDRESS_HEX_LEN)?).ok()?);
        if !users.contains(&user) {
            continue;
        }
        // The event's object: no brace comes between its start and "user" (only "time").
        let start = memrchr(b'{', &bytes[..pos])?;
        let status = NodeDataOrderStatus::deserialize(&mut serde_json::Deserializer::from_str(line.get(start..)?)).ok()?;
        if status.user != user {
            return None;
        }
        statuses.push(status);
    }
    Some(statuses)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::node_data::Batch;
    use std::{fs, str::FromStr, time::Instant};

    /// Unfiltered hl-node order-status lines (100 blocks); test-only, not committed.
    const RAW_STATUSES: &str = "tmp/fixture/obs_raw_statuses.jsonl";

    fn address(s: &str) -> Address {
        Address::from_str(s).unwrap()
    }

    #[test]
    fn demand_follows_subscriptions_and_disconnects() {
        let (u1, u2) = (address("0x0000000000000000000000000000000000000001"), address("0x00000000000000000000000000000000000000ff"));
        let sub = |user| Subscription::OrderUpdates { user };
        let demand = Arc::new(UserDemand::default());
        let mut a = ClientUserDemand::new(demand.clone());
        let mut b = ClientUserDemand::new(demand.clone());
        a.sync(&[sub(u1), Subscription::Trades { coin: "BTC".into() }].into());
        b.sync(&[sub(u1), sub(u2)].into());
        assert_eq!(*demand.users(), [u1, u2].into());
        b.sync(&[sub(u2)].into());
        assert_eq!(*demand.users(), [u1, u2].into());
        drop(a);
        assert_eq!(*demand.users(), [u2].into());
        b.sync(&HashSet::new());
        assert!(demand.users().is_empty());
        b.sync(&[sub(u1)].into());
        drop(b);
        assert!(demand.users().is_empty() && demand.lock().clients.is_empty());
    }

    #[test]
    fn matches_full_parse_on_raw_lines() {
        let Ok(lines) = fs::read_to_string(RAW_STATUSES) else {
            eprintln!("skipping: {RAW_STATUSES} not present");
            return;
        };
        let batches: Vec<Batch<NodeDataOrderStatus>> = lines.lines().map(|l| serde_json::from_str(l).unwrap()).collect();
        let mut counts: HashMap<Address, usize> = HashMap::new();
        for status in batches.iter().flat_map(Batch::events_ref) {
            *counts.entry(status.user).or_default() += 1;
        }
        let mut by_count: Vec<_> = counts.into_iter().collect();
        by_count.sort_by_key(|&(user, n)| (std::cmp::Reverse(n), user));
        // The busiest users, some rare ones, and one with no events.
        let mut users: HashSet<Address> = by_count.iter().take(10).chain(by_count.iter().rev().take(10)).map(|&(u, _)| u).collect();
        users.insert(address("0x00000000000000000000000000000000000000ff"));

        let (mut scan_time, mut n_statuses) = (std::time::Duration::ZERO, 0);
        for (line, batch) in lines.lines().zip(&batches) {
            let start = Instant::now();
            let (block_number, local_time_us) = header(line).unwrap();
            let statuses = statuses_of(line, &users).unwrap();
            scan_time += start.elapsed();
            assert_eq!((block_number, local_time_us), (batch.block_number(), batch.local_time_us()));
            let expected: Vec<_> = batch.events_ref().iter().filter(|s| users.contains(&s.user)).cloned().collect();
            assert_eq!(statuses, expected, "block {block_number}");
            n_statuses += statuses.len();
        }
        assert!(n_statuses > 1000, "{n_statuses}");
        let absent = [address("0x00000000000000000000000000000000000000ff")].into();
        let start = Instant::now();
        assert!(lines.lines().all(|line| statuses_of(line, &absent) == Some(vec![])));
        let n = batches.len() as u32;
        eprintln!(
            "{n} lines, {n_statuses} statuses of {} users: {:?}/line; no statuses: {:?}/line",
            users.len(),
            scan_time / n,
            start.elapsed() / n,
        );
        assert_eq!(statuses_of(lines.lines().next().unwrap(), &HashSet::new()), Some(vec![]));
    }

    #[test]
    fn rejects_what_it_cannot_parse() {
        let user = address("0x0a8817c801b0801c2f2acfc8778a7cd807161e07");
        let users = [user].into();
        let Ok(lines) = fs::read_to_string(RAW_STATUSES) else {
            eprintln!("skipping: {RAW_STATUSES} not present");
            return;
        };
        let line = lines.lines().next().unwrap();
        assert!(!statuses_of(line, &users).unwrap().is_empty());
        // Cut inside the user's first event.
        let pos = line.find(&format!(r#""user":"{user:#x}""#)).unwrap();
        assert_eq!(statuses_of(&line[..pos + 100], &users), None);
        assert_eq!(statuses_of(&line[..pos + 20], &users), None);
        assert_eq!(header(&line[..50]), None);
        assert_eq!(header(r#"{"block_number":1,"events":[]}"#), None);
    }
}
