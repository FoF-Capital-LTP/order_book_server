use crate::{
    latency,
    listeners::order_book::{
        ClientL2Demand, InternalMessage, L2SnapshotParams, L2Snapshots, OrderBookListener, hl_listen,
    },
    order_book::{Coin, Side},
    prelude::*,
    types::{
        Fill, L2Book, L4Book, L4BookUpdates, L4Order, Trade, WsOrder, WsUserFills,
        node_data::{Batch, NodeDataFill, NodeDataOrderDiff, NodeDataOrderStatus},
        subscription::{ClientMessage, DEFAULT_LEVELS, ServerResponse, Subscription, SubscriptionManager},
    },
};
use alloy::primitives::Address;
use axum::{Router, response::IntoResponse, routing::get, serve::ListenerExt};
use futures_util::{SinkExt, StreamExt};
use log::{error, info, warn};
use std::{
    collections::{HashMap, HashSet},
    env::home_dir,
    sync::{Arc, OnceLock},
    time::{Duration, Instant},
};
use tokio::select;
use tokio::{
    net::TcpListener,
    sync::{
        Mutex,
        broadcast::{Sender, channel, error::RecvError},
    },
};
use yawc::{FrameView, OpCode, WebSocket};

/// Broadcast backlog per client, in messages (~40/s: fills, L4 updates and an
/// L2 snapshot per block), so ~25 s. A client further behind than this loses
/// messages (see `handle_socket`). Slots are freed as soon as every client has
/// read them, so the backlog only holds memory while some client is behind.
const BROADCAST_CAPACITY: usize = 1024;

/// Rayon threads for the per-block L2 recompute and the snapshot-validation clone,
/// unless RAYON_NUM_THREADS is set. Rayon's default of one per core (32 on the node)
/// cost ~12% of obs CPU in idle spinning (crossbeam_epoch, sched_yield; perf 2026-09-28).
const DEFAULT_RAYON_THREADS: usize = 16;

pub async fn run_websocket_server(address: &str, ignore_spot: bool, compression_level: u32) -> Result<()> {
    if std::env::var_os("RAYON_NUM_THREADS").is_none() {
        let threads = DEFAULT_RAYON_THREADS.min(std::thread::available_parallelism().map_or(1, usize::from));
        if let Err(err) = rayon::ThreadPoolBuilder::new().num_threads(threads).build_global() {
            warn!("Could not set rayon threads to {threads}: {err}");
        }
    }
    info!("rayon threads: {}", rayon::current_num_threads());
    let (internal_message_tx, _) = channel::<Arc<InternalMessage>>(BROADCAST_CAPACITY);

    // Central task: listen to messages and forward them for distribution
    let home_dir = home_dir().ok_or("Could not find home directory")?;
    let listener = {
        let internal_message_tx = internal_message_tx.clone();
        OrderBookListener::new(Some(internal_message_tx), ignore_spot)
    };
    let listener = Arc::new(Mutex::new(listener));
    tokio::spawn(latency::report_loop(Duration::from_secs(60)));
    {
        let listener = listener.clone();
        tokio::spawn(async move {
            if let Err(err) = hl_listen(listener, home_dir).await {
                error!("Listener fatal error: {err}");
                std::process::exit(1);
            }
        });
    }

    let websocket_opts =
        yawc::Options::default().with_compression_level(yawc::CompressionLevel::new(compression_level));
    let app = Router::new().route(
        "/ws",
        get({
            let internal_message_tx = internal_message_tx.clone();
            async move |ws_upgrade| {
                ws_handler(ws_upgrade, internal_message_tx.clone(), listener.clone(), websocket_opts)
            }
        }),
    );

    let listener = TcpListener::bind(address).await?
        .tap_io(|tcp_stream| {
            if let Err(err) = tcp_stream.set_nodelay(true) {
                log::error!("Failed to set TCP_NODELAY on incoming connection: {err:#}");
            }
        });
    info!("WebSocket server running at ws://{address}");

    if let Err(err) = axum::serve(listener, app.into_make_service()).await {
        error!("Server fatal error: {err}");
        std::process::exit(2);
    }

    Ok(())
}

fn ws_handler(
    incoming: yawc::IncomingUpgrade,
    internal_message_tx: Sender<Arc<InternalMessage>>,
    listener: Arc<Mutex<OrderBookListener>>,
    websocket_opts: yawc::Options,
) -> impl IntoResponse {
    let (resp, fut) = incoming.upgrade(websocket_opts).unwrap();
    tokio::spawn(async move {
        let ws = match fut.await {
            Ok(ok) => ok,
            Err(err) => {
                log::error!("failed to upgrade websocket connection: {err}");
                return;
            }
        };

        handle_socket(ws, internal_message_tx, listener).await
    });

    resp
}

async fn handle_socket(
    mut socket: WebSocket,
    internal_message_tx: Sender<Arc<InternalMessage>>,
    listener: Arc<Mutex<OrderBookListener>>,
) {
    let mut internal_message_rx = internal_message_tx.subscribe();
    let (is_ready, mut universe, l2_demand) = {
        let listener = listener.lock().await;
        (listener.is_ready(), listener.universe(), listener.l2_demand())
    };
    let mut manager = SubscriptionManager::default();
    // Dropped on every return below, which releases this client's sig-fig L2 demand.
    let mut l2_demand = ClientL2Demand::new(l2_demand);
    if !is_ready {
        let msg = ServerResponse::Error("Order book not ready for streaming (waiting for snapshot)".to_string());
        send_socket_message(&mut socket, msg).await;
        return;
    }
    loop {
        select! {
            recv_result = internal_message_rx.recv() => {
                match recv_result {
                    Ok(msg) => {
                        latency::CLIENT_QUEUE_LEN.record(internal_message_rx.len() as u64);
                        let handle_start = Instant::now();
                        // (histogram, hl-node write time) for each stream of this message the client subscribes to
                        let delivered: [Option<(&latency::Histogram, u64)>; 2] = match msg.as_ref() {
                            InternalMessage::Snapshot{ l2_snapshots, time, local_time_us, shared, universe: coins } => {
                                universe = coins.clone();
                                for sub in manager.subscriptions() {
                                    send_ws_data_from_snapshot(&mut socket, sub, l2_snapshots, *time, shared).await;
                                }
                                let l2 = manager
                                    .subscriptions()
                                    .iter()
                                    .any(|sub| matches!(sub, Subscription::L2Book { .. }))
                                    .then_some((&latency::CLIENT_L2_AFTER_WRITE_US, *local_time_us));
                                [l2, None]
                            },
                            InternalMessage::Fills{ batch } => {
                                let wanted = WantedKeys::new(manager.subscriptions());
                                let mut trades = if wanted.trades { coin_to_trades(batch) } else { HashMap::new() };
                                let mut user_fills = user_to_fills(batch, &wanted.user_fills);
                                for sub in manager.subscriptions() {
                                    send_ws_data_from_trades(&mut socket, sub, &mut trades).await;
                                    send_ws_data_from_user_fills(&mut socket, sub, &mut user_fills).await;
                                }
                                let fills = (wanted.trades || !wanted.user_fills.is_empty())
                                    .then(|| (&latency::CLIENT_FILLS_AFTER_WRITE_US, batch.local_time_us()));
                                [fills, None]
                            },
                            InternalMessage::L4BookUpdates{ diff_batch, status_batch } => {
                                let wanted = WantedKeys::new(manager.subscriptions());
                                let mut book_updates = coin_to_book_updates(diff_batch, status_batch, &wanted.l4_books);
                                let mut user_orders = user_to_order_updates(status_batch, &wanted.order_updates);
                                for sub in manager.subscriptions() {
                                    send_ws_data_from_book_updates(&mut socket, sub, &mut book_updates).await;
                                    send_ws_data_from_order_updates(&mut socket, sub, &mut user_orders).await;
                                }
                                let local_time_us = diff_batch.local_time_us().max(status_batch.local_time_us());
                                [
                                    (!wanted.l4_books.is_empty()).then_some((&latency::CLIENT_L4_AFTER_WRITE_US, local_time_us)),
                                    (!wanted.order_updates.is_empty())
                                        .then_some((&latency::CLIENT_ORDER_UPDATES_AFTER_WRITE_US, local_time_us)),
                                ]
                            },
                        };
                        if let Err(err) = socket.flush().await {
                            error!("Failed to send: {err}");
                        }
                        latency::CLIENT_HANDLE_US.record_duration_us(handle_start.elapsed());
                        for (histogram, local_time_us) in delivered.into_iter().flatten() {
                            histogram.record_age_us(local_time_us);
                        }

                    }
                    Err(RecvError::Lagged(skipped)) => {
                        // Skipped L2 snapshots are superseded by the next one, but skipped
                        // trades, fills and L4/order updates are lost for good: drop those
                        // clients so they reconnect and resync.
                        let subs = manager.subscriptions();
                        let l2_only = subs.iter().all(|sub| matches!(sub, Subscription::L2Book { .. }));
                        let summary = subscription_summary(subs);
                        if l2_only {
                            warn!("Receiver lagged by {skipped} messages; continuing (L2-only client: {summary})");
                        } else {
                            error!("Receiver error: channel lagged by {skipped}; disconnecting ({summary})");
                            return;
                        }
                    }
                    Err(err) => {
                        error!("Receiver error: {err}");
                        return;
                    }
                }
            }

            msg = socket.next() => {
                if let Some(frame) = msg {
                    match frame.opcode {
                        OpCode::Text => {
                            let text = match std::str::from_utf8(&frame.payload) {
                                Ok(text) => text,
                                Err(err) => {
                                    log::warn!("unable to parse websocket content: {err}: {:?}", frame.payload.as_ref());
                                    // deserves to close the connection because the payload is not a valid utf8 string.
                                    return;
                                }
                            };

                            // Demoted from info!: clients heartbeat with
                            // {"method":"ping"} every ~3-5 s, and with
                            // multiple subscribers this single line was
                            // ~50% of journald volume. debug! keeps it
                            // available under RUST_LOG=debug for protocol
                            // forensics without flooding production logs.
                            log::debug!("Client message: {text}");

                            match serde_json::from_str::<ClientMessage>(text) {
                                Ok(ClientMessage::Ping) => {
                                    // HyperLiquid official ws protocol: reply with raw {"channel":"pong"}.
                                    // Bypasses ServerResponse because that enum forces a `data` field
                                    // and the official pong has no data.
                                    if let Err(err) = socket.send(FrameView::text(r#"{"channel":"pong"}"#.to_string())).await {
                                        error!("Failed to send pong: {err}");
                                    }
                                }
                                Ok(value) => {
                                    receive_client_message(&mut socket, &mut manager, value, &universe, listener.clone()).await;
                                    l2_demand.sync(manager.subscriptions());
                                }
                                Err(_) => {
                                    let msg = ServerResponse::Error(format!("Error parsing JSON into valid websocket request: {text}"));
                                    send_socket_message(&mut socket, msg).await;
                                }
                            }
                        }
                        OpCode::Close => {
                            info!("Client disconnected");
                            return;
                        }
                        _ => {}
                    }
                } else {
                    info!("Client connection closed");
                    return;
                }
            }
        }
    }
}

async fn receive_client_message(
    socket: &mut WebSocket,
    manager: &mut SubscriptionManager,
    client_message: ClientMessage,
    universe: &HashSet<String>,
    listener: Arc<Mutex<OrderBookListener>>,
) {
    let subscription = match &client_message {
        ClientMessage::Unsubscribe { subscription } | ClientMessage::Subscribe { subscription } => subscription.clone(),
        ClientMessage::Ping => return, // handled by caller before reaching here
    };
    // this is used for display purposes only, hence unwrap_or_default. It also shouldn't fail
    let sub = serde_json::to_string(&subscription).unwrap_or_default();
    if !subscription.validate(universe) {
        let msg = ServerResponse::Error(format!("Invalid subscription: {sub}"));
        send_socket_message(socket, msg).await;
        return;
    }
    let (word, success) = match &client_message {
        ClientMessage::Subscribe { .. } => ("", manager.subscribe(subscription)),
        ClientMessage::Unsubscribe { .. } => ("un", manager.unsubscribe(subscription)),
        ClientMessage::Ping => return, // unreachable; Ping is handled in caller and short-circuited above
    };
    if success {
        let snapshot_msg = if let ClientMessage::Subscribe { subscription } = &client_message {
            let msg = subscription.handle_immediate_snapshot(listener).await;
            match msg {
                Ok(msg) => msg,
                Err(err) => {
                    manager.unsubscribe(subscription.clone());
                    let msg = ServerResponse::Error(format!("Unable to grab order book snapshot: {err}"));
                    send_socket_message(socket, msg).await;
                    return;
                }
            }
        } else {
            None
        };
        let msg = ServerResponse::SubscriptionResponse(client_message);
        send_socket_message(socket, msg).await;
        if let Some(snapshot_msg) = snapshot_msg {
            send_socket_message(socket, snapshot_msg).await;
        }
    } else {
        let msg = ServerResponse::Error(format!("Already {word}subscribed: {sub}"));
        send_socket_message(socket, msg).await;
    }
}

fn serialize_message(msg: &ServerResponse) -> Option<FrameView> {
    match serde_json::to_string(msg) {
        Ok(msg) => Some(FrameView::text(msg)),
        Err(err) => {
            error!("Server response serialization error: {err}");
            None
        }
    }
}

async fn send_socket_message(socket: &mut WebSocket, msg: ServerResponse) {
    if let Some(frame) = serialize_message(&msg) {
        if let Err(err) = socket.send(frame).await {
            error!("Failed to send: {err}");
        }
    }
}

/// Queues `frame` without flushing. `handle_socket` flushes once per broadcast
/// message: the codec writes whenever 8 KiB are buffered, instead of one
/// syscall per frame (~67k sendto/s with per-frame flushes, 2026-09-28).
async fn feed_frame(socket: &mut WebSocket, frame: FrameView) {
    if let Err(err) = socket.feed(frame).await {
        error!("Failed to send: {err}");
    }
}

async fn feed_socket_message(socket: &mut WebSocket, msg: ServerResponse) {
    if let Some(frame) = serialize_message(&msg) {
        feed_frame(socket, frame).await;
    }
}

/// (coin, n_sig_figs, mantissa, n_levels) of an L2Book subscription.
type L2FrameKey = (String, Option<u32>, Option<u64>, usize);

/// Work shared by all clients handling the same snapshot message: each
/// distinct L2Book response is built once, not per client.
///
/// Keys some client requested for the previous snapshot are looked up without
/// a lock; only keys new in this snapshot go through the mutex. One mutex for
/// every lookup cost ~30% of obs CPU in futex contention (perf, 2026-09-28).
#[derive(Default)]
pub(crate) struct SnapshotShared {
    /// Keys requested for the previous snapshot. None: the coin or params are not in the snapshot.
    known: HashMap<Arc<L2FrameKey>, OnceLock<Option<FrameView>>>,
    /// Keys first requested for this snapshot.
    new: std::sync::Mutex<HashMap<Arc<L2FrameKey>, Option<FrameView>>>,
}

impl SnapshotShared {
    /// Lock-free slots for every key some client requested from `prev`; keys
    /// nobody asked for drop out.
    #[allow(clippy::unwrap_used)]
    pub(crate) fn following(prev: Option<&Self>) -> Self {
        let Some(prev) = prev else { return Self::default() };
        let mut known: HashMap<_, _> = prev
            .known
            .iter()
            .filter(|(_, frame)| frame.get().is_some())
            .map(|(key, _)| (key.clone(), OnceLock::new()))
            .collect();
        known.extend(prev.new.lock().unwrap().keys().map(|key| (key.clone(), OnceLock::new())));
        Self { known, new: std::sync::Mutex::default() }
    }

    /// The serialized L2Book response for `key`. For a new key serialization
    /// runs outside the lock; two clients racing on it both build the same bytes.
    #[allow(clippy::unwrap_used)]
    fn l2_frame(&self, key: L2FrameKey, build: impl FnOnce(&L2FrameKey) -> Option<FrameView>) -> Option<FrameView> {
        if let Some(frame) = self.known.get(&key) {
            return frame.get_or_init(|| build(&key)).clone();
        }
        if let Some(frame) = self.new.lock().unwrap().get(&key) {
            return frame.clone();
        }
        let frame = build(&key);
        self.new.lock().unwrap().entry(Arc::new(key)).or_insert(frame).clone()
    }
}

fn build_l2_frame(l2_snapshots: &L2Snapshots, key: &L2FrameKey, time: u64) -> Option<FrameView> {
    let (coin, n_sig_figs, mantissa, n_levels) = key;
    let snapshot = l2_snapshots.as_ref().get(&Coin::new(coin))?.get(&L2SnapshotParams::new(*n_sig_figs, *mantissa))?;
    let snapshot = snapshot.truncate(*n_levels).export_inner_snapshot();
    serialize_message(&ServerResponse::L2Book(L2Book::from_l2_snapshot(coin.clone(), snapshot, time)))
}

async fn send_ws_data_from_snapshot(
    socket: &mut WebSocket,
    subscription: &Subscription,
    l2_snapshots: &L2Snapshots,
    time: u64,
    shared: &SnapshotShared,
) {
    if let Subscription::L2Book { coin, n_sig_figs, n_levels, mantissa } = subscription {
        let key = (coin.clone(), *n_sig_figs, *mantissa, n_levels.unwrap_or(DEFAULT_LEVELS));
        // Snapshots are only computed for the coins and variants some client wants
        // (see `L2Demand`); this one may predate the subscription. The next has it.
        let Some(coin_snapshots) = l2_snapshots.as_ref().get(&Coin::new(coin)) else {
            return;
        };
        if !coin_snapshots.contains_key(&L2SnapshotParams::new(*n_sig_figs, *mantissa)) {
            return;
        }
        if let Some(frame) = shared.l2_frame(key, |key| build_l2_frame(l2_snapshots, key, time)) {
            feed_frame(socket, frame).await;
        }
    }
}

/// e.g. "l2Book=3 trades=1", for logs.
fn subscription_summary(subs: &HashSet<Subscription>) -> String {
    let mut counts: std::collections::BTreeMap<&str, usize> = std::collections::BTreeMap::new();
    for sub in subs {
        let kind = match sub {
            Subscription::Trades { .. } => "trades",
            Subscription::L2Book { .. } => "l2Book",
            Subscription::L4Book { .. } => "l4Book",
            Subscription::OrderUpdates { .. } => "orderUpdates",
            Subscription::UserFills { .. } => "userFills",
        };
        *counts.entry(kind).or_default() += 1;
    }
    if counts.is_empty() {
        return "no subscriptions".to_string();
    }
    counts.iter().map(|(kind, n)| format!("{kind}={n}")).collect::<Vec<_>>().join(" ")
}

/// What one client subscribes to, per block-event stream.
///
/// Every client task receives every block, and used to group (and clone) all
/// of it -- the whole order-status batch twice -- before picking out its few
/// coins or users. With ~70 clients that per-client copying was the largest
/// single chunk of obs CPU (perf, 2026-09-26). Grouping only the subscribed
/// keys produces identical messages.
struct WantedKeys<'a> {
    trades: bool,
    l4_books: HashSet<&'a str>,
    user_fills: HashSet<Address>,
    order_updates: HashSet<Address>,
}

impl<'a> WantedKeys<'a> {
    fn new(subscriptions: &'a HashSet<Subscription>) -> Self {
        let mut wanted =
            Self { trades: false, l4_books: HashSet::new(), user_fills: HashSet::new(), order_updates: HashSet::new() };
        for sub in subscriptions {
            match sub {
                Subscription::Trades { .. } => wanted.trades = true,
                Subscription::L4Book { coin } => {
                    wanted.l4_books.insert(coin.as_str());
                }
                Subscription::UserFills { user, .. } => {
                    wanted.user_fills.insert(*user);
                }
                Subscription::OrderUpdates { user } => {
                    wanted.order_updates.insert(*user);
                }
                Subscription::L2Book { .. } => {}
            }
        }
        wanted
    }
}

// Not filtered by coin like the other groupings: trades pair fills by `tid`,
// and keeping that pairing exactly as before matters more than the saving
// (fills are a small stream). Skipped entirely when nothing needs trades.
fn coin_to_trades(batch: &Batch<NodeDataFill>) -> HashMap<String, Vec<Trade>> {
    // Group fills by trade id. A valid trade has exactly one Ask + one Bid fill
    // sharing the same `tid`. Anything else (single-sided fills, same-side pairs
    // from self-trades or liquidation bookkeeping) is skipped with a warning.
    let fills = batch.clone().events();
    let mut by_tid: HashMap<u64, HashMap<Side, NodeDataFill>> = HashMap::new();
    let mut order: Vec<u64> = Vec::new();
    for fill in fills {
        let tid = fill.1.tid;
        let side = fill.1.side;
        let entry = by_tid.entry(tid).or_insert_with(|| {
            order.push(tid);
            HashMap::new()
        });
        entry.insert(side, fill);
    }
    let mut trades: HashMap<String, Vec<Trade>> = HashMap::new();
    for tid in order {
        if let Some(group) = by_tid.remove(&tid) {
            match Trade::from_fills(group) {
                Some(trade) => {
                    let coin = trade.coin.clone();
                    trades.entry(coin).or_default().push(trade);
                }
                None => {
                    // Expected for self-trades, single-sided fills, and
                    // liquidation bookkeeping (see comment at top of fn).
                    // Demoted from warn to debug because under RUST_LOG=warn
                    // it produced ~76 lines/sec — drowning the real
                    // diagnostics signals (BatchQueue overflow, lag-watchdog,
                    // not-yet-grafted coin warns).
                    log::debug!("Skipping malformed fill group for tid={tid}");
                }
            }
        }
    }
    trades
}

fn coin_to_book_updates(
    diff_batch: &Batch<NodeDataOrderDiff>,
    status_batch: &Batch<NodeDataOrderStatus>,
    coins: &HashSet<&str>,
) -> HashMap<String, L4BookUpdates> {
    let time = diff_batch.block_time();
    let height = diff_batch.block_number();
    let mut updates = HashMap::new();
    if coins.is_empty() {
        return updates;
    }
    for diff in diff_batch.events_ref().iter().filter(|d| coins.contains(d.coin_str())) {
        let coin = diff.coin_str().to_string();
        updates.entry(coin).or_insert_with(|| L4BookUpdates::new(time, height)).book_diffs.push(diff.clone());
    }
    for status in status_batch.events_ref().iter().filter(|s| coins.contains(s.order.coin.as_str())) {
        let coin = status.order.coin.clone();
        updates.entry(coin).or_insert_with(|| L4BookUpdates::new(time, height)).order_statuses.push(status.clone());
    }
    updates
}

async fn send_ws_data_from_book_updates(
    socket: &mut WebSocket,
    subscription: &Subscription,
    book_updates: &mut HashMap<String, L4BookUpdates>,
) {
    if let Subscription::L4Book { coin } = subscription {
        if let Some(updates) = book_updates.remove(coin) {
            let msg = ServerResponse::L4Book(L4Book::Updates(updates));
            feed_socket_message(socket, msg).await;
        }
    }
}

async fn send_ws_data_from_trades(
    socket: &mut WebSocket,
    subscription: &Subscription,
    trades: &mut HashMap<String, Vec<Trade>>,
) {
    if let Subscription::Trades { coin } = subscription {
        if let Some(trades) = trades.remove(coin) {
            let msg = ServerResponse::Trades(trades);
            feed_socket_message(socket, msg).await;
        }
    }
}

/// Group raw node fills by user. Each `NodeDataFill` is a `(Address, Fill)`
/// pair, so the same physical fill belongs to exactly one user (the maker or
/// taker side recorded on that node event); this matches HL's per-user push
/// where each side gets its own `userFills` notification.
fn user_to_fills(batch: &Batch<NodeDataFill>, users: &HashSet<Address>) -> HashMap<Address, Vec<Fill>> {
    let mut by_user: HashMap<Address, Vec<Fill>> = HashMap::new();
    if users.is_empty() {
        return by_user;
    }
    for NodeDataFill(user, fill) in batch.events_ref().iter().filter(|f| users.contains(&f.0)) {
        by_user.entry(*user).or_default().push(fill.clone());
    }
    by_user
}

/// Group node order-status events by user and convert each one to a `WsOrder`.
fn user_to_order_updates(
    status_batch: &Batch<NodeDataOrderStatus>,
    users: &HashSet<Address>,
) -> HashMap<Address, Vec<WsOrder>> {
    let mut by_user: HashMap<Address, Vec<WsOrder>> = HashMap::new();
    if users.is_empty() {
        return by_user;
    }
    for status in status_batch.events_ref().iter().filter(|s| users.contains(&s.user)) {
        by_user.entry(status.user).or_default().push(WsOrder::from_node_status(status));
    }
    by_user
}

async fn send_ws_data_from_user_fills(
    socket: &mut WebSocket,
    subscription: &Subscription,
    user_fills: &mut HashMap<Address, Vec<Fill>>,
) {
    if let Subscription::UserFills { user, .. } = subscription {
        if let Some(fills) = user_fills.remove(user) {
            // Streaming pushes omit `isSnapshot` per HL docs.
            let msg = ServerResponse::UserFills(WsUserFills { is_snapshot: None, user: *user, fills });
            feed_socket_message(socket, msg).await;
        }
    }
}

async fn send_ws_data_from_order_updates(
    socket: &mut WebSocket,
    subscription: &Subscription,
    user_orders: &mut HashMap<Address, Vec<WsOrder>>,
) {
    if let Subscription::OrderUpdates { user } = subscription {
        if let Some(orders) = user_orders.remove(user) {
            let msg = ServerResponse::OrderUpdates(orders);
            feed_socket_message(socket, msg).await;
        }
    }
}

impl Subscription {
    // snapshots that begin a stream
    async fn handle_immediate_snapshot(
        &self,
        listener: Arc<Mutex<OrderBookListener>>,
    ) -> Result<Option<ServerResponse>> {
        if let Self::L4Book { coin } = self {
            // Only this coin's book: snapshotting every coin here held the
            // listener mutex long enough to stall block processing.
            // Deep books take tens of ms to copy and convert (BTC ~59k orders), so both steps
            // run in block_in_place: otherwise the listener, woken into this worker's LIFO slot
            // by the unlock, would wait for the conversion.
            let coin = Coin::new(coin);
            let snapshot = {
                let listener = listener.lock().await;
                tokio::task::block_in_place(|| listener.compute_coin_snapshot(&coin))
            };
            if let Some((time, height, snapshot)) = snapshot {
                let snapshot = tokio::task::block_in_place(|| {
                    snapshot.as_ref().clone().map(|orders| orders.into_iter().map(L4Order::from).collect())
                });
                return Ok(Some(ServerResponse::L4Book(L4Book::Snapshot {
                    coin: coin.value(),
                    time,
                    height,
                    levels: snapshot,
                })));
            }
            return Err("Snapshot Failed".into());
        }
        if let Self::UserFills { user, .. } = self {
            // HL protocol: first message after subscribe carries `isSnapshot: true`.
            // We have no historical fill store, so we honour the wire shape with an
            // empty fills array. Subsequent streaming pushes omit `isSnapshot`.
            return Ok(Some(ServerResponse::UserFills(WsUserFills {
                is_snapshot: Some(true),
                user: *user,
                fills: Vec::new(),
            })));
        }
        // orderUpdates has no documented snapshot push — HL streams events only.
        Ok(None)
    }
}

#[cfg(test)]
mod test {
    use super::coin_to_trades;
    use crate::types::node_data::{Batch, NodeDataFill};

    fn make_batch(fills_json: &str) -> Batch<NodeDataFill> {
        let s = format!(
            r#"{{"local_time":"2026-05-24T00:00:00","block_time":"2026-05-24T00:00:00","block_number":1,"events":{fills_json}}}"#
        );
        serde_json::from_str(&s).expect("batch fixture must parse")
    }

    fn fill(side: &str, tid: u64, coin: &str, crossed: bool) -> String {
        format!(
            r#"["0x0000000000000000000000000000000000000000",{{"coin":"{coin}","px":"100","sz":"1","side":"{side}","time":0,"startPosition":"0","dir":"Buy","closedPnl":"0","hash":"0x0","oid":0,"crossed":{crossed},"fee":"0","tid":{tid},"feeToken":"USDC","liquidation":null}}]"#
        )
    }

    #[test]
    fn coin_to_trades_pairs_ask_and_bid_by_tid() {
        let fills = format!("[{},{}]", fill("A", 1, "BTC", true), fill("B", 1, "BTC", false));
        let batch = make_batch(&fills);
        let trades = coin_to_trades(&batch);
        assert_eq!(trades.get("BTC").map(Vec::len), Some(1));
    }

    #[test]
    fn coin_to_trades_skips_same_side_only_no_panic() {
        // Two Ask fills with the same tid would crash the old impl.
        let fills = format!("[{},{}]", fill("A", 1, "BTC", true), fill("A", 1, "BTC", false));
        let batch = make_batch(&fills);
        let trades = coin_to_trades(&batch);
        assert!(trades.get("BTC").is_none());
    }

    #[test]
    fn coin_to_trades_skips_unpaired_single_fill() {
        let fills = format!("[{}]", fill("A", 5, "ETH", true));
        let batch = make_batch(&fills);
        let trades = coin_to_trades(&batch);
        assert!(trades.is_empty());
    }

    #[test]
    fn coin_to_trades_groups_by_tid_not_adjacency() {
        // Order: Ask(tid=1), Ask(tid=2), Bid(tid=1), Bid(tid=2)
        // Old impl would pair adjacent (tid=1+tid=2) and panic on same-side groups.
        // New impl groups by tid -> two valid trades.
        let fills = format!(
            "[{},{},{},{}]",
            fill("A", 1, "BTC", true),
            fill("A", 2, "BTC", true),
            fill("B", 1, "BTC", false),
            fill("B", 2, "BTC", false),
        );
        let batch = make_batch(&fills);
        let trades = coin_to_trades(&batch);
        assert_eq!(trades.get("BTC").map(Vec::len), Some(2));
    }

    #[test]
    fn coin_to_trades_groups_by_coin() {
        let fills = format!(
            "[{},{},{},{}]",
            fill("A", 1, "BTC", true),
            fill("B", 1, "BTC", false),
            fill("A", 2, "ETH", true),
            fill("B", 2, "ETH", false),
        );
        let batch = make_batch(&fills);
        let trades = coin_to_trades(&batch);
        assert_eq!(trades.get("BTC").map(Vec::len), Some(1));
        assert_eq!(trades.get("ETH").map(Vec::len), Some(1));
    }
}

#[cfg(test)]
mod wanted_keys_test {
    use super::{coin_to_book_updates, user_to_fills, user_to_order_updates};
    use crate::types::{
        Fill, L4BookUpdates, WsOrder,
        node_data::{Batch, NodeDataFill, NodeDataOrderDiff, NodeDataOrderStatus},
    };
    use alloy::primitives::Address;
    use std::collections::{BTreeMap, HashMap, HashSet};

    // The pre-filtering implementations: group everything, then look up.
    fn old_book_updates(
        diff_batch: &Batch<NodeDataOrderDiff>,
        status_batch: &Batch<NodeDataOrderStatus>,
    ) -> HashMap<String, L4BookUpdates> {
        let (time, height) = (diff_batch.block_time(), diff_batch.block_number());
        let mut updates = HashMap::new();
        for diff in diff_batch.clone().events() {
            updates.entry(diff.coin().value()).or_insert_with(|| L4BookUpdates::new(time, height)).book_diffs.push(diff);
        }
        for status in status_batch.clone().events() {
            let coin = status.order.coin.clone();
            updates.entry(coin).or_insert_with(|| L4BookUpdates::new(time, height)).order_statuses.push(status);
        }
        updates
    }

    fn old_order_updates(status_batch: &Batch<NodeDataOrderStatus>) -> HashMap<Address, Vec<WsOrder>> {
        let mut by_user: HashMap<Address, Vec<WsOrder>> = HashMap::new();
        for status in status_batch.clone().events() {
            by_user.entry(status.user).or_default().push(WsOrder::from_node_status(&status));
        }
        by_user
    }

    fn old_user_fills(batch: &Batch<NodeDataFill>) -> HashMap<Address, Vec<Fill>> {
        let mut by_user: HashMap<Address, Vec<Fill>> = HashMap::new();
        for NodeDataFill(user, fill) in batch.clone().events() {
            by_user.entry(user).or_default().push(fill);
        }
        by_user
    }

    // What a client subscribed to `keys` would have been sent, as JSON.
    fn sent<K: std::hash::Hash + Eq + Clone + Ord, V: serde::Serialize>(
        mut map: HashMap<K, V>,
        keys: &[K],
    ) -> BTreeMap<K, serde_json::Value> {
        keys.iter()
            .filter_map(|k| map.remove(k).map(|v| (k.clone(), serde_json::to_value(v).unwrap_or_default())))
            .collect()
    }

    fn load<E: for<'a> serde::Deserialize<'a>>(fixture: &serde_json::Value, name: &str) -> Vec<Batch<E>> {
        fixture[name]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|l| l.as_str().and_then(|l| serde_json::from_str(l).ok()))
            .collect()
    }

    // Real blocks captured from the node (server/tmp/fixture/blocks.json, not
    // committed). Skipped when absent.
    #[test]
    fn filtered_grouping_matches_old_on_real_blocks() {
        let Ok(raw) = std::fs::read_to_string("tmp/fixture/blocks.json") else {
            eprintln!("fixture missing, skipping");
            return;
        };
        let fixture: serde_json::Value = serde_json::from_str(&raw).unwrap_or_default();
        let statuses: Vec<Batch<NodeDataOrderStatus>> = load(&fixture, "statuses");
        let diffs: Vec<Batch<NodeDataOrderDiff>> = load(&fixture, "diffs");
        let fills: Vec<Batch<NodeDataFill>> = load(&fixture, "fills");
        assert!(statuses.len() >= 10 && diffs.len() >= 10 && fills.len() >= 10, "fixture did not parse");

        // Busiest coins/users, a quiet one, and one that never appears.
        let mut coin_count: HashMap<String, usize> = HashMap::new();
        let mut user_count: HashMap<Address, usize> = HashMap::new();
        for b in &statuses {
            for s in b.events_ref() {
                *coin_count.entry(s.order.coin.clone()).or_default() += 1;
                *user_count.entry(s.user).or_default() += 1;
            }
        }
        for b in &fills {
            for f in b.events_ref() {
                *user_count.entry(f.0).or_default() += 1;
            }
        }
        let mut coins: Vec<String> = coin_count.keys().cloned().collect();
        coins.sort_by_key(|c| std::cmp::Reverse(coin_count[c]));
        let mut users: Vec<Address> = user_count.keys().copied().collect();
        users.sort_by_key(|u| std::cmp::Reverse(user_count[u]));
        let coin_sets: Vec<Vec<String>> = vec![
            vec![],
            coins[..3].to_vec(),
            vec![coins[coins.len() - 1].clone(), "NO_SUCH_COIN".to_string()],
            coins.clone(),
        ];
        let fill_users: Vec<Address> = fills.iter().flat_map(|b| b.events_ref().iter().map(|f| f.0)).collect();
        let user_sets: Vec<Vec<Address>> =
            vec![vec![], users[..3].to_vec(), vec![fill_users[0], Address::repeat_byte(0xab)], users.clone()];

        let mut compared = 0usize;
        for (sb, db) in statuses.iter().zip(&diffs) {
            for set in &coin_sets {
                let wanted: HashSet<&str> = set.iter().map(String::as_str).collect();
                let new = sent(coin_to_book_updates(db, sb, &wanted), set);
                let old = sent(old_book_updates(db, sb), set);
                assert_eq!(new, old, "l4Book {set:?}");
                compared += new.len();
            }
            for set in &user_sets {
                let wanted: HashSet<Address> = set.iter().copied().collect();
                assert_eq!(sent(user_to_order_updates(sb, &wanted), set), sent(old_order_updates(sb), set));
            }
        }
        for fb in &fills {
            for set in &user_sets {
                let wanted: HashSet<Address> = set.iter().copied().collect();
                let new = sent(user_to_fills(fb, &wanted), set);
                assert_eq!(new, sent(old_user_fills(fb), set));
                compared += new.len();
            }
        }
        assert!(compared > 100, "comparison was vacuous ({compared} keys)");
    }
}

#[cfg(test)]
mod l2_frame_test {
    use super::*;
    use crate::{
        listeners::order_book::compute_l2_snapshots,
        order_book::multi_book::{OrderBooks, load_snapshots_from_str},
        types::{L4Order, inner::InnerL4Order},
    };

    /// Real hl-node L4 snapshot (copy of /root/out.json); test-only, not committed.
    const FIXTURE: &str = "tmp/fixture/out.snap.json";

    // The per-client path before frames were shared.
    fn old_l2_message(l2_snapshots: &L2Snapshots, sub: &Subscription, time: u64) -> Option<String> {
        let Subscription::L2Book { coin, n_sig_figs, n_levels, mantissa } = sub else { return None };
        let snapshot = l2_snapshots.as_ref().get(&Coin::new(coin))?.get(&L2SnapshotParams::new(*n_sig_figs, *mantissa))?;
        let snapshot = snapshot.truncate(n_levels.unwrap_or(DEFAULT_LEVELS)).export_inner_snapshot();
        let l2_book = L2Book::from_l2_snapshot(coin.clone(), snapshot, time);
        Some(serde_json::to_string(&ServerResponse::L2Book(l2_book)).unwrap())
    }

    fn shared_l2_message(
        shared: &SnapshotShared,
        l2_snapshots: &L2Snapshots,
        sub: &Subscription,
        time: u64,
    ) -> Option<FrameView> {
        let Subscription::L2Book { coin, n_sig_figs, n_levels, mantissa } = sub else { return None };
        let key = (coin.clone(), *n_sig_figs, *mantissa, n_levels.unwrap_or(DEFAULT_LEVELS));
        shared.l2_frame(key, |key| build_l2_frame(l2_snapshots, key, time))
    }

    #[test]
    fn shared_l2_frames_match_per_client_serialization() {
        let Ok(json) = fs::read_to_string(FIXTURE) else {
            eprintln!("skipping: {FIXTURE} not present");
            return;
        };
        let (_, snapshot) = load_snapshots_from_str::<InnerL4Order, (Address, L4Order)>(&json).unwrap();
        drop(json);
        let l2_snapshots = compute_l2_snapshots(&OrderBooks::from_snapshots(snapshot, true));
        let time = 1_790_000_000_123;
        let shared = SnapshotShared::default();

        let params = [
            (None, None),
            (Some(5), None),
            (Some(5), Some(2)),
            (Some(5), Some(5)),
            (Some(4), None),
            (Some(3), None),
            (Some(2), None),
        ];
        let mut coins: Vec<String> = l2_snapshots.as_ref().keys().map(|c| c.value()).collect();
        coins.push("NO_SUCH_COIN".to_string());
        let subs: Vec<Subscription> = coins
            .iter()
            .flat_map(|coin| {
                params.into_iter().flat_map(move |(n_sig_figs, mantissa)| {
                    [None, Some(1), Some(5), Some(100)].map(|n_levels| Subscription::L2Book {
                        coin: coin.clone(),
                        n_sig_figs,
                        n_levels,
                        mantissa,
                    })
                })
            })
            .collect();
        let check = |shared: &SnapshotShared, time: u64| {
            let mut n_compared = 0;
            for sub in &subs {
                let old = old_l2_message(&l2_snapshots, sub, time);
                let new = shared_l2_message(shared, &l2_snapshots, sub, time);
                assert_eq!(old.as_deref().map(str::as_bytes), new.as_ref().map(|f| &f.payload[..]), "{sub:?}");
                // A second client gets the cached frame: same bytes, no copy.
                let again = shared_l2_message(shared, &l2_snapshots, sub, time);
                assert_eq!(new.map(|f| f.payload.as_ptr()), again.map(|f| f.payload.as_ptr()), "{sub:?}");
                n_compared += usize::from(old.is_some());
            }
            assert!(n_compared > 1000 * params.len() * 4, "{n_compared}");
        };
        // First snapshot: every key is new (mutex path).
        check(&shared, time);
        assert!(shared.known.is_empty());
        // Next snapshot: the same keys are served from the lock-free slots, with the new time.
        let next = SnapshotShared::following(Some(&shared));
        assert_eq!(next.known.len(), subs.len());
        check(&next, time + 1);
        assert!(next.new.lock().unwrap().is_empty());
    }

    #[test]
    fn unrequested_keys_drop_out() {
        let key = |coin: &str| -> L2FrameKey { (coin.to_string(), None, None, 20) };
        let frame = |_: &L2FrameKey| Some(FrameView::text("x".to_string()));
        let first = SnapshotShared::default();
        first.l2_frame(key("BTC"), frame);
        first.l2_frame(key("ETH"), frame);
        let second = SnapshotShared::following(Some(&first));
        second.l2_frame(key("BTC"), frame); // lock-free slot
        second.l2_frame(key("SOL"), frame); // new this snapshot
        let third = SnapshotShared::following(Some(&second));
        let mut keys: Vec<_> = third.known.keys().map(|k| k.0.clone()).collect();
        keys.sort();
        assert_eq!(keys, ["BTC", "SOL"]);
        assert!(SnapshotShared::following(None).known.is_empty());
    }

    #[test]
    fn concurrent_clients_get_identical_frames() {
        let key: L2FrameKey = ("BTC".to_string(), None, None, 20);
        let first = SnapshotShared::default();
        first.l2_frame(key.clone(), |_| Some(FrameView::text("previous".to_string())));
        // Mutex path (new key) and lock-free path (key known from the previous snapshot).
        for shared in [SnapshotShared::default(), SnapshotShared::following(Some(&first))] {
            let payloads: Vec<Vec<u8>> = std::thread::scope(|scope| {
                let handles: Vec<_> = (0..8)
                    .map(|i| {
                        let (shared, key) = (&shared, key.clone());
                        scope.spawn(move || {
                            let frame =
                                shared.l2_frame(key, |_| Some(FrameView::text(format!("same bytes (built by {i})"))));
                            frame.unwrap().payload.to_vec()
                        })
                    })
                    .collect();
                handles.into_iter().map(|h| h.join().unwrap()).collect()
            });
            assert!(payloads.iter().all(|p| p == &payloads[0]));
            assert_ne!(payloads[0], b"previous");
        }
    }

    #[test]
    fn subscription_summary_counts_by_kind() {
        let subs: HashSet<Subscription> = [
            Subscription::L2Book { coin: "BTC".into(), n_sig_figs: None, n_levels: None, mantissa: None },
            Subscription::L2Book { coin: "ETH".into(), n_sig_figs: Some(5), n_levels: None, mantissa: None },
            Subscription::Trades { coin: "BTC".into() },
        ]
        .into_iter()
        .collect();
        assert_eq!(subscription_summary(&subs), "l2Book=2 trades=1");
        assert_eq!(subscription_summary(&HashSet::new()), "no subscriptions");
    }
}
