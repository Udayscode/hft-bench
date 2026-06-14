use std::cmp::Reverse;
use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

use parking_lot::Mutex;
use ahash::AHashMap;

use tonic::{transport::Server, Request, Response, Status};

pub mod proto {
    tonic::include_proto!("benchmark");
}

use proto::benchmark_worker_server::{
    BenchmarkWorker,
    BenchmarkWorkerServer,
};

use proto::{
    MatchOrderRequest,
    MatchOrderResponse,
    StartRequest,
    StartResponse,
    TradeSignal,
};

const MAX_TRADES_PER_MATCH: usize = 1024;

// ── Domain types ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Buy,
    Sell,
}

impl TryFrom<i32> for Side {
    type Error = Status;

    #[inline]
    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Buy),
            1 => Ok(Self::Sell),
            _ => Err(Status::invalid_argument("invalid side")),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Order {
    pub id: u64,
    pub price: u64,
    pub quantity: u32,
    pub side: Side,
    pub is_market: bool,
}

#[derive(Debug, Clone)]
pub struct Trade {
    pub maker_order_id: u64,
    pub taker_order_id: u64,
    pub price: u64,
    pub quantity: u32,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy)]
struct OrderIndexEntry {
    price: u64,
    side: Side,
    is_canceled: bool,
}

// ── Order Book ─────────────────────────────────────────────────────────────────

pub struct OrderBook {
    bids: BTreeMap<Reverse<u64>, VecDeque<Order>>,
    asks: BTreeMap<u64, VecDeque<Order>>,
    order_index: AHashMap<u64, OrderIndexEntry>,
}

impl Default for OrderBook {
    #[inline]
    fn default() -> Self { Self::new() }
}

impl OrderBook {
    #[inline]
    pub fn new() -> Self {
        Self {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            order_index: AHashMap::with_capacity(131_072),
        }
    }

    #[inline]
    pub fn reset(&mut self) {
        self.bids.clear();
        self.asks.clear();
        self.order_index.clear();
    }

    #[inline]
    fn split_asks_and_index(
        &mut self,
    ) -> (&mut BTreeMap<u64, VecDeque<Order>>, &mut AHashMap<u64, OrderIndexEntry>) {
        (&mut self.asks, &mut self.order_index)
    }

    #[inline]
    fn split_bids_and_index(
        &mut self,
    ) -> (&mut BTreeMap<Reverse<u64>, VecDeque<Order>>, &mut AHashMap<u64, OrderIndexEntry>) {
        (&mut self.bids, &mut self.order_index)
    }

    // ── Order submission ───────────────────────────────────────────────────────

    #[inline]
    pub fn submit_order(&mut self, mut incoming: Order) -> Vec<Trade> {
        if incoming.id == 0
            || (!incoming.is_market && incoming.price == 0)
            || incoming.quantity == 0
        {
            return Vec::new();
        }

        if self.order_index.contains_key(&incoming.id) {
            return Vec::new();
        }

        let mut trades = Vec::with_capacity(8);

        match incoming.side {
            Side::Buy => {
                self.match_buy(&mut incoming, &mut trades);

                if !incoming.is_market && incoming.quantity > 0 {
                    self.order_index.insert(
                        incoming.id,
                        OrderIndexEntry { price: incoming.price, side: Side::Buy, is_canceled: false },
                    );
                    self.bids
                        .entry(Reverse(incoming.price))
                        .or_default()
                        .push_back(incoming);
                }
            }

            Side::Sell => {
                self.match_sell(&mut incoming, &mut trades);

                if !incoming.is_market && incoming.quantity > 0 {
                    self.order_index.insert(
                        incoming.id,
                        OrderIndexEntry { price: incoming.price, side: Side::Sell, is_canceled: false },
                    );
                    self.asks
                        .entry(incoming.price)
                        .or_default()
                        .push_back(incoming);
                }
            }
        }

        trades
    }

    // ── Match buy ─────────────────────────────────────────────────────────────

    #[inline]
    fn match_buy(&mut self, incoming: &mut Order, trades: &mut Vec<Trade>) {
        while incoming.quantity > 0 {
            // Peek best ask (immutable, dropped immediately)
            let best_ask_price = match self.asks.first_key_value().map(|(&k, _)| k) {
                Some(p) => p,
                None => break,
            };

            if !incoming.is_market && incoming.price < best_ask_price {
                break;
            }

            // Split borrow: distinct fields → simultaneous mutable access OK
            let (asks, index) = self.split_asks_and_index();

            let queue = match asks.get_mut(&best_ask_price) {
                Some(q) => q,
                None => break,
            };

            // ── Lazy deletion sweep ────────────────────────────────────────────
            // Evict canceled orders accumulated at this level's queue front.
            // Each eviction is O(1): AHashMap remove + VecDeque pop_front.
            while queue.front().map_or(false, |o| {
                index.get(&o.id).map_or(true, |e| e.is_canceled)
            }) {
                let dead = queue.pop_front().unwrap();
                index.remove(&dead.id);
            }

            // ── Inner match loop ───────────────────────────────────────────────
            while incoming.quantity > 0 {
                while queue.front().map_or(false, |o| {
                    index.get(&o.id).map_or(true, |e| e.is_canceled)
                }) {
                    let dead = queue.pop_front().unwrap();
                    index.remove(&dead.id);
                }
                let Some(front) = queue.front_mut() else { break; };

                let matched_qty = incoming.quantity.min(front.quantity);
                trades.push(Trade {
                    maker_order_id: front.id,
                    taker_order_id: incoming.id,
                    price: best_ask_price,
                    quantity: matched_qty,
                });

                incoming.quantity -= matched_qty;
                front.quantity -= matched_qty;

                if front.quantity == 0 {
                    let completed_id = front.id;
                    queue.pop_front();
                    index.remove(&completed_id);
                }

                if trades.len() >= MAX_TRADES_PER_MATCH {
                    return;
                }
            }

            if queue.is_empty() {
                asks.remove(&best_ask_price);
            }
        }
    }

    // ── Match sell ────────────────────────────────────────────────────────────

    #[inline]
    fn match_sell(&mut self, incoming: &mut Order, trades: &mut Vec<Trade>) {
        while incoming.quantity > 0 {
            let best_bid_price = match self.bids.first_key_value().map(|(&Reverse(k), _)| k) {
                Some(p) => p,
                None => break,
            };

            if !incoming.is_market && incoming.price > best_bid_price {
                break;
            }

            let (bids, index) = self.split_bids_and_index();

            let queue = match bids.get_mut(&Reverse(best_bid_price)) {
                Some(q) => q,
                None => break,
            };

            // Lazy deletion sweep on bids side
            while queue.front().map_or(false, |o| {
                index.get(&o.id).map_or(true, |e| e.is_canceled)
            }) {
                let dead = queue.pop_front().unwrap();
                index.remove(&dead.id);
            }

            while incoming.quantity > 0 {
                while queue.front().map_or(false, |o| {
                    index.get(&o.id).map_or(true, |e| e.is_canceled)
                }) {
                    let dead = queue.pop_front().unwrap();
                    index.remove(&dead.id);
                }
                let Some(front) = queue.front_mut() else { break; };

                let matched_qty = incoming.quantity.min(front.quantity);
                trades.push(Trade {
                    maker_order_id: front.id,
                    taker_order_id: incoming.id,
                    price: best_bid_price,
                    quantity: matched_qty,
                });

                incoming.quantity -= matched_qty;
                front.quantity -= matched_qty;

                if front.quantity == 0 {
                    let completed_id = front.id;
                    queue.pop_front();
                    index.remove(&completed_id);
                }

                if trades.len() >= MAX_TRADES_PER_MATCH {
                    return;
                }
            }

            if queue.is_empty() {
                bids.remove(&Reverse(best_bid_price));
            }
        }
    }

    // ── Cancel order — O(1) lazy deletion ────────────────────────────
    //
    // BEFORE (O(N)): removed from order_index, then linear-scanned the VecDeque
    //   at the price level to find and splice out the entry.
    //
    // AFTER  (O(1)): sets is_canceled = true in order_index (single hash probe).
    //   The VecDeque entry remains until the matcher naturally sweeps that level,
    //   at which point the lazy deletion sweep above evicts it for free.
    //
    // Memory behaviour: canceled entries in VecDeque are bounded by the number of
    // active resting orders. In the benchmark price range (95–105), levels are
    // swept frequently, so dead entries live for at most a few microseconds.

    #[inline]
    pub fn cancel_order(&mut self, order_id: u64) -> bool {
        match self.order_index.get_mut(&order_id) {
            Some(entry) if !entry.is_canceled => {
                entry.is_canceled = true;
                true
            }
            _ => false,
        }
    }

    // ── Book queries ──────────────────────────────────────────────────────────

    #[inline]
    pub fn best_bid(&self) -> Option<u64> {
        self.bids.first_key_value().map(|(p, _)| p.0)
    }

    #[inline]
    pub fn best_ask(&self) -> Option<u64> {
        self.asks.first_key_value().map(|(p, _)| *p)
    }

    #[inline]
    pub fn spread(&self) -> Option<u64> {
        Some(self.best_ask()? - self.best_bid()?)
    }

    #[inline]
    pub fn total_resting_orders(&self) -> usize {
        // Exclude lazily-deleted entries so the count reflects live orders only
        self.order_index.values().filter(|e| !e.is_canceled).count()
    }
}

// ── gRPC Service Runtime ───────────────────────────────────────────────────────

#[derive(Clone)]
pub struct ShadowWorkerRuntime {
    // Map order books to submission_id to support concurrent benchmarks
    order_books: Arc<parking_lot::RwLock<AHashMap<String, Arc<Mutex<OrderBook>>>>>,
}

impl ShadowWorkerRuntime {
    #[inline]
    pub fn new() -> Self {
        Self {
            order_books: Arc::new(parking_lot::RwLock::new(AHashMap::new())),
        }
    }
}

#[tonic::async_trait]
impl BenchmarkWorker for ShadowWorkerRuntime {
    async fn start_benchmark(
        &self,
        request: Request<StartRequest>,
    ) -> Result<Response<StartResponse>, Status> {
        let req = request.into_inner();

        if req.submission_id.trim().is_empty() {
            return Err(Status::invalid_argument("submission_id cannot be empty"));
        }

        let mut books = self.order_books.write();
        // Prevent unbounded memory growth under continuous load
        if books.len() > 100 {
            books.clear();
        }
        books.insert(req.submission_id.clone(), Arc::new(Mutex::new(OrderBook::new())));

        println!(
            "[shadow-engine] benchmark state reset submission_id={}",
            req.submission_id
        );

        Ok(Response::new(StartResponse {
            success: true,
            message: "orderbook reset completed".to_string(),
        }))
    }

    async fn match_order(
        &self,
        request: Request<MatchOrderRequest>,
    ) -> Result<Response<MatchOrderResponse>, Status> {
        let submission_id = request.metadata()
            .get("submission-id")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string())
            .unwrap_or_else(|| "default".to_string());

        let req = request.into_inner();

        if req.order_id == 0 {
            return Err(Status::invalid_argument("order_id cannot be zero"));
        }
        if !req.is_market && req.price == 0 {
            return Err(Status::invalid_argument("price cannot be zero"));
        }
        if req.quantity == 0 {
            return Err(Status::invalid_argument("quantity cannot be zero"));
        }

        let side = Side::try_from(req.side)?;

        let order = Order {
            id: req.order_id,
            price: req.price,
            quantity: req.quantity,
            side,
            is_market: req.is_market,
        };

        let book_arc = {
            let books = self.order_books.read();
            books.get(&submission_id).cloned()
        };

        let book_arc = match book_arc {
            Some(arc) => arc,
            None => {
                let mut books = self.order_books.write();
                books.entry(submission_id.clone()).or_insert_with(|| Arc::new(Mutex::new(OrderBook::new()))).clone()
            }
        };

        let mut book = book_arc.lock();
        let trades = book.submit_order(order);

        let response_trades = trades
            .into_iter()
            .map(|t| TradeSignal {
                maker_order_id: t.maker_order_id,
                taker_order_id: t.taker_order_id,
                price: t.price,
                quantity: t.quantity,
            })
            .collect();

        Ok(Response::new(MatchOrderResponse { trades: response_trades }))
    }

    async fn cancel_order(
        &self,
        request: Request<proto::CancelOrderRequest>,
    ) -> Result<Response<proto::CancelOrderResponse>, Status> {
        let submission_id = request.metadata()
            .get("submission-id")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string())
            .unwrap_or_else(|| "default".to_string());

        let req = request.into_inner();

        if req.order_id == 0 {
            return Err(Status::invalid_argument("order_id cannot be zero"));
        }

        let book_arc = {
            let books = self.order_books.read();
            books.get(&submission_id).cloned()
        };

        let book_arc = match book_arc {
            Some(arc) => arc,
            None => {
                let mut books = self.order_books.write();
                books.entry(submission_id.clone()).or_insert_with(|| Arc::new(Mutex::new(OrderBook::new()))).clone()
            }
        };

        let mut book = book_arc.lock();
        let success = book.cancel_order(req.order_id);

        Ok(Response::new(proto::CancelOrderResponse { success }))
    }
}

// ── Entry point ───────────────────────────────────────────────────────────────

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let port = std::env::var("PORT").unwrap_or_else(|_| "50053".to_string());
    let addr_str = format!("[::]:{}", port);
    let addr = addr_str.parse()?;

    let runtime = ShadowWorkerRuntime::new();

    println!(
        "[shadow-engine] grpc matching engine listening on {} (parking_lot Mutex + AHashMap)",
        addr_str
    );

    Server::builder()
        .tcp_nodelay(true)
        .http2_keepalive_interval(Some(std::time::Duration::from_secs(15)))
        .http2_keepalive_timeout(Some(std::time::Duration::from_secs(5)))
        .add_service(BenchmarkWorkerServer::new(runtime))
        .serve(addr)
        .await?;

    Ok(())
}