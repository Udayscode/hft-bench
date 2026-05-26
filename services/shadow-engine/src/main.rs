use std::cmp::Reverse;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Arc;

use tokio::sync::RwLock;
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

// We construct SERVER_ADDR dynamically in main to support configurable ports
const MAX_TRADES_PER_MATCH: usize = 1024;

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
}

#[derive(Debug, Clone)]
pub struct Trade {
    pub maker_order_id: u64,
    pub taker_order_id: u64,
    pub price: u64,
    pub quantity: u32,
}

#[derive(Debug, Clone, Copy)]
struct OrderIndexEntry {
    price: u64,
    side: Side,
}

pub struct OrderBook {
    bids: BTreeMap<Reverse<u64>, VecDeque<Order>>,
    asks: BTreeMap<u64, VecDeque<Order>>,
    order_index: HashMap<u64, OrderIndexEntry>,
}

impl Default for OrderBook {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl OrderBook {
    #[inline]
    pub fn new() -> Self {
        Self {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            order_index: HashMap::with_capacity(131_072),
        }
    }

    #[inline]
    pub fn reset(&mut self) {
        self.bids.clear();
        self.asks.clear();
        self.order_index.clear();
    }

    #[inline]
    pub fn submit_order(&mut self, mut incoming: Order) -> Vec<Trade> {
        if incoming.id == 0 || incoming.price == 0 || incoming.quantity == 0 {
            return Vec::new();
        }

        if self.order_index.contains_key(&incoming.id) {
            return Vec::new();
        }

        let mut trades = Vec::with_capacity(8);

        match incoming.side {
            Side::Buy => {
                self.match_buy(&mut incoming, &mut trades);

                if incoming.quantity > 0 {
                    self.order_index.insert(
                        incoming.id,
                        OrderIndexEntry {
                            price: incoming.price,
                            side: Side::Buy,
                        },
                    );

                    self.bids
                        .entry(Reverse(incoming.price))
                        .or_default()
                        .push_back(incoming);
                }
            }

            Side::Sell => {
                self.match_sell(&mut incoming, &mut trades);

                if incoming.quantity > 0 {
                    self.order_index.insert(
                        incoming.id,
                        OrderIndexEntry {
                            price: incoming.price,
                            side: Side::Sell,
                        },
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

    #[inline]
    fn match_buy(&mut self, incoming: &mut Order, trades: &mut Vec<Trade>) {
        while incoming.quantity > 0 {
            let Some(mut best_ask_entry) = self.asks.first_entry() else {
                break;
            };

            let best_ask_price = *best_ask_entry.key();

            if incoming.price < best_ask_price {
                break;
            }

            let queue = best_ask_entry.get_mut();

            while incoming.quantity > 0 {
                let Some(front) = queue.front_mut() else {
                    break;
                };

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
                    let completed_order_id = front.id;
                    queue.pop_front();
                    self.order_index.remove(&completed_order_id);
                }

                if trades.len() >= MAX_TRADES_PER_MATCH {
                    return;
                }
            }

            if queue.is_empty() {
                best_ask_entry.remove();
            }
        }
    }

    #[inline]
    fn match_sell(&mut self, incoming: &mut Order, trades: &mut Vec<Trade>) {
        while incoming.quantity > 0 {
            let Some(mut best_bid_entry) = self.bids.first_entry() else {
                break;
            };

            let Reverse(best_bid_price) = *best_bid_entry.key();

            if incoming.price > best_bid_price {
                break;
            }

            let queue = best_bid_entry.get_mut();

            while incoming.quantity > 0 {
                let Some(front) = queue.front_mut() else {
                    break;
                };

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
                    let completed_order_id = front.id;
                    queue.pop_front();
                    self.order_index.remove(&completed_order_id);
                }

                if trades.len() >= MAX_TRADES_PER_MATCH {
                    return;
                }
            }

            if queue.is_empty() {
                best_bid_entry.remove();
            }
        }
    }

    #[inline]
    pub fn cancel_order(&mut self, order_id: u64) -> bool {
        let Some(index_entry) = self.order_index.remove(&order_id) else {
            return false;
        };

        match index_entry.side {
            Side::Buy => {
                let key = Reverse(index_entry.price);

                let should_remove_level = if let Some(queue) = self.bids.get_mut(&key) {
                    if let Some(position) = queue.iter().position(|o| o.id == order_id) {
                        queue.remove(position);
                    }

                    queue.is_empty()
                } else {
                    false
                };

                if should_remove_level {
                    self.bids.remove(&key);
                }
            }

            Side::Sell => {
                let should_remove_level = if let Some(queue) = self.asks.get_mut(&index_entry.price)
                {
                    if let Some(position) = queue.iter().position(|o| o.id == order_id) {
                        queue.remove(position);
                    }

                    queue.is_empty()
                } else {
                    false
                };

                if should_remove_level {
                    self.asks.remove(&index_entry.price);
                }
            }
        }

        true
    }

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
        self.order_index.len()
    }
}

#[derive(Clone)]
pub struct ShadowWorkerRuntime {
    order_book: Arc<RwLock<OrderBook>>,
}

impl ShadowWorkerRuntime {
    #[inline]
    pub fn new() -> Self {
        Self {
            order_book: Arc::new(RwLock::new(OrderBook::new())),
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
            return Err(Status::invalid_argument(
                "submission_id cannot be empty",
            ));
        }

        {
            let mut order_book = self.order_book.write().await;
            order_book.reset();
        }

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
        let req = request.into_inner();

        if req.order_id == 0 {
            return Err(Status::invalid_argument("order_id cannot be zero"));
        }

        if req.price == 0 {
            return Err(Status::invalid_argument("price cannot be zero"));
        }

        if req.quantity == 0 {
            return Err(Status::invalid_argument("quantity cannot be zero"));
        }

        let side = Side::try_from(req.side)?;

        println!(
            "[shadow-engine] matching order id={} price={} qty={} side={:?}",
            req.order_id, req.price, req.quantity, side
        );

        let order = Order {
            id: req.order_id,
            price: req.price,
            quantity: req.quantity,
            side,
        };

        let trades = {
            let mut order_book = self.order_book.write().await;
            order_book.submit_order(order)
        };

        if !trades.is_empty() {
            println!(
                "[shadow-engine] order id={} generated {} trades",
                req.order_id, trades.len()
            );
        }

        let response_trades = trades
            .into_iter()
            .map(|trade| TradeSignal {
                maker_order_id: trade.maker_order_id,
                taker_order_id: trade.taker_order_id,
                price: trade.price,
                quantity: trade.quantity,
            })
            .collect();

        Ok(Response::new(MatchOrderResponse {
            trades: response_trades,
        }))
    }

    async fn cancel_order(
        &self,
        request: Request<proto::CancelOrderRequest>,
    ) -> Result<Response<proto::CancelOrderResponse>, Status> {
        let req = request.into_inner();

        if req.order_id == 0 {
            return Err(Status::invalid_argument("order_id cannot be zero"));
        }

        println!("[shadow-engine] canceling order id={}", req.order_id);

        let success = {
            let mut order_book = self.order_book.write().await;
            order_book.cancel_order(req.order_id)
        };

        if success {
            println!("[shadow-engine] cancel successful for order id={}", req.order_id);
        } else {
            println!("[shadow-engine] cancel failed (order not found) for id={}", req.order_id);
        }

        Ok(Response::new(proto::CancelOrderResponse { success }))
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let port = std::env::var("PORT").unwrap_or_else(|_| "50053".to_string());
    let addr_str = format!("[::]:{}", port);
    let addr = addr_str.parse()?;

    let runtime = ShadowWorkerRuntime::new();

    println!(
        "[shadow-engine] grpc matching engine listening on {}",
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