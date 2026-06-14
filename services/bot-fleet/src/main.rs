use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::Utc;
use rand::Rng;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde::Serialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::signal;
use tokio::sync::{mpsc, Semaphore};
use tokio::time::timeout;
use tonic::{transport::Server, Request, Response, Status};
use tracing::{error, info, warn};

pub mod benchmark_proto {
    tonic::include_proto!("benchmark");
}

use benchmark_proto::benchmark_controller_server::{
    BenchmarkController,
    BenchmarkControllerServer,
};

use benchmark_proto::{
    BenchmarkRequest,
    BenchmarkResponse,
    LatencyPercentiles,
    StartRequest,
    MatchOrderRequest,
    CancelOrderRequest,
};

use benchmark_proto::benchmark_worker_client::BenchmarkWorkerClient;

const SERVER_ADDR: &str = "[::1]:50052";

const MAX_CONCURRENCY: usize = 10_000;

const MAX_TOTAL_ORDERS: usize = 1_000_000;

const GLOBAL_MAX_ACTIVE_BENCHMARKS: usize = 8;

const SOCKET_TIMEOUT: Duration =
    Duration::from_secs(3);

const REDPANDA_BROKER: &str =
    "127.0.0.1:9092";

const REDPANDA_TOPIC: &str =
    "benchmark-results";

const WORKER_CHANNEL_SIZE: usize = 4096;

#[derive(Serialize, Clone, Debug)]
#[serde(tag = "type")]
enum Order {

    #[serde(rename = "limit")]
    Limit {
        price: u64,
        qty: u64,
        side: String,
        order_id: u64,
    },

    #[serde(rename = "market")]
    Market {
        qty: u64,
        side: String,
        order_id: u64,
    },

    #[serde(rename = "cancel")]
    Cancel {
        order_id: u64,
    },
}

#[derive(serde::Deserialize, Debug, Clone)]
struct ContestantTrade {
    maker_order_id: u64,
    taker_order_id: u64,
    price: u64,
    quantity: u32,
}

#[derive(serde::Deserialize, Debug, Clone)]
struct ContestantResponse {
    status: Option<String>,
    trades: Option<Vec<ContestantTrade>>,
}

fn compare_trades(
    shadow: &[benchmark_proto::TradeSignal],
    contestant: &[ContestantTrade],
) -> bool {
    if shadow.len() != contestant.len() {
        return false;
    }
    for (s, c) in shadow.iter().zip(contestant.iter()) {
        if s.maker_order_id != c.maker_order_id
            || s.taker_order_id != c.taker_order_id
            || s.price != c.price
            || s.quantity != c.quantity
        {
            return false;
        }
    }
    true
}

#[derive(Serialize, Debug)]
struct TelemetryPayload {

    submission_id: String,

    total_orders: u64,

    successful_orders: u64,

    failed_orders: u64,

    success_rate: f64,

    p50_micros: u64,

    p90_micros: u64,

    p99_micros: u64,

    total_time_ms: i64,

    timestamp: i64,
}

#[derive(Clone)]
pub struct BenchmarkService {

    producer: FutureProducer,

    benchmark_limiter: Arc<Semaphore>,
}

impl BenchmarkService {

    pub fn new() -> Self {

        let producer: FutureProducer =
            ClientConfig::new()
                .set(
                    "bootstrap.servers",
                    REDPANDA_BROKER,
                )
                .set(
                    "message.timeout.ms",
                    "5000",
                )
                .set(
                    "queue.buffering.max.messages",
                    "100000",
                )
                .set(
                    "compression.type",
                    "lz4",
                )
                .set(
                    "acks",
                    "1",
                )
                .create()
                .expect(
                    "failed creating kafka producer",
                );

        Self {
            producer,
            benchmark_limiter: Arc::new(
                Semaphore::new(
                    GLOBAL_MAX_ACTIVE_BENCHMARKS,
                ),
            ),
        }
    }
}

#[tonic::async_trait]
impl BenchmarkController
for BenchmarkService {

    async fn start_benchmark(
        &self,
        request: Request<BenchmarkRequest>,
    ) -> Result<
        Response<BenchmarkResponse>,
        Status,
    > {

        let benchmark_permit =
            self.benchmark_limiter
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| {
                    Status::internal(
                        "benchmark limiter failure",
                    )
                })?;

        let req = request.into_inner();

        validate_request(&req)?;

        let total_orders =
            req.total_orders as usize;

        let concurrency =
            req.concurrency as usize;

        let target_endpoint = format!(
            "{}:{}",
            req.target_ip,
            req.port,
        );

        info!(
            vm_id = %req.vm_id,
            mode = %req.mode,
            target = %target_endpoint,
            total_orders = total_orders,
            concurrency = concurrency,
            "benchmark started",
        );

        let benchmark_start =
            Instant::now();

        let shadow_engine_url = std::env::var("SHADOW_ENGINE_URL")
            .unwrap_or_else(|_| "http://localhost:50053".to_string());

        let mut shadow_client = match BenchmarkWorkerClient::connect(shadow_engine_url).await {
            Ok(client) => Some(client),
            Err(e) => {
                warn!("failed to connect to shadow engine: {}", e);
                None
            }
        };

        if let Some(ref mut client) = shadow_client {
            let start_req = StartRequest {
                submission_id: req.vm_id.clone(),
            };
            if let Err(e) = client.start_benchmark(start_req).await {
                warn!("failed to start benchmark in shadow engine: {}", e);
            }
        }

        let (job_tx, job_rx) =
            mpsc::channel::<(usize, Order)>(
                WORKER_CHANNEL_SIZE,
            );

        let (result_tx, mut result_rx) =
            mpsc::channel::<Result<u64, ()>>(
                WORKER_CHANNEL_SIZE,
            );

        let shared_job_rx =
            Arc::new(tokio::sync::Mutex::new(
                job_rx,
            ));

        let mut workers =
            Vec::with_capacity(concurrency);

        for _ in 0..concurrency {

            let job_rx_clone =
                shared_job_rx.clone();

            let result_tx_clone =
                result_tx.clone();

            let target_clone =
                target_endpoint.clone();

            let mut shadow_client_clone = shadow_client.clone();
            let vm_id_clone = req.vm_id.clone();
            let vsock_path_clone = req.vsock_path.clone();
            let target_port = req.port;

            let worker =
                tokio::spawn(async move {

                    let is_vsock = !vsock_path_clone.trim().is_empty();
                    let mut stream_unix = None;
                    let mut stream_tcp = None;

                    if is_vsock {
                        let mut us = loop {
                            let mut s = match tokio::time::timeout(std::time::Duration::from_millis(100), tokio::net::UnixStream::connect(vsock_path_clone.trim())).await {
                                Ok(Ok(s)) => s,
                                _ => {
                                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                                    continue;
                                }
                            };
                            let handshake = format!("CONNECT {}\n", target_port);
                            if s.write_all(handshake.as_bytes()).await.is_err() {
                                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                                continue;
                            }
                            let mut buf = [0u8; 32];
                            if let Ok(Ok(n)) = tokio::time::timeout(tokio::time::Duration::from_millis(500), s.read(&mut buf)).await {
                                let resp = String::from_utf8_lossy(&buf[..n]);
                                if resp.starts_with("OK ") {
                                    break s;
                                }
                            }
                            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                        };
                        stream_unix = Some(us);
                    } else {
                        let us = loop {
                            match tokio::time::timeout(std::time::Duration::from_millis(100), tokio::net::TcpStream::connect(&target_clone)).await {
                                Ok(Ok(s)) => break s,
                                _ => {
                                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                                    continue;
                                }
                            }
                        };
                        us.set_nodelay(true).ok();
                        stream_tcp = Some(us);
                    }

                    loop {

                        let next_payload = {

                            let mut receiver =
                                job_rx_clone
                                    .lock()
                                    .await;

                            receiver.recv().await
                        };

                        let (index, order) =
                            match next_payload {

                                Some(payload) => payload,

                                None => break,
                            };

                        let payload_str = format!(
                            "{}\n",
                            serde_json::to_string(&order).unwrap(),
                        );

                        let result = if is_vsock {
                            blast_single_order(
                                stream_unix.as_mut().unwrap(),
                                payload_str,
                            ).await
                        } else {
                            blast_single_order(
                                stream_tcp.as_mut().unwrap(),
                                payload_str,
                            ).await
                        };

                        let mut success = false;
                        let mut order_latency = 0;

                        if let Ok((latency, resp_str)) = result {
                            order_latency = latency;

                            let contestant_resp: Result<ContestantResponse, _> =
                                serde_json::from_str(&resp_str);

                            if let Ok(resp) = contestant_resp {
                                let contestant_trades = resp.trades.unwrap_or_default();

                                let mut shadow_trades = Vec::new();
                                let mut grpc_success = true;

                                if let Some(ref mut client) = shadow_client_clone {
                                    match order {
                                        Order::Limit { price, qty, ref side, order_id } => {
                                            let grpc_side = if side == "buy" { 0 } else { 1 };
                                            let match_req = MatchOrderRequest {
                                                order_id,
                                                price,
                                                quantity: qty as u32,
                                                side: grpc_side,
                                                is_market: false,
                                            };
                                            let mut grpc_req = tonic::Request::new(match_req);
                                            if let Ok(mval) = tonic::metadata::MetadataValue::try_from(&vm_id_clone) {
                                                grpc_req.metadata_mut().insert("submission-id", mval);
                                            }
                                            match client.match_order(grpc_req).await {
                                                Ok(resp) => {
                                                    shadow_trades = resp.into_inner().trades;
                                                }
                                                Err(e) => {
                                                    warn!("shadow-engine MatchOrder failed: {}", e);
                                                    grpc_success = false;
                                                }
                                            }
                                        }
                                        Order::Market { qty, ref side, order_id } => {
                                            let grpc_side = if side == "buy" { 0 } else { 1 };
                                            let match_req = MatchOrderRequest {
                                                order_id,
                                                price: 0,
                                                quantity: qty as u32,
                                                side: grpc_side,
                                                is_market: true,
                                            };
                                            let mut grpc_req = tonic::Request::new(match_req);
                                            if let Ok(mval) = tonic::metadata::MetadataValue::try_from(&vm_id_clone) {
                                                grpc_req.metadata_mut().insert("submission-id", mval);
                                            }
                                            match client.match_order(grpc_req).await {
                                                Ok(resp) => {
                                                    shadow_trades = resp.into_inner().trades;
                                                }
                                                Err(e) => {
                                                    warn!("shadow-engine MatchOrder failed: {}", e);
                                                    grpc_success = false;
                                                }
                                            }
                                        }
                                        Order::Cancel { order_id } => {
                                            let cancel_req = CancelOrderRequest {
                                                order_id,
                                            };
                                            let mut grpc_req = tonic::Request::new(cancel_req);
                                            if let Ok(mval) = tonic::metadata::MetadataValue::try_from(&vm_id_clone) {
                                                grpc_req.metadata_mut().insert("submission-id", mval);
                                            }
                                            if let Err(e) = client.cancel_order(grpc_req).await {
                                                warn!("shadow-engine CancelOrder failed: {}", e);
                                                grpc_success = false;
                                            }
                                        }
                                    }
                                }

                                if grpc_success {
                                    if compare_trades(&shadow_trades, &contestant_trades) {
                                        success = true;
                                    } else {
                                        warn!(
                                            "Trade mismatch for order {}: shadow={:?}, contestant={:?}",
                                            index + 1,
                                            shadow_trades,
                                            contestant_trades
                                        );
                                    }
                                }
                            } else {
                                warn!("failed to parse contestant response: {}", resp_str);
                            }
                        }

                        let result_payload = if success {
                            Ok(order_latency)
                        } else {
                            Err(())
                        };

                        if result_tx_clone
                            .send(result_payload)
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                });

            workers.push(worker);
        }

        drop(result_tx);

        let mut active_limit_orders = Vec::new();
        for i in 0..total_orders {

            let order =
                generate_single_order(i, &mut active_limit_orders);

            if job_tx.send((i, order))
                .await
                .is_err()
            {
                return Err(
                    Status::internal(
                        "failed dispatching jobs",
                    ),
                );
            }
        }

        drop(job_tx);

        for worker in workers {

            if let Err(e) = worker.await {

                error!(
                    error = ?e,
                    "worker task failed",
                );
            }
        }

        let mut latencies =
            Vec::with_capacity(total_orders);

        let mut successful_acks = 0u64;

        let mut failed_orders = 0u64;

        while let Some(result) =
            result_rx.recv().await
        {

            match result {

                Ok(latency) => {

                    successful_acks += 1;

                    latencies.push(latency);
                }

                Err(_) => {

                    failed_orders += 1;
                }
            }
        }

        let total_time_ms =
            benchmark_start
                .elapsed()
                .as_millis() as i64;

        let percentiles =
            calculate_percentiles(
                &mut latencies,
            );

        info!(
            vm_id = %req.vm_id,
            total_time_ms = total_time_ms,
            successful_orders = successful_acks,
            failed_orders = failed_orders,
            p99_micros = percentiles.p99_micros,
            "benchmark completed",
        );

        let success_rate =
            calculate_success_rate(
                successful_acks,
                failed_orders,
            );

        let telemetry =
            TelemetryPayload {

                submission_id:
                    req.vm_id.clone(),

                total_orders:
                    total_orders as u64,

                successful_orders:
                    successful_acks,

                failed_orders,

                success_rate,

                p50_micros:
                    percentiles.p50_micros,

                p90_micros:
                    percentiles.p90_micros,

                p99_micros:
                    percentiles.p99_micros,

                total_time_ms,

                timestamp:
                    Utc::now().timestamp(),
            };

        publish_telemetry(
            &self.producer,
            telemetry,
            &req.vm_id,
        ).await;

        drop(benchmark_permit);

        Ok(Response::new(
            BenchmarkResponse {
                success: true,
                error_message:
                    String::new(),
                total_time_ms,
                successful_acks:
                    successful_acks as i32,
                failed_orders:
                    failed_orders as i32,
                latencies:
                    Some(percentiles),
            },
        ))
    }
}

fn validate_request(
    req: &BenchmarkRequest,
) -> Result<(), Status> {

    if req.vm_id.trim().is_empty() {

        return Err(
            Status::invalid_argument(
                "vm_id is required",
            ),
        );
    }

    if req.target_ip.trim().is_empty() {

        return Err(
            Status::invalid_argument(
                "target_ip is required",
            ),
        );
    }

    if req.port <= 0 {

        return Err(
            Status::invalid_argument(
                "invalid port",
            ),
        );
    }

    if req.total_orders <= 0 {

        return Err(
            Status::invalid_argument(
                "invalid total_orders",
            ),
        );
    }

    if req.total_orders as usize
        > MAX_TOTAL_ORDERS
    {

        return Err(
            Status::invalid_argument(
                "total_orders exceeds max",
            ),
        );
    }

    if req.concurrency <= 0 {

        return Err(
            Status::invalid_argument(
                "invalid concurrency",
            ),
        );
    }

    if req.concurrency as usize
        > MAX_CONCURRENCY
    {

        return Err(
            Status::invalid_argument(
                "concurrency exceeds max",
            ),
        );
    }

    Ok(())
}

fn generate_single_order(
    index: usize,
    active_limit_orders: &mut Vec<u64>,
) -> Order {

    let mut rng =
        rand::thread_rng();

    let roll =
        rng.gen_range(1..=100);

    let side = if roll % 2 == 0 {
        "buy".to_string()
    } else {
        "sell".to_string()
    };

    let order_id = (index + 1) as u64;

    match roll {

        1..=60 => {
            active_limit_orders.push(order_id);
            Order::Limit {
                price: rng.gen_range(90..110),
                qty: rng.gen_range(1..50),
                side,
                order_id,
            }
        }

        61..=85 => Order::Market {
            qty:
                rng.gen_range(1..20),
            side,
            order_id,
        },

        _ => {
            let target_order_id = if active_limit_orders.is_empty() {
                order_id
            } else {
                let idx = rng.gen_range(0..active_limit_orders.len());
                active_limit_orders.remove(idx)
            };
            Order::Cancel {
                order_id: target_order_id,
            }
        }
    }
}

async fn blast_single_order<S>(
    stream: &mut S,
    payload: String,
) -> Result<(u64, String), ()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send,
{

    let start =
        Instant::now();

    let write_result = timeout(
        SOCKET_TIMEOUT,
        stream.write_all(
            payload.as_bytes(),
        ),
    ).await;

    match write_result {

        Ok(Ok(_)) => {}

        _ => return Err(()),
    }

    let mut buffer =
        [0u8; 1024];

    let read_result = timeout(
        SOCKET_TIMEOUT,
        stream.read(&mut buffer),
    ).await;

    let n = match read_result {

        Ok(Ok(n)) => n,

        _ => return Err(()),
    };

    let resp_str = String::from_utf8_lossy(&buffer[..n]).into_owned();

    Ok((
        start.elapsed()
            .as_micros() as u64,
        resp_str,
    ))
}

fn calculate_percentiles(
    latencies: &mut Vec<u64>,
) -> LatencyPercentiles {

    if latencies.is_empty() {

        return LatencyPercentiles {
            p50_micros: 0,
            p90_micros: 0,
            p99_micros: 0,
            p99_9_micros: 0,
        };
    }

    latencies.sort_unstable();

    let len =
        latencies.len();

    let p99_9_index = ((len * 999) / 1000).min(len - 1);

    LatencyPercentiles {

        p50_micros:
            percentile(
                latencies,
                len,
                50,
            ),

        p90_micros:
            percentile(
                latencies,
                len,
                90,
            ),

        p99_micros:
            percentile(
                latencies,
                len,
                99,
            ),

        p99_9_micros:
            latencies[p99_9_index],
    }
}

fn percentile(
    latencies: &[u64],
    len: usize,
    p: usize,
) -> u64 {

    let index =
        ((len * p) / 100)
            .min(len - 1);

    latencies[index]
}

fn calculate_success_rate(
    success: u64,
    failed: u64,
) -> f64 {

    let total =
        success + failed;

    if total == 0 {
        return 0.0;
    }

    success as f64 / total as f64
}

async fn publish_telemetry(
    producer: &FutureProducer,
    telemetry: TelemetryPayload,
    key: &str,
) {

    let payload =
        match serde_json::to_string(
            &telemetry,
        ) {

            Ok(payload) => payload,

            Err(e) => {

                error!(
                    error = ?e,
                    "telemetry serialization failed",
                );

                return;
            }
        };

    let record =
        FutureRecord::to(
            REDPANDA_TOPIC,
        )
        .payload(&payload)
        .key(key);

    match producer
        .send(
            record,
            Duration::from_secs(2),
        )
        .await
    {

        Ok(_) => {

            info!(
                key = %key,
                "telemetry published",
            );
        }

        Err((e, _)) => {

            warn!(
                error = ?e,
                "telemetry publish failed",
            );
        }
    }
}

#[tokio::main]
async fn main()
-> Result<
    (),
    Box<dyn std::error::Error>,
> {

    tracing_subscriber::fmt::init();

    let addr =
        SERVER_ADDR.parse()?;

    let controller =
        BenchmarkService::new();

    info!(
        address = SERVER_ADDR,
        "benchmark controller started",
    );

    Server::builder()
        .add_service(
            BenchmarkControllerServer::new(
                controller,
            ),
        )
        .serve_with_shutdown(
            addr,
            async {

                signal::ctrl_c()
                    .await
                    .expect(
                        "failed installing shutdown handler",
                    );

                info!(
                    "shutdown signal received",
                );
            },
        )
        .await?;

    info!(
        "benchmark controller shutdown complete",
    );

    Ok(())
}