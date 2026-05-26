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
};

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
    },

    #[serde(rename = "market")]
    Market {
        qty: u64,
        side: String,
    },

    #[serde(rename = "cancel")]
    Cancel {
        order_id: String,
    },
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

        let (job_tx, job_rx) =
            mpsc::channel::<String>(
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

            let worker =
                tokio::spawn(async move {

                    loop {

                        let next_payload = {

                            let mut receiver =
                                job_rx_clone
                                    .lock()
                                    .await;

                            receiver.recv().await
                        };

                        let payload =
                            match next_payload {

                                Some(payload) => payload,

                                None => break,
                            };

                        let result =
                            blast_single_order(
                                &target_clone,
                                payload,
                            ).await;

                        if result_tx_clone
                            .send(result)
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

        for i in 0..total_orders {

            let payload =
                generate_single_order(i);

            if job_tx.send(payload)
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
) -> String {

    let mut rng =
        rand::thread_rng();

    let roll =
        rng.gen_range(1..=100);

    let side = if roll % 2 == 0 {
        "buy".to_string()
    } else {
        "sell".to_string()
    };

    let order = match roll {

        1..=60 => Order::Limit {
            price:
                rng.gen_range(90..110),
            qty:
                rng.gen_range(1..50),
            side,
        },

        61..=85 => Order::Market {
            qty:
                rng.gen_range(1..20),
            side,
        },

        _ => Order::Cancel {
            order_id:
                format!("orig-{}", index),
        },
    };

    format!(
        "{}\n",
        serde_json::to_string(&order)
            .unwrap(),
    )
}

async fn blast_single_order(
    target: &str,
    payload: String,
) -> Result<u64, ()> {

    let start =
        Instant::now();

    let connection = timeout(
        SOCKET_TIMEOUT,
        TcpStream::connect(target),
    ).await;

    let mut stream =
        match connection {

            Ok(Ok(stream)) => stream,

            _ => return Err(()),
        };

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
        [0u8; 64];

    let read_result = timeout(
        SOCKET_TIMEOUT,
        stream.read(&mut buffer),
    ).await;

    match read_result {

        Ok(Ok(_)) => {}

        _ => return Err(()),
    }

    Ok(
        start.elapsed()
            .as_micros() as u64
    )
}

fn calculate_percentiles(
    latencies: &mut Vec<u64>,
) -> LatencyPercentiles {

    if latencies.is_empty() {

        return LatencyPercentiles {
            p50_micros: 0,
            p90_micros: 0,
            p99_micros: 0,
        };
    }

    latencies.sort_unstable();

    let len =
        latencies.len();

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