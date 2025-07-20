// Mock ingestd implementation for testing
//
// Provides a simplified gRPC server that accepts events and can optionally:
// - Store events in database
// - Publish to Redis streams
// - Simulate various failure scenarios

use crate::common::prelude::*;
use redis::aio::MultiplexedConnection;
use sinex_events::RawEvent;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tonic::{transport::Server, Request, Response, Status};
// Using fully qualified paths for tonic types

/// Configuration for mock ingestd behavior
#[derive(Debug, Clone)]
pub struct MockIngestdConfig {
    /// Store received events in database
    pub store_in_database: bool,

    /// Publish events to Redis stream
    pub publish_to_redis: bool,

    /// Redis stream key to publish to
    pub redis_stream_key: String,

    /// Simulate network latency (milliseconds)
    pub latency_ms: u64,

    /// Failure rate (0.0 - 1.0, where 1.0 = always fail)
    pub failure_rate: f64,

    /// Maximum events to accept before stopping
    pub max_events: Option<usize>,
}

impl Default for MockIngestdConfig {
    fn default() -> Self {
        Self {
            store_in_database: true,
            publish_to_redis: false,
            redis_stream_key: "test:events".to_string(),
            latency_ms: 0,
            failure_rate: 0.0,
            max_events: None,
        }
    }
}

/// Mock ingestd server for testing
pub struct MockIngestd {
    pub socket_path: String,
    pub config: MockIngestdConfig,
    pub events_received: Arc<Mutex<Vec<RawEvent>>>,
    pub server_handle: Option<JoinHandle<Result<(), tonic::transport::Error>>>,
    pool: Option<sqlx::PgPool>,
    redis: Option<sinex_satellite_sdk::redis_client::RedisStreamClient>,
}

impl MockIngestd {
    /// Create a new mock ingestd
    pub fn new(socket_path: String, config: MockIngestdConfig) -> Self {
        Self {
            socket_path,
            config,
            events_received: Arc::new(Mutex::new(Vec::new())),
            server_handle: None,
            pool: None,
            redis: None,
        }
    }

    /// Set database pool for event storage
    pub fn with_database(mut self, pool: sqlx::PgPool) -> Self {
        self.pool = Some(pool);
        self
    }

    /// Set Redis connection for stream publishing
    pub fn with_redis(
        mut self,
        redis: sinex_satellite_sdk::redis_client::RedisStreamClient,
    ) -> Self {
        self.redis = Some(redis);
        self
    }

    /// Start the mock ingestd server
    pub async fn start(&mut self) -> AnyhowResult<()> {
        // Remove existing socket
        let _ = tokio::fs::remove_file(&self.socket_path).await;

        // Create parent directory if needed
        if let Some(parent) = std::path::Path::new(&self.socket_path).parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        let socket_path = self.socket_path.clone();
        let config = self.config.clone();
        let events = self.events_received.clone();
        let pool = self.pool.clone();
        let redis = self.redis.clone();

        let server_handle = tokio::spawn(async move {
            let service = MockIngestService {
                config,
                events,
                pool,
                redis: Arc::new(Mutex::new(redis)),
                event_count: Arc::new(Mutex::new(0)),
            };

            let uds = tokio::net::UnixListener::bind(&socket_path)
                .map_err(|e| tonic::transport::Error::from_source(e))?;
            let uds_stream = tokio_stream::wrappers::UnixListenerStream::new(uds);

            Server::builder()
                .add_service(MockIngestServiceServer::new(service))
                .serve_with_incoming(uds_stream)
                .await
        });

        self.server_handle = Some(server_handle);

        // Give server time to start
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        Ok(())
    }

    /// Stop the mock ingestd server
    pub async fn stop(mut self) -> AnyhowResult<()> {
        if let Some(handle) = self.server_handle.take() {
            handle.abort();
        }

        let _ = tokio::fs::remove_file(&self.socket_path).await;
        Ok(())
    }

    /// Get all events received by the mock ingestd
    pub async fn get_received_events(&self) -> Vec<RawEvent> {
        self.events_received.lock().await.clone()
    }

    /// Get count of events received
    pub async fn event_count(&self) -> usize {
        self.events_received.lock().await.len()
    }

    /// Wait for expected number of events
    pub async fn wait_for_events(&self, expected: usize, timeout_secs: u64) -> AnyhowResult<()> {
        let timeout = std::time::Duration::from_secs(timeout_secs);
        let start = std::time::Instant::now();

        loop {
            let count = self.event_count().await;
            if count >= expected {
                return Ok(());
            }

            if start.elapsed() > timeout {
                return Err(anyhow::anyhow!(
                    "Timeout waiting for {} events, got {}",
                    expected,
                    count
                ));
            }

            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }

    /// Clear received events
    pub async fn clear_events(&self) {
        self.events_received.lock().await.clear();
    }
}

// Mock gRPC service implementation
#[derive(Clone)]
struct MockIngestService {
    config: MockIngestdConfig,
    events: Arc<Mutex<Vec<RawEvent>>>,
    pool: Option<sqlx::PgPool>,
    redis: Arc<Mutex<Option<sinex_satellite_sdk::redis_client::RedisStreamClient>>>,
    event_count: Arc<Mutex<usize>>,
}

// Define the gRPC service trait (simplified for testing)
// In real implementation, this would use sinex_ingestd::proto types
#[tonic::async_trait]
trait IngestServiceTrait {
    async fn send_event(
        &self,
        request: Request<EventMessage>,
    ) -> AnyhowResult<Response<EventResponse>, Status>;

    async fn send_event_batch(
        &self,
        request: Request<EventBatch>,
    ) -> AnyhowResult<Response<BatchResponse>, Status>;
}

// Simplified message types for testing
#[derive(Debug)]
pub struct EventMessage {
    pub event: RawEvent,
}

#[derive(Debug)]
pub struct EventResponse {
    pub success: bool,
    pub message: String,
}

#[derive(Debug)]
pub struct EventBatch {
    pub events: Vec<EventMessage>,
}

#[derive(Debug)]
pub struct BatchResponse {
    pub success_count: u32,
    pub error_count: u32,
    pub message: String,
}

// Mock server type (placeholder for real gRPC server)
#[derive(Clone)]
struct MockIngestServiceServer {
    service: MockIngestService,
}

impl MockIngestServiceServer {
    fn new(service: MockIngestService) -> Self {
        Self { service }
    }
}

// Implement required tonic traits
impl tonic::codegen::Service<tonic::codegen::http::Request<tonic::transport::Body>> for MockIngestServiceServer {
    type Response = tonic::codegen::http::Response<tonic::body::BoxBody>;
    type Error = std::convert::Infallible;
    type Future = std::pin::Pin<Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;

    fn poll_ready(&mut self, _cx: &mut std::task::Context<'_>) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, _req: tonic::codegen::http::Request<tonic::transport::Body>) -> Self::Future {
        Box::pin(async move {
            Ok(tonic::codegen::http::Response::builder()
                .status(200)
                .body(Default::default())
                .unwrap())
        })
    }
}

impl tonic::server::NamedService for MockIngestServiceServer {
    const NAME: &'static str = "MockIngestService";
}

#[tonic::async_trait]
impl IngestServiceTrait for MockIngestService {
    async fn send_event(
        &self,
        request: Request<EventMessage>,
    ) -> AnyhowResult<Response<EventResponse>, Status> {
        // Simulate latency
        if self.config.latency_ms > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(self.config.latency_ms)).await;
        }

        // Simulate failures
        if self.config.failure_rate > 0.0 {
            use rand::Rng;
            let mut rng = rand::thread_rng();
            if rng.gen::<f64>() < self.config.failure_rate {
                return Err(Status::internal("Simulated failure"));
            }
        }

        let event_msg = request.into_inner();
        let event = event_msg.event;

        // Check max events limit
        let mut count = self.event_count.lock().await;
        if let Some(max) = self.config.max_events {
            if *count >= max {
                return Err(Status::resource_exhausted("Max events reached"));
            }
        }
        *count += 1;
        drop(count);

        // Store event in memory
        self.events.lock().await.push(event.clone());

        // Store in database if configured
        if self.config.store_in_database {
            if let Some(pool) = &self.pool {
                let _ = sinex_db::insert_event_with_validator(pool, &event, None).await;
            }
        }

        // Publish to Redis if configured
        if self.config.publish_to_redis {
            let mut redis_guard = self.redis.lock().await;
            if let Some(redis) = redis_guard.as_mut() {
                use redis::AsyncCommands;

                if let Ok(mut conn) = redis.get_connection().await {
                    let event_json = serde_json::to_string(&event).unwrap_or_default();
                    let _: Result<String, redis::RedisError> = conn
                        .xadd(
                            &self.config.redis_stream_key,
                            "*",
                            &[
                                ("event", event_json),
                                ("source", &event.source),
                                ("event_type", &event.event_type),
                                ("id", &event.id.to_string()),
                            ],
                        )
                        .await;
                }
            }
        }

        Ok(Response::new(EventResponse {
            success: true,
            message: "Event received".to_string(),
        }))
    }

    async fn send_event_batch(
        &self,
        request: Request<EventBatch>,
    ) -> AnyhowResult<Response<BatchResponse>, Status> {
        let batch = request.into_inner();
        let mut success_count = 0;
        let mut error_count = 0;

        for event_msg in batch.events {
            match self.send_event(Request::new(event_msg)).await {
                Ok(_) => success_count += 1,
                Err(_) => error_count += 1,
            }
        }

        Ok(Response::new(BatchResponse {
            success_count,
            error_count,
            message: format!(
                "Processed {}/{} events",
                success_count,
                success_count + error_count
            ),
        }))
    }
}

/// Builder for mock ingestd configuration
pub struct MockIngestdBuilder {
    socket_path: String,
    config: MockIngestdConfig,
    pool: Option<sqlx::PgPool>,
    redis: Option<sinex_satellite_sdk::redis_client::RedisStreamClient>,
}

impl MockIngestdBuilder {
    /// Create a new mock ingestd builder
    pub fn new(socket_path: String) -> Self {
        Self {
            socket_path,
            config: MockIngestdConfig::default(),
            pool: None,
            redis: None,
        }
    }

    /// Enable database storage
    pub fn with_database_storage(mut self, pool: sqlx::PgPool) -> Self {
        self.config.store_in_database = true;
        self.pool = Some(pool);
        self
    }

    /// Enable Redis publishing
    pub fn with_redis_publishing(
        mut self,
        redis: sinex_satellite_sdk::redis_client::RedisStreamClient,
        stream_key: &str,
    ) -> Self {
        self.config.publish_to_redis = true;
        self.config.redis_stream_key = stream_key.to_string();
        self.redis = Some(redis);
        self
    }

    /// Set latency simulation
    pub fn with_latency(mut self, latency_ms: u64) -> Self {
        self.config.latency_ms = latency_ms;
        self
    }

    /// Set failure rate simulation
    pub fn with_failure_rate(mut self, failure_rate: f64) -> Self {
        self.config.failure_rate = failure_rate;
        self
    }

    /// Set maximum events to accept
    pub fn with_max_events(mut self, max_events: usize) -> Self {
        self.config.max_events = Some(max_events);
        self
    }

    /// Build the mock ingestd
    pub fn build(self) -> MockIngestd {
        let mut mock = MockIngestd::new(self.socket_path, self.config);

        if let Some(pool) = self.pool {
            mock = mock.with_database(pool);
        }

        if let Some(redis) = self.redis {
            mock = mock.with_redis(redis);
        }

        mock
    }
}
