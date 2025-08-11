//! gRPC client for communicating with sinex-ingestd

use crate::proto::{
    ingest_service_client::IngestServiceClient, EventBatch, HealthRequest,
    RawEvent as ProtoRawEvent,
};
use crate::{SatelliteError, SatelliteResult};
use sinex_core::db::models::RawEvent;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tonic::transport::{Channel, Endpoint, Uri};
use tracing::{debug, error, info, instrument, warn};

/// Default schema version for events
const DEFAULT_SCHEMA_VERSION: &str = "1.0.0";

/// Default timeout for normal gRPC operations
const DEFAULT_OPERATION_TIMEOUT: Duration = Duration::from_secs(30);

/// Default timeout for health checks
const DEFAULT_HEALTH_TIMEOUT: Duration = Duration::from_secs(5);

/// Circuit breaker states
#[derive(Debug, Clone, PartialEq)]
enum CircuitState {
    Closed,   // Normal operation
    Open,     // Circuit breaker is open, fail fast
    HalfOpen, // Testing if service is back
}

/// Circuit breaker for gRPC client
#[derive(Debug)]
struct CircuitBreaker {
    state: Arc<RwLock<CircuitState>>,
    failure_count: Arc<RwLock<u32>>,
    failure_threshold: u32,
    recovery_timeout: Duration,
    last_failure_time: Arc<RwLock<Option<std::time::Instant>>>,
}

impl CircuitBreaker {
    fn new(failure_threshold: u32, recovery_timeout: Duration) -> Self {
        Self {
            state: Arc::new(RwLock::new(CircuitState::Closed)),
            failure_count: Arc::new(RwLock::new(0)),
            failure_threshold,
            recovery_timeout,
            last_failure_time: Arc::new(RwLock::new(None)),
        }
    }

    async fn can_execute(&self) -> bool {
        let state = self.state.read().await;
        match *state {
            CircuitState::Closed => true,
            CircuitState::Open => {
                drop(state); // Release read lock
                let last_failure = self.last_failure_time.read().await;
                if let Some(time) = *last_failure {
                    if time.elapsed() > self.recovery_timeout {
                        drop(last_failure); // Release read lock
                        let mut state = self.state.write().await;
                        *state = CircuitState::HalfOpen;
                        info!("Circuit breaker transitioning to half-open state");
                        return true;
                    }
                }
                false
            }
            CircuitState::HalfOpen => true,
        }
    }

    async fn record_success(&self) {
        let mut failure_count = self.failure_count.write().await;
        *failure_count = 0;
        let mut state = self.state.write().await;
        *state = CircuitState::Closed;
        debug!("Circuit breaker reset to closed state");
    }

    async fn record_failure(&self) {
        let mut failure_count = self.failure_count.write().await;
        *failure_count += 1;

        if *failure_count >= self.failure_threshold {
            let mut state = self.state.write().await;
            *state = CircuitState::Open;
            let mut last_failure = self.last_failure_time.write().await;
            *last_failure = Some(std::time::Instant::now());
            warn!(
                failure_count = *failure_count,
                threshold = self.failure_threshold,
                "Circuit breaker opened due to repeated failures"
            );
        }
    }
}

/// Configuration for gRPC client timeouts and reliability
#[derive(Debug, Clone)]
pub struct GrpcClientConfig {
    /// Timeout for normal operations (ingest_event, ingest_batch)
    pub operation_timeout: Duration,
    /// Timeout for health checks
    pub health_timeout: Duration,
    /// Maximum retries for failed operations
    pub max_retries: u32,
    /// Circuit breaker failure threshold
    pub circuit_breaker_threshold: u32,
    /// Circuit breaker recovery timeout
    pub circuit_breaker_recovery: Duration,
}

impl Default for GrpcClientConfig {
    fn default() -> Self {
        Self {
            operation_timeout: DEFAULT_OPERATION_TIMEOUT,
            health_timeout: DEFAULT_HEALTH_TIMEOUT,
            max_retries: 3,
            circuit_breaker_threshold: 5,
            circuit_breaker_recovery: Duration::from_secs(30),
        }
    }
}

/// Client for communicating with sinex-ingestd via gRPC over Unix Domain Socket
#[derive(Clone, Debug)]
pub struct IngestClient {
    client: IngestServiceClient<Channel>,
    config: GrpcClientConfig,
    circuit_breaker: CircuitBreaker,
}

impl IngestClient {
    /// Create a new client connected to the specified Unix Domain Socket
    #[instrument(fields(socket_path))]
    pub async fn new(socket_path: &str) -> SatelliteResult<Self> {
        let socket_path_owned = socket_path.to_string();
        // Create a channel to the Unix Domain Socket
        let channel = Endpoint::try_from("http://[::]:50051")?
            .connect_with_connector(tower::service_fn(move |_: Uri| {
                tokio::net::UnixStream::connect(socket_path_owned.clone())
            }))
            .await?;

        let client = IngestServiceClient::new(channel);

        debug!("Connected to ingestd at {}", socket_path);

        Ok(Self { client })
    }

    /// Send a single event to ingestd
    #[instrument(skip(self, event), fields(source = %event.source, event_type = %event.event_type, host = %event.host))]
    pub async fn ingest_event(&mut self, event: &RawEvent) -> SatelliteResult<String> {
        let proto_event = self.convert_to_proto(event)?;

        let request = tonic::Request::new(proto_event);
        let response = self.client.ingest_event(request).await.map_err(|e| {
            error!(error = %e, "gRPC call to ingest_event failed");
            e
        })?;

        let inner = response.into_inner();
        if inner.success {
            debug!(event_id = %inner.event_id.as_ref().unwrap_or(&"unknown".to_string()), "Successfully ingested event");
            Ok(inner.event_id.unwrap_or_default())
        } else {
            let error_msg = inner.error.unwrap_or_else(|| "Unknown error".to_string());
            error!(
                "Failed to ingest event (ID: {:?}): {}",
                inner.event_id, error_msg
            );
            Err(SatelliteError::Processing(format!(
                "Event ingestion failed: {}",
                error_msg
            )))
        }
    }

    /// Send a batch of events to ingestd
    #[instrument(skip(self, events), fields(batch_size = events.len()))]
    pub async fn ingest_batch(&mut self, events: &[RawEvent]) -> SatelliteResult<BatchResult> {
        if events.is_empty() {
            return Ok(BatchResult {
                success: true,
                event_ids: Vec::new(),
                processed_count: 0,
                failed_count: 0,
                error: None,
            });
        }

        let mut proto_events = Vec::with_capacity(events.len());
        for event in events {
            proto_events.push(self.convert_to_proto(event)?);
        }
        let batch = EventBatch {
            events: proto_events,
        };

        let request = tonic::Request::new(batch);
        let response = self.client.ingest_batch(request).await.map_err(|e| {
            error!(error = %e, batch_size = events.len(), "gRPC call to ingest_batch failed");
            e
        })?;

        let inner = response.into_inner();
        info!(
            processed_count = inner.processed_count,
            failed_count = inner.failed_count,
            success = inner.success,
            "Batch ingestion completed"
        );

        Ok(BatchResult {
            success: inner.success,
            event_ids: inner.event_ids,
            processed_count: inner.processed_count,
            failed_count: inner.failed_count,
            error: inner.error,
        })
    }

    /// Check health of ingestd service
    #[instrument(skip(self))]
    pub async fn health_check(&mut self) -> SatelliteResult<HealthStatus> {
        let request = tonic::Request::new(HealthRequest {});
        let response = self.client.health(request).await.map_err(|e| {
            error!(error = %e, "gRPC health check failed");
            e
        })?;

        let inner = response.into_inner();
        debug!("Health check result: {} - {}", inner.healthy, inner.status);

        Ok(HealthStatus {
            healthy: inner.healthy,
            status: inner.status,
            message: inner.message,
        })
    }

    /// Convert Event to protobuf format
    fn convert_to_proto(&self, event: &RawEvent) -> SatelliteResult<ProtoRawEvent> {
        let payload_json = serde_json::to_string(&event.payload)?;

        Ok(ProtoRawEvent {
            source: event.source.to_string(),
            event_type: event.event_type.to_string(),
            host: event.host.to_string(),
            payload: payload_json,
            schema_name: Some(
                event
                    .payload_schema_id
                    .as_ref()
                    .map(|id| id.to_string())
                    .unwrap_or_default(),
            ),
            schema_version: Some(DEFAULT_SCHEMA_VERSION.to_string()),
            blob_id: None, // No blob_id field in current Event structure
        })
    }
}

/// Result of batch ingestion
#[derive(Debug)]
pub struct BatchResult {
    pub success: bool,
    pub event_ids: Vec<String>,
    pub processed_count: u32,
    pub failed_count: u32,
    pub error: Option<String>,
}

/// Health status of ingestd service
#[derive(Debug)]
pub struct HealthStatus {
    pub healthy: bool,
    pub status: String,
    pub message: Option<String>,
}

/// Helper for batching events before sending
pub struct EventBatcher {
    client: IngestClient,
    batch: Vec<RawEvent>,
    batch_size: usize,
    timeout: tokio::time::Duration,
    last_flush: tokio::time::Instant,
}

impl EventBatcher {
    /// Create a new event batcher
    #[inline]
    pub fn new(client: IngestClient, batch_size: usize, timeout_secs: u64) -> Self {
        Self {
            client,
            batch: Vec::with_capacity(batch_size),
            batch_size,
            timeout: tokio::time::Duration::from_secs(timeout_secs),
            last_flush: tokio::time::Instant::now(),
        }
    }

    /// Add an event to the batch, flushing if necessary
    #[instrument(skip(self, event), fields(batch_size = self.batch.len(), source = %event.source, event_type = %event.event_type))]
    pub async fn add_event(&mut self, event: RawEvent) -> SatelliteResult<Option<BatchResult>> {
        self.batch.push(event);

        // Check if we should flush
        if self.should_flush() {
            self.flush().await.map(Some)
        } else {
            Ok(None)
        }
    }

    /// Force flush any pending events
    #[instrument(skip(self), fields(batch_size = self.batch.len()))]
    pub async fn flush(&mut self) -> SatelliteResult<BatchResult> {
        if self.batch.is_empty() {
            return Ok(BatchResult {
                success: true,
                event_ids: vec![],
                processed_count: 0,
                failed_count: 0,
                error: None,
            });
        }

        let result = self.client.ingest_batch(&self.batch).await?;

        if !result.success {
            warn!(
                failed_count = result.failed_count,
                total_events = self.batch.len(),
                processed_count = result.processed_count,
                "Batch ingestion partially failed"
            );
        } else {
            debug!(
                processed_count = result.processed_count,
                batch_size = self.batch.len(),
                "Successfully flushed event batch"
            );
        }

        self.batch.clear();
        self.last_flush = tokio::time::Instant::now();

        Ok(result)
    }

    /// Check if the batch should be flushed
    fn should_flush(&self) -> bool {
        self.batch.len() >= self.batch_size || self.last_flush.elapsed() >= self.timeout
    }

    /// Get current batch size
    pub fn batch_len(&self) -> usize {
        self.batch.len()
    }
}
