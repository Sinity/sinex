//! gRPC client for communicating with sinex-ingestd

use crate::proto::{
    ingest_service_client::IngestServiceClient, EventBatch, HealthRequest,
    RawEvent as ProtoRawEvent,
};
use crate::{SatelliteError, SatelliteResult};
use sinex_events::RawEvent;
use tonic::transport::{Channel, Endpoint, Uri};
use tracing::{debug, error, warn};

/// Client for communicating with sinex-ingestd via gRPC over Unix Domain Socket
#[derive(Clone, Debug)]
pub struct IngestClient {
    client: IngestServiceClient<Channel>,
}

impl IngestClient {
    /// Create a new client connected to the specified Unix Domain Socket
    pub async fn new(socket_path: &str) -> SatelliteResult<Self> {
        let socket_path_owned = socket_path.to_string();
        let socket_path_for_log = socket_path_owned.clone();
        // Create a channel to the Unix Domain Socket
        let channel = Endpoint::try_from("http://[::]:50051")?
            .connect_with_connector(tower::service_fn(move |_: Uri| {
                tokio::net::UnixStream::connect(socket_path_owned.clone())
            }))
            .await?;

        let client = IngestServiceClient::new(channel);

        debug!("Connected to ingestd at {}", socket_path_for_log);

        Ok(Self { client })
    }

    /// Send a single event to ingestd
    pub async fn ingest_event(&mut self, event: &RawEvent) -> SatelliteResult<String> {
        let proto_event = self.convert_to_proto(event)?;

        let request = tonic::Request::new(proto_event);
        let response = self.client.ingest_event(request).await?;

        let inner = response.into_inner();
        if inner.success {
            debug!("Successfully ingested event");
            Ok(inner.event_id.unwrap_or_default())
        } else {
            let error_msg = inner.error.unwrap_or_else(|| "Unknown error".to_string());
            error!("Failed to ingest event: {}", error_msg);
            Err(SatelliteError::Processing(error_msg))
        }
    }

    /// Send a batch of events to ingestd
    pub async fn ingest_batch(&mut self, events: &[RawEvent]) -> SatelliteResult<BatchResult> {
        if events.is_empty() {
            return Ok(BatchResult {
                success: true,
                event_ids: vec![],
                processed_count: 0,
                failed_count: 0,
                error: None,
            });
        }

        let proto_events: Result<Vec<_>, _> = events
            .iter()
            .map(|event| self.convert_to_proto(event))
            .collect();

        let proto_events = proto_events?;
        let batch = EventBatch {
            events: proto_events,
        };

        let request = tonic::Request::new(batch);
        let response = self.client.ingest_batch(request).await?;

        let inner = response.into_inner();
        debug!(
            "Batch ingestion result: {} processed, {} failed",
            inner.processed_count, inner.failed_count
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
    pub async fn health_check(&mut self) -> SatelliteResult<HealthStatus> {
        let request = tonic::Request::new(HealthRequest {});
        let response = self.client.health(request).await?;

        let inner = response.into_inner();
        debug!("Health check result: {} - {}", inner.healthy, inner.status);

        Ok(HealthStatus {
            healthy: inner.healthy,
            status: inner.status,
            message: inner.message,
        })
    }

    /// Convert RawEvent to protobuf format
    fn convert_to_proto(&self, event: &RawEvent) -> SatelliteResult<ProtoRawEvent> {
        let payload_json = serde_json::to_string(&event.payload)?;

        Ok(ProtoRawEvent {
            source: event.source.clone(),
            event_type: event.event_type.clone(),
            host: event.host.clone(),
            payload: payload_json,
            schema_name: Some(
                event
                    .payload_schema_id
                    .as_ref()
                    .map(|id| id.to_string())
                    .unwrap_or_default(),
            ),
            schema_version: Some("1.0.0".to_string()), // Default version since we only have schema_id
            blob_id: None, // No blob_id field in current RawEvent structure
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
                "Batch ingestion partially failed: {} of {} events failed",
                result.failed_count,
                self.batch.len()
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
