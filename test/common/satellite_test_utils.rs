// Test utilities for satellite architecture
//
// Provides helpers for testing satellites, ingestd, Redis Streams,
// and automaton interactions.

use crate::common::prelude::*;
use redis::aio::MultiplexedConnection;
use redis::AsyncCommands;
use std::str::FromStr;
use sinex_satellite_sdk::{
    checkpoint::{CheckpointManager, CheckpointState},
    config::{EventSourceConfig, SatelliteConfig},
    grpc_client::IngestClient,
    stream_processor::Checkpoint,
};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tonic::transport::Server;
use sinex_db::queries::{EventQueries, CheckpointQueries};
use sinex_db::query_builder::{QueryBuilder, QueryParam};

// Helper function to convert proto::RawEvent to RawEvent
fn proto_to_raw_event(proto: sinex_ingestd::proto::RawEvent) -> AnyhowResult<RawEvent> {
    let payload: serde_json::Value = serde_json::from_str(&proto.payload)?;
    
    let id = Ulid::from_str(&proto.id)?;
    let ts_orig = if !proto.ts_orig.is_empty() {
        Some(chrono::DateTime::parse_from_rfc3339(&proto.ts_orig)?
            .with_timezone(&chrono::Utc))
    } else {
        None
    };
    
    Ok(RawEvent {
        id,
        ts_orig,
        ts_ingest: chrono::Utc::now(),
        source: proto.source,
        event_type: proto.event_type,
        host: proto.host.unwrap_or_else(|| "test-host".to_string()),
        ingestor_version: None,
        payload_schema_id: if let Some(schema_id_str) = proto.schema_name {
            // schema_name was used as schema_id in proto, try to parse as ULID
            Ulid::from_str(&schema_id_str).ok()
        } else {
            None
        },
        payload,
        source_event_ids: None,
        source_material_id: None,
        source_material_offset_start: None,
        source_material_offset_end: None,
        anchor_byte: None,
        associated_blob_ids: None,
    })
}

/// Handle to a running test ingestd instance
pub struct TestIngestdHandle {
    pub socket_path: String,
    pub server_handle: JoinHandle<()>,
    pub events_received: Arc<Mutex<Vec<sinex_core_types::RawEvent>>>,
}

impl TestIngestdHandle {
    /// Stop the test ingestd
    pub async fn stop(self) {
        self.server_handle.abort();
        let _ = tokio::fs::remove_file(&self.socket_path).await;
    }

    /// Get events received by this ingestd
    pub async fn get_received_events(&self) -> Vec<sinex_core_types::RawEvent> {
        self.events_received.lock().await.clone()
    }
}

/// Handle to a running test satellite
pub struct TestSatelliteHandle {
    pub satellite_id: String,
    pub task_handle: JoinHandle<()>,
    pub events_sent: Arc<Mutex<Vec<sinex_core_types::RawEvent>>>,
}

impl TestSatelliteHandle {
    /// Stop the satellite gracefully
    pub async fn shutdown(self) {
        self.task_handle.abort();
    }

    /// Simulate a crash
    pub async fn crash(self) {
        self.task_handle.abort();
    }

    /// Get count of events sent
    pub async fn events_sent_count(&self) -> usize {
        self.events_sent.lock().await.len()
    }

    /// Start a test satellite with configuration
    pub async fn start(config: SatelliteConfig, pool: sqlx::PgPool) -> AnyhowResult<Self> {
        let satellite_id = format!("test-satellite-{}", Ulid::new());
        let satellite_id_clone = satellite_id.clone();
        let events_sent = Arc::new(Mutex::new(Vec::new()));
        let events_sent_clone = events_sent.clone();

        // Create a mock satellite that sends test events
        let task_handle = tokio::spawn(async move {
            // Simplified satellite behavior for testing
            let mut interval = tokio::time::interval(std::time::Duration::from_millis(500));

            loop {
                interval.tick().await;

                // Generate a test event
                let factory = EventFactory::new("test.satellite");
                let event = factory.create_event(
                    "test.event",
                    serde_json::json!({
                        "satellite_id": satellite_id_clone,
                        "timestamp": chrono::Utc::now(),
                        "sequence": events_sent_clone.lock().await.len()
                    }),
                );

                events_sent_clone.lock().await.push(event);
            }
        });

        Ok(TestSatelliteHandle {
            satellite_id,
            task_handle,
            events_sent,
        })
    }
}

/// Handle to a running test automaton
pub struct TestAutomatonHandle {
    pub id: String,
    pub task_handle: JoinHandle<()>,
    pub checkpoint_manager: CheckpointManager,
    pub processed_events: Arc<Mutex<Vec<String>>>,
}

impl TestAutomatonHandle {
    /// Stop the automaton gracefully
    pub async fn shutdown(self) {
        self.task_handle.abort();
    }

    /// Simulate a crash
    pub async fn crash(self) {
        self.task_handle.abort();
    }

    /// Get current checkpoint
    pub async fn get_checkpoint(&self) -> AnyhowResult<CheckpointState> {
        self.checkpoint_manager
            .load_checkpoint()
            .await
            .map_err(|e| anyhow::anyhow!(e))
    }

    /// Get processed event count
    pub async fn processed_count(&self) -> usize {
        self.processed_events.lock().await.len()
    }

    /// Start a test automaton
    pub async fn start(
        automaton_type: &str,
        pool: sqlx::PgPool,
        redis: MultiplexedConnection,
    ) -> AnyhowResult<Self> {
        let automaton_id = format!("test-{}-{}", automaton_type, Ulid::new());
        let checkpoint_manager = CheckpointManager::new(
            pool.clone(),
            automaton_id.clone(),
            format!("{}-group", automaton_type),
            automaton_id.clone(),
        );

        let processed_events = Arc::new(Mutex::new(Vec::new()));
        let processed_events_clone = processed_events.clone();
        let automaton_id_clone = automaton_id.clone();

        let task_handle = tokio::spawn(async move {
            // Simplified automaton that processes events from database
            let mut last_id: Option<Ulid> = None;

            loop {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;

                // Query for new events using centralized query system
                let query = if let Some(id) = last_id {
                    QueryBuilder::select()
                        .where_clause("event_id::uuid > $1::uuid")
                        .order_by("event_id")
                        .limit(10)
                        .params(vec![QueryParam::Uuid(id.to_uuid())])
                        .build()
                } else {
                    QueryBuilder::select()
                        .order_by("event_id")
                        .limit(10)
                        .build()
                };

                let rows = EventQueries::query_events_with_fields(&pool, &query, &["event_id::text as id", "source", "event_type", "payload"])
                    .await;

                if let Ok(rows) = rows {
                    for row in rows {
                        if let Ok(id) = Ulid::from_str(&row.id) {
                            processed_events_clone.lock().await.push(row.id.clone());
                            last_id = Some(id);
                        }
                    }
                }
            }
        });

        Ok(TestAutomatonHandle {
            id: automaton_id,
            task_handle,
            checkpoint_manager,
            processed_events,
        })
    }
}

/// Extensions to TestContext for satellite architecture
impl TestContext {
    /// Start a test ingestd server
    pub async fn start_test_ingestd(&self) -> AnyhowResult<TestIngestdHandle> {
        start_test_ingestd_with_config(self, TestIngestdConfig::default()).await
    }

    /// Start a test satellite
    pub async fn start_test_satellite(
        &self,
        config: SatelliteConfig,
    ) -> AnyhowResult<TestSatelliteHandle> {
        let satellite_id = format!("test-satellite-{}", Ulid::new());
        let events_sent = Arc::new(Mutex::new(Vec::new()));
        let events_sent_clone = events_sent.clone();

        // Create a mock satellite that sends test events
        let task_handle = tokio::spawn(async move {
            // Simplified satellite behavior for testing
            let mut interval = tokio::time::interval(std::time::Duration::from_millis(100));

            loop {
                interval.tick().await;

                // Generate a test event
                let factory = EventFactory::new("test.satellite");
                let event = factory.create_event(
                    "test.event",
                    serde_json::json!({"satellite_id": satellite_id, "timestamp": chrono::Utc::now()})
                );

                events_sent_clone.lock().await.push(event);
            }
        });

        Ok(TestSatelliteHandle {
            satellite_id,
            task_handle,
            events_sent,
        })
    }

    /// Start a test automaton
    pub async fn start_test_automaton(
        &self,
        automaton_type: &str,
    ) -> AnyhowResult<TestAutomatonHandle> {
        let automaton_id = format!("test-{}-{}", automaton_type, Ulid::new());
        let checkpoint_manager = CheckpointManager::new(
            self.pool().clone(),
            automaton_id.clone(),
            format!("{}-group", automaton_type),
            automaton_id.clone(),
        );

        let processed_events = Arc::new(Mutex::new(Vec::new()));
        let processed_events_clone = processed_events.clone();
        let pool = self.pool().clone();
        let automaton_id_clone = automaton_id.clone();

        let task_handle = tokio::spawn(async move {
            // Simplified automaton that processes events from database
            let mut last_id: Option<Ulid> = None;

            loop {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;

                // Query for new events using centralized query system
                let query = if let Some(id) = last_id {
                    QueryBuilder::select()
                        .where_clause("event_id::uuid > $1::uuid")
                        .order_by("event_id")
                        .limit(10)
                        .params(vec![QueryParam::Uuid(id.to_uuid())])
                        .build()
                } else {
                    QueryBuilder::select()
                        .order_by("event_id")
                        .limit(10)
                        .build()
                };

                let rows = EventQueries::query_events_with_fields(pool, &query, &["event_id::text as id", "source", "event_type", "payload"])
                    .await;

                if let Ok(rows) = rows {
                    for row in rows {
                        if let Ok(id) = Ulid::from_str(&row.id) {
                            processed_events_clone.lock().await.push(row.id.clone());
                            last_id = Some(id);
                        }
                    }
                }
            }
        });

        Ok(TestAutomatonHandle {
            id: automaton_id,
            task_handle,
            checkpoint_manager,
            processed_events,
        })
    }

    /// Wait for events to appear in Redis stream
    pub async fn wait_for_redis_stream_length(
        &self,
        redis: &mut MultiplexedConnection,
        stream: &str,
        expected: usize,
    ) -> AnyhowResult<()> {
        use redis::AsyncCommands;

        let timeout = std::time::Duration::from_secs(5);
        let start = std::time::Instant::now();

        loop {
            let len: usize = redis.xlen::<_, usize>(stream).await.unwrap_or(0);
            if len >= expected {
                return Ok(());
            }

            if start.elapsed() > timeout {
                return Err(anyhow::anyhow!(
                    "Timeout waiting for {} events in stream {}, got {}",
                    expected,
                    stream,
                    len
                ));
            }

            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    }

    /// Wait for automaton checkpoint to reach expected state
    pub async fn wait_for_checkpoint_progress(
        &self,
        automaton: &TestAutomatonHandle,
        expected_count: u64,
    ) -> AnyhowResult<()> {
        let timeout = std::time::Duration::from_secs(10);
        let start = std::time::Instant::now();

        loop {
            let checkpoint = automaton.get_checkpoint().await?;
            if checkpoint.processed_count >= expected_count {
                return Ok(());
            }

            if start.elapsed() > timeout {
                return Err(anyhow::anyhow!(
                    "Timeout waiting for checkpoint to reach {}, got {}",
                    expected_count,
                    checkpoint.processed_count
                ));
            }

            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    }

    /// Verify automaton checkpoint state
    pub async fn verify_automaton_checkpoint(
        &self,
        automaton_id: &str,
    ) -> AnyhowResult<CheckpointState> {
        let checkpoint = CheckpointQueries::get_latest(self.pool(), automaton_id)
            .await?;

        match checkpoint {
            Some(row) => {
                let checkpoint = if let Some(last_processed_id) = row.last_processed_id {
                    // Try to parse the last_processed_id as a ULID
                    match last_processed_id.parse::<Ulid>() {
                        Ok(ulid) => Checkpoint::Internal {
                            event_id: ulid,
                            message_count: row.processed_count as u64,
                        },
                        Err(_) => Checkpoint::None, // Invalid ULID, fallback to None
                    }
                } else {
                    Checkpoint::None
                };
                
                Ok(CheckpointState {
                    checkpoint,
                    processed_count: row.processed_count as u64,
                    last_activity: row.last_activity,
                    data: row.data,
                    version: row.version as u32,
                })
            }
            None => Ok(CheckpointState::default()),
        }
    }

    /// Create a test Redis connection
    pub async fn create_redis_connection(&self) -> AnyhowResult<MultiplexedConnection> {
        let client = redis::Client::open("redis://127.0.0.1/")?;
        Ok(client.get_multiplexed_async_connection().await?)
    }

    /// Publish event to Redis stream
    pub async fn publish_to_redis_stream(
        &self,
        redis: &mut MultiplexedConnection,
        stream: &str,
        event: &sinex_core_types::RawEvent,
    ) -> AnyhowResult<String> {
        use redis::AsyncCommands;

        let event_json = serde_json::to_string(event)?;
        let message_id: String = redis
            .xadd(
                stream,
                "*",
                &[
                    ("event", event_json),
                    ("source", &event.source),
                    ("event_type", &event.event_type),
                ],
            )
            .await?;

        Ok(message_id)
    }

    /// Consume from Redis stream using consumer group
    pub async fn consume_from_redis_stream(
        &self,
        redis: &mut MultiplexedConnection,
        stream: &str,
        group: &str,
        consumer: &str,
    ) -> AnyhowResult<Vec<StreamMessage>> {
        use redis::cmd;

        // Use Redis XREADGROUP command
        let result: redis::streams::StreamReadReply = cmd("XREADGROUP")
            .arg("GROUP")
            .arg(group)
            .arg(consumer)
            .arg("COUNT")
            .arg(10)
            .arg("STREAMS")
            .arg(stream)
            .arg(">")
            .query_async(redis)
            .await?;

        let mut messages = Vec::new();
        for stream_key in result.keys {
            for stream_id in stream_key.ids {
                messages.push(StreamMessage { 
                    id: stream_id.id, 
                    fields: stream_id.map.into_iter()
                        .filter_map(|(k, v)| {
                            if let redis::Value::Data(data) = v {
                                Some((k, String::from_utf8_lossy(&data).to_string()))
                            } else {
                                None
                            }
                        })
                        .collect()
                });
            }
        }

        Ok(messages)
    }
}

/// Configuration for test ingestd
#[derive(Clone)]
pub struct TestIngestdConfig {
    pub store_events: bool,
    pub publish_to_redis: bool,
    pub redis_stream_key: String,
}

impl Default for TestIngestdConfig {
    fn default() -> Self {
        Self {
            store_events: true,
            publish_to_redis: false,
            redis_stream_key: "test:events".to_string(),
        }
    }
}

/// Start a test ingestd with custom configuration
pub async fn start_test_ingestd_with_config(
    ctx: &TestContext,
    config: TestIngestdConfig,
) -> AnyhowResult<TestIngestdHandle> {
    let socket_path = ctx
        .work_dir()
        .join(format!("test-ingestd-{}.sock", Ulid::new()));
    let socket_path_str = socket_path.to_string_lossy().to_string();

    // Ensure parent directory exists
    if let Some(parent) = socket_path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }

    // Remove socket if it exists
    let _ = tokio::fs::remove_file(&socket_path).await;

    let events_received = Arc::new(Mutex::new(Vec::new()));
    let events_received_clone = events_received.clone();
    let pool = ctx.pool();

    // Create a minimal gRPC server that accepts events
    let server_handle = tokio::spawn(async move {
        use tonic::{Request, Response, Status};

        #[derive(Default)]
        struct TestIngestService {
            events: Arc<Mutex<Vec<sinex_core_types::RawEvent>>>,
            pool: Option<sqlx::PgPool>,
            config: TestIngestdConfig,
        }

        #[tonic::async_trait]
        impl sinex_ingestd::proto::ingest_service_server::IngestService for TestIngestService {
            async fn ingest_event(
                &self,
                request: Request<sinex_ingestd::proto::RawEvent>,
            ) -> AnyhowResult<Response<sinex_ingestd::proto::IngestResponse>, Status> {
                let event_msg = request.into_inner();

                // Convert proto message to RawEvent
                if let Ok(event) = proto_to_raw_event(event_msg) {
                    self.events.lock().await.push(event.clone());

                    // Optionally store in database
                    if self.config.store_events && self.pool.is_some() {
                        let _ = sinex_db::insert_event_with_validator(
                            self.pool.as_ref().unwrap(),
                            &event,
                            None,
                        )
                        .await;
                    }
                }

                Ok(Response::new(sinex_ingestd::proto::IngestResponse {
                    success: true,
                    error: None,
                    event_id: Some(sinex_ulid::Ulid::new().to_string()),
                }))
            }

            async fn ingest_batch(
                &self,
                request: Request<sinex_ingestd::proto::EventBatch>,
            ) -> AnyhowResult<Response<sinex_ingestd::proto::BatchResponse>, Status> {
                let batch = request.into_inner();
                let mut success_count = 0;

                for event_msg in batch.events {
                    if let Ok(event) = proto_to_raw_event(event_msg) {
                        self.events.lock().await.push(event.clone());
                        success_count += 1;

                        if self.config.store_events && self.pool.is_some() {
                            let _ = sinex_db::insert_event_with_validator(
                                self.pool.as_ref().unwrap(),
                                &event,
                                None,
                            )
                            .await;
                        }
                    }
                }

                Ok(Response::new(sinex_ingestd::proto::BatchResponse {
                    success: true,
                    error: None,
                    event_ids: vec![sinex_ulid::Ulid::new().to_string(); success_count],
                    processed_count: success_count as u32,
                    failed_count: (batch.events.len() - success_count) as u32,
                }))
            }

            async fn health(
                &self,
                _request: Request<sinex_ingestd::proto::HealthRequest>,
            ) -> AnyhowResult<Response<sinex_ingestd::proto::HealthResponse>, Status> {
                Ok(Response::new(sinex_ingestd::proto::HealthResponse {
                    healthy: true,
                    status: "OK".to_string(),
                    message: None,
                }))
            }
        }

        let service = TestIngestService {
            events: events_received_clone,
            pool: if config.store_events {
                Some(pool)
            } else {
                None
            },
            config,
        };

        let addr = format!("unix://{}", socket_path_str);

        Server::builder()
            .add_service(
                sinex_ingestd::proto::ingest_service_server::IngestServiceServer::new(service),
            )
            .serve(addr.parse().unwrap())
            .await
            .unwrap();
    });

    // Wait for server to start
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    Ok(TestIngestdHandle {
        socket_path: socket_path_str,
        server_handle,
        events_received,
    })
}

/// Message from a Redis stream
#[derive(Debug, Clone)]
pub struct StreamMessage {
    pub id: String,
    pub fields: Vec<(String, String)>,
}

impl StreamMessage {
    /// Get the event from the message
    pub fn get_event(&self) -> AnyhowResult<sinex_core_types::RawEvent> {
        for (key, value) in &self.fields {
            if key == "event" {
                return serde_json::from_str(value)
                    .map_err(|e| anyhow::anyhow!("Failed to parse event: {}", e));
            }
        }
        Err(anyhow::anyhow!("No event field in stream message"))
    }
}

/// Simulate a consumer reading from Redis Streams
pub async fn simulate_redis_consumer(
    redis: MultiplexedConnection,
    stream_key: String,
    group_name: String,
    consumer_name: String,
) -> JoinHandle<Vec<String>> {
    tokio::spawn(async move {
        let mut processed = Vec::new();
        let mut redis_conn = redis;

        loop {
            use redis::{cmd, AsyncCommands};

            // Use Redis XREADGROUP command
            let result: redis::RedisResult<redis::streams::StreamReadReply> = cmd("XREADGROUP")
                .arg("GROUP")
                .arg(&group_name)
                .arg(&consumer_name)
                .arg("COUNT")
                .arg(10)
                .arg("STREAMS")
                .arg(&stream_key)
                .arg(">")
                .query_async(&mut redis_conn)
                .await;

            match result {
                Ok(reply) => {
                    let mut found_messages = false;
                    for stream_key_data in reply.keys {
                        if !stream_key_data.ids.is_empty() {
                            found_messages = true;
                            for stream_id in stream_key_data.ids {
                                processed.push(stream_id.id.clone());

                                // Acknowledge the message
                                let _: redis::RedisResult<i64> =
                                    redis_conn.xack(&stream_key, &group_name, &[&stream_id.id]).await;
                            }
                        }
                    }
                    
                    if !found_messages {
                        break; // No more messages
                    }
                }
                Err(_) => break,
            }
        }

        processed
    })
}

/// Create a test event source configuration
pub fn create_test_satellite_config(service_name: &str, ingest_socket: &str) -> SatelliteConfig {
    SatelliteConfig {
        service_name: service_name.to_string(),
        log_level: "debug".to_string(),
        ingest_socket_path: ingest_socket.to_string(),
        redis_url: "redis://localhost:6379".to_string(),
        database_url: None,
        database_pool_size: 10,
        work_dir: PathBuf::from("/tmp/sinex-test"),
        dry_run: false,
        replay: None,
    }
}

/// Create a standard event source configuration for testing
pub fn create_test_event_source_config(
    service_name: &str,
    ingest_socket: &str,
) -> EventSourceConfig {
    EventSourceConfig {
        base: create_test_satellite_config(service_name, ingest_socket),
        batch_size: 100,
        batch_timeout_secs: 1,
        source_config: std::collections::HashMap::new(),
    }
}
