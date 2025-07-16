//! Test utilities for satellite architecture
//! 
//! Provides helpers for testing satellites, ingestd, Redis Streams,
//! and automaton interactions.

use crate::common::prelude::*;
use redis::aio::MultiplexedConnection;
use sinex_satellite_sdk::{
    checkpoint::{CheckpointManager, CheckpointState},
    config::{EventSourceConfig, SatelliteConfig},
    grpc_client::IngestClient,
};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tonic::transport::Server;

/// Handle to a running test ingestd instance
pub struct TestIngestdHandle {
    pub socket_path: String,
    pub server_handle: JoinHandle<()>,
    pub events_received: Arc<Mutex<Vec<sinex_core::RawEvent>>>,
}

impl TestIngestdHandle {
    /// Stop the test ingestd
    pub async fn stop(self) {
        self.server_handle.abort();
        let _ = tokio::fs::remove_file(&self.socket_path).await;
    }

    /// Get events received by this ingestd
    pub async fn get_received_events(&self) -> Vec<sinex_core::RawEvent> {
        self.events_received.lock().await.clone()
    }
}

/// Handle to a running test satellite
pub struct TestSatelliteHandle {
    pub satellite_id: String,
    pub task_handle: JoinHandle<()>,
    pub events_sent: Arc<Mutex<Vec<sinex_core::RawEvent>>>,
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
    pub async fn start(
        config: SatelliteConfig,
        pool: sqlx::PgPool,
    ) -> Result<Self> {
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
                let event = sinex_events::RawEventBuilder::new(
                    "test.satellite",
                    "test.event",
                    serde_json::json!({
                        "satellite_id": satellite_id_clone,
                        "timestamp": chrono::Utc::now(),
                        "sequence": events_sent_clone.lock().await.len()
                    })
                ).build();
                
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
    pub async fn get_checkpoint(&self) -> Result<CheckpointState> {
        self.checkpoint_manager.load_checkpoint().await.map_err(|e| anyhow::anyhow!(e))
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
    ) -> Result<Self> {
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
                
                // Query for new events
                let query = if let Some(id) = last_id {
                    sqlx::query!(
                        "SELECT event_id::text, source, event_type, payload 
                         FROM core.events 
                         WHERE event_id::uuid > $1::uuid 
                         ORDER BY event_id 
                         LIMIT 10",
                        id.to_uuid()
                    )
                } else {
                    sqlx::query!(
                        "SELECT event_id::text, source, event_type, payload 
                         FROM core.events 
                         ORDER BY event_id 
                         LIMIT 10"
                    )
                };
                
                if let Ok(rows) = query.fetch_all(&pool).await {
                    for row in rows {
                        if let Ok(id) = Ulid::from_string(&row.id) {
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
    pub async fn start_test_ingestd(&self) -> Result<TestIngestdHandle> {
        start_test_ingestd_with_config(self, TestIngestdConfig::default()).await
    }

    /// Start a test satellite
    pub async fn start_test_satellite(&self, config: SatelliteConfig) -> Result<TestSatelliteHandle> {
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
                let event = sinex_events::RawEventBuilder::new(
                    "test.satellite",
                    "test.event",
                    serde_json::json!({"satellite_id": satellite_id, "timestamp": chrono::Utc::now()})
                ).build();
                
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
    pub async fn start_test_automaton(&self, automaton_type: &str) -> Result<TestAutomatonHandle> {
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
                
                // Query for new events
                let query = if let Some(id) = last_id {
                    sqlx::query!(
                        "SELECT event_id::text, source, event_type, payload 
                         FROM core.events 
                         WHERE event_id::uuid > $1::uuid 
                         ORDER BY event_id 
                         LIMIT 10",
                        id.to_uuid()
                    )
                } else {
                    sqlx::query!(
                        "SELECT event_id::text, source, event_type, payload 
                         FROM core.events 
                         ORDER BY event_id 
                         LIMIT 10"
                    )
                };
                
                if let Ok(rows) = query.fetch_all(&pool).await {
                    for row in rows {
                        if let Ok(id) = Ulid::from_string(&row.id) {
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
    ) -> Result<()> {
        use redis::AsyncCommands;
        
        let timeout = std::time::Duration::from_secs(5);
        let start = std::time::Instant::now();
        
        loop {
            let len: usize = redis.xlen(stream).await.unwrap_or(0);
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
    ) -> Result<()> {
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
    ) -> Result<CheckpointState> {
        let checkpoint = sqlx::query!(
            r#"
            SELECT 
                last_processed_id,
                processed_count,
                last_activity,
                state_data,
                checkpoint_version
            FROM core.automaton_checkpoints
            WHERE automaton_name = $1
            ORDER BY last_activity DESC
            LIMIT 1
            "#,
            automaton_id
        )
        .fetch_optional(self.pool())
        .await?;

        match checkpoint {
            Some(row) => Ok(CheckpointState {
                last_processed_id: row.last_processed_id,
                processed_count: row.processed_count as u64,
                last_activity: row.last_activity,
                data: row.data,
                version: row.version as u32,
            }),
            None => Ok(CheckpointState::default()),
        }
    }

    /// Create a test Redis connection
    pub async fn create_redis_connection(&self) -> Result<MultiplexedConnection> {
        let client = redis::Client::open("redis://127.0.0.1/")?;
        Ok(client.get_multiplexed_async_connection().await?)
    }

    /// Publish event to Redis stream
    pub async fn publish_to_redis_stream(
        &self,
        redis: &mut MultiplexedConnection,
        stream: &str,
        event: &sinex_core::RawEvent,
    ) -> Result<String> {
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
    ) -> Result<Vec<StreamMessage>> {
        use redis::AsyncCommands;
        
        let result: Vec<(String, Vec<(String, String)>)> = redis
            .xreadgroup(
                group,
                consumer,
                Some(10), // Read up to 10 messages
                false,    // Don't use NOACK
                &[(stream, ">")],
            )
            .await?;
        
        let mut messages = Vec::new();
        for (_stream, stream_messages) in result {
            for (id, fields) in stream_messages {
                messages.push(StreamMessage { id, fields });
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
) -> Result<TestIngestdHandle> {
    let socket_path = ctx.work_dir().join(format!("test-ingestd-{}.sock", Ulid::new()));
    let socket_path_str = socket_path.to_string_lossy().to_string();
    
    // Ensure parent directory exists
    if let Some(parent) = socket_path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    
    // Remove socket if it exists
    let _ = tokio::fs::remove_file(&socket_path).await;
    
    let events_received = Arc::new(Mutex::new(Vec::new()));
    let events_received_clone = events_received.clone();
    let pool = ctx.pool().clone();
    
    // Create a minimal gRPC server that accepts events
    let server_handle = tokio::spawn(async move {
        use tonic::{Request, Response, Status};
        
        #[derive(Default)]
        struct TestIngestService {
            events: Arc<Mutex<Vec<sinex_core::RawEvent>>>,
            pool: Option<sqlx::PgPool>,
            config: TestIngestdConfig,
        }
        
        #[tonic::async_trait]
        impl sinex_ingestd::proto::ingest_service_server::IngestService for TestIngestService {
            async fn ingest_event(
                &self,
                request: Request<sinex_ingestd::proto::RawEvent>,
            ) -> Result<Response<sinex_ingestd::proto::IngestResponse>, Status> {
                let event_msg = request.into_inner();
                
                // Convert proto message to RawEvent
                if let Ok(event) = sinex_core::RawEvent::try_from(event_msg) {
                    self.events.lock().await.push(event.clone());
                    
                    // Optionally store in database
                    if self.config.store_events && self.pool.is_some() {
                        let _ = sinex_db::events::insert_event_with_validator(
                            self.pool.as_ref().unwrap(),
                            &event,
                            None,
                        ).await;
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
            ) -> Result<Response<sinex_ingestd::proto::BatchResponse>, Status> {
                let batch = request.into_inner();
                let mut success_count = 0;
                
                for event_msg in batch.events {
                    if let Ok(event) = sinex_core::RawEvent::try_from(event_msg) {
                        self.events.lock().await.push(event.clone());
                        success_count += 1;
                        
                        if self.config.store_events && self.pool.is_some() {
                            let _ = sinex_db::events::insert_event_with_validator(
                                self.pool.as_ref().unwrap(),
                                &event,
                                None,
                            ).await;
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
            ) -> Result<Response<sinex_ingestd::proto::HealthResponse>, Status> {
                Ok(Response::new(sinex_ingestd::proto::HealthResponse {
                    healthy: true,
                    status: "OK".to_string(),
                    message: None,
                }))
            }
        }
        
        let service = TestIngestService {
            events: events_received_clone,
            pool: if config.store_events { Some(pool) } else { None },
            config,
        };
        
        let addr = format!("unix://{}", socket_path_str);
        
        Server::builder()
            .add_service(sinex_ingestd::proto::ingest_service_server::IngestServiceServer::new(service))
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
    pub fn get_event(&self) -> Result<sinex_core::RawEvent> {
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
            use redis::AsyncCommands;
            
            let result: redis::RedisResult<Vec<(String, Vec<(String, String)>)>> = redis_conn
                .xreadgroup(
                    &group_name,
                    &consumer_name,
                    Some(10),
                    false,
                    &[(&stream_key, ">")],
                )
                .await;
                
            match result {
                Ok(streams) => {
                    if streams.is_empty() || streams[0].1.is_empty() {
                        break; // No more messages
                    }
                    
                    for (_stream, messages) in streams {
                        for (id, _fields) in messages {
                            processed.push(id.clone());
                            
                            // Acknowledge the message
                            let _: redis::RedisResult<i64> = redis_conn
                                .xack(&stream_key, &group_name, &[&id])
                                .await;
                        }
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