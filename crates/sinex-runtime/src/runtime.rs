use anyhow::Result;
use async_trait::async_trait;
use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
use std::sync::Mutex;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time;
use tracing::{debug, error, info, warn};

use crate::{
    dlq::DlqManager,
    sink::EventSink,
    metrics::{AgentMetrics, ErrorSeverity, create_heartbeat_event, create_error_event},
};
use sinex_core::{RawEvent, AgentStatus};

/// Retry configuration for operations
#[derive(Debug, Clone)]
pub struct RetryConfig {
    pub max_retries: u32,
    pub initial_delay: Duration,
    pub max_delay: Duration,
    pub exponential_base: u32,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(5),
            exponential_base: 2,
        }
    }
}

/// Retry an operation with exponential backoff
pub async fn retry_db_operation<F, Fut, T>(
    config: &RetryConfig,
    mut operation: F,
) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let mut delay = config.initial_delay;
    let mut last_error = None;

    for attempt in 0..=config.max_retries {
        match operation().await {
            Ok(result) => return Ok(result),
            Err(e) => {
                last_error = Some(e);
                
                if attempt < config.max_retries {
                    warn!("Attempt {} failed, retrying in {:?}: {}", 
                        attempt + 1, delay, last_error.as_ref().unwrap());
                    time::sleep(delay).await;
                    delay = std::cmp::min(
                        delay * config.exponential_base,
                        config.max_delay
                    );
                }
            }
        }
    }

    Err(last_error.unwrap())
}

/// Trait for simplified ingestors that focus only on event capture
#[async_trait]
pub trait SimpleIngestor: Send + Sync + 'static {
    /// Get the ingestor name
    fn name() -> &'static str;
    
    /// Get the ingestor version
    fn version() -> &'static str;
    
    /// Just capture events, no lifecycle management
    /// This method should run continuously until shutdown
    async fn capture_events(&mut self, event_tx: mpsc::Sender<RawEvent>) -> Result<()>;
}

/// Configuration for the ingestor runtime
#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    pub heartbeat_interval_secs: u64,
    pub retry_config: RetryConfig,
    pub batch_size: Option<usize>,
    pub batch_timeout_ms: Option<u64>,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            heartbeat_interval_secs: 60,
            retry_config: RetryConfig::default(),
            batch_size: None,
            batch_timeout_ms: None,
        }
    }
}

/// Runtime that handles common lifecycle management for all ingestors
pub struct IngestorRuntime<I: SimpleIngestor> {
    ingestor: I,
    event_sink: Arc<dyn EventSink>,
    metrics: Arc<Mutex<AgentMetrics>>,
    dlq: Arc<DlqManager>,
    config: RuntimeConfig,
    shutdown: Arc<AtomicBool>,
}

impl<I: SimpleIngestor> IngestorRuntime<I> {
    /// Create a new runtime for an ingestor
    pub fn new(
        ingestor: I,
        event_sink: Arc<dyn EventSink>,
        config: RuntimeConfig,
    ) -> Result<Self> {
        let agent_name = I::name();
        let version = I::version();
        
        let dlq = Arc::new(DlqManager::new(agent_name)?);
        let metrics = Arc::new(Mutex::new(AgentMetrics::new(agent_name, version)));
        let shutdown = Arc::new(AtomicBool::new(false));
        
        Ok(Self {
            ingestor,
            event_sink,
            metrics,
            dlq,
            config,
            shutdown,
        })
    }
    
    /// Run the ingestor with full lifecycle management
    pub async fn run(mut self) -> Result<()> {
        info!(
            agent_name = I::name(),
            version = I::version(),
            "Starting ingestor runtime"
        );
        
        // Create channel for events
        let (event_tx, event_rx) = mpsc::channel(1000);
        
        // Spawn shutdown handler
        let shutdown_handle = self.spawn_shutdown_handler();
        
        // Spawn heartbeat task
        let heartbeat_handle = self.spawn_heartbeat_task();
        
        // Spawn event processor
        let processor_handle = self.spawn_event_processor(event_rx);
        
        // Run the ingestor's capture logic
        let capture_result = self.ingestor.capture_events(event_tx.clone()).await;
        
        // Handle capture completion/error
        match &capture_result {
            Ok(_) => info!("Ingestor capture completed normally"),
            Err(e) => error!("Ingestor capture failed: {}", e),
        }
        
        // Signal shutdown
        self.shutdown.store(true, Ordering::Relaxed);
        
        // Cancel background tasks
        shutdown_handle.abort();
        heartbeat_handle.abort();
        processor_handle.abort();
        
        // Final metrics
        let final_metrics = self.metrics.lock().unwrap();
        info!(
            events_processed = final_metrics.events_processed,
            dlq_count = final_metrics.dlq_count,
            uptime_seconds = final_metrics.uptime_seconds(),
            "Ingestor runtime shutdown complete"
        );
        
        capture_result
    }
    
    /// Spawn shutdown signal handler
    fn spawn_shutdown_handler(&self) -> tokio::task::JoinHandle<()> {
        let shutdown = Arc::clone(&self.shutdown);
        
        tokio::spawn(async move {
            match tokio::signal::ctrl_c().await {
                Ok(()) => {
                    info!("Received shutdown signal");
                    shutdown.store(true, Ordering::Relaxed);
                }
                Err(e) => {
                    error!("Failed to listen for shutdown signal: {}", e);
                }
            }
        })
    }
    
    /// Spawn heartbeat task
    fn spawn_heartbeat_task(&self) -> tokio::task::JoinHandle<()> {
        let event_sink = Arc::clone(&self.event_sink);
        let metrics = Arc::clone(&self.metrics);
        let heartbeat_interval = self.config.heartbeat_interval_secs;
        let shutdown = Arc::clone(&self.shutdown);
        
        tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_secs(heartbeat_interval));
            
            loop {
                interval.tick().await;
                
                if shutdown.load(Ordering::Relaxed) {
                    break;
                }
                
                let heartbeat = {
                    let m = metrics.lock().unwrap();
                    m.create_heartbeat(AgentStatus::Running)
                };
                
                let event = create_heartbeat_event(heartbeat);
                
                if let Err(e) = event_sink.send_event(&event).await {
                    warn!("Failed to send heartbeat: {}", e);
                }
            }
        })
    }
    
    /// Spawn event processor
    fn spawn_event_processor(
        &self,
        event_rx: mpsc::Receiver<RawEvent>,
    ) -> tokio::task::JoinHandle<()> {
        let event_sink = Arc::clone(&self.event_sink);
        let dlq = Arc::clone(&self.dlq);
        let metrics = Arc::clone(&self.metrics);
        let retry_config = self.config.retry_config.clone();
        let batch_size = self.config.batch_size;
        let batch_timeout_ms = self.config.batch_timeout_ms;
        let shutdown = Arc::clone(&self.shutdown);
        let agent_name = I::name().to_string();
        
        tokio::spawn(async move {
            // Handle batching if configured
            if let (Some(size), Some(timeout_ms)) = (batch_size, batch_timeout_ms) {
                Self::process_events_batched(
                    event_rx,
                    event_sink,
                    dlq,
                    metrics,
                    retry_config,
                    size,
                    timeout_ms,
                    shutdown,
                    agent_name,
                )
                .await;
            } else {
                Self::process_events_single(
                    event_rx,
                    event_sink,
                    dlq,
                    metrics,
                    retry_config,
                    shutdown,
                    agent_name,
                )
                .await;
            }
        })
    }
    
    /// Process events one by one
    async fn process_events_single(
        mut event_rx: mpsc::Receiver<RawEvent>,
        event_sink: Arc<dyn EventSink>,
        dlq: Arc<DlqManager>,
        metrics: Arc<Mutex<AgentMetrics>>,
        retry_config: RetryConfig,
        shutdown: Arc<AtomicBool>,
        agent_name: String,
    ) {
        while let Some(event) = event_rx.recv().await {
            if shutdown.load(Ordering::Relaxed) {
                break;
            }
            
            Self::process_single_event(
                &event_sink,
                &dlq,
                &retry_config,
                &metrics,
                event,
                &agent_name,
            )
            .await;
        }
    }
    
    /// Process events in batches
    async fn process_events_batched(
        mut event_rx: mpsc::Receiver<RawEvent>,
        event_sink: Arc<dyn EventSink>,
        dlq: Arc<DlqManager>,
        metrics: Arc<Mutex<AgentMetrics>>,
        retry_config: RetryConfig,
        batch_size: usize,
        batch_timeout_ms: u64,
        shutdown: Arc<AtomicBool>,
        agent_name: String,
    ) {
        let mut batch = Vec::with_capacity(batch_size);
        let mut interval = time::interval(Duration::from_millis(batch_timeout_ms));
        
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if !batch.is_empty() {
                        Self::flush_batch(
                            &event_sink,
                            &dlq,
                            &retry_config,
                            &metrics,
                            &mut batch,
                            &agent_name,
                        )
                        .await;
                    }
                }
                Some(event) = event_rx.recv() => {
                    batch.push(event);
                    
                    if batch.len() >= batch_size {
                        Self::flush_batch(
                            &event_sink,
                            &dlq,
                            &retry_config,
                            &metrics,
                            &mut batch,
                            &agent_name,
                        )
                        .await;
                    }
                }
                else => break,
            }
            
            if shutdown.load(Ordering::Relaxed) {
                // Flush remaining events before shutdown
                if !batch.is_empty() {
                    Self::flush_batch(
                        &event_sink,
                        &dlq,
                        &retry_config,
                        &metrics,
                        &mut batch,
                        &agent_name,
                    )
                    .await;
                }
                break;
            }
        }
    }
    
    /// Process a single event with retry logic
    async fn process_single_event(
        event_sink: &Arc<dyn EventSink>,
        dlq: &Arc<DlqManager>,
        retry_config: &RetryConfig,
        metrics: &Arc<Mutex<AgentMetrics>>,
        event: RawEvent,
        agent_name: &str,
    ) {
        let result = retry_db_operation(retry_config, || async {
            event_sink.send_event(&event).await.map_err(|e| e.into())
        })
        .await;
        
        match result {
            Ok(_) => {
                metrics.lock().unwrap().increment_processed();
                debug!("Successfully sent event: {} {}", event.source, event.event_type);
            }
            Err(e) => {
                error!("Failed to send event after retries: {}", e);
                Self::handle_event_failure(dlq, metrics, event_sink, event, e, retry_config, agent_name).await;
            }
        }
    }
    
    /// Flush a batch of events
    async fn flush_batch(
        event_sink: &Arc<dyn EventSink>,
        dlq: &Arc<DlqManager>,
        retry_config: &RetryConfig,
        metrics: &Arc<Mutex<AgentMetrics>>,
        batch: &mut Vec<RawEvent>,
        agent_name: &str,
    ) {
        if batch.is_empty() {
            return;
        }
        
        let events = std::mem::take(batch);
        let event_count = events.len();
        
        debug!("Flushing batch of {} events", event_count);
        
        let result = retry_db_operation(retry_config, || async {
            event_sink.send_batch(&events).await.map_err(|e| e.into())
        })
        .await;
        
        match result {
            Ok(_) => {
                let mut m = metrics.lock().unwrap();
                for _ in 0..event_count {
                    m.increment_processed();
                }
                info!("Successfully sent batch of {} events", event_count);
            }
            Err(e) => {
                error!("Failed to send event batch after retries: {}", e);
                
                // Write each event to DLQ
                for event in events {
                    Self::handle_event_failure(
                        dlq,
                        metrics,
                        event_sink,
                        event,
                        anyhow::anyhow!("{}", e),
                        retry_config,
                        agent_name,
                    )
                    .await;
                }
            }
        }
    }
    
    /// Handle event failure by writing to DLQ
    async fn handle_event_failure(
        dlq: &Arc<DlqManager>,
        metrics: &Arc<Mutex<AgentMetrics>>,
        event_sink: &Arc<dyn EventSink>,
        event: RawEvent,
        error: anyhow::Error,
        retry_config: &RetryConfig,
        agent_name: &str,
    ) {
        match dlq.write_event(event.clone(), error.to_string(), retry_config.max_retries).await {
            Ok(dlq_path) => {
                metrics.lock().unwrap().increment_dlq();
                
                // Try to emit DLQ notification
                let dlq_event = dlq.create_dlq_notification(&event, dlq_path, error.to_string());
                
                if let Err(e2) = event_sink.send_event(&dlq_event).await {
                    // Critical failure - can't even write DLQ notifications
                    let _ = dlq.log_critical_failure(&format!(
                        "Failed to emit DLQ notification: {} (original error: {})",
                        e2, error
                    ));
                }
            }
            Err(dlq_err) => {
                // Can't even write to DLQ
                let _ = dlq.log_critical_failure(&format!(
                    "Failed to write to DLQ: {} (original error: {})",
                    dlq_err, error
                ));
                
                // Send error event
                let error_event = create_error_event(crate::agent_events::AgentError {
                    agent_name: agent_name.to_string(),
                    error_message: format!("Critical DLQ failure: {}", dlq_err),
                    error_context: "dlq_write_failure".to_string(),
                    severity: ErrorSeverity::Critical,
                    original_event_id_if_related: None,
                });
                
                let _ = event_sink.send_event(&error_event).await;
            }
        }
    }
}