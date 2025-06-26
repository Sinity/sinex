use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sinex_core::{RawEvent, Timestamp, EventSender};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::fs;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

use crate::agent::{AgentError, DlqEventWritten, ErrorSeverity};

/// Unified error type for collector operations with rich context
#[derive(Debug, Error)]
pub enum CollectorError {
    #[error("Configuration error: {message}")]
    Configuration {
        message: String,
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },
    
    #[error("Connection error to {service}: {message}")]
    Connection {
        service: String,
        message: String,
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },
    
    #[error("Event processing error: {message}")]
    EventProcessing {
        message: String,
        event_type: Option<String>,
        event_id: Option<String>,
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },
    
    #[error("Resource exhausted: {resource}")]
    ResourceExhausted {
        resource: String,
        limit: Option<String>,
    },
    
    #[error("Validation error: {message}")]
    Validation {
        message: String,
        field: Option<String>,
        value: Option<String>,
    },
    
    #[error("Temporary error (retry possible): {message}")]
    Temporary {
        message: String,
        retry_after: Option<Duration>,
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },
}

/// Error context that can be attached to any error
#[derive(Debug, Clone)]
pub struct ErrorContext {
    pub collector: String,
    pub operation: String,
    pub timestamp: Timestamp,
    pub trace_id: Option<String>,
    pub additional: HashMap<String, String>,
}

/// Error categorization for proper handling
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCategory {
    /// Error that should trigger a retry
    Retryable,
    /// Error that should go to DLQ
    Permanent,
    /// Error that indicates system issue (alert ops)
    System,
    /// Error in user data/configuration
    User,
}

impl CollectorError {
    pub fn category(&self) -> ErrorCategory {
        match self {
            Self::Configuration { .. } => ErrorCategory::User,
            Self::Connection { .. } => ErrorCategory::Retryable,
            Self::EventProcessing { .. } => ErrorCategory::Permanent,
            Self::ResourceExhausted { .. } => ErrorCategory::System,
            Self::Validation { .. } => ErrorCategory::User,
            Self::Temporary { .. } => ErrorCategory::Retryable,
        }
    }
    
    pub fn should_retry(&self) -> bool {
        matches!(self.category(), ErrorCategory::Retryable)
    }
    
    pub fn is_critical(&self) -> bool {
        matches!(self.category(), ErrorCategory::System)
    }
    
    pub fn to_agent_error(&self, agent_name: &str, event_id: Option<String>) -> AgentError {
        let severity = match self.category() {
            ErrorCategory::System => ErrorSeverity::Critical,
            ErrorCategory::Permanent => ErrorSeverity::Error,
            ErrorCategory::Retryable | ErrorCategory::User => ErrorSeverity::Warning,
        };
        
        AgentError {
            agent_name: agent_name.to_string(),
            error_message: self.to_string(),
            error_context: format!("{:?}", self),
            severity,
            original_event_id_if_related: event_id,
        }
    }
}

/// Failed event wrapper for DLQ storage
#[derive(Debug, Serialize, Deserialize)]
pub struct DlqEntry {
    pub failed_at: Timestamp,
    pub failure_reason: String,
    pub retry_count: u32,
    pub original_event: RawEvent,
    pub error_category: String,
}

/// Retry policy based on error type
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub base_delay: Duration,
    pub max_delay: Duration,
    pub exponential_base: f64,
}

impl RetryPolicy {
    pub fn for_error(error: &CollectorError) -> Option<Self> {
        match error.category() {
            ErrorCategory::Retryable => Some(Self {
                max_attempts: 3,
                base_delay: Duration::from_millis(100),
                max_delay: Duration::from_secs(30),
                exponential_base: 2.0,
            }),
            _ => None,
        }
    }
    
    pub fn delay_for_attempt(&self, attempt: u32) -> Duration {
        let delay = self.base_delay.as_millis() as f64 * self.exponential_base.powi(attempt as i32);
        Duration::from_millis((delay as u64).min(self.max_delay.as_millis() as u64))
    }
}

/// Circuit breaker for handling repeated failures
pub struct CircuitBreaker {
    failure_count: AtomicU32,
    last_failure: Mutex<Option<Instant>>,
    threshold: u32,
    timeout: Duration,
}

impl CircuitBreaker {
    pub fn new(threshold: u32, timeout: Duration) -> Self {
        Self {
            failure_count: AtomicU32::new(0),
            last_failure: Mutex::new(None),
            threshold,
            timeout,
        }
    }
    
    pub fn record_success(&self) {
        self.failure_count.store(0, Ordering::Relaxed);
        if let Ok(mut guard) = self.last_failure.lock() {
            *guard = None;
        }
    }
    
    pub fn record_failure(&self) {
        self.failure_count.fetch_add(1, Ordering::Relaxed);
        if let Ok(mut guard) = self.last_failure.lock() {
            *guard = Some(Instant::now());
        }
    }
    
    pub fn is_open(&self) -> bool {
        let count = self.failure_count.load(Ordering::Relaxed);
        if count < self.threshold {
            return false;
        }
        
        if let Ok(guard) = self.last_failure.lock() {
            if let Some(last) = *guard {
                if last.elapsed() > self.timeout {
                    // Reset after timeout
                    self.failure_count.store(0, Ordering::Relaxed);
                    return false;
                }
            }
        }
        
        true
    }
}

/// Dead Letter Queue manager
pub struct DlqManager {
    agent_name: String,
    base_path: PathBuf,
    critical_failures_log: PathBuf,
    event_tx: Option<EventSender>,
}

impl DlqManager {
    pub fn new(agent_name: impl Into<String>) -> Result<Self> {
        let agent_name = agent_name.into();
        
        // Allow overriding paths via environment variables for testing
        let dlq_base = std::env::var("SINEX_DLQ_BASE")
            .unwrap_or_else(|_| "/var/lib/sinex/dlq".to_string());
        let log_base = std::env::var("SINEX_LOG_BASE")
            .unwrap_or_else(|_| "/var/log/sinex".to_string());
            
        let base_path = PathBuf::from(dlq_base).join(&agent_name);
        let critical_failures_log = PathBuf::from(log_base)
            .join(&agent_name)
            .join("critical_meta_failures.log");

        Ok(Self {
            agent_name,
            base_path,
            critical_failures_log,
            event_tx: None,
        })
    }
    
    pub fn with_event_sender(mut self, event_tx: EventSender) -> Self {
        self.event_tx = Some(event_tx);
        self
    }
    
    /// Ensure directories exist
    pub async fn initialize(&self) -> Result<()> {
        fs::create_dir_all(&self.base_path)
            .await
            .with_context(|| format!("Failed to create DLQ directory: {:?}", self.base_path))?;
        
        if let Some(parent) = self.critical_failures_log.parent() {
            fs::create_dir_all(parent)
                .await
                .with_context(|| format!("Failed to create log directory: {:?}", parent))?;
        }
        
        Ok(())
    }

    /// Write a failed event to DLQ
    pub async fn write_event(
        &self,
        event: RawEvent,
        error: &CollectorError,
        retry_count: u32,
    ) -> Result<String> {
        let entry = DlqEntry {
            failed_at: Utc::now(),
            failure_reason: error.to_string(),
            retry_count,
            original_event: event.clone(),
            error_category: format!("{:?}", error.category()),
        };

        // Generate filename with timestamp and event type (include microseconds for uniqueness)
        let filename = format!(
            "{}_{:06}_{}_{}.json",
            entry.failed_at.format("%Y%m%d_%H%M%S"),
            entry.failed_at.timestamp_subsec_micros(),
            event.source.replace('.', "_"),
            event.event_type.replace('.', "_")
        );
        let file_path = self.base_path.join(&filename);

        // Serialize and write to file
        let json = serde_json::to_string_pretty(&entry)
            .context("Failed to serialize DLQ entry")?;
        
        fs::write(&file_path, json)
            .await
            .with_context(|| format!("Failed to write DLQ file: {:?}", file_path))?;

        info!(
            "Written event to DLQ: {} (reason: {})",
            file_path.display(),
            entry.failure_reason
        );

        // Send DLQ notification event if we have an event sender
        if let Some(tx) = &self.event_tx {
            let dlq_event = DlqEventWritten {
                agent_name: self.agent_name.clone(),
                failed_event_source: event.source,
                failed_event_type: event.event_type,
                dlq_file_path: file_path.to_string_lossy().into_owned(),
                failure_reason: entry.failure_reason,
            };
            
            let notification = crate::agent::create_dlq_event(dlq_event);
            if let Err(e) = tx.send(notification).await {
                warn!("Failed to send DLQ notification event: {}", e);
            }
        }

        Ok(file_path.to_string_lossy().into_owned())
    }

    /// Log a critical meta-failure (when we can't even write DLQ notifications)
    pub async fn log_critical_failure(&self, error: &str) -> Result<()> {
        let timestamp = Utc::now().to_rfc3339();
        let log_entry = format!("{} CRITICAL: {}\n", timestamp, error);
        
        // Append to critical failures log
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.critical_failures_log)
            .await?;
        
        use tokio::io::AsyncWriteExt;
        file.write_all(log_entry.as_bytes()).await
            .with_context(|| {
                format!(
                    "Failed to write to critical failures log: {:?}",
                    self.critical_failures_log
                )
            })?;

        error!("Critical failure logged: {}", error);
        Ok(())
    }

    /// Get count of files in DLQ
    pub async fn get_dlq_size(&self) -> Result<u64> {
        let mut entries = fs::read_dir(&self.base_path)
            .await
            .context("Failed to read DLQ directory")?;
        
        let mut count = 0;
        while let Some(entry) = entries.next_entry().await? {
            if entry.file_type().await?.is_file() {
                count += 1;
            }
        }

        Ok(count)
    }

    /// Read all DLQ entries (for potential replay)
    pub async fn read_all_entries(&self) -> Result<Vec<(PathBuf, DlqEntry)>> {
        let mut results = Vec::new();
        let mut entries = fs::read_dir(&self.base_path).await?;

        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            
            if path.is_file() && path.extension().map_or(false, |ext| ext == "json") {
                match fs::read_to_string(&path).await {
                    Ok(content) => match serde_json::from_str::<DlqEntry>(&content) {
                        Ok(dlq_entry) => results.push((path, dlq_entry)),
                        Err(e) => warn!("Failed to parse DLQ file {:?}: {}", path, e),
                    },
                    Err(e) => warn!("Failed to read DLQ file {:?}: {}", path, e),
                }
            }
        }

        Ok(results)
    }

    /// Remove a DLQ entry (after successful replay)
    pub async fn remove_entry(&self, path: &Path) -> Result<()> {
        fs::remove_file(path)
            .await
            .with_context(|| format!("Failed to remove DLQ file: {:?}", path))?;
        info!("Removed DLQ entry: {:?}", path);
        Ok(())
    }
}

/// Retry executor with exponential backoff
pub struct RetryExecutor {
    policy: RetryPolicy,
    circuit_breaker: Arc<CircuitBreaker>,
}

impl RetryExecutor {
    pub fn new(policy: RetryPolicy, circuit_breaker: Arc<CircuitBreaker>) -> Self {
        Self {
            policy,
            circuit_breaker,
        }
    }
    
    /// Execute operation with retry logic
    pub async fn execute<F, Fut, T>(&self, mut operation: F) -> Result<T, CollectorError>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<T, CollectorError>>,
    {
        if self.circuit_breaker.is_open() {
            return Err(CollectorError::ResourceExhausted {
                resource: "circuit_breaker".to_string(),
                limit: Some("Circuit breaker is open".to_string()),
            });
        }
        
        let mut last_error_msg = None;
        
        for attempt in 0..self.policy.max_attempts {
            match operation().await {
                Ok(result) => {
                    self.circuit_breaker.record_success();
                    return Ok(result);
                }
                Err(error) => {
                    last_error_msg = Some(error.to_string());
                    
                    if !error.should_retry() || attempt == self.policy.max_attempts - 1 {
                        self.circuit_breaker.record_failure();
                        return Err(error);
                    }
                    
                    let delay = self.policy.delay_for_attempt(attempt);
                    warn!(
                        "Operation failed (attempt {}/{}), retrying in {:?}: {}",
                        attempt + 1,
                        self.policy.max_attempts,
                        delay,
                        error
                    );
                    
                    tokio::time::sleep(delay).await;
                }
            }
        }
        
        self.circuit_breaker.record_failure();
        Err(CollectorError::EventProcessing {
            message: last_error_msg.unwrap_or_else(|| "Unknown error".to_string()),
            event_type: None,
            event_id: None,
            source: None,
        })
    }
}

/// Recovery manager combining DLQ, retries, and circuit breakers
pub struct RecoveryManager {
    dlq: DlqManager,
    retry_executor: RetryExecutor,
}

impl RecoveryManager {
    pub fn new(agent_name: impl Into<String>) -> Result<Self> {
        let agent_name = agent_name.into();
        let dlq = DlqManager::new(&agent_name)?;
        
        let policy = RetryPolicy {
            max_attempts: 3,
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(30),
            exponential_base: 2.0,
        };
        
        let circuit_breaker = Arc::new(CircuitBreaker::new(5, Duration::from_secs(60)));
        let retry_executor = RetryExecutor::new(policy, circuit_breaker);
        
        Ok(Self {
            dlq,
            retry_executor,
        })
    }
    
    pub fn with_event_sender(mut self, event_tx: EventSender) -> Self {
        self.dlq = self.dlq.with_event_sender(event_tx);
        self
    }
    
    pub async fn initialize(&self) -> Result<()> {
        self.dlq.initialize().await
    }
    
    /// Handle event processing with automatic retry and DLQ fallback
    pub async fn handle_event_processing<F, Fut>(
        &self,
        event: RawEvent,
        operation: F,
    ) -> Result<(), CollectorError>
    where
        F: FnMut() -> Fut + Send,
        Fut: std::future::Future<Output = Result<(), CollectorError>> + Send,
    {
        let event_clone = event.clone();
        
        match self.retry_executor.execute(operation).await {
            Ok(()) => Ok(()),
            Err(error) => {
                // For permanent errors or after retry exhaustion, send to DLQ
                if let Err(dlq_error) = self.dlq.write_event(event_clone, &error, 0).await {
                    // If we can't even write to DLQ, log critically
                    let critical_msg = format!(
                        "Failed to write event to DLQ after processing failure: {}, DLQ error: {}",
                        error, dlq_error
                    );
                    if let Err(log_error) = self.dlq.log_critical_failure(&critical_msg).await {
                        error!("Cannot even log critical failure: {}", log_error);
                    }
                }
                
                Err(error)
            }
        }
    }
    
    pub async fn get_dlq_size(&self) -> Result<u64> {
        self.dlq.get_dlq_size().await
    }
}

/// Implement From conversions for common error types
impl From<std::io::Error> for CollectorError {
    fn from(err: std::io::Error) -> Self {
        Self::Connection {
            service: "filesystem".to_string(),
            message: err.to_string(),
            source: Some(Box::new(err)),
        }
    }
}

impl From<serde_json::Error> for CollectorError {
    fn from(err: serde_json::Error) -> Self {
        Self::EventProcessing {
            message: "JSON serialization failed".to_string(),
            event_type: None,
            event_id: None,
            source: Some(Box::new(err)),
        }
    }
}

impl From<sqlx::Error> for CollectorError {
    fn from(err: sqlx::Error) -> Self {
        match err {
            sqlx::Error::PoolTimedOut => Self::ResourceExhausted {
                resource: "database_connections".to_string(),
                limit: Some("connection pool exhausted".to_string()),
            },
            _ => Self::Connection {
                service: "database".to_string(),
                message: err.to_string(),
                source: Some(Box::new(err)),
            },
        }
    }
}

impl From<anyhow::Error> for CollectorError {
    fn from(err: anyhow::Error) -> Self {
        Self::EventProcessing {
            message: err.to_string(),
            event_type: None,
            event_id: None,
            source: Some(err.into()),
        }
    }
}