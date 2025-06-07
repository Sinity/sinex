use thiserror::Error;
use tracing::error;

/// Unified error type for all ingestors with rich context
#[derive(Debug, Error)]
pub enum IngestorError {
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
        retry_after: Option<std::time::Duration>,
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },
}

/// Error context that can be attached to any error
#[derive(Debug, Clone)]
pub struct ErrorContext {
    pub ingestor: String,
    pub operation: String,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub trace_id: Option<String>,
    pub additional: std::collections::HashMap<String, String>,
}

/// Extension trait for adding context to errors
pub trait ErrorExt<T> {
    fn with_context<F>(self, f: F) -> Result<T, IngestorError>
    where
        F: FnOnce() -> String;
        
    fn with_ingestor_context(self, ingestor: &str, operation: &str) -> Result<T, IngestorError>;
}

impl<T, E> ErrorExt<T> for Result<T, E>
where
    E: Into<IngestorError>,
{
    fn with_context<F>(self, f: F) -> Result<T, IngestorError>
    where
        F: FnOnce() -> String,
    {
        self.map_err(|e| {
            let err: IngestorError = e.into();
            error!("{}: {}", f(), err);
            err
        })
    }
    
    fn with_ingestor_context(self, ingestor: &str, operation: &str) -> Result<T, IngestorError> {
        self.map_err(|e| {
            let err: IngestorError = e.into();
            error!(
                ingestor = %ingestor,
                operation = %operation,
                error = %err,
                "Operation failed"
            );
            err
        })
    }
}

/// Result type alias for ingestor operations
pub type IngestorResult<T> = Result<T, IngestorError>;

/// Categorize errors for proper handling
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

impl IngestorError {
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
}

/// Retry policy based on error type
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub base_delay: std::time::Duration,
    pub max_delay: std::time::Duration,
    pub exponential_base: f64,
}

impl RetryPolicy {
    pub fn for_error(error: &IngestorError) -> Option<Self> {
        match error.category() {
            ErrorCategory::Retryable => Some(Self {
                max_attempts: 3,
                base_delay: std::time::Duration::from_millis(100),
                max_delay: std::time::Duration::from_secs(30),
                exponential_base: 2.0,
            }),
            _ => None,
        }
    }
}

/// Circuit breaker for handling repeated failures
pub struct CircuitBreaker {
    failure_count: std::sync::atomic::AtomicU32,
    last_failure: std::sync::Mutex<Option<std::time::Instant>>,
    threshold: u32,
    timeout: std::time::Duration,
}

impl CircuitBreaker {
    pub fn new(threshold: u32, timeout: std::time::Duration) -> Self {
        Self {
            failure_count: std::sync::atomic::AtomicU32::new(0),
            last_failure: std::sync::Mutex::new(None),
            threshold,
            timeout,
        }
    }
    
    pub fn record_success(&self) {
        self.failure_count.store(0, std::sync::atomic::Ordering::Relaxed);
        if let Ok(mut guard) = self.last_failure.lock() {
            *guard = None;
        }
    }
    
    pub fn record_failure(&self) {
        self.failure_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if let Ok(mut guard) = self.last_failure.lock() {
            *guard = Some(std::time::Instant::now());
        }
    }
    
    pub fn is_open(&self) -> bool {
        let count = self.failure_count.load(std::sync::atomic::Ordering::Relaxed);
        if count < self.threshold {
            return false;
        }
        
        if let Ok(guard) = self.last_failure.lock() {
            if let Some(last) = *guard {
                if last.elapsed() > self.timeout {
                    // Reset after timeout
                    self.failure_count.store(0, std::sync::atomic::Ordering::Relaxed);
                    return false;
                }
            }
        }
        
        true
    }
}

/// Implement From conversions for common error types
impl From<std::io::Error> for IngestorError {
    fn from(err: std::io::Error) -> Self {
        Self::Connection {
            service: "filesystem".to_string(),
            message: err.to_string(),
            source: Some(Box::new(err)),
        }
    }
}

impl From<serde_json::Error> for IngestorError {
    fn from(err: serde_json::Error) -> Self {
        Self::EventProcessing {
            message: "JSON serialization failed".to_string(),
            event_type: None,
            event_id: None,
            source: Some(Box::new(err)),
        }
    }
}

impl From<sqlx::Error> for IngestorError {
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