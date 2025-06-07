pub mod runtime;
pub mod sink;
pub mod dlq;
pub mod metrics;

// Re-export key types
pub use runtime::{SimpleIngestor, IngestorRuntime, RuntimeConfig, RetryConfig, retry_db_operation};
pub use sink::{EventSink, DatabaseSink, LogSink, FileSink, MemorySink, MultiSink};
pub use dlq::DlqManager;
pub use metrics::{AgentMetrics, AgentHeartbeat, AgentError, ErrorSeverity};

// Re-export from sinex-core for convenience
pub use sinex_core::{RawEvent, RawEventBuilder, sources, event_type_constants, AgentStatus};