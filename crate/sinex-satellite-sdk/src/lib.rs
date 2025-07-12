//! Sinex Satellite SDK
//!
//! Shared library for building Sinex satellite services (event sources and automata).
//! This crate provides:
//! - Common traits and interfaces
//! - gRPC client for communicating with sinex-ingestd
//! - Redis Streams client for message bus communication
//! - Configuration management
//! - Lifecycle management and graceful shutdown
//! - State persistence and checkpointing
//! - Historical replay capabilities

pub mod automaton;
pub mod checkpoint;
pub mod cli;
pub mod config;
pub mod event_source;
pub mod examples;
pub mod grpc_client;
pub mod heartbeat;
pub mod lifecycle;
pub mod redis_client;
pub mod replay;
pub mod stream_processor;

pub use automaton::{
    HotlogAutomaton, HotlogAutomatonContext, HotlogAutomatonEvent, HotlogAutomatonRunner,
    EventFilter, PayloadFilter, FilterOperation, ProcessingResult
};
pub use checkpoint::{CheckpointManager, CheckpointState};
pub use config::{AutomatonConfig, EventSourceConfig, SatelliteConfig};
pub use event_source::{
    EventSource, EventSourceContext, EventSourceRunner, ScannerArgs, 
    ScannerEstimate, VersionInfo
};
pub use grpc_client::IngestClient;
pub use heartbeat::{HeartbeatEmitter, HeartbeatCounterHandle, HeartbeatMetrics};
pub use lifecycle::{LifecycleManager, ServiceStatus};
pub use redis_client::{RedisStreamClient, StreamMessage};
pub use replay::ReplayMode;
pub use stream_processor::{
    StatefulStreamProcessor, StreamProcessorRunner, StreamProcessorContext,
    TimeHorizon, Checkpoint, ProcessorType, ProcessorCapabilities,
    ScanArgs, ScanReport, ScanEstimate, EventStream, EventSender
};
pub use cli::{
    ProcessorCli, ProcessorCommand, ProcessorCliRunner, ExplorationProvider,
    SourceState, IngestionHistoryEntry, CoverageAnalysis, MissingItem,
    ExportFormat, parse_checkpoint, parse_time_horizon
};

/// Common CLI arguments for satellite services
#[derive(clap::Parser, Debug, Clone)]
pub struct SatelliteArgs {
    /// Socket path for ingestd communication
    #[arg(long, default_value = "/tmp/sinex-ingestd.sock")]
    pub ingest_socket_path: String,

    /// Service name for identification
    #[arg(long, default_value = "satellite")]
    pub service_name: String,

    /// Event batch size
    #[arg(long, default_value = "100")]
    pub batch_size: usize,

    /// Batch timeout in seconds
    #[arg(long, default_value = "5")]
    pub batch_timeout: u64,

    /// Working directory for temporary files
    #[arg(long)]
    pub work_dir: Option<std::path::PathBuf>,

    /// Enable dry run mode (no actual event ingestion)
    #[arg(long)]
    pub dry_run: bool,
}

// Re-export generated gRPC types
pub mod proto {
    tonic::include_proto!("sinex.ingest");
}

// Re-export commonly used types from dependencies
pub use sinex_core::{ErrorContext, ValidationChain};
pub use sinex_events::{RawEvent, RawEventBuilder};
pub use sinex_ulid::Ulid;

/// Result type for satellite operations
pub type SatelliteResult<T> = std::result::Result<T, SatelliteError>;

/// Common error types for satellite services
#[derive(thiserror::Error, Debug)]
pub enum SatelliteError {
    #[error("Configuration error: {0}")]
    Config(#[from] config::ConfigError),

    #[error("gRPC communication error: {0}")]
    Grpc(#[from] tonic::Status),

    #[error("gRPC transport error: {0}")]
    GrpcTransport(#[from] tonic::transport::Error),

    #[error("Redis error: {0}")]
    Redis(#[from] redis::RedisError),

    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("General error: {0}")]
    General(#[from] anyhow::Error),

    #[error("Event source error: {0}")]
    EventSource(String),

    #[error("Automaton error: {0}")]
    Automaton(String),

    #[error("Checkpoint error: {0}")]
    Checkpoint(String),

    #[error("Lifecycle error: {0}")]
    Lifecycle(String),
}