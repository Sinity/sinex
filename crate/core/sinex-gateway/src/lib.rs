//! Sinex Gateway Library
//!
//! Provides the service container, replay system, and related functionality for the Sinex Gateway.
//!
//! ## Architecture Overview
//!
//! The Sinex Gateway acts as the central API hub for the Sinex event capture system:
//!
//! ```text
//! ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
//! │   CLI Tools     │────│  JSON-RPC API   │────│ Service Layer   │
//! └─────────────────┘    └─────────────────┘    └─────────────────┘
//!                                │                        │
//! ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
//! │ Browser Ext.    │────│ Native Messaging │    │ Database Layer  │
//! └─────────────────┘    └─────────────────┘    └─────────────────┘
//! ```
//!
//! ## Core Components
//!
//! - **RPC Server**: JSON-RPC 2.0 API for CLI communication
//! - **Native Messaging**: Browser extension communication protocol
//! - **Replay State Machine**: Distributed replay operation management
//! - **Cascade Analyzer**: Dependency graph analysis for safe operations
//! - **Service Container**: Dependency injection and service lifecycle
//!
//! ## Usage Examples
//!
//! Starting the gateway server:
//! ```rust,no_run
//! use sinex_gateway::ServiceContainer;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let container = ServiceContainer::new().await?;
//!     container.start_rpc_server("127.0.0.1:8080").await?;
//!     Ok(())
//! }
//! ```
//!
//! ## Error Handling Patterns
//!
//! All operations return `color_eyre::Result<T>` for comprehensive error context.
//! Errors are logged and sanitized before being returned to clients.

// Expose modules for testing and external use
pub mod cascade_analyzer;
pub mod handlers;
pub mod prelude;
pub mod replay_state_machine;
pub mod rpc_server;
pub mod service_container;

// Re-export commonly used types
pub use cascade_analyzer::{
    CascadeAnalysis, CascadeAnalyzerConfig, CircularDependency, IntegrityViolation, Severity,
    StreamingCascadeAnalyzer, ViolationType,
};
pub use replay_state_machine::{
    ReplayCheckpoint, ReplayOperation, ReplayScope, ReplayState, ReplayStateMachine,
};
pub use service_container::ServiceContainer;
