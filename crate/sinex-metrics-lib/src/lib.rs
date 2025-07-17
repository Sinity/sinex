//! Sinex Metrics Library
//!
//! This library provides the core metrics collection functionality for the Sinex system.
//! It includes automatic metrics generation, collection, and export capabilities.
//!
//! # Usage
//!
//! ## Basic Usage
//! ```rust
//! use sinex_metrics_lib::{init_metrics, export_prometheus, export_json};
//!
//! #[tokio::main]
//! async fn main() {
//!     init_metrics().await;
//!     
//!     // Your application code here
//!     
//!     // Export metrics
//!     let prometheus_output = export_prometheus();
//!     let json_output = export_json();
//! }
//! ```

pub mod auto_metrics;
pub mod cli;
pub mod collectors;
pub mod database;
pub mod events;
pub mod export;
pub mod registry;
pub mod resources;
pub mod satellite;
pub mod storage;

// Re-export the main functionality
pub use auto_metrics::*;
pub use cli::*;
pub use collectors::*;
pub use database::*;
pub use events::*;
pub use export::*;
pub use registry::*;
pub use resources::*;
pub use satellite::*;
pub use storage::*;

/// Initialize the metrics system
pub async fn init_metrics() {
    registry::init_global_registry().await;
    collectors::start_background_collectors().await;
}
