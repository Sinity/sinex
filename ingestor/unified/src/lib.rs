// Unified collector library exports

pub mod config;
pub mod collector;

// Re-export main types for convenience
pub use config::{CollectionConfig, DatabaseConfig, LoggingConfig, UnifiedConfig};
pub use collector::{UnifiedCollector, UnifiedIngestor};