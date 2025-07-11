//! Configuration management utilities for the Sinex event capture system
//!
//! This crate provides type-safe configuration extraction, validation,
//! and merging utilities for the Sinex ecosystem.

pub mod duration_parser;
pub mod extractors;
pub mod helpers;
pub mod validators;

// Re-export main types
pub use duration_parser::parse_duration;
pub use extractors::{ConfigExtractor, ConfigValidator};
pub use helpers::{
    CollectorConfig, ConfigExtraction, ConfigFactory, ConfigMerger, DatabaseConfig,
    ObservabilityConfig, SourcesConfig,
};
pub use validators::*;

// Common type alias for external compatibility
pub type ConfigValue = toml::Value;
