//! Configuration management utilities for the Sinex event capture system
//!
//! This crate provides type-safe configuration extraction, validation,
//! and merging utilities for the Sinex ecosystem.

pub mod extractors;
pub mod helpers;
pub mod validators;
pub mod duration_parser;

// Re-export main types
pub use extractors::{ConfigExtractor, ConfigValidator};
pub use helpers::{ConfigFactory, ConfigExtraction, ConfigMerger, DatabaseConfig, CollectorConfig, ObservabilityConfig, SourcesConfig};
pub use validators::*;
pub use duration_parser::parse_duration;

// Common type alias for external compatibility
pub type ConfigValue = toml::Value;