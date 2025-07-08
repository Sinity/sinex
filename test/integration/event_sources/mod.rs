//! Event source integration tests
//!
//! This module contains integration tests for various event sources,
//! including filesystem monitoring, terminal command tracking, clipboard
//! monitoring, and window manager events.

#![allow(dead_code)]
//!
//! # Test Coverage
//! - Event source lifecycle management (start/stop)
//! - Event generation and validation
//! - Source-specific configuration handling
//! - Integration with unified collector
//! - Error handling and recovery
//! - Performance under load

/// Atuin shell history integration tests (original - may have syntax errors)
pub mod atuin_tests;

/// Real Atuin integration tests (corrected version)
pub mod atuin_tests_real;

/// Generic event source functionality tests
pub mod event_source_tests;

/// Event source lifecycle management tests
pub mod lifecycle_management_test;

/// Terminal event source tests
pub mod terminal_tests;

/// Comprehensive Kitty terminal integration tests
pub mod kitty_comprehensive_test;

/// Common utilities for event source testing
pub mod utils {
    // use crate::common::event_builders::EventBuilder;
    use crate::common::prelude::*;
    use serde_json::{json, Value};
    use std::path::Path;
    use tokio::time::Duration;

    /// Create filesystem event source configuration
    pub fn create_filesystem_config(watch_path: &str) -> Value {
        json!({
            "enabled": true,
            "watch_patterns": [format!("{}/**/*", watch_path)],
            "ignore_patterns": ["*.tmp", "*.log", ".git/**/*"],
            "debounce_ms": 100,
            "recursive": true
        })
    }

    /// Create terminal event source configuration
    pub fn create_terminal_config(socket_path: &str) -> Value {
        json!({
            "enabled": true,
            "socket_path": socket_path,
            "polling_interval_secs": 1,
            "command_timeout_secs": 30
        })
    }

    /// Create clipboard event source configuration
    pub fn create_clipboard_config() -> Value {
        json!({
            "enabled": true,
            "monitor_clipboard": true,
            "monitor_primary": false,
            "poll_interval_ms": 500,
            "max_content_size": 1024000
        })
    }

    /// Create window manager event source configuration
    pub fn create_hyprland_config() -> Value {
        json!({
            "enabled": true,
            "socket_path": "/tmp/hypr/.hyprland.sock",
            "events": ["workspace", "window", "monitor"]
        })
    }

    /// Create test file for filesystem monitoring
    pub async fn create_test_file<P: AsRef<Path>>(path: P, content: &str) -> Result<()> {
        if let Some(parent) = path.as_ref().parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::write(path, content).await?;
        Ok(())
    }

    /// Wait for event source to produce events
    pub async fn wait_for_events_from_source(
        pool: &DbPool,
        source_name: &str,
        min_events: usize,
        timeout_secs: u64,
    ) -> Result<Vec<RawEvent>> {
        let start = std::time::Instant::now();
        let timeout_duration = Duration::from_secs(timeout_secs);

        loop {
            let events = sinex_db::queries::get_events_by_source(pool, source_name, 100).await?;

            if events.len() >= min_events {
                return Ok(events);
            }

            if start.elapsed() > timeout_duration {
                anyhow::bail!(
                    "Timeout waiting for {} events from source '{}', got {}",
                    min_events,
                    source_name,
                    events.len()
                );
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Test event source configuration validation
    pub fn validate_event_source_config(config: &Value) -> Result<(), ValidationError> {
        if !config
            .get("enabled")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
        {
            return Err(ValidationError::MissingField {
                field: "enabled".to_string(),
            });
        }

        // Add more validation as needed
        Ok(())
    }

    /// Validation error types
    #[derive(Debug)]
    pub enum ValidationError {
        MissingField { field: String },
        InvalidValue { field: String, value: String },
    }

    impl std::fmt::Display for ValidationError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                ValidationError::MissingField { field } => {
                    write!(f, "Missing required field: {}", field)
                }
                ValidationError::InvalidValue { field, value } => {
                    write!(f, "Invalid field value: {} = {}", field, value)
                }
            }
        }
    }

    impl std::error::Error for ValidationError {}

    /// Create mock event source for testing
    pub struct MockEventSource {
        pub name: String,
        pub events_to_generate: Vec<RawEvent>,
        pub events_sent: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    }

    impl MockEventSource {
        pub fn new(name: &str) -> Self {
            Self {
                name: name.to_string(),
                events_to_generate: Vec::new(),
                events_sent: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            }
        }

        pub fn with_events(mut self, events: Vec<RawEvent>) -> Self {
            self.events_to_generate = events;
            self
        }

        pub async fn simulate_events(&self, tx: tokio::sync::mpsc::Sender<RawEvent>) -> Result<()> {
            for event in &self.events_to_generate {
                tx.send(event.clone())
                    .await
                    .map_err(|e| anyhow::anyhow!("Failed to send event: {}", e))?;

                self.events_sent
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

                // Small delay to simulate realistic timing
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Ok(())
        }

        pub fn events_sent_count(&self) -> usize {
            self.events_sent.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    /// Performance metrics for event source testing
    #[derive(Debug, Clone)]
    pub struct EventSourcePerformanceMetrics {
        pub source_name: String,
        pub events_per_second: f64,
        pub average_latency: Duration,
        pub total_events: usize,
        pub test_duration: Duration,
    }

    impl EventSourcePerformanceMetrics {
        pub fn print_report(&self) {
            println!("=== Event Source Performance Report ===");
            println!("Source: {}", self.source_name);
            println!("Events/sec: {:.2}", self.events_per_second);
            println!("Avg latency: {:?}", self.average_latency);
            println!("Total events: {}", self.total_events);
            println!("Test duration: {:?}", self.test_duration);
        }
    }
}
