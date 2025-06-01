pub mod cli;
pub mod config;
pub mod database;
pub mod error;
pub mod events;
pub mod enhanced_events;
pub mod logging;
pub mod shutdown;

pub use error::{IngestorError, Result};

#[cfg(test)]
mod integration_tests {
    use super::*;
    use crate::config::Config;
    use crate::database::{DatabaseService, EventRecord};
    use serde_json::json;
    use std::sync::Arc;

    // These tests require a running PostgreSQL instance
    // They should be run with: cargo test --features integration-tests

    #[tokio::test]
    #[ignore] // Ignore by default since it requires a database
    async fn test_database_integration() {
        let config = config::DatabaseConfig {
            url: "postgresql://localhost/sinex_test".to_string(),
            ..Default::default()
        };

        let db = DatabaseService::new(config).await.unwrap();

        // Test health check
        db.health_check().await.unwrap();

        // Test event insertion
        let event = EventRecord::new(
            "test",
            json!({"type": "test_event", "data": {"message": "hello"}}),
            json!({"test": true}),
        );

        db.insert_event(event).await.unwrap();

        // Test statistics
        let stats = db.get_stats().await.unwrap();
        assert!(stats.total_events > 0);
        assert!(stats.sources.contains(&"test".to_string()));
    }

    #[test]
    fn test_config_loading() {
        let config = Config::default();
        assert_eq!(config.database.url, "postgresql://localhost/sinex");
        assert_eq!(config.logging.level, "info");
        assert!(config.hyprland.capture_window_events);
    }

    #[test]
    fn test_error_types() {
        let db_error = IngestorError::database_connection("test error");
        assert!(matches!(db_error, IngestorError::DatabaseConnection(_)));

        let app_error = IngestorError::application("test error");
        assert!(matches!(app_error, IngestorError::Application(_)));
    }
}