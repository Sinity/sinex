pub mod cli;
pub mod config;
pub mod error;
pub mod simple_watcher;
pub mod simple_ingestor;

pub use error::{IngestorError, Result};

#[cfg(test)]
mod integration_tests {
    use super::*;
    use crate::config::{Config, WindowAugmentation};
    use serde_json::json;

    // These tests require a running PostgreSQL instance
    // They should be run with: cargo test --features integration-tests

    #[tokio::test]
    #[ignore] // Ignore by default since it requires a database
    async fn test_database_integration() {
        use sinex_shared::{DatabaseConfig, DatabaseService, event_types::RawEventBuilder, sources};
        
        let config = DatabaseConfig {
            url: "postgresql://localhost/sinex_test".to_string(),
            ..Default::default()
        };

        let db = DatabaseService::new(config).await.unwrap();

        // Test health check
        db.health_check().await.unwrap();

        // Test event insertion
        let event = RawEventBuilder::new(
            sources::SINEX,
            "test_event",
            json!({"type": "test_event", "data": {"message": "hello"}}),
        ).build();

        db.insert_event(&event).await.unwrap();
    }

    #[test]
    fn test_config_loading() {
        let config = Config::default();
        assert_eq!(config.database.url, "postgresql://localhost/sinex");
        assert_eq!(config.logging.level, "info");
        assert_eq!(config.hyprland.window_augmentation, WindowAugmentation::Basic);
    }

    #[test]
    fn test_error_types() {
        let db_error = IngestorError::database_connection("test error");
        assert!(matches!(db_error, IngestorError::DatabaseConnection(_)));

        let app_error = IngestorError::application("test error");
        assert!(matches!(app_error, IngestorError::Application(_)));
    }
}