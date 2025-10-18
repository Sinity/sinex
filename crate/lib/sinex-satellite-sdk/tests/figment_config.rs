use camino::Utf8PathBuf;
use sinex_satellite_sdk::figment_config::{
    AutomatonFigmentConfig, EventSourceFigmentConfig, SatelliteFigmentConfig,
};
use sinex_test_utils::sinex_test;
use validator::Validate;

#[sinex_test]
fn satellite_config_defaults_validate() -> color_eyre::eyre::Result<()> {
    let config = SatelliteFigmentConfig {
        service_name: "test-satellite".to_string(),
        log_level: "info".to_string(),
        socket_path: "/tmp/sinex-ingestd.sock".to_string(),
        redis_url: "redis://localhost:6379".to_string(),
        enable_replay: false,
        work_dir: Utf8PathBuf::from("/tmp/sinex"),
        health_port: 0,
        checkpoint_interval_secs: 300,
        database_url: None,
    };

    assert!(config.validate().is_ok());
    assert_eq!(config.log_level, "info");
    assert_eq!(config.checkpoint_interval_secs, 300);
    Ok(())
}

#[sinex_test]
fn event_source_config_requires_service_name() -> color_eyre::eyre::Result<()> {
    let mut config = EventSourceFigmentConfig {
        base: SatelliteFigmentConfig {
            service_name: "".to_string(),
            log_level: "info".to_string(),
            socket_path: "/tmp/test.sock".to_string(),
            redis_url: "redis://localhost".to_string(),
            enable_replay: false,
            work_dir: Utf8PathBuf::from("/tmp"),
            health_port: 0,
            checkpoint_interval_secs: 300,
            database_url: None,
        },
        batch_size: 100,
        batch_wait_secs: 5,
        max_retries: 3,
        retry_backoff_multiplier: 2.0,
    };

    assert!(config.validate().is_err());

    config.base.service_name = "valid-name".to_string();
    assert!(config.validate().is_ok());
    Ok(())
}

#[sinex_test]
fn automaton_config_validates() -> color_eyre::eyre::Result<()> {
    let config = AutomatonFigmentConfig {
        base: SatelliteFigmentConfig {
            service_name: "test-automaton".to_string(),
            log_level: "debug".to_string(),
            socket_path: "/tmp/test.sock".to_string(),
            redis_url: "redis://localhost".to_string(),
            enable_replay: false,
            work_dir: Utf8PathBuf::from("/tmp"),
            health_port: 9090,
            checkpoint_interval_secs: 60,
            database_url: None,
        },
        consumer_group: "test-group".to_string(),
        consumer_name: "test-consumer".to_string(),
        streams: vec!["test:stream".to_string()],
        stream_batch_size: 10,
        stream_timeout_secs: 5,
        max_processing_time_secs: 30,
    };

    assert!(config.validate().is_ok());
    Ok(())
}
