use chrono::Utc;
use sinex_core::types::ulid::Ulid;
use sinex_core::types::validation::FileWatchingSecurityPolicy;
use sinex_sensd::config::SensorConfig;
use sinex_sensd::job_manager::{SensorJob, SensorType};
use sinex_sensd::sensors::tree_watch::TreeWatchSensor;
use sinex_sensd::temporal_ledger::TemporalLedger;
use std::sync::Arc;
use tempfile::TempDir;

#[tokio::test]
async fn rejects_dangerous_paths() {
    let temp_ledger = Arc::new(
        TemporalLedger::new_in_memory()
            .await
            .expect("Failed to create in-memory temporal ledger for testing"),
    );
    let config = SensorConfig::default();

    let sensor = TreeWatchSensor::new(temp_ledger.clone(), config)
        .expect("Failed to create TreeWatchSensor for testing");

    let dangerous_job = SensorJob {
        id: Ulid::new(),
        sensor_type: SensorType::TreeWatch.to_string(),
        target_uri: "/etc/passwd".to_string(),
        config: serde_json::Value::Null,
        status: "active".to_string(),
        priority: 0,
        updated_at: Utc::now(),
    };

    let result = sensor.process_job(&dangerous_job, &temp_ledger).await;
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Security validation failed"));
}

#[tokio::test]
async fn accepts_safe_paths() {
    let temp_ledger = Arc::new(
        TemporalLedger::new_in_memory()
            .await
            .expect("Failed to create in-memory temporal ledger for testing"),
    );
    let config = SensorConfig::default();

    let permissive_policy = FileWatchingSecurityPolicy::permissive();
    let sensor = TreeWatchSensor::with_policy(temp_ledger.clone(), config, permissive_policy)
        .expect("Failed to create TreeWatchSensor with permissive policy for testing");

    let temp_dir = TempDir::new().expect("Failed to create temporary directory for testing");
    let temp_path = temp_dir
        .path()
        .to_str()
        .expect("Failed to convert temp path to string");

    let safe_job = SensorJob {
        id: Ulid::new(),
        sensor_type: SensorType::TreeWatch.to_string(),
        target_uri: temp_path.to_string(),
        config: serde_json::Value::Null,
        status: "active".to_string(),
        priority: 0,
        updated_at: Utc::now(),
    };

    let result = sensor.process_job(&safe_job, &temp_ledger).await;
    assert!(result.is_ok());
}
