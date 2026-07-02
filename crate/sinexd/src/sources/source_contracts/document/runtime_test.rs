use super::DocumentSourceDriver;
use crate::runtime::ExplorationProvider;
use crate::runtime::stream::Checkpoint;
use serde_json::json;
use sinex_primitives::temporal::Timestamp;
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn test_completed_report_uses_elapsed_window() -> ::xtask::sandbox::TestResult<()> {
    let started_at =
        Timestamp::from_unix_timestamp(1_700_000_000).expect("timestamp should be valid");
    let finished_at =
        Timestamp::from_unix_timestamp(1_700_000_123).expect("timestamp should be valid");
    let report = DocumentSourceDriver::completed_report(
        started_at,
        finished_at,
        std::time::Duration::from_secs(2),
        3,
        vec!["/tmp/doc.txt".to_string()],
        Vec::new(),
        Vec::new(),
    );

    assert_eq!(
        report.final_checkpoint,
        Checkpoint::timestamp(finished_at, None)
    );
    assert_eq!(report.time_range, Some((started_at, finished_at)));
    Ok(())
}

#[sinex_test]
async fn document_source_state_is_unhealthy_before_initialize()
-> ::xtask::sandbox::TestResult<()> {
    let source = DocumentSourceDriver::new();
    let state = source.get_source_state()?;

    assert!(!state.is_connected);
    assert!(!state.healthy);
    assert_eq!(state.last_updated, None);
    assert!(state.description.contains("not initialized"));
    assert_eq!(state.metadata.get("initialized"), Some(&json!(false)));
    Ok(())
}

#[sinex_test]
async fn document_source_state_surfaces_invalid_config() -> ::xtask::sandbox::TestResult<()> {
    let source = DocumentSourceDriver::new();
    let state = source.get_source_state()?;

    assert_eq!(
        state.metadata.get("config_error"),
        Some(&json!(
            "Configuration error: Allowed roots must be configured for document ingestion"
        ))
    );
    assert!(!state.healthy);
    Ok(())
}

#[sinex_test]
async fn document_source_reports_empty_ingestion_history_before_scan()
-> ::xtask::sandbox::TestResult<()> {
    let source = DocumentSourceDriver::new();
    let history = source.get_ingestion_history(10)?;

    assert!(history.is_empty());
    Ok(())
}

#[sinex_test]
async fn document_source_reports_last_scan_as_activity_and_history()
-> ::xtask::sandbox::TestResult<()> {
    let source = DocumentSourceDriver::new();
    let started_at =
        Timestamp::from_unix_timestamp(1_700_000_000).expect("timestamp should be valid");
    let finished_at =
        Timestamp::from_unix_timestamp(1_700_000_123).expect("timestamp should be valid");
    let report = DocumentSourceDriver::completed_report(
        started_at,
        finished_at,
        std::time::Duration::from_secs(2),
        2,
        vec!["/tmp/doc-a.txt".to_string(), "/tmp/doc-b.txt".to_string()],
        vec![(
            "/tmp/doc-c.txt".to_string(),
            "permission denied".to_string(),
        )],
        vec!["Skipped 1 unchanged document(s)".to_string()],
    );

    source.record_scan_report(&report)?;

    let state = source.get_source_state()?;
    assert_eq!(state.last_updated, Some(finished_at));
    assert_eq!(state.recent_activity.len(), 1);
    assert!(state.recent_activity[0].description.contains("emitted 2"));
    assert_eq!(
        state.metadata.get("last_scan_failed_targets"),
        Some(&json!(1))
    );

    let history = source.get_ingestion_history(10)?;
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].started_at, started_at);
    assert_eq!(history[0].completed_at, Some(finished_at));
    assert_eq!(history[0].events_generated, 2);
    assert_eq!(
        history[0].error.as_deref(),
        Some("1 document target(s) failed")
    );
    assert_eq!(
        history[0]
            .scan_report
            .as_ref()
            .map(|report| report.failed_targets.len()),
        Some(1)
    );

    assert!(source.get_ingestion_history(0)?.is_empty());
    Ok(())
}
