use super::*;
use crate::sandbox::sinex_test;
use sinex_primitives::events::{EventEngineBatchStatsPayload, EventPayload};

#[sinex_test]
async fn test_batch_stats_source_matches_payload_contract() -> xtask::sandbox::TestResult<()> {
    assert_eq!(
        event_engine_batch_stats_source(),
        EventEngineBatchStatsPayload::SOURCE.as_static_str()
    );
    Ok(())
}

#[sinex_test]
async fn test_summary_fragment_marks_stale_samples() -> xtask::sandbox::TestResult<()> {
    let metrics = RuntimeMetrics {
        event_engine_status: EventEngineStatus::Down,
        last_heartbeat_age_secs: Some(300),
        consumer_lag_pending: Some(42.0),
        consumer_lag_age_secs: Some(300),
        last_batch_latency_ms: Some(12.0),
        last_batch_latency_age_secs: Some(300),
        query_error: None,
    };

    assert_eq!(
        metrics.summary_fragment(),
        "event_engine:down lag:stale batch:stale"
    );
    Ok(())
}

#[sinex_test]
async fn test_summary_fragment_uses_fresh_samples() -> xtask::sandbox::TestResult<()> {
    let metrics = RuntimeMetrics {
        event_engine_status: EventEngineStatus::Healthy,
        last_heartbeat_age_secs: Some(5),
        consumer_lag_pending: Some(7.0),
        consumer_lag_age_secs: Some(10),
        last_batch_latency_ms: Some(125.0),
        last_batch_latency_age_secs: Some(10),
        query_error: None,
    };

    assert_eq!(
        metrics.summary_fragment(),
        "event_engine:ok lag:7 batch:125ms"
    );
    Ok(())
}

#[sinex_test]
async fn test_runtime_assessment_marks_unknown_runtime_unavailable()
-> xtask::sandbox::TestResult<()> {
    let metrics = RuntimeMetrics {
        event_engine_status: EventEngineStatus::Unknown,
        last_heartbeat_age_secs: None,
        consumer_lag_pending: None,
        consumer_lag_age_secs: None,
        last_batch_latency_ms: None,
        last_batch_latency_age_secs: None,
        query_error: None,
    };

    let assessment = metrics.assessment();
    assert_eq!(assessment.status, RuntimeHealthStatus::Unavailable);
    assert_eq!(
        assessment.warnings,
        vec!["Runtime health: event_engine status is unknown".to_string()]
    );
    Ok(())
}

#[sinex_test]
async fn test_runtime_assessment_marks_degraded_on_stale_signals() -> xtask::sandbox::TestResult<()>
{
    let metrics = RuntimeMetrics {
        event_engine_status: EventEngineStatus::Stale,
        last_heartbeat_age_secs: Some(300),
        consumer_lag_pending: Some(42.0),
        consumer_lag_age_secs: Some(300),
        last_batch_latency_ms: Some(12.0),
        last_batch_latency_age_secs: Some(300),
        query_error: None,
    };

    let assessment = metrics.assessment();
    assert_eq!(assessment.status, RuntimeHealthStatus::Degraded);
    assert!(
        assessment
            .warnings
            .iter()
            .any(|warning| warning.contains("event_engine heartbeat is stale"))
    );
    assert!(
        assessment
            .warnings
            .iter()
            .any(|warning| warning.contains("consumer lag telemetry is stale"))
    );
    Ok(())
}

#[sinex_test]
async fn test_runtime_assessment_preserves_unknown_stale_sample_age()
-> xtask::sandbox::TestResult<()> {
    let metrics = RuntimeMetrics {
        event_engine_status: EventEngineStatus::Healthy,
        last_heartbeat_age_secs: Some(5),
        consumer_lag_pending: Some(42.0),
        consumer_lag_age_secs: None,
        last_batch_latency_ms: Some(125.0),
        last_batch_latency_age_secs: None,
        query_error: None,
    };

    assert_eq!(
        metrics.consumer_lag_stale_note().as_deref(),
        Some("sample age unavailable")
    );
    assert_eq!(
        metrics.batch_latency_stale_note().as_deref(),
        Some("sample age unavailable")
    );

    let warnings = metrics.warnings();
    assert!(warnings.iter().any(|warning| {
        warning.contains("consumer lag telemetry is stale (sample age unavailable)")
    }));
    assert!(warnings.iter().any(|warning| {
        warning.contains("batch latency telemetry is stale (sample age unavailable)")
    }));
    Ok(())
}

#[sinex_test]
async fn test_summary_fragment_marks_query_failures() -> xtask::sandbox::TestResult<()> {
    let metrics = RuntimeMetrics {
        event_engine_status: EventEngineStatus::Unknown,
        last_heartbeat_age_secs: None,
        consumer_lag_pending: None,
        consumer_lag_age_secs: None,
        last_batch_latency_ms: None,
        last_batch_latency_age_secs: None,
        query_error: Some("connection refused".to_string()),
    };

    assert_eq!(
        metrics.summary_fragment(),
        "event_engine:unknown lag:- batch:- query:error"
    );
    assert!(
        metrics
            .warnings()
            .iter()
            .any(|warning| warning.contains("failed to query runtime metrics"))
    );
    Ok(())
}

#[sinex_test]
async fn test_interpret_event_engine_status_accepts_running_and_active()
-> xtask::sandbox::TestResult<()> {
    assert_eq!(
        interpret_event_engine_status(Some("running"), Some(5)),
        EventEngineStatus::Healthy
    );
    assert_eq!(
        interpret_event_engine_status(Some("active"), Some(5)),
        EventEngineStatus::Healthy
    );
    Ok(())
}

#[sinex_test]
async fn test_interpret_event_engine_status_marks_live_rows_stale_or_down_when_needed()
-> xtask::sandbox::TestResult<()> {
    assert_eq!(
        interpret_event_engine_status(Some("running"), Some(HEARTBEAT_STALE_SECS + 1)),
        EventEngineStatus::Stale
    );
    assert_eq!(
        interpret_event_engine_status(Some("active"), None),
        EventEngineStatus::Down
    );
    Ok(())
}

#[sinex_test]
async fn test_interpret_event_engine_status_rejects_non_live_statuses()
-> xtask::sandbox::TestResult<()> {
    assert_eq!(
        interpret_event_engine_status(Some("inactive"), Some(1)),
        EventEngineStatus::Down
    );
    assert_eq!(
        interpret_event_engine_status(None, Some(1)),
        EventEngineStatus::Down
    );
    Ok(())
}
