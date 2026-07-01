use super::*;
use crate::temporal::Timestamp;
use time::Duration;
use xtask::sandbox::prelude::*;

fn base(now: Timestamp) -> SourceStatus {
    SourceStatus {
        module_name: ModuleName::new("test-unit"),
        version: "0.0.0".into(),
        description: None,
        manifest_status: "active".into(),
        live: true,
        service_name: None,
        instance_id: None,
        module_run_id: None,
        host: None,
        run_status: Some("running".into()),
        started_at: Some(now - Duration::seconds(3600)),
        last_heartbeat_at: Some(now - Duration::seconds(5)),
        current_health: None,
        health_changed_at: None,
        health_reason: None,
        recent_output_count: 0,
        last_output_at: None,
    }
}

fn thresholds() -> EmitStallThresholds {
    EmitStallThresholds {
        uptime_gate_secs: 600,
        quiet_secs: 600,
    }
}

#[sinex_test]
async fn stalled_when_alive_and_quiet_past_window() -> TestResult<()> {
    let now = Timestamp::now();
    let mut s = base(now);
    s.last_output_at = Some(now - Duration::seconds(1200));
    assert_eq!(
        s.classify_emit_stall(thresholds(), now),
        EmitStallVerdict::Stalled,
    );
    Ok(())
}

#[sinex_test]
async fn stalled_when_never_emitted_past_uptime_gate() -> TestResult<()> {
    let now = Timestamp::now();
    let s = base(now); // uptime 3600s, recent_output_count = 0, last_output_at = None
    assert_eq!(
        s.classify_emit_stall(thresholds(), now),
        EmitStallVerdict::Stalled,
    );
    Ok(())
}

#[sinex_test]
async fn emitting_when_recent_output_present() -> TestResult<()> {
    let now = Timestamp::now();
    let mut s = base(now);
    s.recent_output_count = 42;
    s.last_output_at = Some(now - Duration::seconds(30));
    assert_eq!(
        s.classify_emit_stall(thresholds(), now),
        EmitStallVerdict::Emitting,
    );
    Ok(())
}

#[sinex_test]
async fn initializing_inside_uptime_gate() -> TestResult<()> {
    let now = Timestamp::now();
    let mut s = base(now);
    s.started_at = Some(now - Duration::seconds(60));
    assert_eq!(
        s.classify_emit_stall(thresholds(), now),
        EmitStallVerdict::Initializing,
    );
    Ok(())
}

#[sinex_test]
async fn not_live_when_unit_offline() -> TestResult<()> {
    let now = Timestamp::now();
    let mut s = base(now);
    s.live = false;
    assert_eq!(
        s.classify_emit_stall(thresholds(), now),
        EmitStallVerdict::NotLive,
    );
    Ok(())
}

#[sinex_test]
async fn unknown_when_no_started_at() -> TestResult<()> {
    let now = Timestamp::now();
    let mut s = base(now);
    s.started_at = None;
    assert_eq!(
        s.classify_emit_stall(thresholds(), now),
        EmitStallVerdict::Unknown,
    );
    Ok(())
}

#[sinex_test]
async fn emitting_when_last_output_inside_quiet_window() -> TestResult<()> {
    let now = Timestamp::now();
    let mut s = base(now);
    s.last_output_at = Some(now - Duration::seconds(120));
    assert_eq!(
        s.classify_emit_stall(thresholds(), now),
        EmitStallVerdict::Emitting,
    );
    Ok(())
}

#[sinex_test]
async fn defaults_from_env_match_constants() -> TestResult<()> {
    let t = EmitStallThresholds::default();
    assert_eq!(t.uptime_gate_secs, DEFAULT_EMIT_STALL_UPTIME_GATE_SECS);
    assert_eq!(t.quiet_secs, DEFAULT_EMIT_STALL_QUIET_SECS);
    Ok(())
}

#[sinex_test]
async fn status_view_request_defaults_to_exact_unfiltered_counts() -> TestResult<()> {
    let request = SourcesStatusViewRequest::default();

    assert_eq!(request.source, None);
    assert_eq!(request.family, None);
    assert!(request.exact_counts);

    let decoded: SourcesStatusViewRequest = serde_json::from_value(serde_json::json!({}))?;
    assert!(decoded.exact_counts);
    Ok(())
}

#[sinex_test]
async fn status_view_request_accepts_filtered_presence_mode() -> TestResult<()> {
    let request: SourcesStatusViewRequest = serde_json::from_value(serde_json::json!({
        "source": "browser.history",
        "family": "browser",
        "exact_counts": false
    }))?;

    assert_eq!(request.source.as_deref(), Some("browser.history"));
    assert_eq!(request.family.as_deref(), Some("browser"));
    assert!(!request.exact_counts);
    Ok(())
}

#[sinex_test]
async fn label_and_is_degraded() -> TestResult<()> {
    assert_eq!(EmitStallVerdict::Stalled.label(), "stalled");
    assert!(EmitStallVerdict::Stalled.is_degraded());
    assert!(!EmitStallVerdict::Emitting.is_degraded());
    assert!(!EmitStallVerdict::Initializing.is_degraded());
    assert!(!EmitStallVerdict::NotLive.is_degraded());
    Ok(())
}
