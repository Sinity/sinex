use super::*;
use crate::api::service_container::{
    GatewayHealthStatus, NatsHealthProbe, ReplayControlStatus, aggregate_gateway_health_report,
};
use sinex_primitives::domain::{ModuleKind, ModuleName};
use sinex_primitives::{
    RuntimeLivenessAggregate, RuntimeLivenessEvidence, RuntimeLivenessMembership,
    RuntimeLivenessPolicy, Timestamp,
};
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn system_ping_returns_pong_string() -> TestResult<()> {
    let response = system_ping_response();
    assert_eq!(response, "pong");
    Ok(())
}

#[sinex_test]
async fn system_version_returns_gateway_package_version() -> TestResult<()> {
    let response = system_version_response();
    assert_eq!(response, env!("CARGO_PKG_VERSION"));
    Ok(())
}

#[sinex_test]
async fn system_health_response_uses_typed_contract() -> TestResult<()> {
    let response = system_health_response(GatewayHealthReport {
        status: GatewayHealthStatus::Degraded,
        db_ok: true,
        db_latency_ms: Some(7),
        db_detail: "ok".to_string(),
        nats: NatsHealthProbe {
            connected: false,
            latency_ms: Some(42),
            detail: "timed out".to_string(),
        },
        raw_ingest_dlq: crate::api::service_container::RawIngestDlqHealth {
            status: HealthStatus::Degraded,
            connected: true,
            pending_messages: Some(3),
            pending_sequence_span: Some(5),
            detail: "raw-ingest DLQ pressure: 3 pending message(s), sequence span 5".to_string(),
        },
        replay: ReplayControlStatus {
            enabled: true,
            connected: false,
            last_error: None,
        },
        sse_confirmation: crate::api::service_container::SseConfirmationStatus {
            running: true,
            degraded: true,
            detail: "pending_retries=2".to_string(),
        },
        runtime_liveness: RuntimeLivenessAggregate::evaluate(
            Vec::new(),
            RuntimeLivenessPolicy::default(),
            Timestamp::now(),
        ),
        healthy: false,
        serving: true,
        degradation_reasons: vec!["NATS unavailable".to_string()],
    });

    assert_eq!(response.status, HealthStatus::Degraded);
    assert!(response.components.database.connected);
    assert_eq!(response.components.database.latency_ms, Some(7.0));
    assert_eq!(response.components.database.detail.as_deref(), Some("ok"));
    assert_eq!(response.components.nats.status, HealthStatus::Unhealthy);
    assert_eq!(response.components.nats.latency_ms, Some(42.0));
    assert_eq!(
        response.components.nats.detail.as_deref(),
        Some("timed out")
    );
    assert_eq!(
        response.components.raw_ingest_dlq.status,
        HealthStatus::Degraded
    );
    assert_eq!(
        response.components.raw_ingest_dlq.detail.as_deref(),
        Some("raw-ingest DLQ pressure: 3 pending message(s), sequence span 5")
    );
    assert!(response.components.replay_control.enabled);
    assert_eq!(
        response.components.sse_confirmation.status,
        HealthStatus::Degraded
    );
    assert!(response.components.sse_confirmation.connected);
    assert_eq!(
        response.components.sse_confirmation.detail.as_deref(),
        Some("pending_retries=2")
    );
    assert_eq!(
        serde_json::to_value(&response)?["degradation_reasons"][0],
        "NATS unavailable"
    );
    Ok(())
}

#[sinex_test]
async fn failed_and_stale_runtime_evidence_prevents_infrastructure_all_clear() -> TestResult<()> {
    let now = Timestamp::now();
    let runtime_liveness = RuntimeLivenessAggregate::evaluate(
        [
            RuntimeLivenessEvidence {
                module_name: ModuleName::new("failed-source"),
                module_kind: ModuleKind::Source,
                membership: RuntimeLivenessMembership::Assessed,
                run_status: Some("failed".to_string()),
                health_status: Some(HealthStatus::Healthy),
                last_heartbeat_at: Some(now),
                last_output_at: None,
            },
            RuntimeLivenessEvidence {
                module_name: ModuleName::new("stale-automaton"),
                module_kind: ModuleKind::Automaton,
                membership: RuntimeLivenessMembership::Assessed,
                run_status: Some("running".to_string()),
                health_status: Some(HealthStatus::Healthy),
                last_heartbeat_at: Some(now - time::Duration::seconds(301)),
                last_output_at: None,
            },
        ],
        RuntimeLivenessPolicy::default(),
        now,
    );
    let report = aggregate_gateway_health_report(
        true,
        Some(1),
        "ok".to_string(),
        NatsHealthProbe {
            connected: true,
            latency_ms: Some(1),
            detail: "ok".to_string(),
        },
        crate::api::service_container::RawIngestDlqHealth {
            status: HealthStatus::Healthy,
            connected: true,
            pending_messages: Some(0),
            pending_sequence_span: Some(0),
            detail: "raw-ingest DLQ empty".to_string(),
        },
        ReplayControlStatus {
            enabled: true,
            connected: true,
            last_error: None,
        },
        crate::api::service_container::SseConfirmationStatus {
            running: true,
            degraded: false,
            detail: "healthy".to_string(),
        },
        runtime_liveness,
    );

    assert!(
        report.serving,
        "runtime evidence must not change /ready semantics"
    );
    assert!(
        !report.healthy,
        "failed or stale runtime evidence must defeat all-clear"
    );
    assert_eq!(report.status, HealthStatus::Unhealthy);

    let response = system_health_response(report);
    assert!(!response.healthy);
    assert_eq!(response.components.runtime_liveness.unhealthy_count, 1);
    assert_eq!(response.components.runtime_liveness.stale_count, 1);
    assert!(
        response
            .degradation_reasons
            .iter()
            .any(|reason| reason.contains("runtime liveness"))
    );
    Ok(())
}
