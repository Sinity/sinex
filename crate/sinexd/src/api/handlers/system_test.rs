use super::*;
use crate::api::service_container::{
    GatewayHealthStatus, NatsHealthProbe, ReplayControlStatus,
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
            detail: "raw-ingest DLQ pressure: 3 pending message(s), sequence span 5"
                .to_string(),
        },
        confirmation_buffer: crate::api::service_container::ConfirmationBufferHealth {
            status: HealthStatus::Degraded,
            connected: true,
            memory_owner:
                crate::api::service_container::ConfirmationBufferMemoryOwner::TimedOutGracePayloads,
            pressure_level: sinex_primitives::RuntimePressureLevel::Critical,
            runtime_action: sinex_primitives::RuntimePressureAction::AdmitWithPressure,
            observed_buffers: 1,
            pending_count: 3,
            timed_out_retained_count: 1,
            rejected_count: 2,
            late_confirmation_count: 5,
            retained_payload_bytes: 4096,
            approximate_payload_bytes: 4096,
            active_payload_bytes: 1024,
            timed_out_retained_payload_bytes: 3072,
            approximate_payload_bytes_by_kind: std::collections::BTreeMap::from([(
                "system.journald:journald.entry.written".to_string(),
                4096,
            )]),
            detail: "confirmation buffers: observed=1, pending=3, timed_out_retained=1, rejected=2, late_confirmations=5, pressure_level=critical, runtime_action=admit_with_pressure, retained_payload_bytes=4096, approximate_payload_bytes=4096, active_payload_bytes=1024, timed_out_retained_payload_bytes=3072, memory_owner=timed_out_grace_payloads"
                .to_string(),
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
    assert_eq!(
        response.components.confirmation_buffer.status,
        HealthStatus::Degraded
    );
    assert_eq!(
        response.components.confirmation_buffer.detail.as_deref(),
        Some(
            "confirmation buffers: observed=1, pending=3, timed_out_retained=1, rejected=2, late_confirmations=5, pressure_level=critical, runtime_action=admit_with_pressure, retained_payload_bytes=4096, approximate_payload_bytes=4096, active_payload_bytes=1024, timed_out_retained_payload_bytes=3072, memory_owner=timed_out_grace_payloads"
        )
    );
    assert_eq!(
        response
            .components
            .confirmation_buffer
            .attributes
            .get("memory_owner")
            .map(String::as_str),
        Some("timed_out_grace_payloads")
    );
    assert_eq!(
        response
            .components
            .confirmation_buffer
            .attributes
            .get("pressure_level")
            .map(String::as_str),
        Some("critical")
    );
    assert_eq!(
        response
            .components
            .confirmation_buffer
            .attributes
            .get("runtime_action")
            .map(String::as_str),
        Some("admit_with_pressure")
    );
    assert_eq!(
        response
            .components
            .confirmation_buffer
            .attributes
            .get("retained_payload_bytes")
            .map(String::as_str),
        Some("4096")
    );
    assert_eq!(
        response
            .components
            .confirmation_buffer
            .attributes
            .get("active_payload_bytes")
            .map(String::as_str),
        Some("1024")
    );
    assert_eq!(
        response
            .components
            .confirmation_buffer
            .attributes
            .get("timed_out_retained_payload_bytes")
            .map(String::as_str),
        Some("3072")
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
