//! System RPC handlers.

use crate::api::service_container::{GatewayHealthReport, ServiceContainer};
use sinex_primitives::Result;
use sinex_primitives::domain::HealthStatus;
use sinex_primitives::rpc::system::{
    ComponentHealthReport, ComponentsHealth, ReplayControlHealth, SystemHealthRequest,
    SystemHealthResponse, SystemPingRequest, SystemVersionRequest,
};
use std::collections::BTreeMap;

pub async fn handle_system_ping(
    _services: &ServiceContainer,
    _request: SystemPingRequest,
) -> Result<String> {
    Ok(system_ping_response())
}

pub async fn handle_system_version(
    _services: &ServiceContainer,
    _request: SystemVersionRequest,
) -> Result<String> {
    Ok(system_version_response())
}

pub async fn handle_system_health(
    services: &ServiceContainer,
    _request: SystemHealthRequest,
) -> Result<SystemHealthResponse> {
    Ok(system_health_response(services.health_report().await))
}

pub(crate) fn system_health_response(report: GatewayHealthReport) -> SystemHealthResponse {
    let GatewayHealthReport {
        status,
        db_ok,
        db_latency_ms,
        db_detail,
        nats,
        raw_ingest_dlq,
        confirmation_buffer,
        replay,
        sse_confirmation,
        healthy,
        serving,
        degradation_reasons,
    } = report;

    SystemHealthResponse {
        status,
        healthy,
        serving,
        degradation_reasons,
        components: ComponentsHealth {
            database: ComponentHealthReport {
                status: if db_ok {
                    HealthStatus::Healthy
                } else {
                    HealthStatus::Unhealthy
                },
                connected: db_ok,
                latency_ms: db_latency_ms.map(|value| value as f64),
                detail: (!db_detail.trim().is_empty()).then_some(db_detail),
                attributes: BTreeMap::new(),
            },
            nats: system_component_health(
                nats.connected,
                nats.latency_ms.map(|value| value as f64),
                (!nats.detail.trim().is_empty()).then_some(nats.detail),
            ),
            raw_ingest_dlq: ComponentHealthReport {
                status: raw_ingest_dlq.status,
                connected: raw_ingest_dlq.connected,
                latency_ms: None,
                detail: Some(raw_ingest_dlq.detail),
                attributes: BTreeMap::new(),
            },
            confirmation_buffer: ComponentHealthReport {
                status: confirmation_buffer.status,
                connected: confirmation_buffer.connected,
                latency_ms: None,
                detail: Some(confirmation_buffer.detail),
                attributes: BTreeMap::from([
                    (
                        "memory_owner".to_string(),
                        confirmation_buffer.memory_owner.as_str().to_string(),
                    ),
                    (
                        "pressure_level".to_string(),
                        confirmation_buffer.pressure_level,
                    ),
                    (
                        "runtime_action".to_string(),
                        confirmation_buffer.runtime_action,
                    ),
                ]),
            },
            replay_control: ReplayControlHealth {
                status: if replay.connected {
                    HealthStatus::Healthy
                } else {
                    HealthStatus::Unhealthy
                },
                enabled: replay.enabled,
                connected: replay.connected,
                last_error: replay.last_error.map(|error| error.message),
            },
            sse_confirmation: ComponentHealthReport {
                status: if !sse_confirmation.running {
                    HealthStatus::Unhealthy
                } else if sse_confirmation.degraded {
                    HealthStatus::Degraded
                } else {
                    HealthStatus::Healthy
                },
                connected: sse_confirmation.running,
                latency_ms: None,
                detail: Some(sse_confirmation.detail),
                attributes: BTreeMap::new(),
            },
        },
    }
}

fn system_component_health(
    connected: bool,
    latency_ms: Option<f64>,
    detail: Option<String>,
) -> ComponentHealthReport {
    ComponentHealthReport {
        status: if connected {
            HealthStatus::Healthy
        } else {
            HealthStatus::Unhealthy
        },
        connected,
        latency_ms,
        detail,
        attributes: BTreeMap::new(),
    }
}

fn system_ping_response() -> String {
    "pong".to_string()
}

fn system_version_response() -> String {
    env!("CARGO_PKG_VERSION").to_string()
}

#[cfg(test)]
mod tests {
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
                pressure_level: "critical".to_string(),
                runtime_action: "admit_with_pressure".to_string(),
                observed_buffers: 1,
                pending_count: 3,
                timed_out_retained_count: 1,
                rejected_count: 2,
                late_confirmation_count: 5,
                approximate_payload_bytes: 4096,
                active_payload_bytes: 1024,
                timed_out_retained_payload_bytes: 3072,
                approximate_payload_bytes_by_kind: std::collections::BTreeMap::from([(
                    "system.journald:journald.entry.written".to_string(),
                    4096,
                )]),
                detail: "confirmation buffers: observed=1, pending=3, timed_out_retained=1, rejected=2, late_confirmations=5, pressure_level=critical, runtime_action=admit_with_pressure, approximate_payload_bytes=4096, active_payload_bytes=1024, timed_out_retained_payload_bytes=3072, memory_owner=timed_out_grace_payloads"
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
                "confirmation buffers: observed=1, pending=3, timed_out_retained=1, rejected=2, late_confirmations=5, pressure_level=critical, runtime_action=admit_with_pressure, approximate_payload_bytes=4096, active_payload_bytes=1024, timed_out_retained_payload_bytes=3072, memory_owner=timed_out_grace_payloads"
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
}
