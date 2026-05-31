//! System RPC handlers.

use crate::api::service_container::{GatewayHealthReport, ServiceContainer};
use sinex_primitives::Result;
use sinex_primitives::domain::HealthStatus;
use sinex_primitives::rpc::system::{
    ComponentHealthReport, ComponentsHealth, ReplayControlHealth, SystemHealthRequest,
    SystemHealthResponse, SystemPingRequest, SystemVersionRequest,
};

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
            },
            nats: system_component_health(
                nats.connected,
                nats.latency_ms.map(|value| value as f64),
                (!nats.detail.trim().is_empty()).then_some(nats.detail),
            ),
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
