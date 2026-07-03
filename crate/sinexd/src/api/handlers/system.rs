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
#[path = "system_test.rs"]
mod tests;
