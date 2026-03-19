//! System RPC handlers.

use crate::service_container::ServiceContainer;
use color_eyre::eyre::Result;
use serde_json::{Value, json};

pub async fn handle_system_health(services: &ServiceContainer, _params: Value) -> Result<Value> {
    let report = services.health_report().await;
    let crate::service_container::GatewayHealthReport {
        status: overall_status,
        db_ok,
        nats,
        replay,
        healthy,
        serving,
        degradation_reasons,
    } = report;

    Ok(json!({
        "status": overall_status,
        "healthy": healthy,
        "serving": serving,
        "degradation_reasons": degradation_reasons,
        "components": {
            "database": {
                "status": if db_ok { "healthy" } else { "unhealthy" },
                "connected": db_ok
            },
            "nats": {
                "status": if nats.connected { "healthy" } else { "unhealthy" },
                "connected": nats.connected,
                "latency_ms": nats.latency_ms,
                "detail": nats.detail
            },
            "replay_control": {
                "status": if replay.connected { "healthy" } else { "unhealthy" },
                "enabled": replay.enabled,
                "connected": replay.connected,
                "last_error": replay.last_error
            }
        }
    }))
}
