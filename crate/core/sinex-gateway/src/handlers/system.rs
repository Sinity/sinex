//! System RPC handlers.

use crate::service_container::ServiceContainer;
use color_eyre::eyre::Result;
use serde_json::{Value, json};

pub async fn handle_system_health(services: &ServiceContainer, _params: Value) -> Result<Value> {
    let replay_control = services.replay_control_status();

    let db_healthy = sqlx::query("SELECT 1")
        .execute(services.pool())
        .await
        .is_ok();

    let nats_connected = services.nats_client().is_some_and(|client| {
        matches!(
            client.connection_state(),
            async_nats::connection::State::Connected
        )
    });

    let overall_status = if db_healthy && (nats_connected || replay_control.bypass_active) {
        "healthy"
    } else if db_healthy {
        "degraded"
    } else {
        "unhealthy"
    };

    Ok(json!({
        "status": overall_status,
        "components": {
            "database": {
                "status": if db_healthy { "healthy" } else { "unhealthy" },
                "connected": db_healthy
            },
            "nats": {
                "status": if nats_connected { "healthy" } else { "unhealthy" },
                "connected": nats_connected
            },
            "replay_control": {
                "status": if replay_control.connected { "healthy" } else if replay_control.bypass_active { "bypassed" } else { "unhealthy" },
                "enabled": replay_control.enabled,
                "bypass_allowed": replay_control.bypass_allowed,
                "bypass_active": replay_control.bypass_active,
                "connected": replay_control.connected,
                "last_error": replay_control.last_error
            }
        }
    }))
}
