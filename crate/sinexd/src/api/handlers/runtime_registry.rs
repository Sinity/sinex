//! RuntimeModule operations handlers
//!
//! This module provides RPC endpoints for managing runtime operations:
//! - List modules and their status
//! - Drain modules (pause event processing)
//! - Resume modules (restart event processing)
//! - Set processing horizon (control replay boundaries)

use crate::api::service_container::ServiceContainer;
use serde_json::{Value, json};
use sinex_db::repositories::Operation;
use sinex_db::{DbPool, DbPoolExt};
use sinex_primitives::temporal::Timestamp;
use sinex_primitives::{
    SinexError, domain::OperationStatus, environment::SinexEnvironment, transport,
};
use std::error::Error as _;

// Re-export shared types for use by other modules
pub use sinex_primitives::rpc::runtime::{
    RuntimeDrainRequest, RuntimeDrainResponse, RuntimeListRequest, RuntimeListResponse,
    RuntimeResumeRequest, RuntimeResumeResponse, RuntimeSetHorizonRequest,
    RuntimeSetHorizonResponse, RuntimeStatus,
};

type Result<T> = std::result::Result<T, SinexError>;

const RUNTIME_CONTROL_SURFACE: &str = "runtime_module_control";

async fn start_runtime_control_operation(
    pool: &DbPool,
    operation_type: &'static str,
    actor: &str,
    scope: Value,
    preview_summary: Value,
) -> Result<String> {
    let record = pool
        .state()
        .log_operation(Operation {
            id: None,
            operation_type: operation_type.to_string(),
            operator: actor.to_string(),
            scope: Some(scope),
            result_status: OperationStatus::Running,
            result_message: Some("runtime control message published".to_string()),
            preview_summary: Some(preview_summary),
            duration_ms: None,
        })
        .await?;
    Ok(record.id.to_uuid().to_string())
}

async fn finalize_runtime_control_operation(
    pool: &DbPool,
    operation_id: &str,
    status: OperationStatus,
    message: &str,
    preview_summary: Value,
) -> Result<()> {
    let operation_uuid = operation_id.parse().map_err(|error| {
        SinexError::validation("runtime control operation id is invalid")
            .with_context("operation_id", operation_id)
            .with_std_error(&error)
    })?;
    let operation_id = sinex_db::Id::<Operation>::from_uuid(operation_uuid);
    pool.state()
        .update_operation_meta(&operation_id, status, Some(message), preview_summary)
        .await
}

fn runtime_control_preview(
    action: &'static str,
    module_name: &sinex_primitives::domain::ModuleName,
    subject: &str,
) -> Value {
    json!({
        "surface": RUNTIME_CONTROL_SURFACE,
        "action": action,
        "module_name": module_name,
        "control_subject": subject,
    })
}

async fn publish_runtime_control_operation(
    services: &ServiceContainer,
    operation_type: &'static str,
    action: &'static str,
    actor: &str,
    module_name: &sinex_primitives::domain::ModuleName,
    subject: String,
    mut scope: Value,
    mut payload: Value,
    command_label: &'static str,
) -> Result<String> {
    scope["surface"] = json!(RUNTIME_CONTROL_SURFACE);
    scope["action"] = json!(action);
    scope["module_name"] = json!(module_name);
    scope["control_subject"] = json!(subject);

    let operation_id = start_runtime_control_operation(
        services.pool(),
        operation_type,
        actor,
        scope.clone(),
        runtime_control_preview(action, module_name, &subject),
    )
    .await?;

    payload["operation_id"] = json!(operation_id);

    let nats_client = services
        .nats_client()
        .ok_or_else(|| SinexError::configuration("NATS client is not available"))?;
    if let Err(error) =
        publish_runtime_control(nats_client, subject.clone(), payload, command_label).await
    {
        let message = error.to_string();
        scope["error"] = json!(message);
        let _ = finalize_runtime_control_operation(
            services.pool(),
            &operation_id,
            OperationStatus::Failed,
            &message,
            scope,
        )
        .await;
        return Err(error);
    }

    Ok(operation_id)
}

async fn publish_runtime_control(
    nats_client: &async_nats::Client,
    subject: String,
    payload: Value,
    operation: &'static str,
) -> Result<()> {
    let mut headers = async_nats::HeaderMap::new();
    transport::insert_transport_class_headers(&mut headers, transport::Class::Control);

    nats_client
        .publish_with_headers(
            subject.clone(),
            headers,
            serde_json::to_vec(&payload)
                .map_err(|e| {
                    SinexError::serialization(format!("failed to serialize {operation} payload"))
                        .with_std_error(&e)
                })?
                .into(),
        )
        .await
        .map_err(|e| {
            SinexError::nats_publish(operation)
                .with_context("subject", &subject)
                .with_std_error(&e)
        })
}

fn is_missing_runtime_state_bucket(error: &async_nats::jetstream::context::KeyValueError) -> bool {
    use async_nats::jetstream::ErrorCode;
    use async_nats::jetstream::context::{GetStreamError, GetStreamErrorKind, KeyValueErrorKind};

    if error.kind() != KeyValueErrorKind::GetBucket {
        return false;
    }

    let Some(source) = error.source() else {
        return false;
    };
    let Some(stream_error) = source.downcast_ref::<GetStreamError>() else {
        return false;
    };

    matches!(
        stream_error.kind(),
        GetStreamErrorKind::JetStream(js_error)
            if js_error.error_code() == ErrorCode::STREAM_NOT_FOUND
    )
}

/// Handle GET /modules request - list all modules
pub async fn handle_runtime_list(
    nats_client: &async_nats::Client,
    env: &SinexEnvironment,
    _request: RuntimeListRequest,
) -> Result<RuntimeListResponse> {
    // Query runtime module status from KV store.
    let js = async_nats::jetstream::new(nats_client.clone());

    let kv_bucket_name = env.nats_kv_bucket_name("sinex_runtime_state");

    // Treat the missing bucket as an honest empty registry, but surface every
    // other JetStream failure instead of pretending there are no modules.
    let kv = match js.get_key_value(&kv_bucket_name).await {
        Ok(kv) => kv,
        Err(error) if is_missing_runtime_state_bucket(&error) => {
            return Ok(RuntimeListResponse {
                modules: Vec::new(),
            });
        }
        Err(error) => {
            return Err(SinexError::kv("Failed to open runtime module state bucket")
                .with_context("bucket", kv_bucket_name)
                .with_source(error));
        }
    };

    // Get all keys in the bucket (each key is a module ID).
    let mut modules = Vec::new();

    // Watch for all entries (one-time scan)
    let mut entries = kv
        .keys()
        .await
        .map_err(|e| SinexError::kv("Failed to list module keys").with_source(e))?;

    use futures::StreamExt;
    while let Some(key) = entries.next().await {
        let key = key.map_err(|e| SinexError::kv("Failed to read key").with_source(e))?;

        // Get the value for this key
        let entry = kv
            .get(&key)
            .await
            .map_err(|e| {
                SinexError::kv("Failed to fetch runtime module state")
                    .with_context("runtime_state_key", key.clone())
                    .with_source(e)
            })?
            .ok_or_else(|| {
                SinexError::not_found("RuntimeModule state disappeared during listing")
                    .with_context("runtime_state_key", key.clone())
            })?;

        let state_json = String::from_utf8(entry.to_vec()).map_err(|error| {
            SinexError::serialization("RuntimeModule state is not valid UTF-8")
                .with_context("runtime_state_key", key.clone())
                .with_std_error(&error)
        })?;
        let state = serde_json::from_str::<RuntimeStatus>(&state_json).map_err(|error| {
            SinexError::serialization("RuntimeModule state is not valid JSON")
                .with_context("runtime_state_key", key.clone())
                .with_std_error(&error)
        })?;
        modules.push(state);
    }

    Ok(RuntimeListResponse { modules })
}

/// Handle POST /modules/{id}/drain - pause runtime processing
///
/// # Authorization
///
/// RuntimeModule drain is a production-impacting operation. The auth context is
/// logged for audit purposes.
pub async fn handle_runtime_drain(
    services: &ServiceContainer,
    drain_params: RuntimeDrainRequest,
    auth: &crate::api::rpc_server::RpcAuthContext,
) -> Result<RuntimeDrainResponse> {
    use tracing::info;
    let env = services.environment();
    let actor = auth.actor_id().to_string();

    info!(
        actor = %actor,
        module_name = %drain_params.module_name,
        reason = ?drain_params.reason,
        "RuntimeModule drain initiated"
    );

    // Publish drain command to NATS control subject
    let subject = env.nats_subject(&format!(
        "sinex.control.sources.{}.drain",
        drain_params.module_name
    ));
    let operation_id = publish_runtime_control_operation(
        services,
        "runtime.drain",
        "drain",
        &actor,
        &drain_params.module_name,
        subject,
        json!({
            "reason": drain_params.reason,
        }),
        json!({
            "action": "drain",
            "module_name": drain_params.module_name,
            "reason": drain_params.reason,
            "timestamp": Timestamp::now(),
        }),
        "drain command",
    )
    .await?;

    Ok(RuntimeDrainResponse {
        status: OperationStatus::Pending,
        module_name: drain_params.module_name,
        operation_id,
    })
}

/// Handle POST /modules/{id}/resume - resume runtime processing
///
/// # Authorization
///
/// RuntimeModule resume is a production-impacting operation. The auth context is
/// logged for audit purposes.
pub async fn handle_runtime_resume(
    services: &ServiceContainer,
    resume_params: RuntimeResumeRequest,
    auth: &crate::api::rpc_server::RpcAuthContext,
) -> Result<RuntimeResumeResponse> {
    use tracing::info;
    let env = services.environment();
    let actor = auth.actor_id().to_string();

    info!(
        actor = %actor,
        module_name = %resume_params.module_name,
        "RuntimeModule resume initiated"
    );

    // Publish resume command to NATS control subject
    let subject = env.nats_subject(&format!(
        "sinex.control.sources.{}.resume",
        resume_params.module_name
    ));
    let operation_id = publish_runtime_control_operation(
        services,
        "runtime.resume",
        "resume",
        &actor,
        &resume_params.module_name,
        subject,
        json!({}),
        json!({
            "action": "resume",
            "module_name": resume_params.module_name,
            "timestamp": Timestamp::now(),
        }),
        "resume command",
    )
    .await?;

    Ok(RuntimeResumeResponse {
        status: OperationStatus::Pending,
        module_name: resume_params.module_name,
        operation_id,
    })
}

/// Handle POST /modules/{id}/set-horizon - set processing horizon
///
/// # Authorization
///
/// Setting the replay horizon can cause data reprocessing or loss.
/// The auth context is logged for audit purposes.
pub async fn handle_runtime_set_horizon(
    services: &ServiceContainer,
    horizon_params: RuntimeSetHorizonRequest,
    auth: &crate::api::rpc_server::RpcAuthContext,
) -> Result<RuntimeSetHorizonResponse> {
    use tracing::info;
    let env = services.environment();
    let actor = auth.actor_id().to_string();

    info!(
        actor = %actor,
        module_name = %horizon_params.module_name,
        horizon = %horizon_params.horizon,
        "RuntimeModule set-horizon initiated"
    );

    // Publish set-horizon command to NATS control subject
    let subject = env.nats_subject(&format!(
        "sinex.control.sources.{}.set-horizon",
        horizon_params.module_name
    ));
    let operation_id = publish_runtime_control_operation(
        services,
        "runtime.set_horizon",
        "set_horizon",
        &actor,
        &horizon_params.module_name,
        subject,
        json!({
            "horizon": horizon_params.horizon,
        }),
        json!({
            "action": "set_horizon",
            "module_name": horizon_params.module_name,
            "horizon": horizon_params.horizon,
            "timestamp": Timestamp::now(),
        }),
        "set-horizon command",
    )
    .await?;

    Ok(RuntimeSetHorizonResponse {
        status: OperationStatus::Pending,
        module_name: horizon_params.module_name,
        horizon: horizon_params.horizon,
        operation_id,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use xtask::sandbox::sinex_test;

    #[sinex_test]
    async fn runtime_control_operation_records_actor_scope_and_preview(
        ctx: xtask::sandbox::TestContext,
    ) -> xtask::sandbox::TestResult<()> {
        let module_name = sinex_primitives::domain::ModuleName::from("terminal-source");
        let operation_id = start_runtime_control_operation(
            ctx.pool(),
            "runtime.drain",
            "operator:alice",
            json!({
                "surface": RUNTIME_CONTROL_SURFACE,
                "action": "drain",
                "module_name": module_name,
                "reason": "maintenance",
                "control_subject": "dev.sinex.control.sources.terminal-source.drain",
            }),
            runtime_control_preview(
                "drain",
                &module_name,
                "dev.sinex.control.sources.terminal-source.drain",
            ),
        )
        .await?;

        let operation_id = operation_id.parse()?;
        let operation = ctx
            .pool()
            .state()
            .get_operation(&operation_id)
            .await?
            .expect("runtime control operation should be persisted");

        assert_eq!(operation.operation_type, "runtime.drain");
        assert_eq!(operation.operator, "operator:alice");
        assert_eq!(operation.result_status, OperationStatus::Running);
        assert_eq!(
            operation.scope.as_ref().unwrap()["surface"],
            RUNTIME_CONTROL_SURFACE
        );
        assert_eq!(operation.scope.as_ref().unwrap()["action"], "drain");
        assert_eq!(operation.scope.as_ref().unwrap()["reason"], "maintenance");
        assert_eq!(
            operation.preview_summary.as_ref().unwrap()["control_subject"],
            "dev.sinex.control.sources.terminal-source.drain"
        );
        Ok(())
    }

    #[sinex_test]
    async fn runtime_control_operation_records_publish_failure(
        ctx: xtask::sandbox::TestContext,
    ) -> xtask::sandbox::TestResult<()> {
        let module_name = sinex_primitives::domain::ModuleName::from("terminal-source");
        let operation_id = start_runtime_control_operation(
            ctx.pool(),
            "runtime.resume",
            "operator:alice",
            json!({
                "surface": RUNTIME_CONTROL_SURFACE,
                "action": "resume",
                "module_name": module_name,
                "control_subject": "dev.sinex.control.sources.terminal-source.resume",
            }),
            runtime_control_preview(
                "resume",
                &module_name,
                "dev.sinex.control.sources.terminal-source.resume",
            ),
        )
        .await?;

        finalize_runtime_control_operation(
            ctx.pool(),
            &operation_id,
            OperationStatus::Failed,
            "publish failed",
            json!({
                "surface": RUNTIME_CONTROL_SURFACE,
                "action": "resume",
                "module_name": module_name,
                "error": "publish failed",
            }),
        )
        .await?;

        let operation_id = operation_id.parse()?;
        let operation = ctx
            .pool()
            .state()
            .get_operation(&operation_id)
            .await?
            .expect("runtime control operation should be persisted");

        assert_eq!(operation.operation_type, "runtime.resume");
        assert_eq!(operation.result_status, OperationStatus::Failed);
        assert_eq!(operation.result_message.as_deref(), Some("publish failed"));
        assert_eq!(
            operation.preview_summary.as_ref().unwrap()["error"],
            "publish failed"
        );
        Ok(())
    }
}
