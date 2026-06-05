//! RuntimeActor operations handlers
//!
//! This module provides RPC endpoints for managing runtime operations:
//! - List modules and their status
//! - Drain modules (pause event processing)
//! - Resume modules (restart event processing)
//! - Set processing horizon (control replay boundaries)

use serde_json::{Value, json};
use sinex_primitives::temporal::Timestamp;
use sinex_primitives::{
    SinexError, domain::OperationStatus, environment::SinexEnvironment, transport,
};
use std::error::Error as _;

// Re-export shared types for use by other modules
pub use sinex_primitives::rpc::runtime::{
    RuntimeDrainRequest, RuntimeDrainResponse, RuntimeResumeRequest, RuntimeResumeResponse,
    RuntimeSetHorizonRequest, RuntimeSetHorizonResponse, RuntimeStatus, RuntimeListRequest, RuntimeListResponse,
};

type Result<T> = std::result::Result<T, SinexError>;

async fn publish_node_control(
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
    // Query node status from KV store
    let js = async_nats::jetstream::new(nats_client.clone());

    let kv_bucket_name = env.nats_kv_bucket_name("sinex_runtime_state");

    // Treat the missing bucket as an honest empty registry, but surface every
    // other JetStream failure instead of pretending there are no modules.
    let kv = match js.get_key_value(&kv_bucket_name).await {
        Ok(kv) => kv,
        Err(error) if is_missing_runtime_state_bucket(&error) => {
            return Ok(RuntimeListResponse { modules: Vec::new() });
        }
        Err(error) => {
            return Err(SinexError::kv("Failed to open node state bucket")
                .with_context("bucket", kv_bucket_name)
                .with_source(error));
        }
    };

    // Get all keys in the bucket (each key is a node ID)
    let mut modules = Vec::new();

    // Watch for all entries (one-time scan)
    let mut entries = kv
        .keys()
        .await
        .map_err(|e| SinexError::kv("Failed to list node keys").with_source(e))?;

    use futures::StreamExt;
    while let Some(key) = entries.next().await {
        let key = key.map_err(|e| SinexError::kv("Failed to read key").with_source(e))?;

        // Get the value for this key
        let entry = kv
            .get(&key)
            .await
            .map_err(|e| {
                SinexError::kv("Failed to fetch node state")
                    .with_context("runtime_state_key", key.clone())
                    .with_source(e)
            })?
            .ok_or_else(|| {
                SinexError::not_found("RuntimeActor state disappeared during listing")
                    .with_context("runtime_state_key", key.clone())
            })?;

        let state_json = String::from_utf8(entry.to_vec()).map_err(|error| {
            SinexError::serialization("RuntimeActor state is not valid UTF-8")
                .with_context("runtime_state_key", key.clone())
                .with_std_error(&error)
        })?;
        let state = serde_json::from_str::<RuntimeStatus>(&state_json).map_err(|error| {
            SinexError::serialization("RuntimeActor state is not valid JSON")
                .with_context("runtime_state_key", key.clone())
                .with_std_error(&error)
        })?;
        modules.push(state);
    }

    Ok(RuntimeListResponse { modules })
}

/// Handle POST /modules/{id}/drain - pause node processing
///
/// # Authorization
///
/// RuntimeActor drain is a production-impacting operation. The auth context is
/// logged for audit purposes.
pub async fn handle_runtime_drain(
    nats_client: &async_nats::Client,
    env: &SinexEnvironment,
    drain_params: RuntimeDrainRequest,
    auth: &crate::api::rpc_server::RpcAuthContext,
) -> Result<RuntimeDrainResponse> {
    use tracing::info;

    info!(
        actor = %auth.actor_id(),
        module_name = %drain_params.module_name,
        reason = ?drain_params.reason,
        "RuntimeActor drain initiated"
    );

    // Publish drain command to NATS control subject
    let subject = env.nats_subject(&format!(
        "sinex.control.sources.{}.drain",
        drain_params.module_name
    ));

    let payload = json!({
        "action": "drain",
        "module_name": drain_params.module_name,
        "reason": drain_params.reason,
        "timestamp": Timestamp::now(),
    });

    publish_node_control(nats_client, subject, payload, "drain command").await?;

    Ok(RuntimeDrainResponse {
        status: OperationStatus::Pending,
        module_name: drain_params.module_name,
    })
}

/// Handle POST /modules/{id}/resume - resume node processing
///
/// # Authorization
///
/// RuntimeActor resume is a production-impacting operation. The auth context is
/// logged for audit purposes.
pub async fn handle_runtime_resume(
    nats_client: &async_nats::Client,
    env: &SinexEnvironment,
    resume_params: RuntimeResumeRequest,
    auth: &crate::api::rpc_server::RpcAuthContext,
) -> Result<RuntimeResumeResponse> {
    use tracing::info;

    info!(
        actor = %auth.actor_id(),
        module_name = %resume_params.module_name,
        "RuntimeActor resume initiated"
    );

    // Publish resume command to NATS control subject
    let subject = env.nats_subject(&format!(
        "sinex.control.sources.{}.resume",
        resume_params.module_name
    ));

    let payload = json!({
        "action": "resume",
        "module_name": resume_params.module_name,
        "timestamp": Timestamp::now(),
    });

    publish_node_control(nats_client, subject, payload, "resume command").await?;

    Ok(RuntimeResumeResponse {
        status: OperationStatus::Pending,
        module_name: resume_params.module_name,
    })
}

/// Handle POST /modules/{id}/set-horizon - set processing horizon
///
/// # Authorization
///
/// Setting the replay horizon can cause data reprocessing or loss.
/// The auth context is logged for audit purposes.
pub async fn handle_runtime_set_horizon(
    nats_client: &async_nats::Client,
    env: &SinexEnvironment,
    horizon_params: RuntimeSetHorizonRequest,
    auth: &crate::api::rpc_server::RpcAuthContext,
) -> Result<RuntimeSetHorizonResponse> {
    use tracing::info;

    info!(
        actor = %auth.actor_id(),
        module_name = %horizon_params.module_name,
        horizon = %horizon_params.horizon,
        "RuntimeActor set-horizon initiated"
    );

    // Publish set-horizon command to NATS control subject
    let subject = env.nats_subject(&format!(
        "sinex.control.sources.{}.set-horizon",
        horizon_params.module_name
    ));

    let payload = json!({
        "action": "set_horizon",
        "module_name": horizon_params.module_name,
        "horizon": horizon_params.horizon,
        "timestamp": Timestamp::now(),
    });

    publish_node_control(nats_client, subject, payload, "set-horizon command").await?;

    Ok(RuntimeSetHorizonResponse {
        status: OperationStatus::Pending,
        module_name: horizon_params.module_name,
        horizon: horizon_params.horizon,
    })
}
