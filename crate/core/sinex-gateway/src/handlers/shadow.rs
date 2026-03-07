//! Shadow consumer management for The Tether
//!
//! This module provides RPC endpoints for managing shadow consumers,
//! which allow development tools to subscribe to production event streams
//! without affecting production consumers.
//!
//! Shadow consumers:
//! - Are durable consumers with unique names (dev-{user}-{timestamp})
//! - Use fan-out delivery (don't steal messages from production)
//! - Can be created, listed, and deleted via RPC

use async_nats::jetstream;
use color_eyre::eyre::{Context, Result, eyre};
use serde_json::Value;
use sinex_node_sdk::runtime::stream::{
    ShadowConsumerSpec, create_shadow_consumer, delete_consumer, list_consumers,
};
use sinex_primitives::environment::SinexEnvironment;
use std::time::Duration;
use tracing::{info, warn};

// Re-export shared types
pub use sinex_primitives::rpc::shadow::{
    ShadowConsumerInfo, ShadowCreateRequest, ShadowCreateResponse, ShadowDeleteRequest,
    ShadowDeleteResponse, ShadowListRequest, ShadowListResponse,
};

fn env_var_duration_secs(name: &str, default: u64) -> Duration {
    Duration::from_secs(
        std::env::var(name)
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(default),
    )
}

/// Create a shadow consumer for development/testing
///
/// This creates a durable consumer that receives copies of all events
/// matching the filter without affecting production consumers.
pub async fn handle_shadow_create(
    nats_client: &async_nats::Client,
    env: &SinexEnvironment,
    params: Value,
) -> Result<Value> {
    let request: ShadowCreateRequest =
        serde_json::from_value(params).wrap_err("Invalid shadow.create parameters")?;

    // Validate consumer name format (must start with "dev-" for safety)
    if !request.consumer_name.starts_with("dev-") {
        return Err(eyre!(
            "Shadow consumer names must start with 'dev-' prefix for safety"
        ));
    }

    let js = jetstream::new(nats_client.clone());
    let stream_name = env.nats_stream_name("EVENTS");

    // Require explicit subject filter - no default to prevent unintended access
    let Some(subject_filter) = request.subject_filter else {
        return Err(eyre!(
            "subject_filter is required for shadow consumers (use 'events.>' explicitly if needed)"
        ));
    };

    // Warn on overly broad patterns
    if subject_filter.ends_with(".>") || subject_filter == "*" {
        warn!(
            consumer_name = %request.consumer_name,
            subject_filter = %subject_filter,
            "Shadow consumer created with broad subject filter"
        );
    }

    // Create durable consumer with explicit ack for proper tracking
    let timeout = env_var_duration_secs("SINEX_NATS_CONSUMER_CREATE_TIMEOUT_SECS", 10);
    let mut spec = ShadowConsumerSpec::new(
        stream_name.clone(),
        request.consumer_name.clone(),
        subject_filter.clone(),
    );
    spec.from_sequence = request.from_sequence;
    spec.from_beginning = request.from_beginning;
    spec.create_timeout = timeout;
    let info = create_shadow_consumer(&js, &spec).await.map_err(|e| {
        eyre!(
            "Failed to create shadow consumer '{}': {}",
            request.consumer_name,
            e
        )
    })?;

    info!(
        consumer_name = %request.consumer_name,
        stream = %stream_name,
        subject_filter = %subject_filter,
        num_pending = info.num_pending,
        "Created shadow consumer for The Tether"
    );

    let response = ShadowCreateResponse {
        consumer: ShadowConsumerInfo {
            consumer_name: request.consumer_name,
            stream_name,
            subject_filter,
            num_pending: info.num_pending,
            first_sequence: info.delivered.stream_sequence,
        },
    };

    Ok(serde_json::to_value(response)?)
}

/// List active shadow consumers
pub async fn handle_shadow_list(
    nats_client: &async_nats::Client,
    env: &SinexEnvironment,
    params: Value,
) -> Result<Value> {
    let request: ShadowListRequest = serde_json::from_value(params).unwrap_or_default();

    let js = jetstream::new(nats_client.clone());
    let stream_name = env.nats_stream_name("EVENTS");

    let consumers = list_consumers(&js, &stream_name).await.map_err(|e| {
        eyre!(
            "Failed to list consumers for stream '{}': {}",
            stream_name,
            e
        )
    })?;
    let mut shadow_consumers = Vec::new();

    for info in consumers {
        // Filter to only shadow consumers (dev- prefix)
        if let Some(ref name) = info.config.name
            && name.starts_with("dev-")
        {
            // Apply optional prefix filter
            let include = match &request.prefix {
                Some(prefix) => name.starts_with(prefix),
                None => true,
            };

            if include {
                shadow_consumers.push(ShadowConsumerInfo {
                    consumer_name: name.clone(),
                    stream_name: stream_name.clone(),
                    subject_filter: info.config.filter_subject.clone(),
                    num_pending: info.num_pending,
                    first_sequence: info.delivered.stream_sequence,
                });
            }
        }
    }

    let response = ShadowListResponse {
        consumers: shadow_consumers,
    };

    Ok(serde_json::to_value(response)?)
}

/// Delete a shadow consumer
///
/// # Authorization
///
/// This is a dangerous operation that deletes a NATS consumer.
/// The auth context is logged for audit purposes.
pub async fn handle_shadow_delete(
    nats_client: &async_nats::Client,
    env: &SinexEnvironment,
    params: Value,
    auth: &crate::rpc_server::RpcAuthContext,
) -> Result<Value> {
    let request: ShadowDeleteRequest =
        serde_json::from_value(params).wrap_err("Invalid shadow.delete parameters")?;

    // Safety check: only allow deleting dev- prefixed consumers
    if !request.consumer_name.starts_with("dev-") {
        return Err(eyre!("Can only delete shadow consumers with 'dev-' prefix"));
    }

    let js = jetstream::new(nats_client.clone());
    let stream_name = env.nats_stream_name("EVENTS");
    delete_consumer(&js, &stream_name, &request.consumer_name)
        .await
        .map_err(|e| {
            eyre!(
                "Failed to delete consumer '{}': {}",
                request.consumer_name,
                e
            )
        })?;

    info!(
        token_prefix = %auth.token_prefix,
        consumer_name = %request.consumer_name,
        "Shadow consumer deleted"
    );

    let response = ShadowDeleteResponse {
        status: "success".to_string(),
        consumer_name: request.consumer_name,
    };

    Ok(serde_json::to_value(response)?)
}
