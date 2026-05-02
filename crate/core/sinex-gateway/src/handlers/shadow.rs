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

use crate::service_container::ServiceContainer;
use async_nats::jetstream;
use serde_json::Value;
use sinex_node_sdk::runtime::stream::{
    ShadowConsumerSpec, create_shadow_consumer, delete_consumer, list_consumers,
};
use sinex_primitives::{Result, SinexError};
use tracing::{info, warn};

// Re-export shared types
pub use sinex_primitives::rpc::shadow::{
    ShadowConsumerInfo, ShadowCreateRequest, ShadowCreateResponse, ShadowDeleteRequest,
    ShadowDeleteResponse, ShadowListRequest, ShadowListResponse,
};

/// Create a shadow consumer for development/testing
///
/// This creates a durable consumer that receives copies of all events
/// matching the filter without affecting production consumers.
pub async fn handle_shadow_create(services: &ServiceContainer, params: Value) -> Result<Value> {
    let nats_client = services
        .nats_client()
        .ok_or_else(|| SinexError::configuration("NATS client is not available"))?;
    let env = services.environment();
    let request: ShadowCreateRequest = serde_json::from_value(params).map_err(|error| {
        SinexError::serialization("Invalid shadow.create parameters").with_std_error(&error)
    })?;

    // Validate consumer name format (must start with "dev-" for safety)
    if !request.consumer_name.starts_with("dev-") {
        return Err(SinexError::validation(
            "Shadow consumer names must start with 'dev-' prefix for safety",
        ));
    }

    let js = jetstream::new(nats_client.clone());
    let stream_name = env.nats_stream_name("EVENTS");

    // Require explicit subject filter - no default to prevent unintended access
    let Some(subject_filter) = request.subject_filter else {
        return Err(SinexError::validation(
            "subject_filter is required for shadow consumers (use 'events.>' explicitly if needed)",
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
    let mut spec = ShadowConsumerSpec::new(
        stream_name.clone(),
        request.consumer_name.clone(),
        subject_filter.clone(),
    );
    spec.from_sequence = request.from_sequence;
    spec.from_beginning = request.from_beginning;
    spec.create_timeout = services.config().nats_consumer_create_timeout();
    let info = create_shadow_consumer(&js, &spec).await.map_err(|e| {
        SinexError::nats("Failed to create shadow consumer")
            .with_context("consumer_name", &request.consumer_name)
            .with_std_error(&e)
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

    serde_json::to_value(response).map_err(|error| {
        SinexError::serialization("failed to serialize shadow.create response")
            .with_std_error(&error)
    })
}

/// List active shadow consumers
pub async fn handle_shadow_list(services: &ServiceContainer, params: Value) -> Result<Value> {
    let nats_client = services
        .nats_client()
        .ok_or_else(|| SinexError::configuration("NATS client is not available"))?;
    let env = services.environment();
    let request: ShadowListRequest = super::parse_default_on_null(params).map_err(|error| {
        SinexError::serialization("Invalid shadow.list parameters").with_std_error(&error)
    })?;

    let js = jetstream::new(nats_client.clone());
    let stream_name = env.nats_stream_name("EVENTS");

    let consumers = list_consumers(&js, &stream_name).await.map_err(|e| {
        SinexError::nats("Failed to list shadow consumers")
            .with_context("stream_name", &stream_name)
            .with_std_error(&e)
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

    serde_json::to_value(response).map_err(|error| {
        SinexError::serialization("failed to serialize shadow.list response").with_std_error(&error)
    })
}

/// Delete a shadow consumer
///
/// # Authorization
///
/// This is a dangerous operation that deletes a NATS consumer.
/// The auth context is logged for audit purposes.
pub async fn handle_shadow_delete(
    services: &ServiceContainer,
    params: Value,
    auth: &crate::rpc_server::RpcAuthContext,
) -> Result<Value> {
    let nats_client = services
        .nats_client()
        .ok_or_else(|| SinexError::configuration("NATS client is not available"))?;
    let env = services.environment();
    let request: ShadowDeleteRequest = serde_json::from_value(params).map_err(|error| {
        SinexError::serialization("Invalid shadow.delete parameters").with_std_error(&error)
    })?;

    // Safety check: only allow deleting dev- prefixed consumers
    if !request.consumer_name.starts_with("dev-") {
        return Err(SinexError::validation(
            "Can only delete shadow consumers with 'dev-' prefix",
        ));
    }

    let js = jetstream::new(nats_client.clone());
    let stream_name = env.nats_stream_name("EVENTS");
    delete_consumer(&js, &stream_name, &request.consumer_name)
        .await
        .map_err(|e| {
            SinexError::nats("Failed to delete shadow consumer")
                .with_context("stream_name", &stream_name)
                .with_context("consumer_name", &request.consumer_name)
                .with_std_error(&e)
        })?;

    info!(
        actor = %auth.actor_id(),
        consumer_name = %request.consumer_name,
        "Shadow consumer deleted"
    );

    let response = ShadowDeleteResponse {
        status: "success".to_string(),
        consumer_name: request.consumer_name,
    };

    serde_json::to_value(response).map_err(|error| {
        SinexError::serialization("failed to serialize shadow.delete response")
            .with_std_error(&error)
    })
}
