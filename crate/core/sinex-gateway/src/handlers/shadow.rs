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
use color_eyre::eyre::{eyre, Context, Result};
use serde_json::Value;
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

    let stream = js
        .get_stream(&stream_name)
        .await
        .map_err(|e| eyre!("Failed to get events stream: {}", e))?;

    // Require explicit subject filter - no default to prevent unintended access
    let subject_filter = match request.subject_filter {
        Some(filter) => filter,
        None => {
            return Err(eyre!(
                "subject_filter is required for shadow consumers (use 'events.>' explicitly if needed)"
            ));
        }
    };

    // Warn on overly broad patterns
    if subject_filter.ends_with(".>") || subject_filter == "*" {
        warn!(
            consumer_name = %request.consumer_name,
            subject_filter = %subject_filter,
            "Shadow consumer created with broad subject filter"
        );
    }

    // Determine deliver policy
    let deliver_policy = if let Some(seq) = request.from_sequence {
        jetstream::consumer::DeliverPolicy::ByStartSequence {
            start_sequence: seq,
        }
    } else if request.from_beginning {
        jetstream::consumer::DeliverPolicy::All
    } else {
        jetstream::consumer::DeliverPolicy::New
    };

    // Create durable consumer with explicit ack for proper tracking
    // Issue 126: Add timeout to NATS consumer creation
    let timeout = env_var_duration_secs("SINEX_NATS_CONSUMER_CREATE_TIMEOUT_SECS", 10);
    let mut consumer = tokio::time::timeout(
        timeout,
        stream.create_consumer(jetstream::consumer::pull::Config {
            name: Some(request.consumer_name.clone()),
            durable_name: Some(request.consumer_name.clone()),
            filter_subject: subject_filter.clone(),
            ack_policy: jetstream::consumer::AckPolicy::Explicit,
            deliver_policy,
            // Allow reasonable redelivery for development
            max_deliver: 3,
            // Ack wait timeout
            ack_wait: std::time::Duration::from_secs(30),
            ..Default::default()
        }),
    )
    .await
    .map_err(|_| eyre!("Consumer creation timed out after {:?}", timeout))?
    .map_err(|e| eyre!("Failed to create shadow consumer: {}", e))?;

    let info = consumer
        .info()
        .await
        .map_err(|e| eyre!("Failed to get consumer info: {}", e))?;

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
    use futures::StreamExt;

    let request: ShadowListRequest = serde_json::from_value(params).unwrap_or_default();

    let js = jetstream::new(nats_client.clone());
    let stream_name = env.nats_stream_name("EVENTS");

    let stream = js
        .get_stream(&stream_name)
        .await
        .map_err(|e| eyre!("Failed to get events stream: {}", e))?;

    let mut consumers = stream.consumers();
    let mut shadow_consumers = Vec::new();

    while let Some(result) = consumers.next().await {
        match result {
            Ok(info) => {
                // The consumers() iterator yields Info structs directly
                // Filter to only shadow consumers (dev- prefix)
                if let Some(ref name) = info.config.name {
                    if name.starts_with("dev-") {
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
            }
            Err(e) => {
                tracing::warn!("Error listing consumer: {}", e);
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

    let stream = js
        .get_stream(&stream_name)
        .await
        .map_err(|e| eyre!("Failed to get events stream: {}", e))?;

    stream
        .delete_consumer(&request.consumer_name)
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use sinex_primitives::environment;
    use sinex_primitives::temporal;
    use xtask::sandbox::{sinex_test, EphemeralNats};

    #[sinex_test]
    async fn shadow_create_requires_dev_prefix() -> TestResult<()> {
        let nats = EphemeralNats::start().await?;
        let client = nats.connect().await?;
        let env = environment();

        // Should fail without dev- prefix
        let err = handle_shadow_create(
            &client,
            &env,
            json!({
                "consumer_name": "production-consumer",
                "from_beginning": true
            }),
        )
        .await
        .unwrap_err();

        assert!(err.to_string().contains("dev-"));
        Ok(())
    }

    #[sinex_test]
    async fn shadow_create_and_list() -> TestResult<()> {
        let nats = EphemeralNats::start().await?;
        let client = nats.connect().await?;
        let env = environment();
        let js = jetstream::new(client.clone());

        // Create the events stream first
        let stream_name = env.nats_stream_name("EVENTS");
        js.get_or_create_stream(jetstream::stream::Config {
            name: stream_name.clone(),
            subjects: vec![env.nats_subject("events.>")],
            retention: jetstream::stream::RetentionPolicy::Limits,
            max_messages: 10000,
            storage: jetstream::stream::StorageType::File,
            ..Default::default()
        })
        .await?;

        // Create a shadow consumer
        let result = handle_shadow_create(
            &client,
            &env,
            json!({
                "consumer_name": "dev-test-123",
                "subject_filter": env.nats_subject("events.>"),
                "from_beginning": true
            }),
        )
        .await?;

        let create_response: ShadowCreateResponse = serde_json::from_value(result)?;
        assert_eq!(create_response.consumer.consumer_name, "dev-test-123");
        assert_eq!(create_response.consumer.stream_name, stream_name);

        // List should show the consumer
        let list_result = handle_shadow_list(&client, &env, json!({})).await?;
        let list_response: ShadowListResponse = serde_json::from_value(list_result)?;
        assert_eq!(list_response.consumers.len(), 1);

        // Delete the consumer
        let test_auth = crate::rpc_server::RpcAuthContext {
            token_prefix: "test****".to_string(),
            authenticated_at: temporal::now(),
            role: crate::auth::Role::Admin,
        };
        let delete_result = handle_shadow_delete(
            &client,
            &env,
            json!({
                "consumer_name": "dev-test-123"
            }),
            &test_auth,
        )
        .await?;

        let delete_response: ShadowDeleteResponse = serde_json::from_value(delete_result)?;
        assert_eq!(delete_response.status, "success");

        // List should now be empty
        let list_result = handle_shadow_list(&client, &env, json!({})).await?;
        let list_response: ShadowListResponse = serde_json::from_value(list_result)?;
        assert!(list_response.consumers.is_empty());

        Ok(())
    }

    #[sinex_test]
    async fn shadow_create_requires_subject_filter() -> TestResult<()> {
        let nats = EphemeralNats::start().await?;
        let client = nats.connect().await?;
        let env = environment();
        let js = jetstream::new(client.clone());

        // Create the events stream first
        let stream_name = env.nats_stream_name("EVENTS");
        js.get_or_create_stream(jetstream::stream::Config {
            name: stream_name.clone(),
            subjects: vec![env.nats_subject("events.>")],
            retention: jetstream::stream::RetentionPolicy::Limits,
            max_messages: 10000,
            storage: jetstream::stream::StorageType::File,
            ..Default::default()
        })
        .await?;

        // Should fail without explicit subject_filter
        let err = handle_shadow_create(
            &client,
            &env,
            json!({
                "consumer_name": "dev-test-456",
                "from_beginning": true
            }),
        )
        .await
        .unwrap_err();

        assert!(err.to_string().contains("subject_filter is required"));
        Ok(())
    }

    #[sinex_test]
    async fn shadow_delete_requires_dev_prefix() -> TestResult<()> {
        let nats = EphemeralNats::start().await?;
        let client = nats.connect().await?;
        let env = environment();

        let test_auth = crate::rpc_server::RpcAuthContext {
            token_prefix: "test****".to_string(),
            authenticated_at: temporal::now(),
            role: crate::auth::Role::Admin,
        };

        // Should fail without dev- prefix
        let err = handle_shadow_delete(
            &client,
            &env,
            json!({
                "consumer_name": "production-consumer"
            }),
            &test_auth,
        )
        .await
        .unwrap_err();

        assert!(err.to_string().contains("dev-"));
        Ok(())
    }
}
