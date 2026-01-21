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
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sinex_core::environment::SinexEnvironment;
use std::time::Duration;
use tracing::{info, warn};

fn env_var_duration_secs(name: &str, default: u64) -> Duration {
    Duration::from_secs(
        std::env::var(name)
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(default),
    )
}

/// Response for shadow consumer creation
#[derive(Debug, Serialize)]
pub struct ShadowConsumerInfo {
    /// The unique consumer name
    pub consumer_name: String,
    /// The stream this consumer is attached to
    pub stream_name: String,
    /// The subject filter pattern
    pub subject_filter: String,
    /// Number of pending messages
    pub num_pending: u64,
    /// First available sequence number
    pub first_sequence: u64,
}

/// Parameters for creating a shadow consumer
#[derive(Debug, Deserialize)]
pub struct ShadowCreateParams {
    /// Unique identifier for this shadow consumer (e.g., "dev-user-20250117")
    pub consumer_name: String,
    /// Subject filter pattern (required - must be explicitly specified for security)
    #[serde(default)]
    pub subject_filter: Option<String>,
    /// Start from the beginning of the stream (required, must be explicitly specified)
    pub from_beginning: bool,
    /// Start from a specific sequence number
    #[serde(default)]
    pub from_sequence: Option<u64>,
}

/// Parameters for listing shadow consumers
#[derive(Debug, Deserialize)]
pub struct ShadowListParams {
    /// Optional prefix filter for consumer names
    #[serde(default)]
    pub prefix: Option<String>,
}

/// Parameters for deleting a shadow consumer
#[derive(Debug, Deserialize)]
pub struct ShadowDeleteParams {
    /// The consumer name to delete
    pub consumer_name: String,
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
    let create_params: ShadowCreateParams =
        serde_json::from_value(params).wrap_err("Invalid shadow.create parameters")?;

    // Validate consumer name format (must start with "dev-" for safety)
    if !create_params.consumer_name.starts_with("dev-") {
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
    let subject_filter = match create_params.subject_filter {
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
            consumer_name = %create_params.consumer_name,
            subject_filter = %subject_filter,
            "Shadow consumer created with broad subject filter"
        );
    }

    // Determine deliver policy
    let deliver_policy = if let Some(seq) = create_params.from_sequence {
        jetstream::consumer::DeliverPolicy::ByStartSequence {
            start_sequence: seq,
        }
    } else if create_params.from_beginning {
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
            name: Some(create_params.consumer_name.clone()),
            durable_name: Some(create_params.consumer_name.clone()),
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
        consumer_name = %create_params.consumer_name,
        stream = %stream_name,
        subject_filter = %subject_filter,
        num_pending = info.num_pending,
        "Created shadow consumer for The Tether"
    );

    let response = ShadowConsumerInfo {
        consumer_name: create_params.consumer_name,
        stream_name,
        subject_filter,
        num_pending: info.num_pending,
        first_sequence: info.delivered.stream_sequence,
    };

    Ok(json!(response))
}

/// List active shadow consumers
pub async fn handle_shadow_list(
    nats_client: &async_nats::Client,
    env: &SinexEnvironment,
    params: Value,
) -> Result<Value> {
    use futures::StreamExt;

    let list_params: ShadowListParams =
        serde_json::from_value(params).wrap_err("Invalid shadow.list parameters")?;

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
                        let include = match &list_params.prefix {
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

    Ok(json!({
        "consumers": shadow_consumers,
        "count": shadow_consumers.len(),
    }))
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
    let delete_params: ShadowDeleteParams =
        serde_json::from_value(params).wrap_err("Invalid shadow.delete parameters")?;

    // Safety check: only allow deleting dev- prefixed consumers
    if !delete_params.consumer_name.starts_with("dev-") {
        return Err(eyre!("Can only delete shadow consumers with 'dev-' prefix"));
    }

    let js = jetstream::new(nats_client.clone());
    let stream_name = env.nats_stream_name("EVENTS");

    let stream = js
        .get_stream(&stream_name)
        .await
        .map_err(|e| eyre!("Failed to get events stream: {}", e))?;

    stream
        .delete_consumer(&delete_params.consumer_name)
        .await
        .map_err(|e| {
            eyre!(
                "Failed to delete consumer '{}': {}",
                delete_params.consumer_name,
                e
            )
        })?;

    info!(
        token_prefix = %auth.token_prefix,
        consumer_name = %delete_params.consumer_name,
        "Shadow consumer deleted"
    );

    Ok(json!({
        "deleted": delete_params.consumer_name,
        "status": "success",
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinex_core::environment;
    use sinex_test_utils::{sinex_test, EphemeralNats};

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
                "consumer_name": "production-consumer"
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

        assert_eq!(result["consumer_name"], "dev-test-123");
        assert_eq!(result["stream_name"], stream_name);

        // List should show the consumer
        let list_result = handle_shadow_list(&client, &env, json!({})).await?;
        assert_eq!(list_result["count"], 1);

        // Delete the consumer
        let test_auth = crate::rpc_server::RpcAuthContext {
            token_prefix: "test****".to_string(),
            authenticated_at: chrono::Utc::now(),
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

        assert_eq!(delete_result["status"], "success");

        // List should now be empty
        let list_result = handle_shadow_list(&client, &env, json!({})).await?;
        assert_eq!(list_result["count"], 0);

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
            authenticated_at: chrono::Utc::now(),
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
