//! Shared RPC method handlers
//!
//! TODO: Refactor this monolithic module into domain-specific handlers (analytics, pkm, etc.)
//! as identified in the module survey (analysis/crates/sinex-gateway/_module-survey.md).

use crate::replay_control::ReplayControlClient;
use crate::replay_state_machine::{ReplayScope, ReplayState};
use crate::service_container::ServiceContainer;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use color_eyre::eyre::{eyre, Context, ContextCompat, Result};
use serde_json::{json, Value};
use sinex_primitives::{
    coordination::CoordinationKvClient, domain::Entity, temporal, temporal::OffsetDateTime, Event,
    Id, JsonValue, Ulid,
};
use sinex_services::{AnalyticsService, ContentService, PkmService, SearchQuery, SearchService};
use std::sync::OnceLock;

struct RpcParams<'a> {
    inner: &'a Value,
}

impl<'a> RpcParams<'a> {
    fn new(inner: &'a Value) -> Self {
        Self { inner }
    }

    fn require_str(&self, key: &str) -> Result<&'a str> {
        self.inner
            .get(key)
            .and_then(|v| v.as_str())
            .ok_or_else(|| eyre!("missing string parameter '{}'", key))
    }

    fn optional_str(&self, key: &str) -> Option<&'a str> {
        self.inner.get(key).and_then(|v| v.as_str())
    }

    fn optional_array(&self, key: &str) -> Option<&'a [Value]> {
        self.inner
            .get(key)
            .and_then(|v| v.as_array())
            .map(Vec::as_slice)
    }

    fn optional_object(&self, key: &str) -> Option<&'a serde_json::Map<String, Value>> {
        self.inner.get(key).and_then(|v| v.as_object())
    }

    fn optional_i64(&self, key: &str) -> Option<i64> {
        self.inner.get(key).and_then(|v| v.as_i64())
    }

    fn require_value(&self, key: &str) -> Result<&'a Value> {
        self.inner
            .get(key)
            .ok_or_else(|| eyre!("missing parameter '{}'", key))
    }

    fn require_ulid(&self, key: &str) -> Result<Ulid> {
        let value = self.require_str(key)?;
        value
            .parse::<Ulid>()
            .map_err(|e| eyre!("invalid ULID for '{}': {}", key, e))
    }
}

// Default values for created_by fields when not provided by caller
const DEFAULT_CREATOR_HOST: &str = "sinex-host";
const DEFAULT_CREATOR_GATEWAY: &str = "sinex-gateway";
const DEFAULT_CREATOR_CLI: &str = "sinex-cli";

// Default values for analytics parameters
const DEFAULT_ANALYTICS_DAYS_BACK: i64 = 7;
const DEFAULT_HEATMAP_BUCKET_SIZE_MINUTES: i64 = 60;
const DEFAULT_HEATMAP_LIMIT: i64 = 100;
const MAX_HEATMAP_BUCKET_SIZE_MINUTES: i64 = 1440;

// Default values for content/blob handling
const DEFAULT_BLOB_FILENAME: &str = "content.txt";
const DEFAULT_BLOB_CONTENT_TYPE: &str = "text/plain";
const DEFAULT_BLOB_SIZE_BYTES: usize = 5 * 1024 * 1024; // 5MB

pub(crate) fn validate_bucket_size_minutes(size: i64) -> Result<i32> {
    if size <= 0 {
        return Err(eyre!("bucket_size_minutes must be positive"));
    }
    if size > MAX_HEATMAP_BUCKET_SIZE_MINUTES {
        return Err(eyre!("bucket_size_minutes cannot exceed 1440 (24 hours)"));
    }
    Ok(size as i32)
}

pub(crate) fn decode_note_content(base64_content: &str) -> Result<String> {
    let decoded_bytes = BASE64_STANDARD
        .decode(base64_content)
        .wrap_err("Invalid base64 content")?;

    String::from_utf8(decoded_bytes).wrap_err("Decoded note content is not valid UTF-8")
}

pub(crate) fn validate_entity_name(name: &str) -> Result<()> {
    if name.trim().is_empty() {
        return Err(eyre!("Entity name cannot be empty"));
    }
    if name.len() > 255 {
        return Err(eyre!("Entity name cannot exceed 255 characters"));
    }
    if name.contains(';') || name.contains("--") || name.contains("/*") {
        return Err(eyre!("Entity name contains invalid characters"));
    }
    Ok(())
}

pub(crate) fn validate_entity_link_ids(from: &Id<Entity>, to: &Id<Entity>) -> Result<()> {
    if from == to {
        return Err(eyre!("Cannot link entity to itself"));
    }
    Ok(())
}

/// Decode base64 blob content with size validation
///
/// # Issue 144 (LOW): Base64 Expansion and Body Limits
///
/// Base64 encoding expands data by ~1.33x (4 chars per 3 bytes). When handling
/// blob uploads via RPC, ensure:
///
/// - SINEX_GATEWAY_MAX_BODY_BYTES >= SINEX_GATEWAY_MAX_BLOB_BYTES * 1.4
///   (1.4 accounts for base64 overhead plus JSON envelope)
///
/// Default configuration:
/// - Body limit: 2MB (SINEX_GATEWAY_MAX_BODY_BYTES)
/// - Blob limit: 5MB (SINEX_GATEWAY_MAX_BLOB_BYTES)
///
/// This mismatch is intentional: the body limit applies to the raw HTTP request,
/// while the blob limit applies to decoded content. For large blobs, clients should
/// increase SINEX_GATEWAY_MAX_BODY_BYTES proportionally.
pub(crate) fn decode_blob_content(content_b64: &str, limit: usize) -> Result<Vec<u8>> {
    let max_encoded = max_base64_length(limit);
    if content_b64.len() > max_encoded {
        return Err(eyre!(
            "Blob content exceeds maximum allowed size of {} bytes",
            limit
        ));
    }

    let content = BASE64_STANDARD
        .decode(content_b64)
        .wrap_err("Invalid base64 content")?;

    if content.len() > limit {
        return Err(eyre!(
            "Blob content exceeds maximum allowed size of {} bytes",
            limit
        ));
    }

    Ok(content)
}

// System handlers

pub async fn handle_system_health(services: &ServiceContainer, _params: Value) -> Result<Value> {
    // Issue 146: Enhanced health endpoint with component status
    let replay_control = services.replay_control_status();

    // Check database connectivity
    let db_healthy = sqlx::query("SELECT 1")
        .execute(services.pool())
        .await
        .is_ok();

    // Check NATS connectivity
    let nats_connected = services
        .nats_client()
        .map(|client| {
            matches!(
                client.connection_state(),
                async_nats::connection::State::Connected
            )
        })
        .unwrap_or(false);

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

// Analytics handlers

pub async fn handle_event_count_by_source(
    service: &AnalyticsService,
    params: Value,
) -> Result<Value> {
    use time::{Duration, OffsetDateTime};

    let params = RpcParams::new(&params);
    let days_back = params
        .optional_i64("days_back")
        .unwrap_or(DEFAULT_ANALYTICS_DAYS_BACK);

    let end_time = OffsetDateTime::now_utc();
    let start_time = end_time - Duration::days(days_back);

    let counts = service
        .get_event_count_by_source(Some(start_time), Some(end_time))
        .await?;
    Ok(json!(counts))
}

pub async fn handle_activity_heatmap(service: &AnalyticsService, params: Value) -> Result<Value> {
    use time::{Duration, OffsetDateTime};

    let params = RpcParams::new(&params);
    let bucket_size_minutes_raw = params
        .optional_i64("bucket_size_minutes")
        .unwrap_or(DEFAULT_HEATMAP_BUCKET_SIZE_MINUTES);
    let bucket_size_minutes = validate_bucket_size_minutes(bucket_size_minutes_raw)?;

    let limit = params
        .optional_i64("limit")
        .unwrap_or(DEFAULT_HEATMAP_LIMIT) as i32;

    let days_back = params
        .optional_i64("days_back")
        .unwrap_or(DEFAULT_ANALYTICS_DAYS_BACK);

    let end_time = OffsetDateTime::now_utc();
    let start_time = end_time - Duration::days(days_back);

    let heatmap = service
        .activity_heatmap(Some(start_time), Some(end_time), bucket_size_minutes, limit)
        .await?;
    Ok(json!(heatmap))
}

pub async fn handle_sources_statistics(service: &AnalyticsService, params: Value) -> Result<Value> {
    use time::{Duration, OffsetDateTime};

    let params = RpcParams::new(&params);
    let limit = params.optional_i64("limit").unwrap_or(100);

    let days_back = params
        .optional_i64("days_back")
        .unwrap_or(DEFAULT_ANALYTICS_DAYS_BACK);

    let end_time = OffsetDateTime::now_utc();
    let start_time = end_time - Duration::days(days_back);

    let stats = service
        .get_source_statistics(Some(start_time), Some(end_time), limit)
        .await?;
    Ok(json!(stats))
}

// PKM handlers

pub async fn handle_create_note(service: &PkmService, params: Value) -> Result<Value> {
    let params = RpcParams::new(&params);
    let event_id = Id::<Event<JsonValue>>::from_ulid(params.require_ulid("event_id")?);
    let content_b64 = params.require_str("content").wrap_err("Missing content")?;

    let content = decode_note_content(content_b64)?;

    let tags = params
        .optional_array("tags")
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();

    let created_by = params
        .optional_str("created_by")
        .unwrap_or(DEFAULT_CREATOR_HOST);

    let annotation_id = service
        .create_note(event_id, &content, tags, created_by, None)
        .await?;
    Ok(json!({ "annotation_id": annotation_id.to_string() }))
}

pub async fn handle_create_entities(service: &PkmService, params: Value) -> Result<Value> {
    let params = RpcParams::new(&params);
    let source_material_id = params.require_ulid("source_material_id")?;

    let entities = params
        .optional_array("entities")
        .map(|arr| {
            arr.iter()
                .map(|v| {
                    let name = v
                        .get("name")
                        .and_then(|value| value.as_str())
                        .wrap_err("Missing entity name")?;
                    validate_entity_name(name)?;
                    let entity_type = v
                        .get("type")
                        .and_then(|value| value.as_str())
                        .wrap_err("Missing entity type")?;
                    Ok((name.to_string(), entity_type.to_string()))
                })
                .collect::<Result<Vec<_>>>()
        })
        .transpose()?
        .unwrap_or_default();

    let created_by = params
        .optional_str("created_by")
        .unwrap_or(DEFAULT_CREATOR_GATEWAY);

    let entity_ids = service
        .create_entities_from_source_material(source_material_id, entities, created_by)
        .await?;
    Ok(json!({ "entity_ids": entity_ids.iter().map(|id| id.to_string()).collect::<Vec<_>>() }))
}

pub async fn handle_link_entities(service: &PkmService, params: Value) -> Result<Value> {
    let params = RpcParams::new(&params);
    let from_entity_id = Id::<Entity>::from_ulid(params.require_ulid("from_entity_id")?);
    let to_entity_id = Id::<Entity>::from_ulid(params.require_ulid("to_entity_id")?);
    validate_entity_link_ids(&from_entity_id, &to_entity_id)?;

    let relationship_type = params.require_str("relationship_type")?;
    let properties = params
        .optional_object("properties")
        .map(|obj| obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
        .unwrap_or_default();

    let relation_id = service
        .link_entities(
            from_entity_id,
            to_entity_id,
            relationship_type,
            properties,
            None,
        )
        .await?;

    Ok(json!({ "relation_id": relation_id.to_string() }))
}

// Search handlers

pub async fn handle_search_events(service: &SearchService, params: Value) -> Result<Value> {
    let query: SearchQuery =
        serde_json::from_value(params).wrap_err("Invalid search query parameters")?;

    let results = service.search_events(query).await?;
    Ok(json!(results))
}

// Content handlers

pub async fn handle_store_blob(service: &ContentService, params: Value) -> Result<Value> {
    let params = RpcParams::new(&params);
    let content_b64 = params.require_str("content").wrap_err("Missing content")?;

    let limit = blob_size_limit_bytes();
    let content = decode_blob_content(content_b64, limit)?;

    let filename = params
        .optional_str("filename")
        .unwrap_or(DEFAULT_BLOB_FILENAME);
    let content_type = params
        .optional_str("content_type")
        .unwrap_or(DEFAULT_BLOB_CONTENT_TYPE);
    let source = params
        .optional_str("source")
        .unwrap_or(DEFAULT_CREATOR_HOST);

    let annex_key = service
        .store_content(&content, filename, content_type, source)
        .await?;

    Ok(json!({ "annex_key": annex_key }))
}

// Replay handlers

pub async fn handle_replay_create_operation(
    client: &ReplayControlClient,
    params: Value,
) -> Result<Value> {
    let params = RpcParams::new(&params);
    let actor = params
        .optional_str("actor")
        .unwrap_or(DEFAULT_CREATOR_CLI)
        .to_string();

    let scope_val = params.require_value("scope")?.clone();
    let scope: ReplayScope =
        serde_json::from_value(scope_val).wrap_err("Invalid replay scope payload")?;

    let operation = client.plan(actor, scope).await?;
    Ok(json!({ "operation": operation }))
}

pub async fn handle_replay_preview_operation(
    client: &ReplayControlClient,
    params: Value,
) -> Result<Value> {
    let params = RpcParams::new(&params);
    let operation_id = params.require_ulid("operation_id")?;
    let (operation, preview) = client.preview(operation_id).await?;
    Ok(json!({ "operation": operation, "preview": preview }))
}

pub async fn handle_replay_approve_operation(
    client: &ReplayControlClient,
    params: Value,
) -> Result<Value> {
    let params = RpcParams::new(&params);
    let operation_id = params.require_ulid("operation_id")?;
    let approver = params
        .optional_str("approver")
        .unwrap_or(DEFAULT_CREATOR_CLI)
        .to_string();
    let operation = client.approve(operation_id, approver).await?;
    Ok(json!({ "operation": operation }))
}

pub async fn handle_replay_execute_operation(
    client: &ReplayControlClient,
    params: Value,
) -> Result<Value> {
    let params = RpcParams::new(&params);
    let operation_id = params.require_ulid("operation_id")?;
    let executor = params
        .optional_str("executor")
        .unwrap_or(DEFAULT_CREATOR_CLI)
        .to_string();
    let operation = client.execute(operation_id, executor).await?;
    Ok(json!({ "operation": operation }))
}

pub async fn handle_replay_cancel_operation(
    client: &ReplayControlClient,
    params: Value,
) -> Result<Value> {
    let params = RpcParams::new(&params);
    let operation_id = params.require_ulid("operation_id")?;
    let reason = params.optional_str("reason").map(|s| s.to_string());
    let operation = client.cancel(operation_id, reason).await?;
    Ok(json!({ "cancelled": true, "operation": operation }))
}

pub async fn handle_replay_operation_status(
    client: &ReplayControlClient,
    params: Value,
) -> Result<Value> {
    let params = RpcParams::new(&params);
    let operation_id = params.require_ulid("operation_id")?;
    let operation = client.status(operation_id).await?;
    Ok(json!({ "operation": operation }))
}

pub async fn handle_replay_list_operations(
    client: &ReplayControlClient,
    params: Value,
) -> Result<Value> {
    let params = RpcParams::new(&params);
    let state = params
        .optional_str("state")
        .map(parse_replay_state)
        .transpose()?;
    let operations = client.list(state).await?;
    Ok(json!({ "operations": operations }))
}

// Coordination handlers

use sinex_primitives::rpc::coordination::{
    InstanceHealthResponse, InstanceInfo, ListInstancesResponse,
};

/// Convert InstanceMetadata to InstanceInfo for RPC response
fn metadata_to_instance_info(
    meta: &sinex_primitives::coordination::InstanceMetadata,
    is_leader: bool,
) -> InstanceInfo {
    use sinex_primitives::domain::{HostName, InstanceId, NodeType};

    InstanceInfo {
        instance_id: InstanceId::new(&meta.instance_id),
        node_type: NodeType::Service, // InstanceMetadata doesn't have node_type, assume Service
        hostname: Some(HostName::new(&meta.hostname)),
        last_heartbeat: Some(
            OffsetDateTime::from_unix_timestamp(meta.last_heartbeat)
                .unwrap_or(OffsetDateTime::UNIX_EPOCH),
        ),
        is_leader,
    }
}

/// List all registered instances for a service
pub async fn handle_coordination_list_instances(
    kv_client: &CoordinationKvClient,
    _params: Value,
) -> Result<Value> {
    let instances = kv_client.list_instances().await?;
    let leader = kv_client.get_leader().await?.unwrap_or_default();

    let instance_infos: Vec<InstanceInfo> = instances
        .iter()
        .map(|meta| metadata_to_instance_info(meta, meta.instance_id == leader))
        .collect();

    let response = ListInstancesResponse {
        instances: instance_infos,
    };
    Ok(serde_json::to_value(response)?)
}

/// Get the current leader for a service
pub async fn handle_coordination_get_leader(
    kv_client: &CoordinationKvClient,
    _params: Value,
) -> Result<Value> {
    let leader = kv_client.get_leader().await?;
    Ok(json!({ "leader": leader }))
}

/// Check instance health based on heartbeat age
pub async fn handle_coordination_instance_health(
    kv_client: &CoordinationKvClient,
    params: Value,
) -> Result<Value> {
    let params = RpcParams::new(&params);
    let instance_id = params.require_str("instance_id")?;

    let metadata = kv_client.get_instance(instance_id).await?;
    let leader = kv_client.get_leader().await?.unwrap_or_default();

    match metadata {
        Some(meta) => {
            let now = temporal::now().unix_timestamp();
            let heartbeat_age_secs = now - meta.last_heartbeat;
            let is_healthy = heartbeat_age_secs < 60; // Consider healthy if heartbeat within 60s
            let is_leader = meta.instance_id == leader;

            let response = InstanceHealthResponse {
                instance: metadata_to_instance_info(&meta, is_leader),
                healthy: is_healthy,
                last_error: None,
            };
            Ok(serde_json::to_value(response)?)
        }
        None => Err(eyre!("Instance not found: {}", instance_id)),
    }
}

pub(crate) fn parse_replay_state(value: &str) -> Result<ReplayState> {
    match value.to_lowercase().as_str() {
        "planning" => Ok(ReplayState::Planning),
        "previewed" => Ok(ReplayState::Previewed),
        "approved" => Ok(ReplayState::Approved),
        "executing" => Ok(ReplayState::Executing),
        "committing" => Ok(ReplayState::Committing),
        "completed" => Ok(ReplayState::Completed),
        "failed" => Ok(ReplayState::Failed),
        "cancelled" => Ok(ReplayState::Cancelled),
        other => Err(eyre!("Unknown replay state '{other}'")),
    }
}

pub async fn handle_retrieve_blob(service: &ContentService, params: Value) -> Result<Value> {
    let params = RpcParams::new(&params);
    let annex_key = params
        .require_str("annex_key")
        .wrap_err("Missing annex_key")?;

    let content = service.retrieve_content(annex_key).await?;
    let metadata = service.get_content_metadata(annex_key).await?;

    Ok(blob_response_payload(&content, &metadata))
}

fn blob_response_payload(content: &[u8], metadata: &sinex_node_sdk::annex::BlobMetadata) -> Value {
    json!({
        "content_base64": BASE64_STANDARD.encode(content),
        "mime_type": metadata.mime_type.clone(),
        "size_bytes": metadata.size_bytes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinex_db::models::blob::Blob;
    use xtask::sandbox::sinex_test;

    #[sinex_test]
    fn blob_response_payload_encodes_base64() -> TestResult<()> {
        let blob = Blob::builder()
            .annex_backend("SHA256".into())
            .content_hash("deadbeef".into())
            .original_filename("blob.bin".into())
            .size_bytes(2)
            .mime_type("application/octet-stream".into())
            .build();

        let json = blob_response_payload(b"hi", &blob);
        assert_eq!(json["content_base64"], "aGk=");
        assert_eq!(json["mime_type"], "application/octet-stream");
        assert_eq!(json["size_bytes"], 2);
        Ok(())
    }

    #[sinex_test]
    fn parse_replay_state_accepts_known_variants() -> TestResult<()> {
        let states = [
            ("planning", ReplayState::Planning),
            ("PREVIEWED", ReplayState::Previewed),
            ("Approved", ReplayState::Approved),
        ];
        for (input, expected) in states {
            assert_eq!(parse_replay_state(input).unwrap(), expected);
        }
        assert!(parse_replay_state("unknown").is_err());
        Ok(())
    }

    #[sinex_test]
    fn rpc_params_ulid_parses_input() -> TestResult<()> {
        let id = Ulid::new();
        let params = json!({"operation_id": id.to_string()});
        let rpc_params = RpcParams::new(&params);
        assert_eq!(rpc_params.require_ulid("operation_id").unwrap(), id);

        let invalid = json!({"operation_id": "not-ulid"});
        let rpc_params = RpcParams::new(&invalid);
        assert!(rpc_params.require_ulid("operation_id").is_err());
        Ok(())
    }
}
fn blob_size_limit_bytes() -> usize {
    static LIMIT: OnceLock<usize> = OnceLock::new();
    *LIMIT.get_or_init(|| {
        std::env::var("SINEX_GATEWAY_MAX_BLOB_BYTES")
            .ok()
            .and_then(|raw| raw.parse::<usize>().ok())
            .unwrap_or(DEFAULT_BLOB_SIZE_BYTES)
    })
}

fn max_base64_length(limit_bytes: usize) -> usize {
    // Each 3 bytes become 4 base64 chars. Round up to ensure we account for padding.
    ((limit_bytes + 2) / 3) * 4
}
