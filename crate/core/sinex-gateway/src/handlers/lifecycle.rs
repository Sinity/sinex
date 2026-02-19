//! Data lifecycle RPC handlers
//!
//! Implements the three-tier data lifecycle: Live ↔ Archive → Tombstone

use serde_json::Value;
use sinex_db::DbPoolExt;
use sinex_primitives::domain::EventSource;
use sinex_primitives::rpc::lifecycle::{
    LifecycleArchiveRequest, LifecycleArchiveResponse, LifecycleRestoreRequest,
    LifecycleRestoreResponse, LifecycleStatusRequest, LifecycleStatusResponse, TierStatus,
};
use sinex_primitives::{SinexError, Timestamp, Ulid};
use sqlx::PgPool;
use std::str::FromStr;
use tracing::info;

type Result<T> = std::result::Result<T, SinexError>;

/// Handle lifecycle.status - get status of all lifecycle tiers
pub async fn handle_lifecycle_status(pool: &PgPool, params: Value) -> Result<Value> {
    let _request: LifecycleStatusRequest = serde_json::from_value(params).unwrap_or_default();

    let repo = pool.events();
    let tiers = repo.lifecycle_tier_status().await.map_err(|e| {
        SinexError::database("Failed to get lifecycle tier status").with_source(e.to_string())
    })?;

    let total_events: i64 = tiers.iter().map(|t| t.event_count).sum();

    let response = LifecycleStatusResponse {
        tiers: tiers
            .into_iter()
            .map(|t| TierStatus {
                tier: t.tier,
                event_count: t.event_count,
                oldest_ts: t.oldest_ts.map(|ts| ts.format_rfc3339()),
                newest_ts: t.newest_ts.map(|ts| ts.format_rfc3339()),
                distinct_sources: t.distinct_sources,
            })
            .collect(),
        total_events,
    };

    Ok(serde_json::to_value(response)?)
}

/// Handle lifecycle.archive - move live events to archive
///
/// Archive is triggered by DELETE on core.events with `sinex.operation_id` set.
/// The trigger `fn_archive_before_delete` copies rows to `audit.archived_events`.
///
/// This handler:
/// 1. Parses filter criteria (source, before, `event_ids`)
/// 2. Creates a cascade session to find all dependent events
/// 3. Expands cascade to include children (events with `source_event_ids` pointing to these)
/// 4. If `dry_run`: returns preview without archiving
/// 5. If !`dry_run`: executes DELETE with session variables set, trigger archives
pub async fn handle_lifecycle_archive(
    pool: &PgPool,
    params: Value,
    auth: &crate::rpc_server::RpcAuthContext,
) -> Result<Value> {
    let request: LifecycleArchiveRequest = serde_json::from_value(params)?;

    info!(
        token_prefix = %auth.token_prefix,
        source = ?request.source,
        before = ?request.before,
        dry_run = request.dry_run,
        "Lifecycle archive operation initiated"
    );

    let repo = pool.events();

    // Parse filters
    let before_ts = if let Some(before_str) = &request.before {
        parse_duration_to_timestamp(before_str)?
    } else {
        None
    };

    let source = request.source.as_ref().map(|s| EventSource::new(s.clone()));
    if let Some(ref src) = source {
        src.validate()
            .map_err(|reason| SinexError::validation(format!("Invalid source: {reason}")))?;
    }

    // Get live event IDs matching filters
    let event_ids = if let Some(ids) = &request.event_ids {
        ids.iter()
            .map(|s| {
                Ulid::from_str(s).map_err(|_| SinexError::validation(format!("Invalid ULID: {s}")))
            })
            .collect::<Result<Vec<_>>>()?
    } else {
        repo.get_live_event_ids(source.as_ref(), before_ts, request.limit)
            .await
            .map_err(|e| {
                SinexError::database("Failed to get live event IDs").with_source(e.to_string())
            })?
    };

    if event_ids.is_empty() {
        return Err(SinexError::validation(
            "No live events match the filter criteria",
        ));
    }

    // Create cascade session and analyze dependencies
    let session_id = Ulid::new().to_string();
    let table_name = repo
        .prepare_cascade_session(&session_id, true)
        .await
        .map_err(|e| {
            SinexError::database("Failed to prepare cascade session").with_source(e.to_string())
        })?;

    // Populate with live event roots
    repo.populate_cascade_roots_from_live(&table_name, &event_ids)
        .await
        .map_err(|e| {
            SinexError::database("Failed to populate cascade roots").with_source(e.to_string())
        })?;

    // Expand cascade to find all dependent events
    let cascade_depth = repo
        .expand_cascade_from_live(&table_name, 100)
        .await
        .map_err(|e| SinexError::database("Failed to expand cascade").with_source(e.to_string()))?;

    // Get all cascade IDs
    let cascade_ids = repo.get_cascade_ids(&table_name).await.map_err(|e| {
        SinexError::database("Failed to get cascade IDs").with_source(e.to_string())
    })?;
    let cascade_total = cascade_ids.len();

    // Cleanup cascade session
    repo.cleanup_cascade_session(&table_name)
        .await
        .map_err(|e| {
            SinexError::database("Failed to cleanup cascade session").with_source(e.to_string())
        })?;

    let operation_id = Ulid::new();

    if request.dry_run {
        let response = LifecycleArchiveResponse {
            archived_count: 0,
            cascade_depth,
            cascade_total,
            operation_id: operation_id.to_string(),
            dry_run: true,
        };
        return Ok(serde_json::to_value(response)?);
    }

    // Execute archive operation
    let reason = request
        .reason
        .as_deref()
        .unwrap_or("Lifecycle archive operation");
    let archived_count = repo
        .execute_cascade_archive(
            &cascade_ids,
            reason,
            &operation_id.to_string(),
            &auth.token_prefix,
        )
        .await
        .map_err(|e| {
            SinexError::database("Failed to execute cascade archive").with_source(e.to_string())
        })?;

    info!(
        operation_id = %operation_id,
        archived_count = archived_count,
        cascade_total = cascade_total,
        "Archive operation completed"
    );

    let response = LifecycleArchiveResponse {
        archived_count,
        cascade_depth,
        cascade_total,
        operation_id: operation_id.to_string(),
        dry_run: false,
    };

    Ok(serde_json::to_value(response)?)
}

/// Handle lifecycle.restore - move archived events back to live
pub async fn handle_lifecycle_restore(
    pool: &PgPool,
    params: Value,
    auth: &crate::rpc_server::RpcAuthContext,
) -> Result<Value> {
    let request: LifecycleRestoreRequest = serde_json::from_value(params)?;

    if request.event_ids.is_empty() {
        return Err(SinexError::validation("No event IDs provided for restore"));
    }

    info!(
        token_prefix = %auth.token_prefix,
        event_count = request.event_ids.len(),
        dry_run = request.dry_run,
        "Lifecycle restore operation initiated"
    );

    let repo = pool.events();

    // Parse event IDs
    let event_ids: Vec<Ulid> = request
        .event_ids
        .iter()
        .map(|s| {
            Ulid::from_str(s).map_err(|_| SinexError::validation(format!("Invalid ULID: {s}")))
        })
        .collect::<Result<Vec<_>>>()?;

    // Analyze cascade from archived events
    let session_id = Ulid::new().to_string();
    let table_name = repo
        .prepare_cascade_session(&session_id, true)
        .await
        .map_err(|e| {
            SinexError::database("Failed to prepare cascade session").with_source(e.to_string())
        })?;

    // Populate with archived event roots
    repo.populate_cascade_roots_from_archive(&table_name, &event_ids)
        .await
        .map_err(|e| {
            SinexError::database("Failed to populate cascade roots").with_source(e.to_string())
        })?;

    // Expand cascade
    let max_depth = repo
        .expand_cascade_from_archive(&table_name, 100)
        .await
        .map_err(|e| SinexError::database("Failed to expand cascade").with_source(e.to_string()))?;

    // Get all cascade IDs
    let cascade_ids = repo.get_cascade_ids(&table_name).await.map_err(|e| {
        SinexError::database("Failed to get cascade IDs").with_source(e.to_string())
    })?;
    let cascade_total = cascade_ids.len();

    // Cleanup cascade table
    repo.cleanup_cascade_session(&table_name)
        .await
        .map_err(|e| {
            SinexError::database("Failed to cleanup cascade session").with_source(e.to_string())
        })?;

    let operation_id = Ulid::new();

    if request.dry_run {
        let response = LifecycleRestoreResponse {
            restored_count: 0,
            cascade_depth: max_depth,
            cascade_total,
            operation_id: operation_id.to_string(),
            dry_run: true,
        };
        return Ok(serde_json::to_value(response)?);
    }

    // Execute restore
    let restored_count = repo
        .execute_cascade_restore(&cascade_ids, &operation_id.to_string())
        .await
        .map_err(|e| {
            SinexError::database("Failed to execute cascade restore").with_source(e.to_string())
        })?;

    info!(
        operation_id = %operation_id,
        restored_count = restored_count,
        cascade_total = cascade_total,
        "Restore operation completed"
    );

    let response = LifecycleRestoreResponse {
        restored_count,
        cascade_depth: max_depth,
        cascade_total,
        operation_id: operation_id.to_string(),
        dry_run: false,
    };

    Ok(serde_json::to_value(response)?)
}

/// Parse a duration string (e.g., "30d", "90d") to a timestamp in the past
fn parse_duration_to_timestamp(duration_str: &str) -> Result<Option<Timestamp>> {
    let duration = humantime::parse_duration(duration_str)
        .map_err(|e| SinexError::validation(format!("Invalid duration '{duration_str}': {e}")))?;

    let ts = Timestamp::now() - time::Duration::seconds(duration.as_secs() as i64);
    Ok(Some(ts))
}

// ─────────────────────────────────────────────────────────────
// Two-Step Tombstone Operations (SEC-003)
// ─────────────────────────────────────────────────────────────

use sinex_primitives::rpc::lifecycle::{
    TombstoneApproveRequest, TombstoneApproveResponse, TombstoneCancelRequest,
    TombstoneCancelResponse, TombstoneCascadeAnalysis, TombstoneCreateRequest,
    TombstoneCreateResponse, TombstoneListRequest, TombstoneListResponse, TombstoneOperation,
    TombstoneOperationState, TombstonePreviewRequest, TombstonePreviewResponse,
    TombstoneStatusRequest, TombstoneStatusResponse,
};
use std::collections::HashMap;

/// Default TTL for tombstone operations (1 hour)
const TOMBSTONE_OPERATION_TTL_SECS: i64 = 3600;

/// Convert `TombstoneOperationState` to `operations_log` `result_status`
fn state_to_result_status(state: TombstoneOperationState) -> &'static str {
    match state {
        TombstoneOperationState::Pending => "running",
        TombstoneOperationState::Previewed => "running",
        TombstoneOperationState::Approved => "running",
        TombstoneOperationState::Executing => "running",
        TombstoneOperationState::Completed => "success",
        TombstoneOperationState::Cancelled => "cancelled",
        TombstoneOperationState::Failed => "failure",
        TombstoneOperationState::Expired => "cancelled",
    }
}

/// Convert `operations_log` `result_status` to `TombstoneOperationState`
/// Note: Needs scope inspection for finer state resolution
fn result_status_to_state(status: &str, scope: &serde_json::Value) -> TombstoneOperationState {
    // First check if the full state is stored in scope
    if let Some(state_str) = scope.get("state").and_then(|v| v.as_str()) {
        match state_str {
            "pending" => return TombstoneOperationState::Pending,
            "previewed" => return TombstoneOperationState::Previewed,
            "approved" => return TombstoneOperationState::Approved,
            "executing" => return TombstoneOperationState::Executing,
            "completed" => return TombstoneOperationState::Completed,
            "cancelled" => return TombstoneOperationState::Cancelled,
            "failed" => return TombstoneOperationState::Failed,
            "expired" => return TombstoneOperationState::Expired,
            _ => {}
        }
    }

    // Fallback to result_status
    match status {
        "running" => TombstoneOperationState::Previewed,
        "success" => TombstoneOperationState::Completed,
        "cancelled" => TombstoneOperationState::Cancelled,
        "failure" => TombstoneOperationState::Failed,
        _ => TombstoneOperationState::Previewed,
    }
}

/// Convert `OperationRecord` to `TombstoneOperation`
fn operation_record_to_tombstone(
    record: &sinex_db::repositories::state::OperationRecord,
) -> Option<TombstoneOperation> {
    let scope = record.scope.as_ref()?;

    // Deserialize the full operation from scope if available
    if let Ok(op) = serde_json::from_value::<TombstoneOperation>(scope.clone()) {
        return Some(op);
    }

    // Fallback: construct from partial fields
    let state = result_status_to_state(&record.result_status, scope);

    Some(TombstoneOperation {
        operation_id: record.id.to_string(),
        state,
        before: scope
            .get("before")
            .and_then(|v| v.as_str())
            .map(String::from),
        source: scope
            .get("source")
            .and_then(|v| v.as_str())
            .map(String::from),
        event_ids: scope.get("event_ids").and_then(|v| {
            v.as_array().map(|arr| {
                arr.iter()
                    .filter_map(|e| e.as_str().map(String::from))
                    .collect()
            })
        }),
        reason: scope
            .get("reason")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        cascade_analysis: scope
            .get("cascade_analysis")
            .and_then(|v| serde_json::from_value(v.clone()).ok()),
        created_by: record.operator.clone(),
        created_at: scope
            .get("created_at")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        expires_at: scope
            .get("expires_at")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        approved_by: scope
            .get("approved_by")
            .and_then(|v| v.as_str())
            .map(String::from),
        approved_at: scope
            .get("approved_at")
            .and_then(|v| v.as_str())
            .map(String::from),
        started_at: scope
            .get("started_at")
            .and_then(|v| v.as_str())
            .map(String::from),
        finished_at: scope
            .get("finished_at")
            .and_then(|v| v.as_str())
            .map(String::from),
        tombstoned_count: scope
            .get("tombstoned_count")
            .and_then(serde_json::Value::as_u64),
        error_details: scope
            .get("error_details")
            .and_then(|v| v.as_str())
            .map(String::from),
    })
}

/// Handle lifecycle.tombstone.create
///
/// Creates a new tombstone operation with cascade preview.
/// The operation must be approved within 1 hour or it expires.
pub async fn handle_tombstone_create(
    pool: &PgPool,
    params: Value,
    auth: &crate::rpc_server::RpcAuthContext,
) -> Result<Value> {
    let request: TombstoneCreateRequest = serde_json::from_value(params)?;

    let operation_id = Ulid::new().to_string();
    let now = Timestamp::now();
    let expires_at = now + time::Duration::seconds(TOMBSTONE_OPERATION_TTL_SECS);

    info!(
        operation_id = %operation_id,
        token_prefix = %auth.token_prefix,
        source = ?request.source,
        before = ?request.before,
        "Creating tombstone operation"
    );

    // Compute cascade analysis
    let repo = pool.events();
    let before_ts = if let Some(before_str) = &request.before {
        parse_duration_to_timestamp(before_str)?
    } else {
        None
    };

    let source = request.source.as_ref().map(|s| EventSource::new(s.clone()));
    if let Some(ref src) = source {
        src.validate()
            .map_err(|reason| SinexError::validation(format!("Invalid source: {reason}")))?;
    }

    // Get archived event IDs matching filters
    let event_ids = if let Some(ids) = &request.event_ids {
        ids.iter()
            .map(|s| {
                Ulid::from_str(s).map_err(|_| SinexError::validation(format!("Invalid ULID: {s}")))
            })
            .collect::<Result<Vec<_>>>()?
    } else {
        repo.get_archived_event_ids(source.as_ref(), before_ts, request.limit)
            .await
            .map_err(|e| {
                SinexError::database("Failed to get archived event IDs").with_source(e.to_string())
            })?
    };

    if event_ids.is_empty() {
        return Err(SinexError::validation(
            "No archived events match the filter criteria",
        ));
    }

    // Analyze cascade
    let session_id = Ulid::new().to_string();
    let table_name = repo
        .prepare_cascade_session(&session_id, true)
        .await
        .map_err(|e| {
            SinexError::database("Failed to prepare cascade session").with_source(e.to_string())
        })?;

    repo.populate_cascade_roots_from_archive(&table_name, &event_ids)
        .await
        .map_err(|e| {
            SinexError::database("Failed to populate cascade roots").with_source(e.to_string())
        })?;

    let cascade_depth = repo
        .expand_cascade_from_archive(&table_name, 100)
        .await
        .map_err(|e| SinexError::database("Failed to expand cascade").with_source(e.to_string()))?;

    let cascade_ids = repo.get_cascade_ids(&table_name).await.map_err(|e| {
        SinexError::database("Failed to get cascade IDs").with_source(e.to_string())
    })?;

    repo.cleanup_cascade_session(&table_name)
        .await
        .map_err(|e| {
            SinexError::database("Failed to cleanup cascade session").with_source(e.to_string())
        })?;

    // Build cascade analysis
    let cascade_analysis = TombstoneCascadeAnalysis {
        root_event_count: event_ids.len(),
        cascade_total: cascade_ids.len(),
        cascade_depth,
        by_source: HashMap::new(), // Could be populated with source breakdown
        sample_ids: event_ids
            .iter()
            .take(10)
            .map(std::string::ToString::to_string)
            .collect(),
    };

    let operation = TombstoneOperation {
        operation_id: operation_id.clone(),
        state: TombstoneOperationState::Previewed, // Already previewed on create
        before: request.before.clone(),
        source: request.source.clone(),
        event_ids: request.event_ids.clone(),
        reason: request.reason.clone(),
        cascade_analysis: Some(cascade_analysis),
        created_by: auth.token_prefix.clone(),
        created_at: now.format_rfc3339(),
        expires_at: expires_at.format_rfc3339(),
        approved_by: None,
        approved_at: None,
        started_at: None,
        finished_at: None,
        tombstoned_count: None,
        error_details: None,
    };

    // Persist operation to database
    let scope = serde_json::to_value(&operation)?;
    pool.state()
        .create_tombstone_operation(&operation_id, &auth.token_prefix, scope)
        .await
        .map_err(|e| {
            SinexError::database("Failed to persist tombstone operation").with_source(e.to_string())
        })?;

    info!(
        operation_id = %operation_id,
        cascade_total = cascade_ids.len(),
        cascade_depth = cascade_depth,
        expires_at = %expires_at.format_rfc3339(),
        "Tombstone operation created (requires approval, persisted to DB)"
    );

    let response = TombstoneCreateResponse { operation };
    Ok(serde_json::to_value(response)?)
}

/// Handle lifecycle.tombstone.preview
///
/// Returns the cascade analysis for an existing operation.
pub async fn handle_tombstone_preview(
    pool: &PgPool,
    params: Value,
    _auth: &crate::rpc_server::RpcAuthContext,
) -> Result<Value> {
    let request: TombstonePreviewRequest = serde_json::from_value(params)?;

    let record = pool
        .state()
        .get_tombstone_operation(&request.operation_id)
        .await
        .map_err(|e| {
            SinexError::database("Failed to get tombstone operation").with_source(e.to_string())
        })?
        .ok_or_else(|| {
            SinexError::not_found(format!(
                "Tombstone operation {} not found",
                request.operation_id
            ))
        })?;

    let mut operation = operation_record_to_tombstone(&record)
        .ok_or_else(|| SinexError::invalid_state("Failed to deserialize tombstone operation"))?;

    // Check for expiration
    let now = Timestamp::now();
    if let Ok(expires_at) = Timestamp::parse_rfc3339(&operation.expires_at) {
        if now > expires_at && !operation.state.is_terminal() {
            // Mark as expired and persist
            operation.state = TombstoneOperationState::Expired;
            let scope = serde_json::to_value(&operation)?;
            let _ = pool
                .state()
                .update_tombstone_operation(
                    &request.operation_id,
                    state_to_result_status(operation.state),
                    scope,
                    None,
                )
                .await;
            return Err(SinexError::invalid_state(format!(
                "Tombstone operation {} has expired",
                request.operation_id
            )));
        }
    }

    let response = TombstonePreviewResponse { operation };
    Ok(serde_json::to_value(response)?)
}

/// Handle lifecycle.tombstone.approve
///
/// Approves and immediately executes a tombstone operation.
pub async fn handle_tombstone_approve(
    pool: &PgPool,
    params: Value,
    auth: &crate::rpc_server::RpcAuthContext,
) -> Result<Value> {
    let request: TombstoneApproveRequest = serde_json::from_value(params)?;

    if !request.yes_i_understand_data_is_gone {
        return Err(SinexError::validation(
            "You must set yes_i_understand_data_is_gone=true to confirm permanent deletion",
        ));
    }

    let record = pool
        .state()
        .get_tombstone_operation(&request.operation_id)
        .await
        .map_err(|e| {
            SinexError::database("Failed to get tombstone operation").with_source(e.to_string())
        })?
        .ok_or_else(|| {
            SinexError::not_found(format!(
                "Tombstone operation {} not found",
                request.operation_id
            ))
        })?;

    let mut operation = operation_record_to_tombstone(&record)
        .ok_or_else(|| SinexError::invalid_state("Failed to deserialize tombstone operation"))?;

    // Validate state
    if !operation.state.can_approve() {
        return Err(SinexError::invalid_state(format!(
            "Cannot approve operation in state {:?}",
            operation.state
        )));
    }

    // Check expiration
    let now = Timestamp::now();
    if let Ok(expires_at) = Timestamp::parse_rfc3339(&operation.expires_at) {
        if now > expires_at {
            operation.state = TombstoneOperationState::Expired;
            let scope = serde_json::to_value(&operation)?;
            let _ = pool
                .state()
                .update_tombstone_operation(
                    &request.operation_id,
                    state_to_result_status(operation.state),
                    scope,
                    None,
                )
                .await;
            return Err(SinexError::invalid_state(format!(
                "Tombstone operation {} has expired. Create a new operation.",
                request.operation_id
            )));
        }
    }

    // Mark as approved and executing
    operation.state = TombstoneOperationState::Executing;
    operation.approved_by = Some(auth.token_prefix.clone());
    operation.approved_at = Some(now.format_rfc3339());
    operation.started_at = Some(now.format_rfc3339());

    // Persist executing state
    let scope = serde_json::to_value(&operation)?;
    pool.state()
        .update_tombstone_operation(
            &request.operation_id,
            state_to_result_status(operation.state),
            scope,
            None,
        )
        .await
        .map_err(|e| {
            SinexError::database("Failed to update tombstone operation").with_source(e.to_string())
        })?;

    info!(
        operation_id = %request.operation_id,
        approved_by = %auth.token_prefix,
        "Tombstone operation approved, executing..."
    );

    // Execute the tombstone
    let repo = pool.events();
    let before_ts = if let Some(before_str) = &operation.before {
        parse_duration_to_timestamp(before_str)?
    } else {
        None
    };

    let source = operation
        .source
        .as_ref()
        .map(|s| EventSource::new(s.clone()));
    if let Some(ref src) = source {
        src.validate()
            .map_err(|reason| SinexError::validation(format!("Invalid source: {reason}")))?;
    }

    let event_ids = if let Some(ids) = &operation.event_ids {
        ids.iter()
            .map(|s| {
                Ulid::from_str(s).map_err(|_| SinexError::validation(format!("Invalid ULID: {s}")))
            })
            .collect::<Result<Vec<_>>>()?
    } else {
        repo.get_archived_event_ids(source.as_ref(), before_ts, 1000)
            .await
            .map_err(|e| {
                SinexError::database("Failed to get archived event IDs").with_source(e.to_string())
            })?
    };

    // Recompute cascade (IDs may have changed since preview)
    let session_id = Ulid::new().to_string();
    let table_name = repo
        .prepare_cascade_session(&session_id, true)
        .await
        .map_err(|e| {
            SinexError::database("Failed to prepare cascade session").with_source(e.to_string())
        })?;

    repo.populate_cascade_roots_from_archive(&table_name, &event_ids)
        .await
        .map_err(|e| {
            SinexError::database("Failed to populate cascade roots").with_source(e.to_string())
        })?;

    repo.expand_cascade_from_archive(&table_name, 100)
        .await
        .map_err(|e| SinexError::database("Failed to expand cascade").with_source(e.to_string()))?;

    let cascade_ids = repo.get_cascade_ids(&table_name).await.map_err(|e| {
        SinexError::database("Failed to get cascade IDs").with_source(e.to_string())
    })?;

    repo.cleanup_cascade_session(&table_name)
        .await
        .map_err(|e| {
            SinexError::database("Failed to cleanup cascade session").with_source(e.to_string())
        })?;

    let start_time = std::time::Instant::now();

    // Execute tombstone
    let tombstoned_count = match repo
        .execute_cascade_tombstone(
            &cascade_ids,
            &operation.reason,
            Ulid::from_str(&request.operation_id).unwrap_or_else(|_| Ulid::new()),
        )
        .await
    {
        Ok(count) => count,
        Err(e) => {
            // Mark as failed and persist
            operation.state = TombstoneOperationState::Failed;
            operation.error_details = Some(e.to_string());
            let scope = serde_json::to_value(&operation)?;
            let _ = pool
                .state()
                .update_tombstone_operation(
                    &request.operation_id,
                    state_to_result_status(operation.state),
                    scope,
                    None,
                )
                .await;
            return Err(
                SinexError::database("Failed to execute tombstone").with_source(e.to_string())
            );
        }
    };

    let duration_ms = start_time.elapsed().as_millis() as i32;

    // Mark as completed and persist
    let finished_at = Timestamp::now();
    operation.state = TombstoneOperationState::Completed;
    operation.finished_at = Some(finished_at.format_rfc3339());
    operation.tombstoned_count = Some(tombstoned_count);

    let scope = serde_json::to_value(&operation)?;
    pool.state()
        .update_tombstone_operation(
            &request.operation_id,
            state_to_result_status(operation.state),
            scope,
            Some(duration_ms),
        )
        .await
        .map_err(|e| {
            SinexError::database("Failed to finalize tombstone operation")
                .with_source(e.to_string())
        })?;

    info!(
        operation_id = %request.operation_id,
        tombstoned_count = tombstoned_count,
        duration_ms = duration_ms,
        "💀 Tombstone operation completed (PERMANENT)"
    );

    let response = TombstoneApproveResponse { operation };
    Ok(serde_json::to_value(response)?)
}

/// Handle lifecycle.tombstone.cancel
///
/// Cancels a pending tombstone operation.
pub async fn handle_tombstone_cancel(
    pool: &PgPool,
    params: Value,
    auth: &crate::rpc_server::RpcAuthContext,
) -> Result<Value> {
    let request: TombstoneCancelRequest = serde_json::from_value(params)?;

    let record = pool
        .state()
        .get_tombstone_operation(&request.operation_id)
        .await
        .map_err(|e| {
            SinexError::database("Failed to get tombstone operation").with_source(e.to_string())
        })?
        .ok_or_else(|| {
            SinexError::not_found(format!(
                "Tombstone operation {} not found",
                request.operation_id
            ))
        })?;

    let mut operation = operation_record_to_tombstone(&record)
        .ok_or_else(|| SinexError::invalid_state("Failed to deserialize tombstone operation"))?;

    if !operation.state.is_cancellable() {
        return Err(SinexError::invalid_state(format!(
            "Cannot cancel operation in state {:?}",
            operation.state
        )));
    }

    // Update operation state
    operation.state = TombstoneOperationState::Cancelled;
    if let Some(reason) = &request.reason {
        operation.error_details = Some(format!("Cancelled: {reason}"));
    }

    // Persist cancellation
    let scope = serde_json::to_value(&operation)?;
    pool.state()
        .update_tombstone_operation(
            &request.operation_id,
            state_to_result_status(operation.state),
            scope,
            None,
        )
        .await
        .map_err(|e| {
            SinexError::database("Failed to cancel tombstone operation").with_source(e.to_string())
        })?;

    info!(
        operation_id = %request.operation_id,
        cancelled_by = %auth.token_prefix,
        "Tombstone operation cancelled"
    );

    let response = TombstoneCancelResponse {
        status: "cancelled".to_string(),
        operation_id: request.operation_id,
    };
    Ok(serde_json::to_value(response)?)
}

/// Handle lifecycle.tombstone.list
///
/// Lists tombstone operations, optionally filtered by state.
pub async fn handle_tombstone_list(
    pool: &PgPool,
    params: Value,
    _auth: &crate::rpc_server::RpcAuthContext,
) -> Result<Value> {
    let request: TombstoneListRequest = serde_json::from_value(params).unwrap_or_default();

    // Map state filter to result_status
    let status_filter = request.state.map(state_to_result_status);

    let limit = request.limit.unwrap_or(100);
    let records = pool
        .state()
        .list_tombstone_operations(status_filter, limit)
        .await
        .map_err(|e| {
            SinexError::database("Failed to list tombstone operations").with_source(e.to_string())
        })?;

    // Convert records to TombstoneOperations
    let now = Timestamp::now();
    let mut operations: Vec<TombstoneOperation> = records
        .iter()
        .filter_map(operation_record_to_tombstone)
        .map(|mut op| {
            // Check for expiration on non-terminal operations
            if !op.state.is_terminal() {
                if let Ok(expires_at) = Timestamp::parse_rfc3339(&op.expires_at) {
                    if now > expires_at {
                        op.state = TombstoneOperationState::Expired;
                        // Note: We don't persist this on list - it will be lazily updated on access
                    }
                }
            }
            op
        })
        .collect();

    // Apply state filter (needed because DB filter is on result_status, not full state)
    if let Some(filter_state) = request.state {
        operations.retain(|op| op.state == filter_state);
    }

    // Sort by created_at descending (DB already returns in id DESC order, but created_at may differ)
    operations.sort_by(|a, b| b.created_at.cmp(&a.created_at));

    let response = TombstoneListResponse { operations };
    Ok(serde_json::to_value(response)?)
}

/// Handle lifecycle.tombstone.status
///
/// Gets the status of a specific tombstone operation.
pub async fn handle_tombstone_status(
    pool: &PgPool,
    params: Value,
    _auth: &crate::rpc_server::RpcAuthContext,
) -> Result<Value> {
    let request: TombstoneStatusRequest = serde_json::from_value(params)?;

    let record = pool
        .state()
        .get_tombstone_operation(&request.operation_id)
        .await
        .map_err(|e| {
            SinexError::database("Failed to get tombstone operation").with_source(e.to_string())
        })?
        .ok_or_else(|| {
            SinexError::not_found(format!(
                "Tombstone operation {} not found",
                request.operation_id
            ))
        })?;

    let operation = operation_record_to_tombstone(&record)
        .ok_or_else(|| SinexError::invalid_state("Failed to deserialize tombstone operation"))?;

    let response = TombstoneStatusResponse { operation };
    Ok(serde_json::to_value(response)?)
}
