//! Data lifecycle RPC handlers
//!
//! Implements the three-tier data lifecycle: Live ↔ Archive → Tombstone

use serde_json::{Value, json};
use sinex_db::{CascadeSource, DbPoolExt};
use sinex_primitives::rpc::lifecycle::{
    LifecycleArchiveRequest, LifecycleArchiveResponse, LifecycleRestoreRequest,
    LifecycleRestoreResponse, LifecycleStatusRequest, LifecycleStatusResponse, TierStatus,
};
use sinex_primitives::{SinexError, Timestamp, Uuid};
use sqlx::PgPool;
use std::str::FromStr;
use tracing::{info, warn};

type Result<T> = std::result::Result<T, SinexError>;

fn parse_uuid_str(raw: &str) -> Result<Uuid> {
    Uuid::from_str(raw).map_err(|_| SinexError::validation(format!("Invalid UUID: {raw}")))
}

fn parse_operation_uuid(raw: &str) -> Result<Uuid> {
    Uuid::from_str(raw)
        .map_err(|_| SinexError::validation(format!("Invalid tombstone operation ID: {raw}")))
}

fn stringify_event_ids(event_ids: &[Uuid]) -> Vec<String> {
    event_ids
        .iter()
        .map(std::string::ToString::to_string)
        .collect()
}

fn fresh_cascade_session_id(prefix: &str) -> String {
    format!("{prefix}_{}", Uuid::now_v7().simple())
}

fn lifecycle_audit_summary(
    affected_event_ids: &[Uuid],
    cascade_depth: usize,
    cascade_total: usize,
    root_event_count: usize,
    dry_run: bool,
) -> Value {
    json!({
        "affected_event_ids": stringify_event_ids(affected_event_ids),
        "cascade_depth": cascade_depth,
        "cascade_total": cascade_total,
        "root_event_count": root_event_count,
        "dry_run": dry_run,
    })
}

async fn collect_cascade_ids(
    pool: &PgPool,
    session_prefix: &str,
    root_ids: &[Uuid],
    source: CascadeSource,
) -> Result<(Vec<Uuid>, usize)> {
    let mut tx = pool.begin().await.map_err(|e| {
        SinexError::database("Failed to begin cascade analysis transaction").with_source(e.to_string())
    })?;
    let repo = pool.events();
    let mut repo_tx = repo.as_tx(&mut tx);
    let session_id = fresh_cascade_session_id(session_prefix);

    let table_name = repo_tx
        .prepare_cascade_session(&session_id, true)
        .await
        .map_err(|e| {
            SinexError::database("Failed to prepare cascade session").with_source(e.to_string())
        })?;
    repo_tx
        .populate_cascade_roots_from(&table_name, root_ids, source)
        .await
        .map_err(|e| {
            SinexError::database("Failed to populate cascade roots").with_source(e.to_string())
        })?;
    let cascade_depth = repo_tx
        .expand_cascade_from(&table_name, 100, source)
        .await
        .map_err(|e| SinexError::database("Failed to expand cascade").with_source(e.to_string()))?;
    let cascade_ids = repo_tx.get_cascade_ids(&table_name).await.map_err(|e| {
        SinexError::database("Failed to get cascade IDs").with_source(e.to_string())
    })?;

    repo_tx
        .cleanup_cascade_session(&table_name)
        .await
        .map_err(|e| {
            SinexError::database("Failed to cleanup cascade session").with_source(e.to_string())
        })?;
    tx.commit().await.map_err(|e| {
        SinexError::database("Failed to commit cascade analysis transaction").with_source(e.to_string())
    })?;

    Ok((cascade_ids, cascade_depth))
}

/// Handle lifecycle.status - get status of all lifecycle tiers
pub async fn handle_lifecycle_status(pool: &PgPool, params: Value) -> Result<Value> {
    let _request: LifecycleStatusRequest = super::parse_default_on_null(params)?;

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

    // Get live event IDs matching filters
    let event_ids = if let Some(ids) = &request.event_ids {
        ids.iter()
            .map(|s| parse_uuid_str(s))
            .collect::<Result<Vec<_>>>()?
    } else {
        repo.get_live_event_ids(request.source.as_ref(), before_ts, request.limit)
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

    let (cascade_ids, cascade_depth) =
        collect_cascade_ids(pool, "archive", &event_ids, CascadeSource::Live).await?;
    let cascade_total = cascade_ids.len();

    let preview_summary =
        lifecycle_audit_summary(&cascade_ids, cascade_depth, cascade_total, event_ids.len(), request.dry_run);
    let scope = json!({
        "source": request.source.as_ref().map(|source| source.to_string()),
        "before": request.before.clone(),
        "requested_event_ids": request.event_ids.clone(),
        "limit": request.limit,
        "reason": request.reason.clone(),
        "dry_run": request.dry_run,
    });
    let operation = pool
        .state()
        .start_operation("archive", &auth.token_prefix, scope)
        .await
        .map_err(|e| {
            SinexError::database("Failed to persist archive operation").with_source(e.to_string())
        })?;
    pool.state()
        .update_operation_meta(
            &operation.id,
            OperationStatus::Running,
            Some("Archive preview computed"),
            preview_summary.clone(),
        )
        .await
        .map_err(|e| {
            SinexError::database("Failed to persist archive preview").with_source(e.to_string())
        })?;
    let operation_id = operation.id.to_uuid();

    if request.dry_run {
        pool.state()
            .complete_operation(
                &operation.id,
                json!({
                    "message": "Lifecycle archive dry run completed",
                    "archived_count": 0,
                }),
            )
            .await
            .map_err(|e| {
                SinexError::database("Failed to finalize archive dry run").with_source(e.to_string())
            })?;
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
    let archived_count = match repo
        .execute_cascade_archive(
            &cascade_ids,
            reason,
            &operation_id.to_string(),
            &auth.token_prefix,
        )
        .await
    {
        Ok(count) => count,
        Err(error) => {
            let _ = pool
                .state()
                .fail_operation(
                    &operation.id,
                    json!({
                        "error": format!("Failed to execute cascade archive: {error}"),
                    }),
                )
                .await;
            return Err(
                SinexError::database("Failed to execute cascade archive").with_source(error.to_string())
            );
        }
    };
    pool.state()
        .complete_operation(
            &operation.id,
            json!({
                "message": "Lifecycle archive completed",
                "archived_count": archived_count,
            }),
        )
        .await
        .map_err(|e| {
            SinexError::database("Failed to finalize archive operation").with_source(e.to_string())
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
    let event_ids: Vec<Uuid> = request
        .event_ids
        .iter()
        .map(|s| parse_uuid_str(s))
        .collect::<Result<Vec<_>>>()?;

    let (cascade_ids, max_depth) =
        collect_cascade_ids(pool, "restore", &event_ids, CascadeSource::Archive).await?;
    let cascade_total = cascade_ids.len();

    let preview_summary =
        lifecycle_audit_summary(&cascade_ids, max_depth, cascade_total, event_ids.len(), request.dry_run);
    let scope = json!({
        "requested_event_ids": request.event_ids.clone(),
        "dry_run": request.dry_run,
    });
    let operation = pool
        .state()
        .start_operation("restore", &auth.token_prefix, scope)
        .await
        .map_err(|e| {
            SinexError::database("Failed to persist restore operation").with_source(e.to_string())
        })?;
    pool.state()
        .update_operation_meta(
            &operation.id,
            OperationStatus::Running,
            Some("Restore preview computed"),
            preview_summary.clone(),
        )
        .await
        .map_err(|e| {
            SinexError::database("Failed to persist restore preview").with_source(e.to_string())
        })?;
    let operation_id = operation.id.to_uuid();

    if request.dry_run {
        pool.state()
            .complete_operation(
                &operation.id,
                json!({
                    "message": "Lifecycle restore dry run completed",
                    "restored_count": 0,
                }),
            )
            .await
            .map_err(|e| {
                SinexError::database("Failed to finalize restore dry run").with_source(e.to_string())
            })?;
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
    let restored_count = match repo
        .execute_cascade_restore(&cascade_ids, &operation_id.to_string())
        .await
    {
        Ok(count) => count,
        Err(error) => {
            let _ = pool
                .state()
                .fail_operation(
                    &operation.id,
                    json!({
                        "error": format!("Failed to execute cascade restore: {error}"),
                    }),
                )
                .await;
            return Err(
                SinexError::database("Failed to execute cascade restore").with_source(error.to_string())
            );
        }
    };
    pool.state()
        .complete_operation(
            &operation.id,
            json!({
                "message": "Lifecycle restore completed",
                "restored_count": restored_count,
            }),
        )
        .await
        .map_err(|e| {
            SinexError::database("Failed to finalize restore operation").with_source(e.to_string())
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

use sinex_primitives::domain::OperationStatus;
use sinex_primitives::rpc::lifecycle::{
    TombstoneApproveRequest, TombstoneApproveResponse, TombstoneCancelRequest,
    TombstoneCancelResponse, TombstoneCascadeAnalysis, TombstoneCreateRequest,
    TombstoneCreateResponse, TombstoneListRequest, TombstoneListResponse, TombstoneOperation,
    TombstoneOperationPhase, TombstoneOperationState, TombstonePreviewRequest,
    TombstonePreviewResponse, TombstoneStatusRequest, TombstoneStatusResponse,
};
use std::collections::HashMap;

/// Default TTL for tombstone operations (1 hour)
const TOMBSTONE_OPERATION_TTL_SECS: i64 = 3600;

/// Convert canonical tombstone phase to coarse `operations_log.result_status`.
fn phase_to_result_status(phase: TombstoneOperationPhase) -> OperationStatus {
    match phase {
        TombstoneOperationPhase::Pending
        | TombstoneOperationPhase::Previewed
        | TombstoneOperationPhase::Approved
        | TombstoneOperationPhase::Executing => OperationStatus::Running,
        TombstoneOperationPhase::Completed => OperationStatus::Success,
        TombstoneOperationPhase::Cancelled => OperationStatus::Cancelled,
        TombstoneOperationPhase::Failed => OperationStatus::Failed,
        TombstoneOperationPhase::Expired => OperationStatus::Cancelled,
    }
}

fn sync_tombstone_phase(operation: &mut TombstoneOperation) {
    operation.phase = operation.state.into();
}

/// Convert `OperationRecord` to `TombstoneOperation`
fn operation_record_to_tombstone(
    record: &sinex_db::repositories::state::OperationRecord,
) -> Option<TombstoneOperation> {
    let scope = record.scope.as_ref()?;
    match serde_json::from_value::<TombstoneOperation>(scope.clone()) {
        Ok(mut operation) => {
            // Canonical read path: phase is authoritative, state mirrors phase.
            operation.state = operation.phase.into();
            Some(operation)
        }
        Err(error) => {
            warn!(
                operation_id = %record.id,
                error = %error,
                "tombstone operation scope is not in current-state shape"
            );
            None
        }
    }
}

fn tombstone_preview_summary(
    root_event_ids: &[Uuid],
    cascade_event_ids: &[Uuid],
    cascade_analysis: &TombstoneCascadeAnalysis,
    limit: i64,
) -> Value {
    json!({
        "root_event_ids": stringify_event_ids(root_event_ids),
        "affected_event_ids": stringify_event_ids(cascade_event_ids),
        "root_event_count": cascade_analysis.root_event_count,
        "cascade_total": cascade_analysis.cascade_total,
        "cascade_depth": cascade_analysis.cascade_depth,
        "limit": limit,
    })
}

fn merge_preview_summary(preview_summary: Option<Value>, extra: Value) -> Option<Value> {
    match (preview_summary, extra) {
        (Some(mut summary @ Value::Object(_)), Value::Object(extra_fields)) => {
            let Value::Object(summary_fields) = &mut summary else {
                unreachable!();
            };
            summary_fields.extend(extra_fields);
            Some(summary)
        }
        (Some(summary), _) => Some(summary),
        (None, extra) => Some(extra),
    }
}

fn parse_previewed_event_ids(record: &sinex_db::repositories::state::OperationRecord) -> Result<Vec<Uuid>> {
    let Some(summary) = record.preview_summary.as_ref() else {
        return Err(SinexError::invalid_state(
            "Tombstone operation is missing preview_summary",
        ));
    };
    let Some(event_ids) = summary.get("affected_event_ids").and_then(Value::as_array) else {
        return Err(SinexError::invalid_state(
            "Tombstone preview_summary is missing affected_event_ids",
        ));
    };

    event_ids
        .iter()
        .map(|value| {
            value
                .as_str()
                .ok_or_else(|| {
                    SinexError::invalid_state("Tombstone preview_summary contains non-string event IDs")
                })
                .and_then(parse_uuid_str)
        })
        .collect()
}

async fn reconcile_tombstone_expiry(
    pool: &PgPool,
    operation_id: &str,
    operation: &mut TombstoneOperation,
    preview_summary: Option<Value>,
) -> Result<bool> {
    let now = Timestamp::now();
    if !operation.state.is_terminal()
        && let Ok(expires_at) = Timestamp::parse_rfc3339(&operation.expires_at)
        && now > expires_at
    {
        operation.state = TombstoneOperationState::Expired;
        sync_tombstone_phase(operation);
        let scope = serde_json::to_value(&*operation)?;
        pool.state()
            .update_tombstone_operation(
                operation_id,
                phase_to_result_status(operation.phase),
                scope,
                preview_summary,
                Some("Tombstone operation expired"),
                None,
            )
            .await
            .map_err(|e| {
                SinexError::database("Failed to persist tombstone expiration").with_source(e.to_string())
            })?;
        return Ok(true);
    }

    Ok(false)
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

    let operation_id = Uuid::now_v7().to_string();
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

    // Get archived event IDs matching filters
    let event_ids = if let Some(ids) = &request.event_ids {
        ids.iter()
            .map(|s| parse_uuid_str(s))
            .collect::<Result<Vec<_>>>()?
    } else {
        repo.get_archived_event_ids(request.source.as_ref(), before_ts, request.limit)
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

    let (cascade_ids, cascade_depth) =
        collect_cascade_ids(pool, "tombstone", &event_ids, CascadeSource::Archive).await?;

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
        phase: TombstoneOperationPhase::Previewed,
        state: TombstoneOperationState::Previewed, // Already previewed on create
        before: request.before.clone(),
        source: request.source.clone(),
        event_ids: request.event_ids.clone(),
        limit: request.limit,
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
    let preview_summary = tombstone_preview_summary(
        &event_ids,
        &cascade_ids,
        operation
            .cascade_analysis
            .as_ref()
            .ok_or_else(|| SinexError::invalid_state("Missing tombstone cascade analysis"))?,
        operation.limit,
    );
    pool.state()
        .create_tombstone_operation(&operation_id, &auth.token_prefix, scope, preview_summary)
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

    if reconcile_tombstone_expiry(
        pool,
        &request.operation_id,
        &mut operation,
        record.preview_summary.clone(),
    )
    .await?
    {
        return Err(SinexError::invalid_state(format!(
            "Tombstone operation {} has expired",
            request.operation_id
        )));
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
    let preview_summary = record.preview_summary.clone();

    // Validate state
    if !operation.state.can_approve() {
        return Err(SinexError::invalid_state(format!(
            "Cannot approve operation in state {:?}",
            operation.state
        )));
    }

    let now = Timestamp::now();
    if reconcile_tombstone_expiry(
        pool,
        &request.operation_id,
        &mut operation,
        preview_summary.clone(),
    )
    .await?
    {
        return Err(SinexError::invalid_state(format!(
            "Tombstone operation {} has expired. Create a new operation.",
            request.operation_id
        )));
    }

    let previewed_event_ids = parse_previewed_event_ids(&record)?;
    let archived_count = pool
        .state()
        .count_archived_event_ids(&previewed_event_ids)
        .await
        .map_err(|e| {
            SinexError::database("Failed to validate tombstone preview set").with_source(e.to_string())
        })?;
    if archived_count != previewed_event_ids.len() as i64 {
        operation.state = TombstoneOperationState::Failed;
        operation.finished_at = Some(now.format_rfc3339());
        operation.error_details = Some(format!(
            "Preview drift detected: expected {} archived events, found {}",
            previewed_event_ids.len(),
            archived_count
        ));
        sync_tombstone_phase(&mut operation);
        let scope = serde_json::to_value(&operation)?;
        pool.state()
            .update_tombstone_operation(
                &request.operation_id,
                phase_to_result_status(operation.phase),
                scope,
                merge_preview_summary(
                    preview_summary.clone(),
                    json!({
                        "message": "Tombstone preview is no longer valid",
                    }),
                ),
                Some("Tombstone preview is no longer valid"),
                None,
            )
            .await
            .map_err(|e| {
                SinexError::database("Failed to persist tombstone preview drift").with_source(e.to_string())
            })?;
        return Err(SinexError::invalid_state(format!(
            "Tombstone operation {} no longer matches the archived preview set",
            request.operation_id
        )));
    }

    operation.state = TombstoneOperationState::Executing;
    operation.approved_by = Some(auth.token_prefix.clone());
    operation.approved_at = Some(now.format_rfc3339());
    operation.started_at = Some(now.format_rfc3339());
    sync_tombstone_phase(&mut operation);

    // Persist executing state
    let scope = serde_json::to_value(&operation)?;
    pool.state()
        .update_tombstone_operation(
            &request.operation_id,
            phase_to_result_status(operation.phase),
            scope,
            preview_summary.clone(),
            Some("Tombstone operation executing"),
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

    let repo = pool.events();
    let operation_uuid = parse_operation_uuid(&request.operation_id)?;
    let start_time = std::time::Instant::now();

    // Execute tombstone
    let tombstoned_count = match repo
        .execute_cascade_tombstone(&previewed_event_ids, &operation.reason, operation_uuid)
        .await
    {
        Ok(count) => count,
        Err(e) => {
            // Mark as failed and persist
            operation.state = TombstoneOperationState::Failed;
            operation.finished_at = Some(Timestamp::now().format_rfc3339());
            operation.error_details = Some(e.to_string());
            sync_tombstone_phase(&mut operation);
            let scope = serde_json::to_value(&operation)?;
            let _ = pool
                .state()
                .update_tombstone_operation(
                    &request.operation_id,
                    phase_to_result_status(operation.phase),
                    scope,
                    merge_preview_summary(
                        preview_summary.clone(),
                        json!({
                            "message": "Failed to execute tombstone",
                            "error": e.to_string(),
                        }),
                    ),
                    Some("Failed to execute tombstone"),
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
    sync_tombstone_phase(&mut operation);

    let scope = serde_json::to_value(&operation)?;
    pool.state()
        .update_tombstone_operation(
            &request.operation_id,
            phase_to_result_status(operation.phase),
            scope,
            merge_preview_summary(
                preview_summary,
                json!({
                    "message": "Tombstone operation completed",
                    "tombstoned_count": tombstoned_count,
                }),
            ),
            Some("Tombstone operation completed"),
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
    sync_tombstone_phase(&mut operation);

    // Persist cancellation
    let scope = serde_json::to_value(&operation)?;
    pool.state()
        .update_tombstone_operation(
            &request.operation_id,
            phase_to_result_status(operation.phase),
            scope,
            record.preview_summary.clone(),
            Some("Tombstone operation cancelled"),
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
    let request: TombstoneListRequest = super::parse_default_on_null(params)?;

    let limit = request.limit.unwrap_or(100);
    let records = pool
        .state()
        .list_tombstone_operations(limit)
        .await
        .map_err(|e| {
            SinexError::database("Failed to list tombstone operations").with_source(e.to_string())
        })?;

    // Convert records to TombstoneOperations
    let mut operations = Vec::new();
    for record in &records {
        let Some(mut operation) = operation_record_to_tombstone(record) else {
            continue;
        };
        let operation_id = operation.operation_id.clone();
        let _ = reconcile_tombstone_expiry(
            pool,
            &operation_id,
            &mut operation,
            record.preview_summary.clone(),
        )
        .await;
        operations.push(operation);
    }

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

    let mut operation = operation_record_to_tombstone(&record)
        .ok_or_else(|| SinexError::invalid_state("Failed to deserialize tombstone operation"))?;
    let _ = reconcile_tombstone_expiry(
        pool,
        &request.operation_id,
        &mut operation,
        record.preview_summary.clone(),
    )
    .await?;

    let response = TombstoneStatusResponse { operation };
    Ok(serde_json::to_value(response)?)
}
