//! Data lifecycle RPC handlers
//!
//! Implements the three-tier data lifecycle: Live ↔ Archive → Tombstone

use serde_json::{Value, json};
use sinex_db::{CascadeSource, DbPoolExt, Event};
use sinex_primitives::rpc::lifecycle::{
    LifecycleArchiveRequest, LifecycleArchiveResponse, LifecycleRestoreRequest,
    LifecycleRestoreResponse, LifecycleStatusRequest, LifecycleStatusResponse, TierStatus,
    TombstoneApproveRequest, TombstoneApproveResponse, TombstoneCancelRequest,
    TombstoneCancelResponse, TombstoneCascadeAnalysis, TombstoneCreateRequest,
    TombstoneCreateResponse, TombstoneListRequest, TombstoneListResponse, TombstoneOperation,
    TombstoneOperationPhase, TombstoneOperationState, TombstonePreviewRequest,
    TombstonePreviewResponse, TombstoneStatusRequest, TombstoneStatusResponse,
};
use sinex_primitives::temporal::parse_duration;
use sinex_primitives::{Id, SinexError, Timestamp, Uuid};
use sqlx::PgPool;
use std::str::FromStr;
use tracing::{info, warn};

type Result<T> = std::result::Result<T, SinexError>;

fn require_positive_limit(method: &str, limit: i64) -> Result<i64> {
    if limit <= 0 {
        return Err(SinexError::validation(format!(
            "{method} limit must be positive, got {limit}"
        )));
    }
    Ok(limit)
}

fn parse_uuid_str(raw: &str) -> Result<Uuid> {
    Uuid::from_str(raw).map_err(|_| SinexError::validation(format!("Invalid UUID: {raw}")))
}

fn parse_operation_uuid(raw: &str) -> Result<Uuid> {
    Uuid::from_str(raw)
        .map_err(|_| SinexError::validation(format!("Invalid tombstone operation ID: {raw}")))
}

fn reject_conflicting_explicit_event_filters(
    method: &str,
    has_event_ids: bool,
    has_source_filter: bool,
    has_before_filter: bool,
) -> Result<()> {
    if has_event_ids && (has_source_filter || has_before_filter) {
        return Err(SinexError::validation(format!(
            "{method} does not allow `event_ids` together with `source` or `before`; explicit event IDs already define the scope"
        )));
    }
    Ok(())
}

fn stringify_event_ids(event_ids: &[Uuid]) -> Vec<String> {
    event_ids
        .iter()
        .map(std::string::ToString::to_string)
        .collect()
}

fn parse_unique_event_ids(raw_ids: &[String]) -> Result<Vec<Uuid>> {
    let mut event_ids = raw_ids
        .iter()
        .map(|raw| parse_uuid_str(raw))
        .collect::<Result<Vec<_>>>()?;
    event_ids.sort_unstable();
    event_ids.dedup();
    Ok(event_ids)
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
    let session_id = fresh_cascade_session_id(session_prefix);

    pool.with_transaction(async |tx| {
        let repo = pool.events();
        let mut repo_tx = repo.as_tx(tx);

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
            .map_err(|e| {
                SinexError::database("Failed to expand cascade").with_source(e.to_string())
            })?;
        let cascade_ids = repo_tx.get_cascade_ids(&table_name).await.map_err(|e| {
            SinexError::database("Failed to get cascade IDs").with_source(e.to_string())
        })?;

        repo_tx
            .cleanup_cascade_session(&table_name)
            .await
            .map_err(|e| {
                SinexError::database("Failed to cleanup cascade session").with_source(e.to_string())
            })?;

        Ok((cascade_ids, cascade_depth))
    })
    .await
}

/// Handle lifecycle.status - get status of all lifecycle tiers
pub async fn handle_lifecycle_status(
    pool: &PgPool,
    _request: LifecycleStatusRequest,
) -> Result<LifecycleStatusResponse> {
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

    Ok(response)
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
    request: LifecycleArchiveRequest,
    auth: &crate::api::rpc_server::RpcAuthContext,
) -> Result<LifecycleArchiveResponse> {
    let limit = require_positive_limit("lifecycle.archive", request.limit)?;
    reject_conflicting_explicit_event_filters(
        "lifecycle.archive",
        request.event_ids.is_some(),
        request.source.is_some(),
        request.before.is_some(),
    )?;

    info!(
        actor = %auth.actor_id(),
        source = ?request.source,
        before = ?request.before,
        dry_run = request.dry_run,
        "Lifecycle archive operation initiated"
    );

    let repo = pool.events();
    let explicit_event_ids = request.event_ids.is_some();

    // Parse filters
    let before_ts = if let Some(before_str) = &request.before {
        parse_duration_to_timestamp(before_str)?
    } else {
        None
    };

    // Get live event IDs matching filters
    let event_ids = if let Some(ids) = &request.event_ids {
        parse_unique_event_ids(ids)?
    } else {
        repo.get_live_event_ids(request.source.as_ref(), before_ts, limit)
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

    let preview_summary = lifecycle_audit_summary(
        &cascade_ids,
        cascade_depth,
        cascade_total,
        event_ids.len(),
        request.dry_run,
    );
    let mut scope = json!({
        "source": request.source.as_ref().map(std::string::ToString::to_string),
        "before": request.before.clone(),
        "requested_event_ids": Value::Null,
        "limit": limit,
        "reason": request.reason.clone(),
        "dry_run": request.dry_run,
    });
    if explicit_event_ids {
        scope["requested_event_ids"] = json!(stringify_event_ids(&event_ids));
    }
    let operation = pool
        .state()
        .start_operation("archive", auth.actor_id(), scope)
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
                SinexError::database("Failed to finalize archive dry run")
                    .with_source(e.to_string())
            })?;
        let response = LifecycleArchiveResponse {
            archived_count: 0,
            cascade_depth,
            cascade_total,
            operation_id: operation_id.to_string(),
            dry_run: true,
        };
        return Ok(response);
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
            auth.actor_id(),
        )
        .await
    {
        Ok(count) => count,
        Err(error) => {
            if let Err(persist_error) = pool
                .state()
                .fail_operation(
                    &operation.id,
                    json!({
                        "error": format!("Failed to execute cascade archive: {error}"),
                    }),
                )
                .await
            {
                warn!(
                    operation_id = %operation_id,
                    error = %persist_error,
                    "Failed to persist archive operation failure"
                );
            }
            return Err(SinexError::database("Failed to execute cascade archive")
                .with_source(error.to_string()));
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

    Ok(response)
}

/// Handle lifecycle.restore - move archived events back to live
pub async fn handle_lifecycle_restore(
    pool: &PgPool,
    request: LifecycleRestoreRequest,
    auth: &crate::api::rpc_server::RpcAuthContext,
) -> Result<LifecycleRestoreResponse> {
    if request.event_ids.is_empty() {
        return Err(SinexError::validation("No event IDs provided for restore"));
    }

    // Parse event IDs
    let event_ids = parse_unique_event_ids(&request.event_ids)?;

    info!(
        actor = %auth.actor_id(),
        event_count = event_ids.len(),
        dry_run = request.dry_run,
        "Lifecycle restore operation initiated"
    );

    let repo = pool.events();

    let (cascade_ids, max_depth) =
        collect_cascade_ids(pool, "restore", &event_ids, CascadeSource::Archive).await?;
    let cascade_total = cascade_ids.len();

    let preview_summary = lifecycle_audit_summary(
        &cascade_ids,
        max_depth,
        cascade_total,
        event_ids.len(),
        request.dry_run,
    );
    let scope = json!({
        "requested_event_ids": stringify_event_ids(&event_ids),
        "dry_run": request.dry_run,
    });
    let operation = pool
        .state()
        .start_operation("restore", auth.actor_id(), scope)
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
                SinexError::database("Failed to finalize restore dry run")
                    .with_source(e.to_string())
            })?;
        let response = LifecycleRestoreResponse {
            restored_count: 0,
            cascade_depth: max_depth,
            cascade_total,
            operation_id: operation_id.to_string(),
            dry_run: true,
        };
        return Ok(response);
    }

    // Execute restore
    let restored_count = match repo
        .execute_cascade_restore(&cascade_ids, &operation_id.to_string())
        .await
    {
        Ok(count) => count,
        Err(error) => {
            if let Err(persist_error) = pool
                .state()
                .fail_operation(
                    &operation.id,
                    json!({
                        "error": format!("Failed to execute cascade restore: {error}"),
                    }),
                )
                .await
            {
                warn!(
                    operation_id = %operation_id,
                    error = %persist_error,
                    "Failed to persist restore operation failure"
                );
            }
            return Err(SinexError::database("Failed to execute cascade restore")
                .with_source(error.to_string()));
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

    Ok(response)
}

/// Parse a duration string (e.g., "30d", "90d") to a timestamp in the past
fn parse_duration_to_timestamp(duration_str: &str) -> Result<Option<Timestamp>> {
    let duration = parse_duration(duration_str)
        .ok_or_else(|| SinexError::validation(format!("Invalid duration '{duration_str}'")))?;

    let ts = Timestamp::now() - duration;
    Ok(Some(ts))
}

// ─────────────────────────────────────────────────────────────
// Two-Step Tombstone Operations (SEC-003)
// ─────────────────────────────────────────────────────────────

use sinex_primitives::domain::OperationStatus;

/// Default TTL for tombstone operations (1 hour)
const TOMBSTONE_OPERATION_TTL_SECS: i64 = 3600;

/// Convert canonical tombstone phase to coarse `operations_log.result_status`.
fn phase_to_result_status(phase: TombstoneOperationPhase) -> OperationStatus {
    match phase {
        TombstoneOperationPhase::Pending
        | TombstoneOperationPhase::Previewed
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
) -> Result<TombstoneOperation> {
    let scope = record.scope.clone().ok_or_else(|| {
        SinexError::invalid_state(format!(
            "Tombstone operation {} is missing scope",
            record.id
        ))
    })?;
    let mut operation = serde_json::from_value::<TombstoneOperation>(scope).map_err(|error| {
        warn!(
            operation_id = %record.id,
            error = %error,
            "tombstone operation scope is not in current-state shape"
        );
        SinexError::invalid_state(format!(
            "Tombstone operation {} has malformed scope: {error}",
            record.id
        ))
    })?;
    // Canonical read path: phase is authoritative, state mirrors phase.
    operation.state = operation.phase.into();
    Ok(operation)
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

fn merge_preview_summary(preview_summary: Option<Value>, extra: Value) -> Value {
    match (preview_summary, extra) {
        (Some(mut summary @ Value::Object(_)), Value::Object(extra_fields)) => {
            let Value::Object(summary_fields) = &mut summary else {
                unreachable!();
            };
            summary_fields.extend(extra_fields);
            summary
        }
        (Some(summary), _) => summary,
        (None, extra) => extra,
    }
}

fn matches_requested_tombstone_state(
    requested_state: Option<TombstoneOperationState>,
    operation: &TombstoneOperation,
) -> bool {
    requested_state.is_none_or(|state| operation.state == state)
}

fn tombstone_duration_ms(
    operation: &TombstoneOperation,
    finished_at: Timestamp,
) -> Result<Option<i32>> {
    let created_at = Timestamp::parse_rfc3339(&operation.created_at).map_err(|error| {
        SinexError::invalid_state("Tombstone operation has invalid created_at timestamp")
            .with_context("created_at", &operation.created_at)
            .with_std_error(&error)
    })?;
    let elapsed_ms = (finished_at - created_at).whole_milliseconds();
    let clamped = elapsed_ms.clamp(0, i128::from(i32::MAX));
    Ok(Some(clamped as i32))
}

fn parse_previewed_event_ids(
    record: &sinex_db::repositories::state::OperationRecord,
) -> Result<Vec<Id<Event>>> {
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
                    SinexError::invalid_state(
                        "Tombstone preview_summary contains non-string event IDs",
                    )
                })
                .and_then(parse_uuid_str)
                .map(Id::from_uuid)
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
        operation.finished_at = Some(now.format_rfc3339());
        operation.error_details = Some("Expired before approval".to_string());
        sync_tombstone_phase(operation);
        let scope = serde_json::to_value(&*operation)?;
        let duration_ms = tombstone_duration_ms(operation, now)?;
        pool.state()
            .update_tombstone_operation(
                operation_id,
                phase_to_result_status(operation.phase),
                scope,
                Some(merge_preview_summary(
                    preview_summary,
                    json!({
                        "message": "Tombstone operation expired",
                    }),
                )),
                Some("Tombstone operation expired"),
                duration_ms,
            )
            .await
            .map_err(|e| {
                SinexError::database("Failed to persist tombstone expiration")
                    .with_source(e.to_string())
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
    request: TombstoneCreateRequest,
    auth: &crate::api::rpc_server::RpcAuthContext,
) -> Result<TombstoneCreateResponse> {
    let limit = require_positive_limit("lifecycle.tombstone.create", request.limit)?;
    reject_conflicting_explicit_event_filters(
        "lifecycle.tombstone.create",
        request.event_ids.is_some(),
        request.source.is_some(),
        request.before.is_some(),
    )?;

    let operation_id = Uuid::now_v7().to_string();
    let now = Timestamp::now();
    let expires_at = now + time::Duration::seconds(TOMBSTONE_OPERATION_TTL_SECS);

    info!(
        operation_id = %operation_id,
        actor = %auth.actor_id(),
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
        repo.get_archived_event_ids(request.source.as_ref(), before_ts, limit)
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
        limit,
        reason: request.reason.clone(),
        cascade_analysis: Some(cascade_analysis),
        created_by: auth.actor_id().to_string(),
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
        .create_tombstone_operation(&operation_id, auth.actor_id(), scope, preview_summary)
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

    Ok(TombstoneCreateResponse { operation })
}

/// Handle lifecycle.tombstone.preview
///
/// Returns the cascade analysis for an existing operation.
pub async fn handle_tombstone_preview(
    pool: &PgPool,
    request: TombstonePreviewRequest,
    _auth: &crate::api::rpc_server::RpcAuthContext,
) -> Result<TombstonePreviewResponse> {
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

    let mut operation = operation_record_to_tombstone(&record)?;

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

    Ok(TombstonePreviewResponse { operation })
}

/// Handle lifecycle.tombstone.approve
///
/// Approves and immediately executes a tombstone operation. After the
/// SQL cascade tombstone deletes the archived event rows, this also
/// drops any source materials that no live or archived event still
/// references — both the registry row and the underlying CAS blob.
/// (#987 delete-on-tombstone for local CAS.)
pub async fn handle_tombstone_approve(
    services: &crate::api::service_container::ServiceContainer,
    request: TombstoneApproveRequest,
    auth: &crate::api::rpc_server::RpcAuthContext,
) -> Result<TombstoneApproveResponse> {
    let pool = services.pool();

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

    let mut operation = operation_record_to_tombstone(&record)?;
    let preview_summary = record.preview_summary.clone();

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

    if !operation.state.can_approve() {
        return Err(SinexError::invalid_state(format!(
            "Cannot approve operation in state {:?}",
            operation.state
        )));
    }

    let previewed_event_ids = parse_previewed_event_ids(&record)?;
    let archived_count = pool
        .state()
        .count_archived_event_ids(&previewed_event_ids)
        .await
        .map_err(|e| {
            SinexError::database("Failed to validate tombstone preview set")
                .with_source(e.to_string())
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
                Some(merge_preview_summary(
                    preview_summary.clone(),
                    json!({
                        "message": "Tombstone preview is no longer valid",
                    }),
                )),
                Some("Tombstone preview is no longer valid"),
                None,
            )
            .await
            .map_err(|e| {
                SinexError::database("Failed to persist tombstone preview drift")
                    .with_source(e.to_string())
            })?;
        return Err(SinexError::invalid_state(format!(
            "Tombstone operation {} no longer matches the archived preview set",
            request.operation_id
        )));
    }

    operation.state = TombstoneOperationState::Executing;
    operation.approved_by = Some(auth.actor_id().to_string());
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
        actor = %auth.actor_id(),
        "Tombstone operation approved, executing..."
    );

    let repo = pool.events();
    let materials_repo = pool.source_materials();
    let operation_uuid = parse_operation_uuid(&request.operation_id)?;
    let previewed_event_uuids: Vec<Uuid> = previewed_event_ids
        .iter()
        .map(|event_id| *event_id.as_uuid())
        .collect();

    // Capture the source_material_ids referenced by the about-to-be-tombstoned
    // archived events. We must read this BEFORE execute_cascade_tombstone runs
    // because it deletes the archived_events rows we'd need to query.
    let candidate_material_ids = materials_repo
        .material_ids_for_archived_events(&previewed_event_uuids)
        .await
        .map_err(|e| {
            SinexError::database("Failed to collect candidate material IDs")
                .with_source(e.to_string())
        })?;

    // Execute tombstone
    let tombstoned_count = match repo
        .execute_cascade_tombstone(&previewed_event_uuids, &operation.reason, operation_uuid)
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
            if let Err(persist_error) = pool
                .state()
                .update_tombstone_operation(
                    &request.operation_id,
                    phase_to_result_status(operation.phase),
                    scope,
                    Some(merge_preview_summary(
                        preview_summary.clone(),
                        json!({
                            "message": "Failed to execute tombstone",
                            "error": e.to_string(),
                        }),
                    )),
                    Some("Failed to execute tombstone"),
                    None,
                )
                .await
            {
                warn!(
                    operation_id = %request.operation_id,
                    error = %persist_error,
                    "Failed to persist tombstone execution failure"
                );
            }
            return Err(
                SinexError::database("Failed to execute tombstone").with_source(e.to_string())
            );
        }
    };

    // Delete-on-tombstone for source materials whose only references were the
    // events we just tombstoned. (#987.) Failures here are logged but do not
    // fail the tombstone operation — the tombstone itself succeeded; orphan
    // material rows and orphan blobs will get cleaned up by the GC sweeper if
    // this path falls back. Both halves (registry row + CAS blob) are best-effort
    // because either failing would otherwise undo a successful tombstone.
    let orphan_material_ids = match materials_repo
        .find_orphan_materials(&candidate_material_ids)
        .await
    {
        Ok(ids) => ids,
        Err(e) => {
            warn!(
                operation_id = %request.operation_id,
                error = %e,
                "Failed to find orphan materials post-tombstone; GC sweeper will recover"
            );
            Vec::new()
        }
    };

    if !orphan_material_ids.is_empty() {
        let mut blobs_dropped = 0_usize;
        let mut rows_deleted = 0_usize;
        for material_id in &orphan_material_ids {
            // Resolve the material to find its blob_id (if any) before deleting the row.
            let material_record = match materials_repo
                .get_by_id(sinex_primitives::Id::from_uuid(*material_id))
                .await
            {
                Ok(Some(record)) => Some(record),
                Ok(None) => None,
                Err(e) => {
                    warn!(
                        material_id = %material_id,
                        error = %e,
                        "Failed to read material before delete-on-tombstone"
                    );
                    None
                }
            };

            // Drop the CAS blob first (idempotent: drop_content tolerates missing files).
            if let Some(record) = &material_record
                && let Some(blob_uuid) = record.optional_blob_id
            {
                let content_store = services.content.content_store();
                // Translate blob UUID to a content-store key by looking up core.blobs.
                match pool
                    .blobs()
                    .get_by_id(sinex_primitives::Id::from_uuid(blob_uuid))
                    .await
                {
                    Ok(Some(blob_row)) => {
                        if let Err(e) = content_store
                            .drop_content(&blob_row.content_key(), true)
                            .await
                        {
                            warn!(
                                material_id = %material_id,
                                blob_id = %blob_uuid,
                                error = %e,
                                "Failed to drop CAS content for tombstoned material; \
                                 GC sweeper will recover the orphan blob"
                            );
                        } else {
                            blobs_dropped += 1;
                        }
                    }
                    Ok(None) => {
                        // Blob row already gone; treat as success.
                    }
                    Err(e) => {
                        warn!(
                            material_id = %material_id,
                            blob_id = %blob_uuid,
                            error = %e,
                            "Failed to look up blob for delete-on-tombstone"
                        );
                    }
                }
            }

            // Drop the registry row regardless of blob outcome — if the blob drop
            // failed, GC will eventually catch up; what matters is the row goes.
            match materials_repo
                .delete_material(sinex_primitives::Id::from_uuid(*material_id))
                .await
            {
                Ok(true) => rows_deleted += 1,
                Ok(false) => {} // already gone
                Err(e) => warn!(
                    material_id = %material_id,
                    error = %e,
                    "Failed to delete orphan material registry row"
                ),
            }
        }
        info!(
            operation_id = %request.operation_id,
            materials_examined = candidate_material_ids.len(),
            orphans_found = orphan_material_ids.len(),
            rows_deleted = rows_deleted,
            blobs_dropped = blobs_dropped,
            "Delete-on-tombstone for orphan source materials"
        );
    }

    // Mark as completed and persist
    let finished_at = Timestamp::now();
    let duration_ms = tombstone_duration_ms(&operation, finished_at)?.unwrap_or(0);
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
            Some(merge_preview_summary(
                preview_summary,
                json!({
                    "message": "Tombstone operation completed",
                    "tombstoned_count": tombstoned_count,
                }),
            )),
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

    Ok(TombstoneApproveResponse { operation })
}

/// Handle lifecycle.tombstone.cancel
///
/// Cancels a pending tombstone operation.
pub async fn handle_tombstone_cancel(
    pool: &PgPool,
    request: TombstoneCancelRequest,
    auth: &crate::api::rpc_server::RpcAuthContext,
) -> Result<TombstoneCancelResponse> {
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

    let mut operation = operation_record_to_tombstone(&record)?;
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

    if !operation.state.is_cancellable() {
        return Err(SinexError::invalid_state(format!(
            "Cannot cancel operation in state {:?}",
            operation.state
        )));
    }

    let finished_at = Timestamp::now();
    operation.state = TombstoneOperationState::Cancelled;
    operation.finished_at = Some(finished_at.format_rfc3339());
    operation.error_details = Some(match request.reason.as_deref() {
        Some(reason) => format!("Cancelled by {}: {reason}", auth.actor_id()),
        None => format!("Cancelled by {}", auth.actor_id()),
    });
    sync_tombstone_phase(&mut operation);

    let scope = serde_json::to_value(&operation)?;
    pool.state()
        .update_tombstone_operation(
            &request.operation_id,
            phase_to_result_status(operation.phase),
            scope,
            record.preview_summary.clone(),
            Some("Tombstone operation cancelled"),
            tombstone_duration_ms(&operation, finished_at)?,
        )
        .await
        .map_err(|e| {
            SinexError::database("Failed to cancel tombstone operation").with_source(e.to_string())
        })?;

    info!(
        operation_id = %request.operation_id,
        actor = %auth.actor_id(),
        "Tombstone operation cancelled"
    );

    Ok(TombstoneCancelResponse {
        status: "cancelled".to_string(),
        operation_id: request.operation_id,
    })
}

/// Handle lifecycle.tombstone.list
///
/// Lists tombstone operations, optionally filtered by state.
pub async fn handle_tombstone_list(
    pool: &PgPool,
    request: TombstoneListRequest,
    _auth: &crate::api::rpc_server::RpcAuthContext,
) -> Result<TombstoneListResponse> {
    let limit = require_positive_limit("lifecycle.tombstone.list", request.limit.unwrap_or(100))?;
    let records = pool
        .state()
        .list_tombstone_operations(request.state, limit)
        .await
        .map_err(|e| {
            SinexError::database("Failed to list tombstone operations").with_source(e.to_string())
        })?;

    // Convert records to TombstoneOperations
    let mut operations = Vec::new();
    for record in &records {
        let mut operation = operation_record_to_tombstone(record)?;
        let operation_id = operation.operation_id.clone();
        reconcile_tombstone_expiry(
            pool,
            &operation_id,
            &mut operation,
            record.preview_summary.clone(),
        )
        .await
        .map_err(|e| {
            SinexError::database("Failed to reconcile tombstone expiry").with_source(e.to_string())
        })?;
        if matches_requested_tombstone_state(request.state, &operation) {
            operations.push(operation);
        }
    }

    // Sort by created_at descending (DB already returns in id DESC order, but created_at may differ)
    operations.sort_by(|a, b| b.created_at.cmp(&a.created_at));

    Ok(TombstoneListResponse { operations })
}

/// Handle lifecycle.tombstone.status
///
/// Gets the status of a specific tombstone operation.
pub async fn handle_tombstone_status(
    pool: &PgPool,
    request: TombstoneStatusRequest,
    _auth: &crate::api::rpc_server::RpcAuthContext,
) -> Result<TombstoneStatusResponse> {
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

    let mut operation = operation_record_to_tombstone(&record)?;
    reconcile_tombstone_expiry(
        pool,
        &request.operation_id,
        &mut operation,
        record.preview_summary.clone(),
    )
    .await?;

    Ok(TombstoneStatusResponse { operation })
}

#[cfg(test)]
#[path = "lifecycle_test.rs"]
mod tests;
