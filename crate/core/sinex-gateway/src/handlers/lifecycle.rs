//! Data lifecycle RPC handlers
//!
//! Implements the three-tier data lifecycle: Live ↔ Archive → Tombstone

use serde_json::Value;
use sinex_db::DbPoolExt;
use sinex_primitives::domain::EventSource;
use sinex_primitives::rpc::lifecycle::{
    LifecycleArchiveRequest, LifecycleRestoreRequest, LifecycleRestoreResponse,
    LifecycleStatusRequest, LifecycleStatusResponse, LifecycleTombstoneRequest,
    LifecycleTombstoneResponse, TierStatus,
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
/// Note: Archive is triggered by DELETE with sinex.operation_id set.
/// This handler coordinates that process with cascade analysis.
pub async fn handle_lifecycle_archive(
    _pool: &PgPool,
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

    // For now, return an error since archive requires DELETE coordination
    // which is more complex (need to set session variables, cascade analyze, then delete)
    Err(SinexError::invalid_state(
        "Archive operation requires DELETE coordination. \
         Use replay operations to archive events, or implement via cascade analyzer.",
    ))
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
            Ulid::from_str(s).map_err(|_| SinexError::validation(format!("Invalid ULID: {}", s)))
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

/// Handle lifecycle.tombstone - permanently delete archived events
///
/// WARNING: This is a ONE-WAY operation. Data is permanently deleted.
pub async fn handle_lifecycle_tombstone(
    pool: &PgPool,
    params: Value,
    auth: &crate::rpc_server::RpcAuthContext,
) -> Result<Value> {
    let request: LifecycleTombstoneRequest = serde_json::from_value(params)?;

    info!(
        token_prefix = %auth.token_prefix,
        source = ?request.source,
        before = ?request.before,
        dry_run = request.dry_run,
        "Lifecycle tombstone operation initiated"
    );

    let repo = pool.events();

    // Determine which archived events to tombstone
    let before_ts = if let Some(before_str) = &request.before {
        parse_duration_to_timestamp(before_str)?
    } else {
        None
    };

    let source = request.source.as_ref().map(|s| EventSource::new(s.clone()));

    // Get archived event IDs matching filters
    let event_ids = if let Some(ids) = &request.event_ids {
        ids.iter()
            .map(|s| {
                Ulid::from_str(s)
                    .map_err(|_| SinexError::validation(format!("Invalid ULID: {}", s)))
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

    let max_depth = repo
        .expand_cascade_from_archive(&table_name, 100)
        .await
        .map_err(|e| SinexError::database("Failed to expand cascade").with_source(e.to_string()))?;

    let cascade_ids = repo.get_cascade_ids(&table_name).await.map_err(|e| {
        SinexError::database("Failed to get cascade IDs").with_source(e.to_string())
    })?;
    let cascade_total = cascade_ids.len();

    repo.cleanup_cascade_session(&table_name)
        .await
        .map_err(|e| {
            SinexError::database("Failed to cleanup cascade session").with_source(e.to_string())
        })?;

    let operation_id = Ulid::new();

    if request.dry_run {
        let response = LifecycleTombstoneResponse {
            tombstoned_count: 0,
            cascade_depth: max_depth,
            cascade_total,
            operation_id: operation_id.to_string(),
            dry_run: true,
        };
        return Ok(serde_json::to_value(response)?);
    }

    // Execute tombstone (PERMANENT!)
    let tombstoned_count = repo
        .execute_cascade_tombstone(&cascade_ids, &request.reason, operation_id)
        .await
        .map_err(|e| {
            SinexError::database("Failed to execute cascade tombstone").with_source(e.to_string())
        })?;

    info!(
        operation_id = %operation_id,
        tombstoned_count = tombstoned_count,
        cascade_total = cascade_total,
        reason = %request.reason,
        "Tombstone operation completed (PERMANENT)"
    );

    let response = LifecycleTombstoneResponse {
        tombstoned_count,
        cascade_depth: max_depth,
        cascade_total,
        operation_id: operation_id.to_string(),
        dry_run: false,
    };

    Ok(serde_json::to_value(response)?)
}

/// Parse a duration string (e.g., "30d", "90d") to a timestamp in the past
fn parse_duration_to_timestamp(duration_str: &str) -> Result<Option<Timestamp>> {
    let duration = humantime::parse_duration(duration_str).map_err(|e| {
        SinexError::validation(format!("Invalid duration '{}': {}", duration_str, e))
    })?;

    let ts = Timestamp::now() - time::Duration::seconds(duration.as_secs() as i64);
    Ok(Some(ts))
}
