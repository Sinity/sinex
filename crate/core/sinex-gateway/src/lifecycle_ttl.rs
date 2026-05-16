//! Hourly TTL enforcement task (#1172, AC-5).
//!
//! Walks `sinex_schemas.event_payload_schemas.retention_seconds` (added in
//! Phase 1 of #1172) and, for every active schema with a non-NULL retention
//! horizon, archives matching live events older than the horizon via the
//! existing `lifecycle.archive` cascade machinery (`handle_lifecycle_archive`
//! in `handlers/lifecycle.rs`).
//!
//! Leader-elected: only the gateway instance holding the
//! `gateway-ttl-enforcer` leadership key acts in any given tick. The
//! coordinator surface is the existing `CoordinationKvClient` (NATS KV with
//! `max_age = leadership_timeout`). If coordination is unavailable the task
//! degrades to "no-op until next tick" rather than running everywhere.
//!
//! TODO(#1172): swap the raw `sqlx::query!` retention SELECT for the typed
//! `EventPayloadSchemaRecord.retention_seconds` accessor once Phase 1 lands
//! the column on `EventPayloadSchemaRow` and exposes it via the schema
//! repository. Today the repository SELECT lists do not project the new
//! column, so we run a small bespoke query here and revisit.
//!
//! Cadence: once per hour by default. The first tick fires shortly after
//! startup so a freshly deployed gateway begins enforcing without waiting
//! a full hour.

use crate::service_container::ServiceContainer;
use serde_json::json;
use sinex_db::{CascadeSource, DbPoolExt};
use sinex_primitives::domain::OperationStatus;
use sinex_primitives::error::SinexError;
use sinex_primitives::{Result, Uuid};
use sqlx::PgPool;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tracing::{debug, info, warn};

/// Default cadence between TTL sweeps.
const TTL_TICK_INTERVAL: Duration = Duration::from_hours(1);

/// Initial delay before the first sweep, to avoid stampeding the DB at boot.
const TTL_INITIAL_DELAY: Duration = Duration::from_mins(1);

/// Leadership key for the TTL enforcer (single owner across the deployment).
const TTL_LEADERSHIP_KEY: &str = "gateway-ttl-enforcer";

/// Spawn the hourly TTL enforcement task.
///
/// Returns a `JoinHandle`; the caller is responsible for owning it for the
/// gateway's lifetime so the task is cancelled on shutdown via the shared
/// watch channel.
#[must_use]
pub fn spawn_ttl_task(
    services: ServiceContainer,
    instance_id: String,
    mut shutdown: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        // Hold off briefly so a freshly started gateway doesn't race the
        // schema-apply / coordination-bootstrap phase.
        tokio::select! {
            () = tokio::time::sleep(TTL_INITIAL_DELAY) => {}
            _ = shutdown.changed() => return,
        }

        let mut interval = tokio::time::interval(TTL_TICK_INTERVAL);
        // The default behaviour is to drop missed ticks; we want that.
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if let Err(error) = run_ttl_sweep(&services, &instance_id).await {
                        warn!(error = %error, "TTL sweep failed; continuing to next tick");
                    }
                }
                _ = shutdown.changed() => {
                    debug!("TTL enforcement task shutting down");
                    break;
                }
            }
        }
    })
}

async fn run_ttl_sweep(services: &ServiceContainer, instance_id: &str) -> Result<()> {
    // Leader gate.
    let coord = if let Some(client) = services.coordination.as_ref() { Arc::clone(client) } else {
        debug!("TTL sweep skipped — coordination client not configured");
        return Ok(());
    };

    // `acquire_leadership` is keyed by the candidate id only — there is no
    // service-scope key argument in the current `CoordinationKvClient` API,
    // so we use the per-process `instance_id` as the candidate. The KV
    // bucket has `max_age = leadership_timeout`, so a stale leader expires
    // automatically. `TTL_LEADERSHIP_KEY` exists for future log/audit hooks
    // and to anchor the role name across the codebase.
    debug!(
        leadership = TTL_LEADERSHIP_KEY,
        instance = %instance_id,
        "TTL sweep — attempting leadership acquire"
    );
    let acquired = match coord.acquire_leadership(instance_id).await {
        Ok(value) => value,
        Err(error) => {
            warn!(error = %error, "Failed to probe TTL leadership; skipping sweep");
            return Ok(());
        }
    };
    if !acquired {
        debug!("TTL sweep skipped — not the elected leader");
        return Ok(());
    }

    let pool = services.pool();
    let entries = fetch_retention_entries(pool).await?;
    if entries.is_empty() {
        debug!("TTL sweep: no schemas declare retention_seconds");
        return Ok(());
    }

    let mut total_archived = 0usize;
    for entry in &entries {
        match archive_expired_for_event_type(pool, instance_id, entry).await {
            Ok(count) => {
                total_archived += count;
                if count > 0 {
                    info!(
                        source = %entry.source,
                        event_type = %entry.event_type,
                        retention_seconds = entry.retention_seconds,
                        archived = count,
                        "TTL sweep archived expired events"
                    );
                }
            }
            Err(error) => {
                warn!(
                    source = %entry.source,
                    event_type = %entry.event_type,
                    error = %error,
                    "TTL sweep: archive failed for event_type"
                );
            }
        }
    }

    if total_archived > 0 {
        info!(
            archived = total_archived,
            schemas = entries.len(),
            "TTL sweep complete"
        );
    }
    Ok(())
}

/// One row from `event_payload_schemas` with a non-NULL retention horizon.
struct RetentionEntry {
    source: String,
    event_type: String,
    retention_seconds: i64,
}

async fn fetch_retention_entries(pool: &PgPool) -> Result<Vec<RetentionEntry>> {
    // TODO(#1172): swap to a typed `pool.schemas().list_with_retention()`
    // accessor once Phase 1's column lands on the schema repository's SELECT
    // lists. Until then, query directly so we don't depend on the typed row
    // shape (which `sqlx::query!` would lock at compile time).
    let rows = sqlx::query(
        r"
        SELECT source, event_type, retention_seconds
        FROM sinex_schemas.event_payload_schemas
        WHERE retention_seconds IS NOT NULL AND is_active = true
        ",
    )
    .fetch_all(pool)
    .await
    .map_err(|error| {
        SinexError::database("Failed to fetch retention horizons").with_source(error.to_string())
    })?;

    use sqlx::Row;
    Ok(rows
        .into_iter()
        .map(|row| RetentionEntry {
            source: row.get::<String, _>("source"),
            event_type: row.get::<String, _>("event_type"),
            retention_seconds: row.get::<i64, _>("retention_seconds"),
        })
        .collect())
}

/// Limit per (source, `event_type`) per sweep — protects against runaway
/// archives if a horizon is shortened on a high-volume event type.
const TTL_BATCH_LIMIT: i64 = 10_000;

async fn archive_expired_for_event_type(
    pool: &PgPool,
    instance_id: &str,
    entry: &RetentionEntry,
) -> Result<usize> {
    // 1. Find expired live event IDs (cap to TTL_BATCH_LIMIT).
    //    Keyed on `ts_orig` per the plan: "events older than ts_orig - retention".
    let cutoff_secs = entry.retention_seconds;
    let rows = sqlx::query(
        r"
        SELECT id
        FROM core.events
        WHERE source = $1
          AND event_type = $2
          AND ts_orig < (NOW() - make_interval(secs => $3))
        ORDER BY ts_orig ASC
        LIMIT $4
        ",
    )
    .bind(&entry.source)
    .bind(&entry.event_type)
    .bind(cutoff_secs as f64)
    .bind(TTL_BATCH_LIMIT)
    .fetch_all(pool)
    .await
    .map_err(|error| {
        SinexError::database("Failed to find expired events for TTL sweep")
            .with_source(error.to_string())
    })?;

    use sqlx::Row;
    let ids: Vec<Uuid> = rows
        .into_iter()
        .map(|row| row.get::<Uuid, _>("id"))
        .collect();

    if ids.is_empty() {
        return Ok(0);
    }

    // 2. Walk the cascade so dependent synthesis events follow their parents
    //    into the archive.
    let session_prefix = format!("ttl_{instance_id}");
    let (cascade_ids, _depth) =
        collect_cascade_ids(pool, &session_prefix, &ids, CascadeSource::Live).await?;

    let cascade_total = cascade_ids.len();

    // 3. Persist a TTL operation in operations_log for traceability.
    let actor = format!("ttl_enforcer:{instance_id}");
    let scope = json!({
        "policy": "ttl",
        "source": entry.source,
        "event_type": entry.event_type,
        "retention_seconds": entry.retention_seconds,
        "root_event_count": ids.len(),
        "cascade_total": cascade_total,
    });
    let operation = pool
        .state()
        .start_operation("archive", &actor, scope)
        .await
        .map_err(|error| {
            SinexError::database("Failed to record TTL archive operation")
                .with_source(error.to_string())
        })?;

    // 4. Execute the archive — DELETE with session vars triggers
    //    `core.fn_archive_before_delete` which moves rows to
    //    `audit.archived_events`.
    let session_id = format!("{}_{}", session_prefix, Uuid::now_v7().simple());
    let archive_outcome = execute_ttl_archive(
        pool,
        &session_id,
        &operation.id.to_uuid(),
        &actor,
        &cascade_ids,
    )
    .await;

    let archived = match archive_outcome {
        Ok(count) => {
            pool.state()
                .complete_operation(
                    &operation.id,
                    json!({
                        "archived_event_ids": cascade_ids
                            .iter()
                            .map(std::string::ToString::to_string)
                            .collect::<Vec<_>>(),
                        "cascade_total": cascade_total,
                        "policy": "ttl",
                    }),
                )
                .await
                .ok();
            count
        }
        Err(error) => {
            pool.state()
                .update_operation_meta(
                    &operation.id,
                    OperationStatus::Failed,
                    Some("TTL archive failed"),
                    json!({"error": error.to_string()}),
                )
                .await
                .ok();
            return Err(error);
        }
    };

    Ok(archived)
}

async fn execute_ttl_archive(
    pool: &PgPool,
    session_id: &str,
    operation_id: &Uuid,
    actor: &str,
    cascade_ids: &[Uuid],
) -> Result<usize> {
    if cascade_ids.is_empty() {
        return Ok(0);
    }

    let mut tx = pool.begin().await.map_err(|error| {
        SinexError::database("Failed to begin TTL archive transaction")
            .with_source(error.to_string())
    })?;

    // Set session vars consumed by the archive trigger.
    sqlx::query("SELECT pg_catalog.set_config('sinex.operation_id', $1, true)")
        .bind(operation_id.to_string())
        .execute(&mut *tx)
        .await
        .map_err(|error| {
            SinexError::database("Failed to set operation_id for TTL archive")
                .with_source(error.to_string())
        })?;
    sqlx::query("SELECT pg_catalog.set_config('sinex.archived_by', $1, true)")
        .bind(actor)
        .execute(&mut *tx)
        .await
        .map_err(|error| {
            SinexError::database("Failed to set archived_by for TTL archive")
                .with_source(error.to_string())
        })?;
    sqlx::query("SELECT pg_catalog.set_config('sinex.archive_reason', $1, true)")
        .bind(format!("TTL session {session_id}"))
        .execute(&mut *tx)
        .await
        .map_err(|error| {
            SinexError::database("Failed to set archive_reason for TTL archive")
                .with_source(error.to_string())
        })?;

    let result = sqlx::query("DELETE FROM core.events WHERE id = ANY($1)")
        .bind(cascade_ids)
        .execute(&mut *tx)
        .await
        .map_err(|error| {
            SinexError::database("TTL archive DELETE failed").with_source(error.to_string())
        })?;

    tx.commit().await.map_err(|error| {
        SinexError::database("Failed to commit TTL archive").with_source(error.to_string())
    })?;

    Ok(result.rows_affected() as usize)
}

async fn collect_cascade_ids(
    pool: &PgPool,
    session_prefix: &str,
    root_ids: &[Uuid],
    source: CascadeSource,
) -> Result<(Vec<Uuid>, usize)> {
    let session_id = format!("{}_{}", session_prefix, Uuid::now_v7().simple());

    pool.with_transaction(async |tx| {
        let repo = pool.events();
        let mut repo_tx = repo.as_tx(tx);

        let table_name = repo_tx
            .prepare_cascade_session(&session_id, true)
            .await
            .map_err(|e| {
                SinexError::database("Failed to prepare TTL cascade session")
                    .with_source(e.to_string())
            })?;
        repo_tx
            .populate_cascade_roots_from(&table_name, root_ids, source)
            .await
            .map_err(|e| {
                SinexError::database("Failed to populate TTL cascade roots")
                    .with_source(e.to_string())
            })?;
        let depth = repo_tx
            .expand_cascade_from(&table_name, 100, source)
            .await
            .map_err(|e| {
                SinexError::database("Failed to expand TTL cascade").with_source(e.to_string())
            })?;
        let ids = repo_tx.get_cascade_ids(&table_name).await.map_err(|e| {
            SinexError::database("Failed to get TTL cascade IDs").with_source(e.to_string())
        })?;
        repo_tx
            .cleanup_cascade_session(&table_name)
            .await
            .map_err(|e| {
                SinexError::database("Failed to cleanup TTL cascade session")
                    .with_source(e.to_string())
            })?;
        Ok((ids, depth))
    })
    .await
}
