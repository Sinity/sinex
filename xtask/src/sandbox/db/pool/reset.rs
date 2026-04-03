//! Database reset — cleaning, verification, seeding, session state, triggers.

use crate::sandbox::prelude::*;
use crate::sandbox::slog::{Level, slog};
use futures::future::BoxFuture;
use sinex_db::DbPool;
use sqlx::postgres::PgConnection;
use std::sync::atomic::Ordering;
use std::time::Duration;

use super::config::SLOT_MAX_CONNECTIONS;
use super::metrics::POOL_METRICS;
use super::provisioning::{
    is_retryable_connection_report, is_timescaledb_missing_library_report, recreate_pool_database,
};
use super::slot::DatabaseSlot;

// ── Clean database ──────────────────────────────────────────────────────────

/// Clean a database for reuse
pub(super) async fn clean_database(
    slot: &Arc<DatabaseSlot>,
    pool: &DbPool,
    db_name: &str,
    db_url: &str,
) -> TestResult<()> {
    let mut working_pool = pool.clone();
    let mut residuals: Option<Vec<(String, i64)>> = None;
    let mut schema_recreated = false;

    let clean_start = std::time::Instant::now();
    let mut phase_times: Vec<(&str, std::time::Duration)> = Vec::new();
    let mut attempt = 0usize;
    loop {
        attempt += 1;
        let t = std::time::Instant::now();

        // Schema check is cached per-slot: if it passed once, the schema hasn't changed
        // (no migrations run between tests). Only re-check after quarantine/recreation.
        let skip_schema = slot.schema_verified.load(Ordering::Relaxed);
        if !skip_schema {
            if let Some(reason) = schema_mismatch_reason(&working_pool).await? {
                if schema_recreated {
                    let err = eyre!(format!(
                        "Database {db_name} schema mismatch after recreation: {reason}"
                    ));
                    slot.record_clean_result(Err(err.to_string()), residuals.clone());
                    slot.quarantined.store(true, Ordering::SeqCst);
                    slot.schema_verified.store(false, Ordering::SeqCst);
                    return Err(err);
                }

                slog!(
                    Level::Info,
                    "schema_mismatch",
                    slot = db_name,
                    reason = reason
                );
                recreate_pool_database(db_name, db_url)
                    .await
                    .map_err(|recreate_err| {
                        POOL_METRICS.record_cleanup_failure();
                        eyre!(format!(
                            "Schema mismatch recreate failed for {db_name}: {recreate_err}"
                        ))
                    })?;
                let fresh_pool =
                    super::slot_pool_options(SLOT_MAX_CONNECTIONS, Duration::from_secs(5))
                        .connect(db_url)
                        .await?;
                working_pool = fresh_pool;
                schema_recreated = true;
                slot.schema_verified.store(false, Ordering::SeqCst);
                continue;
            }
            // Schema check passed — cache result for future cleanups on this slot
            slot.schema_verified.store(true, Ordering::Relaxed);
        }

        phase_times.push(("schema_check", t.elapsed()));

        // Use the shared db_common implementation (TRUNCATE CASCADE doesn't need
        // exclusive access, so we skip preemptive pg_terminate_backend — it would
        // kill our own pool's idle connections and cause "terminating connection" errors)
        let t = std::time::Instant::now();
        match crate::sandbox::db::pool::reset_database(&working_pool).await {
            Ok(()) => {
                phase_times.push(("reset_database", t.elapsed()));
                let t = std::time::Instant::now();
                if let Err(verify_err) =
                    crate::sandbox::db::pool::verify_clean_state(&working_pool).await
                {
                    if attempt >= 2 {
                        POOL_METRICS.record_cleanup_failure();
                        let residual_probe = log_remaining_rows(&working_pool).await;
                        residuals = residual_probe.as_ref().ok().cloned();
                        let residual_probe_suffix = residual_probe
                            .err()
                            .map(|error| format!("; residual row probe failed: {error}"))
                            .unwrap_or_default();
                        let err = eyre!(format!(
                            "Database {db_name} cleanup failed: {verify_err}{residual_probe_suffix}"
                        ));
                        slot.record_clean_result(Err(err.to_string()), residuals.clone());
                        slot.quarantined.store(true, Ordering::SeqCst);
                        slot.schema_verified.store(false, Ordering::SeqCst);
                        return Err(err);
                    }
                    slog!(
                        Level::Warn,
                        "verify_failed_retry",
                        slot = db_name,
                        error = verify_err
                    );
                    continue;
                }

                phase_times.push(("verify_clean", t.elapsed()));
                let t = std::time::Instant::now();
                ensure_default_session_state(&working_pool).await?;
                phase_times.push(("session_state", t.elapsed()));
                let t = std::time::Instant::now();
                seed_test_fixtures(&working_pool).await?;
                phase_times.push(("seed_fixtures", t.elapsed()));
                slot.quarantined.store(false, Ordering::SeqCst);
                slot.record_clean_result(Ok(()), residuals.clone());

                // Log phase breakdown when cleanup is slow (>2s)
                let total = clean_start.elapsed();
                if total.as_secs() >= 2 {
                    let phases: Vec<String> = phase_times
                        .iter()
                        .filter(|(_, d)| d.as_millis() > 50)
                        .map(|(name, d)| format!("{name}={d:.1?}"))
                        .collect();
                    if !phases.is_empty() {
                        slog!(
                            Level::Warn,
                            "cleanup_slow",
                            slot = db_name,
                            total_ms = total.as_millis(),
                            phases = phases.join(",")
                        );
                    }
                }

                return Ok(());
            }
            Err(e) => {
                let retryable =
                    is_retryable_connection_report(&e) || is_timescaledb_missing_library_report(&e);

                if retryable && attempt < 3 {
                    slog!(
                        Level::Warn,
                        "cleanup_conn_error",
                        slot = db_name,
                        error = e,
                        attempt = attempt
                    );
                    recreate_pool_database(db_name, db_url)
                        .await
                        .map_err(|recreate_err| {
                            POOL_METRICS.record_cleanup_failure();
                            eyre!(format!(
                                "Cleanup failed and recreate failed for {db_name}: {recreate_err}"
                            ))
                        })?;
                    // Fresh pool for the recreated database
                    let fresh_pool =
                        super::slot_pool_options(SLOT_MAX_CONNECTIONS, Duration::from_secs(5))
                            .connect(db_url)
                            .await?;
                    working_pool = fresh_pool;
                    continue;
                }

                slog!(
                    Level::Error,
                    "cleanup_critical_failure",
                    slot = db_name,
                    error = e
                );
                POOL_METRICS.record_cleanup_failure();
                let residual_probe = log_remaining_rows(&working_pool).await;
                residuals = residual_probe.as_ref().ok().cloned();
                let residual_probe_suffix = residual_probe
                    .err()
                    .map(|error| format!("; residual row probe failed: {error}"))
                    .unwrap_or_default();

                // Attempt one last forced cleanup focusing on stubborn event/material rows.
                if let Err(force_err) = force_event_material_cleanup(&working_pool).await {
                    let err = eyre!(format!(
                        "Database {db_name} cleanup failed: {e}{residual_probe_suffix}; forced cleanup also failed: {force_err}"
                    ));
                    slot.record_clean_result(Err(err.to_string()), residuals.clone());
                    slot.quarantined.store(true, Ordering::SeqCst);
                    slot.schema_verified.store(false, Ordering::SeqCst);
                    return Err(err);
                }

                if let Err(verify_err) =
                    crate::sandbox::db::pool::verify_clean_state(&working_pool).await
                {
                    let err = eyre!(format!(
                        "Database {db_name} cleanup failed after forced cleanup: {verify_err}"
                    ));
                    slot.record_clean_result(Err(err.to_string()), residuals.clone());
                    slot.quarantined.store(true, Ordering::SeqCst);
                    slot.schema_verified.store(false, Ordering::SeqCst);
                    return Err(err);
                }

                slog!(Level::Info, "cleanup_recovered", slot = db_name);
                ensure_default_session_state(&working_pool).await?;
                seed_test_fixtures(&working_pool).await?;
                slot.quarantined.store(false, Ordering::SeqCst);
                slot.record_clean_result(Ok(()), residuals.clone());
                return Ok(());
            }
        }
    }
}

// ── Diagnostics ─────────────────────────────────────────────────────────────

async fn log_remaining_rows(pool: &DbPool) -> TestResult<Vec<(String, i64)>> {
    let counts = crate::sandbox::db::common::get_row_counts(pool)
        .await
        .map_err(|error| eyre!("failed to inspect residual row counts: {error}"))?;
    let mut residuals = Vec::new();
    for (table, count) in counts {
        if count > 0 {
            slog!(Level::Warn, "residual_rows", table = table, count = count);
            residuals.push((table, count));
        }
    }
    Ok(residuals)
}

async fn inspect_force_cleanup_counts(pool: &DbPool) -> TestResult<(i64, i64)> {
    let counts = crate::sandbox::db::common::get_row_counts(pool)
        .await
        .map_err(|error| eyre!("forced cleanup could not inspect remaining row counts: {error}"))?;
    Ok((
        *counts.get("core.events").unwrap_or(&0),
        *counts.get("raw.source_material_registry").unwrap_or(&0),
    ))
}

// ── Trigger management ──────────────────────────────────────────────────────

async fn core_events_trigger_exists(pool: &DbPool, trigger_name: &str) -> TestResult<bool> {
    sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS (SELECT 1 FROM pg_trigger \
         WHERE tgrelid = to_regclass('core.events') \
           AND tgname = $1 \
           AND NOT tgisinternal)",
    )
    .bind(trigger_name)
    .fetch_one(pool)
    .await
    .map_err(|e| eyre!(e.to_string()))
}

async fn core_events_triggers_missing_reason(pool: &DbPool) -> TestResult<Option<String>> {
    let has_no_update = core_events_trigger_exists(pool, "trg_events_no_update").await?;
    let has_archive = core_events_trigger_exists(pool, "trg_events_archive_before_delete").await?;

    if has_no_update && has_archive {
        return Ok(None);
    }

    let mut missing = Vec::new();
    if !has_no_update {
        missing.push("trg_events_no_update");
    }
    if !has_archive {
        missing.push("trg_events_archive_before_delete");
    }

    Ok(Some(format!(
        "missing core.events triggers ({})",
        missing.join(", ")
    )))
}

async fn ensure_core_events_triggers(pool: &DbPool) -> TestResult<()> {
    let missing_reason = core_events_triggers_missing_reason(pool).await?;
    if missing_reason.is_none() {
        return Ok(());
    }

    let mut conn = pool.acquire().await?;

    if !core_events_trigger_exists(pool, "trg_events_no_update").await? {
        sqlx::query(
            r"
            CREATE OR REPLACE FUNCTION core.fn_events_no_update()
            RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
                RAISE EXCEPTION 'UPDATE on core.events is forbidden';
            END $$;
            ",
        )
        .execute(&mut *conn)
        .await
        .map_err(|e| eyre!(e.to_string()))?;
        sqlx::query("DROP TRIGGER IF EXISTS trg_events_no_update ON core.events")
            .execute(&mut *conn)
            .await
            .map_err(|e| eyre!(e.to_string()))?;
        sqlx::query(
            "CREATE TRIGGER trg_events_no_update \
             BEFORE UPDATE ON core.events \
             FOR EACH ROW EXECUTE FUNCTION core.fn_events_no_update()",
        )
        .execute(&mut *conn)
        .await
        .map_err(|e| eyre!(e.to_string()))?;
        slog!(
            Level::Warn,
            "trigger_restored",
            trigger = "trg_events_no_update"
        );
    }

    if !core_events_trigger_exists(pool, "trg_events_archive_before_delete").await? {
        sqlx::query(
            r"
            CREATE OR REPLACE FUNCTION core.fn_archive_before_delete()
            RETURNS trigger LANGUAGE plpgsql AS $$
            DECLARE
              op_id TEXT := current_setting('sinex.operation_id', true);
              sup_id uuid := NULLIF(current_setting('sinex.superseded_by_id', true), '');
              who TEXT := current_setting('sinex.archived_by', true);
              why TEXT := current_setting('sinex.archive_reason', true);
            BEGIN
              IF op_id IS NULL OR op_id = '' THEN
                RAISE EXCEPTION 'DELETE on core.events requires sinex.operation_id to be set in this session';
              END IF;

              INSERT INTO audit.archived_events SELECT OLD.*, now(), who, why, sup_id;
              RETURN OLD;
            END $$;
            ",
        )
        .execute(&mut *conn)
        .await
        .map_err(|e| eyre!(e.to_string()))?;
        sqlx::query("DROP TRIGGER IF EXISTS trg_events_archive_before_delete ON core.events")
            .execute(&mut *conn)
            .await
            .map_err(|e| eyre!(e.to_string()))?;
        sqlx::query(
            "CREATE TRIGGER trg_events_archive_before_delete \
             BEFORE DELETE ON core.events \
             FOR EACH ROW EXECUTE FUNCTION core.fn_archive_before_delete()",
        )
        .execute(&mut *conn)
        .await
        .map_err(|e| eyre!(e.to_string()))?;
        slog!(
            Level::Warn,
            "trigger_restored",
            trigger = "trg_events_archive_before_delete"
        );
    }

    Ok(())
}

pub(super) async fn ensure_pool_db_invariants(db_url: &str) -> TestResult<()> {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .acquire_timeout(Duration::from_secs(5))
        .connect(db_url)
        .await
        .map_err(|e| eyre!(e.to_string()))?;

    let result = ensure_core_events_triggers(&pool).await;
    pool.close().await;
    result
}

// ── Schema mismatch detection ───────────────────────────────────────────────

/// Consolidated schema mismatch detection — single query instead of 7 sequential round-trips.
///
/// Previous implementation ran 7 separate queries (~80ms each = ~560ms total).
/// This batches all checks into one query for a single round-trip (~40ms).
pub(super) async fn schema_mismatch_reason(pool: &DbPool) -> TestResult<Option<String>> {
    let drift = sinex_schema::apply::diff(pool).await?;
    if drift.is_empty() {
        Ok(None)
    } else {
        Ok(Some(format!("schema drift: {}", drift.join(", "))))
    }
}

// ── Session state ───────────────────────────────────────────────────────────

async fn ensure_default_session_state_conn(conn: &mut PgConnection) -> TestResult<()> {
    // Check all session settings in one query (3 round-trips → 1).
    let row = sqlx::query_as::<_, (String, String, String)>(
        "SELECT current_setting('session_replication_role'),
                current_setting('row_security'),
                current_setting('synchronous_commit')",
    )
    .fetch_one(&mut *conn)
    .await;

    if let Ok((role, row_sec, sync_commit)) = row {
        // Only issue SET statements if something is actually wrong (fast path = zero SETs).
        let mut resets = Vec::new();
        if role != "origin" {
            resets.push("SET session_replication_role = 'origin'");
            slog!(
                Level::Warn,
                "session_reset",
                setting = "session_replication_role",
                was = role
            );
        }
        if row_sec.to_lowercase() != "on" {
            resets.push("SET row_security = on");
            slog!(
                Level::Warn,
                "session_reset",
                setting = "row_security",
                was = row_sec
            );
        }
        if sync_commit != "on" {
            resets.push("SET synchronous_commit TO ON");
        }
        if !resets.is_empty() {
            // Batch all SET statements into one round-trip
            let batch = resets.join("; ");
            sqlx::query(&batch)
                .execute(&mut *conn)
                .await
                .map_err(|e| eyre!(e.to_string()))?;
        }
    }

    Ok(())
}

/// Ensure a pooled connection is returned to default session state; best-effort only.
pub async fn ensure_default_session_state(pool: &DbPool) -> TestResult<()> {
    let mut conn = pool.acquire().await?;
    ensure_default_session_state_conn(conn.as_mut()).await
}

/// Used by DatabasePool `before_acquire` callback.
pub(super) async fn ensure_default_session_state_conn_pub(
    conn: &mut PgConnection,
) -> TestResult<()> {
    ensure_default_session_state_conn(conn).await
}

// ── Seeding ─────────────────────────────────────────────────────────────────

/// Seed well-known test fixture data after cleanup.
///
/// `sinex_primitives::testing::event_fixture()` uses a hardcoded material_id
/// (`00000000-0000-7000-8000-000000000000`) that must exist in `raw.source_material_registry`
/// for FK constraints on `core.events.source_material_id` to pass. Since cleanup
/// truncates all tables, we re-seed this after every cleanup cycle.
pub async fn seed_test_fixtures(pool: &DbPool) -> TestResult<()> {
    sqlx::query(
        "INSERT INTO raw.source_material_registry \
            (id, material_kind, source_identifier, status, timing_info_type) \
         VALUES ('00000000-0000-7000-8000-000000000000'::uuid, 'annex', 'test-fixture-material', 'completed', 'realtime') \
         ON CONFLICT (id) DO NOTHING",
    )
    .execute(pool)
    .await?;
    Ok(())
}

// ── Force cleanup ───────────────────────────────────────────────────────────

/// Final backstop cleanup when standard reset fails (e.g., FK contention).
pub(crate) async fn force_event_material_cleanup(pool: &DbPool) -> TestResult<()> {
    let config = CleanupConfig::default();
    let cleanup_tables: Vec<String> = config
        .tables_to_clean()
        .map(|table| table.table_name.to_string())
        .collect();

    force_event_material_cleanup_with_tables(pool, cleanup_tables).await
}

async fn force_event_material_cleanup_with_tables(
    pool: &DbPool,
    cleanup_tables: Vec<String>,
) -> TestResult<()> {
    let mut conn = pool.acquire().await?;
    let config = CleanupConfig::default();
    let pool_for_chunks = pool.clone();

    crate::sandbox::db::common::with_cleanup_session(&mut conn, &config, |conn| {
        let fut: BoxFuture<'_, crate::sandbox::prelude::TestResult<()>> = Box::pin(async move {
            let mut attempts = 0;
            let mut last_events = 0_i64;
            let mut last_materials = 0_i64;

            while attempts < 3 {
                attempts += 1;

                // Truncate high-churn tables with CASCADE to avoid FK deadlocks.
                execute_force_cleanup_statement(
                    conn,
                    "TRUNCATE TABLE core.events CASCADE",
                    "forced cleanup truncate failed for core.events",
                )
                .await?;
                execute_force_cleanup_statement(
                    conn,
                    "TRUNCATE TABLE raw.source_material_registry CASCADE",
                    "forced cleanup truncate failed for raw.source_material_registry",
                )
                .await?;

                // Delete from remaining tables (config-driven) after cascades to catch ancillary rows.
                for table in &cleanup_tables {
                    execute_force_cleanup_statement(
                        conn,
                        &format!("DELETE FROM {table}"),
                        &format!("forced cleanup delete failed for {table}"),
                    )
                    .await?;
                }

                // Hypertable cleanup via drop_chunks for events.
                sqlx::query("SELECT drop_chunks('core.events', older_than => INTERVAL '0 seconds')")
                .execute(&pool_for_chunks)
                .await
                .map_err(|error| eyre!("forced cleanup drop_chunks failed: {error}"))?;

                (last_events, last_materials) = inspect_force_cleanup_counts(&pool_for_chunks).await?;
                if last_events == 0 && last_materials <= 1 {
                    break;
                }
            }

            if last_events != 0 || last_materials > 1 {
                // Final aggressive delete before giving up.
                execute_force_cleanup_statement(
                    conn,
                    "DELETE FROM core.events",
                    "forced cleanup final delete failed for core.events",
                )
                .await?;
                execute_force_cleanup_statement(
                    conn,
                    "DELETE FROM raw.source_material_registry",
                    "forced cleanup final delete failed for raw.source_material_registry",
                )
                .await?;

                (last_events, last_materials) = inspect_force_cleanup_counts(&pool_for_chunks).await?;
            }

            if last_events != 0 || last_materials > 1 {
                return Err(eyre!(format!(
                    "Force cleanup left {last_events} events and {last_materials} materials"
                )));
            }

            Ok(())
        });
        fut
    })
    .await
    .map_err(|e| eyre!(e.to_string()))?;

    Ok(())
}

async fn execute_force_cleanup_statement(
    conn: &mut PgConnection,
    statement: &str,
    context: &str,
) -> TestResult<()> {
    sqlx::query(statement)
        .execute(conn.as_mut())
        .await
        .map_err(|error| eyre!("{context}: {error}"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        force_event_material_cleanup, force_event_material_cleanup_with_tables, log_remaining_rows,
        seed_test_fixtures,
    };
    use crate::sandbox::sinex_test;
    use sinex_primitives::Uuid;

    #[sinex_test]
    async fn log_remaining_rows_reports_extra_source_materials(
        ctx: crate::sandbox::Sandbox,
    ) -> ::xtask::sandbox::TestResult<()> {
        let source_identifier = format!("force-cleanup-test-{}", Uuid::now_v7());
        sqlx::query(
            "INSERT INTO raw.source_material_registry \
                (id, material_kind, source_identifier, status, timing_info_type) \
             VALUES ($1, 'annex', $2, 'completed', 'realtime')",
        )
        .bind(Uuid::now_v7())
        .bind(&source_identifier)
        .execute(ctx.pool())
        .await?;

        let residuals = log_remaining_rows(ctx.pool()).await?;

        assert!(residuals
            .iter()
            .any(|(table, count)| table == "raw.source_material_registry" && *count >= 2));
        Ok(())
    }

    #[sinex_test]
    async fn force_event_material_cleanup_clears_extra_source_materials(
        ctx: crate::sandbox::Sandbox,
    ) -> ::xtask::sandbox::TestResult<()> {
        let source_identifier = format!("force-cleanup-test-{}", Uuid::now_v7());
        sqlx::query(
            "INSERT INTO raw.source_material_registry \
                (id, material_kind, source_identifier, status, timing_info_type) \
             VALUES ($1, 'annex', $2, 'completed', 'realtime')",
        )
        .bind(Uuid::now_v7())
        .bind(&source_identifier)
        .execute(ctx.pool())
        .await?;

        force_event_material_cleanup(ctx.pool()).await?;

        let counts = crate::sandbox::db::common::get_row_counts(ctx.pool()).await?;
        assert_eq!(counts.get("core.events").copied().unwrap_or_default(), 0);
        assert!(counts
            .get("raw.source_material_registry")
            .copied()
            .unwrap_or_default()
            <= 1);

        seed_test_fixtures(ctx.pool()).await?;
        Ok(())
    }

    #[sinex_test]
    async fn force_event_material_cleanup_surfaces_delete_failures(
        ctx: crate::sandbox::Sandbox,
    ) -> ::xtask::sandbox::TestResult<()> {
        let err = force_event_material_cleanup_with_tables(
            ctx.pool(),
            vec!["missing_schema.missing_table".to_string()],
        )
        .await
        .expect_err("invalid cleanup table should fail honestly");

        assert!(
            err.to_string()
                .contains("forced cleanup delete failed for missing_schema.missing_table"),
            "unexpected error: {err:#}"
        );
        Ok(())
    }
}
