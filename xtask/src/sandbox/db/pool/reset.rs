//! Database reset — cleaning, verification, seeding, session state, triggers.

use crate::sandbox::prelude::*;
use futures::future::BoxFuture;
use sinex_db::DbPool;
use sqlx::postgres::PgConnection;
use std::sync::atomic::Ordering;
use std::time::Duration;

use super::config::SLOT_MAX_CONNECTIONS;
use super::metrics::POOL_METRICS;
use super::provisioning::{is_timescaledb_missing_library_error_message, recreate_pool_database};
use super::slot::DatabaseSlot;

// ── Clean database ──────────────────────────────────────────────────────────

/// Clean a database for reuse
pub(super) async fn clean_database(
    slot: &Arc<DatabaseSlot>,
    pool: &DbPool,
    db_name: &str,
    db_url: &str,
) -> TestResult<()> {
    eprintln!("🧹 Cleaning database: {db_name}");
    let mut working_pool = pool.clone();
    let mut residuals: Option<Vec<(String, i64)>> = None;
    let mut schema_recreated = false;

    let mut attempt = 0usize;
    loop {
        attempt += 1;
        if let Some(reason) = schema_mismatch_reason(&working_pool).await? {
            if schema_recreated {
                let err = eyre!(format!(
                    "Database {db_name} schema mismatch after recreation: {reason}"
                ));
                slot.record_clean_result(Err(err.to_string()), residuals.clone());
                slot.quarantined.store(true, Ordering::SeqCst);
                return Err(err);
            }

            eprintln!("  ♻️  Database {db_name} schema mismatch ({reason}); recreating");
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
            continue;
        }

        // Use the shared db_common implementation (TRUNCATE CASCADE doesn't need
        // exclusive access, so we skip preemptive pg_terminate_backend — it would
        // kill our own pool's idle connections and cause "terminating connection" errors)
        match crate::sandbox::db::pool::reset_database(&working_pool).await {
            Ok(()) => {
                if let Err(verify_err) =
                    crate::sandbox::db::pool::verify_clean_state(&working_pool).await
                {
                    if attempt >= 2 {
                        POOL_METRICS.record_cleanup_failure();
                        residuals = log_remaining_rows(&working_pool).await;
                        let err = eyre!(format!("Database {db_name} cleanup failed: {verify_err}"));
                        slot.record_clean_result(Err(err.to_string()), residuals.clone());
                        slot.quarantined.store(true, Ordering::SeqCst);
                        return Err(err);
                    }
                    eprintln!(
                        "  ⚠️ Database {db_name} failed clean-state verification: {verify_err}. Retrying cleanup once."
                    );
                    continue;
                }

                eprintln!("  ✅ Database cleanup verified - all tables empty");
                ensure_default_session_state(&working_pool).await?;
                seed_test_fixtures(&working_pool).await?;
                slot.quarantined.store(false, Ordering::SeqCst);
                slot.record_clean_result(Ok(()), residuals.clone());
                return Ok(());
            }
            Err(e) => {
                let msg = e.to_string();
                let retryable = msg.contains("does not exist")
                    || msg.contains("terminating connection")
                    || msg.contains("Broken pipe")
                    || msg.contains("connection")
                    || is_timescaledb_missing_library_error_message(&msg);

                if retryable && attempt < 3 {
                    eprintln!(
                        "  ⚠️  Cleanup for {db_name} failed with connection error ({msg}); attempting to recreate slot and retry."
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
                    let fresh_pool = super::slot_pool_options(
                        SLOT_MAX_CONNECTIONS,
                        Duration::from_secs(5),
                    )
                    .connect(db_url)
                    .await?;
                    working_pool = fresh_pool;
                    continue;
                }

                eprintln!("  ❌ CRITICAL: Database {db_name} cleanup failed: {e}");
                POOL_METRICS.record_cleanup_failure();
                residuals = log_remaining_rows(&working_pool).await;

                // Attempt one last forced cleanup focusing on stubborn event/material rows.
                if let Err(force_err) = force_event_material_cleanup(&working_pool).await {
                    let err = eyre!(format!(
                        "Database {db_name} cleanup failed: {e}; forced cleanup also failed: {force_err}"
                    ));
                    slot.record_clean_result(Err(err.to_string()), residuals.clone());
                    slot.quarantined.store(true, Ordering::SeqCst);
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
                    return Err(err);
                }

                eprintln!("  ✅ Database cleanup recovered after forced truncation");
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

async fn log_remaining_rows(pool: &DbPool) -> Option<Vec<(String, i64)>> {
    match crate::sandbox::db::common::get_row_counts(pool).await {
        Ok(counts) => {
            let mut residuals = Vec::new();
            for (table, count) in counts {
                if count > 0 {
                    eprintln!("     - {table} has {count} rows remaining");
                    residuals.push((table, count));
                }
            }
            Some(residuals)
        }
        Err(_) => None,
    }
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
        eprintln!("  ⚠️  Restored trg_events_no_update on core.events");
    }

    if !core_events_trigger_exists(pool, "trg_events_archive_before_delete").await? {
        sqlx::query(
            r"
            CREATE OR REPLACE FUNCTION core.fn_archive_before_delete()
            RETURNS trigger LANGUAGE plpgsql AS $$
            DECLARE
              op_id TEXT := current_setting('sinex.operation_id', true);
              sup_id ulid := NULLIF(current_setting('sinex.superseded_by_id', true), '');
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
        eprintln!("  ⚠️  Restored trg_events_archive_before_delete on core.events");
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

pub(super) async fn schema_mismatch_reason(pool: &DbPool) -> TestResult<Option<String>> {
    let events_exists =
        sqlx::query_scalar::<_, Option<String>>("SELECT to_regclass('core.events')::text")
            .fetch_one(pool)
            .await?;
    if events_exists.as_deref() != Some("core.events") {
        return Ok(Some("missing core.events schema".to_string()));
    }

    let events_has_blobs = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS (SELECT 1 FROM information_schema.columns \
         WHERE table_schema = 'core' AND table_name = 'events' AND column_name = 'associated_blob_ids')",
    )
    .fetch_one(pool)
    .await?;
    if !events_has_blobs {
        return Ok(Some(
            "missing core.events.associated_blob_ids column".to_string(),
        ));
    }

    let events_has_subnano = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS (SELECT 1 FROM information_schema.columns \
         WHERE table_schema = 'core' AND table_name = 'events' \
           AND column_name = 'ts_orig_subnano' AND data_type = 'integer')",
    )
    .fetch_one(pool)
    .await?;
    if !events_has_subnano {
        return Ok(Some(
            "missing core.events.ts_orig_subnano column".to_string(),
        ));
    }

    let payload_has_updated_at = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS (SELECT 1 FROM information_schema.columns \
         WHERE table_schema = 'sinex_schemas' AND table_name = 'event_payload_schemas' \
           AND column_name = 'updated_at')",
    )
    .fetch_one(pool)
    .await?;
    if !payload_has_updated_at {
        return Ok(Some(
            "missing sinex_schemas.event_payload_schemas.updated_at column".to_string(),
        ));
    }

    // Check for critical indexes that ON CONFLICT clauses depend on
    let has_sm_unique_idx = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS (SELECT 1 FROM pg_indexes \
         WHERE schemaname = 'raw' AND tablename = 'source_material_registry' \
           AND indexname = 'uk_sm_registry_source_identifier')",
    )
    .fetch_one(pool)
    .await?;
    if !has_sm_unique_idx {
        return Ok(Some(
            "missing uk_sm_registry_source_identifier index on raw.source_material_registry"
                .to_string(),
        ));
    }

    core_events_triggers_missing_reason(pool).await
}

// ── Session state ───────────────────────────────────────────────────────────

async fn ensure_default_session_state_conn(conn: &mut PgConnection) -> TestResult<()> {
    if let Ok(role) = sqlx::query_scalar::<_, String>("SHOW session_replication_role")
        .fetch_one(&mut *conn)
        .await
    {
        if role != "origin" {
            sqlx::query("SET session_replication_role = 'origin'")
                .execute(&mut *conn)
                .await
                .map_err(|e| eyre!(e.to_string()))?;
            eprintln!("  ⚠️  Reset session_replication_role from {role} to origin");
        }
    }
    if let Ok(row_sec) = sqlx::query_scalar::<_, String>("SHOW row_security")
        .fetch_one(&mut *conn)
        .await
    {
        if row_sec.to_lowercase() != "on" {
            sqlx::query("SET row_security = on")
                .execute(&mut *conn)
                .await
                .map_err(|e| eyre!(e.to_string()))?;
            eprintln!("  ⚠️  Reset row_security to on");
        }
    }
    // Restore synchronous_commit if apply_test_optimizations() turned it off.
    if let Ok(sync_commit) = sqlx::query_scalar::<_, String>("SHOW synchronous_commit")
        .fetch_one(&mut *conn)
        .await
    {
        if sync_commit != "on" {
            sqlx::query("SET synchronous_commit TO ON")
                .execute(&mut *conn)
                .await
                .map_err(|e| eyre!(e.to_string()))?;
        }
    }

    let config = CleanupConfig::default();
    for table in config.tables_requiring_trigger_disable() {
        let query = format!(
            "SELECT EXISTS (SELECT 1 FROM pg_trigger WHERE tgrelid = '{}'::regclass AND tgenabled NOT IN ('O','A')) AS needs_enable",
            table.table_name
        );
        if let Ok(needs_enable) = sqlx::query_scalar::<_, Option<bool>>(&query)
            .fetch_one(&mut *conn)
            .await
        {
            if needs_enable == Some(true) {
                let enable = sqlx::query(&format!(
                    "ALTER TABLE {} ENABLE TRIGGER ALL",
                    table.table_name
                ))
                .execute(&mut *conn)
                .await;
                match enable {
                    Ok(_) => {
                        eprintln!("  ⚠️  Re-enabled triggers on {}", table.table_name);
                    }
                    Err(err) => {
                        let msg = err.to_string();
                        if msg.contains("hypertables do not support") {
                            eprintln!(
                                "  ⚠️  Skipping trigger enable on hypertable {}",
                                table.table_name
                            );
                        } else {
                            return Err(eyre!(msg));
                        }
                    }
                }
            }
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
/// (`01H00000000000000000000000`) that must exist in `raw.source_material_registry`
/// for FK constraints on `core.events.source_material_id` to pass. Since cleanup
/// truncates all tables, we re-seed this after every cleanup cycle.
pub async fn seed_test_fixtures(pool: &DbPool) -> TestResult<()> {
    sqlx::query(
        "INSERT INTO raw.source_material_registry \
            (id, material_kind, source_identifier, status, timing_info_type) \
         VALUES ('01H00000000000000000000000'::ulid, 'annex', 'test-fixture-material', 'completed', 'realtime') \
         ON CONFLICT (id) DO NOTHING",
    )
    .execute(pool)
    .await?;
    Ok(())
}

// ── Force cleanup ───────────────────────────────────────────────────────────

/// Final backstop cleanup when standard reset fails (e.g., FK contention).
pub(crate) async fn force_event_material_cleanup(pool: &DbPool) -> TestResult<()> {
    let mut conn = pool.acquire().await?;
    let config = CleanupConfig::default();
    let pool_for_chunks = pool.clone();
    let cleanup_tables: Vec<String> = config
        .ordered_tables()
        .into_iter()
        .map(|t| t.table_name.to_string())
        .collect();

    crate::sandbox::db::common::with_cleanup_session(&mut conn, &config, |conn| {
        let fut: BoxFuture<'_, crate::sandbox::prelude::TestResult<()>> = Box::pin(async move {
            let mut attempts = 0;
            let mut last_events = 0_i64;
            let mut last_materials = 0_i64;

            while attempts < 3 {
                attempts += 1;

                // Truncate high-churn tables with CASCADE to avoid FK deadlocks.
                let _ = sqlx::query("TRUNCATE TABLE core.events CASCADE")
                    .execute(conn.as_mut())
                    .await;
                let _ = sqlx::query("TRUNCATE TABLE raw.source_material_registry CASCADE")
                    .execute(conn.as_mut())
                    .await;

                // Delete from remaining tables (config-driven) after cascades to catch ancillary rows.
                for table in &cleanup_tables {
                    let _ = sqlx::query(&format!("DELETE FROM {table}"))
                        .execute(conn.as_mut())
                        .await;
                }

                // Hypertable cleanup via drop_chunks for events.
                let _ = sqlx::query(
                    "SELECT drop_chunks('core.events', older_than => INTERVAL '0 seconds')",
                )
                .execute(&pool_for_chunks)
                .await;

                let counts = crate::sandbox::db::common::get_row_counts(&pool_for_chunks)
                    .await
                    .unwrap_or_default();
                last_events = *counts.get("core.events").unwrap_or(&0);
                last_materials = *counts.get("raw.source_material_registry").unwrap_or(&0);
                if last_events == 0 && last_materials <= 1 {
                    break;
                }
            }

            if last_events != 0 || last_materials > 1 {
                // Final aggressive delete before giving up.
                let _ = sqlx::query("DELETE FROM core.events")
                    .execute(conn.as_mut())
                    .await;
                let _ = sqlx::query("DELETE FROM raw.source_material_registry")
                    .execute(conn.as_mut())
                    .await;
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
