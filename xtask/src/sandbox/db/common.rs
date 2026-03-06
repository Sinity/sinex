use crate::sandbox::prelude::*;
use crate::sandbox::slog::{Level, slog};
use sqlx::PgConnection;
use std::collections::HashMap;

pub async fn verify_clean_state(pool: &DbPool) -> TestResult<()> {
    let counts = get_row_counts(pool).await?;
    let mut failures = Vec::new();
    for (table, count) in counts {
        // raw.source_material_registry is managed by seed_test_fixtures() which
        // inserts well-known fixture rows after every cleanup cycle. Skip it entirely
        // since its contents are always re-established.
        if table == "raw.source_material_registry" {
            continue;
        }
        if count > 0 {
            failures.push(format!("{table}: {count}"));
        }
    }

    if !failures.is_empty() {
        return Err(eyre!(
            "Database not clean! Residual data found: {:?}",
            failures
        ));
    }
    Ok(())
}

pub async fn reset_database(pool: &DbPool) -> TestResult<()> {
    let reset_start = std::time::Instant::now();
    let mut conn = pool.acquire().await?;
    let config = super::cleanup_config::CleanupConfig::default();

    // All tables use TRUNCATE — batched into a single statement.
    // TRUNCATE doesn't fire row-level triggers (archive, append-only constraints),
    // and TimescaleDB 2.x+ supports TRUNCATE on hypertables (drops chunks).
    // CASCADE propagates to dependent tables, one lock acquisition for all.
    let truncate_tables: Vec<&str> = config.truncatable_tables().map(|t| t.table_name).collect();
    if !truncate_tables.is_empty() {
        let table_list = truncate_tables.join(", ");
        sqlx::query(&format!(
            "TRUNCATE TABLE {table_list} RESTART IDENTITY CASCADE"
        ))
        .execute(&mut *conn)
        .await?;
    }

    let total = reset_start.elapsed();
    if total.as_millis() >= 500 {
        slog!(
            Level::Warn,
            "reset_slow",
            duration_ms = total.as_millis(),
            tables = truncate_tables.len()
        );
    }

    Ok(())
}

/// Get row counts for all cleanable tables in a single query.
///
/// Previous implementation ran N separate `SELECT COUNT(*)` queries (~20 round-trips).
/// This batches them into a single query with `UNION ALL` for one round-trip.
pub async fn get_row_counts(pool: &DbPool) -> TestResult<HashMap<String, i64>> {
    let config = super::cleanup_config::CleanupConfig::default();
    let tables: Vec<&str> = config.tables_to_clean().map(|t| t.table_name).collect();

    if tables.is_empty() {
        return Ok(HashMap::new());
    }

    // Build a single UNION ALL query: SELECT 'table_name' AS t, COUNT(*) AS c FROM table_name
    let parts: Vec<String> = tables
        .iter()
        .map(|t| format!("SELECT '{t}' AS t, COUNT(*) AS c FROM {t}"))
        .collect();
    let query = parts.join(" UNION ALL ");

    let rows = sqlx::query_as::<_, (String, i64)>(&query)
        .fetch_all(pool)
        .await?;

    Ok(rows.into_iter().collect())
}

pub async fn apply_test_optimizations(pool: &DbPool) -> TestResult<()> {
    let mut conn = pool.acquire().await?;
    // Example optimization: turn off synchronous commit for tests
    sqlx::query("SET synchronous_commit TO OFF")
        .execute(&mut *conn)
        .await?;
    Ok(())
}

pub async fn with_cleanup_session<F>(
    conn: &mut PgConnection,
    _config: &super::cleanup_config::CleanupConfig,
    f: F,
) -> TestResult<()>
where
    F: for<'a> FnOnce(&'a mut PgConnection) -> futures::future::BoxFuture<'a, TestResult<()>>,
{
    // Implementation for session-based cleanup if needed
    f(conn).await
}
