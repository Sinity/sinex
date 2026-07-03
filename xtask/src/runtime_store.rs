//! Cheap runtime-store and backlog snapshots for agent/operator triage.
//!
//! This intentionally avoids exact full-table counts. The command is for
//! deciding the next devloop move under live load, so bounded recent windows
//! and catalog estimates are more useful than scans that become part of the
//! problem.

use serde::Serialize;
use sqlx::Row;
use sqlx::postgres::PgPoolOptions;
use std::time::Duration;

#[derive(Debug, Clone, Serialize)]
pub struct RuntimeStoreSnapshot {
    pub window_minutes: i64,
    pub top_limit: i64,
    pub estimated_tables: Vec<TableEstimate>,
    pub recent_event_mix: Vec<EventMixRow>,
    pub recent_source_materials: Vec<SourceMaterialRollup>,
    pub browser_history_materials: Vec<SourceMaterialRollup>,
    pub dlq: DlqSummary,
    pub assessment: StoreAssessment,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct TableEstimate {
    pub relation: String,
    pub estimated_rows: Option<i64>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct EventMixRow {
    pub event_type: String,
    pub events: i64,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct SourceMaterialRollup {
    pub source_base: String,
    pub status: String,
    pub materials: i64,
    pub total_bytes: Option<i64>,
    pub parsed_events: i64,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct DlqSummary {
    pub unresolved: i64,
    pub resolved: i64,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct StoreAssessment {
    pub current_ingest_quiet: bool,
    pub top_recent_event_type: Option<String>,
    pub browser_history_materials_total: i64,
    pub browser_history_parsed_events_total: i64,
    pub unresolved_dlq: i64,
    pub warnings: Vec<String>,
}

pub async fn query_runtime_store_snapshot(
    db_url: &str,
    window_minutes: i64,
    top_limit: i64,
) -> Result<RuntimeStoreSnapshot, sqlx::Error> {
    let window_minutes = window_minutes.clamp(1, 24 * 60);
    let top_limit = top_limit.clamp(1, 100);
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_millis(500))
        .connect(db_url)
        .await?;

    sqlx::query("SET statement_timeout = '3000ms'")
        .execute(&pool)
        .await?;

    let estimated_tables = query_table_estimates(&pool).await?;
    let recent_event_mix = query_recent_event_mix(&pool, window_minutes, top_limit).await?;
    let recent_source_materials =
        query_recent_source_materials(&pool, window_minutes, top_limit).await?;
    let browser_history_materials = query_browser_history_materials(&pool).await?;
    let dlq = query_dlq_summary(&pool).await?;
    pool.close().await;

    let assessment = assess_store(
        &recent_event_mix,
        &browser_history_materials,
        &dlq,
        window_minutes,
    );

    Ok(RuntimeStoreSnapshot {
        window_minutes,
        top_limit,
        estimated_tables,
        recent_event_mix,
        recent_source_materials,
        browser_history_materials,
        dlq,
        assessment,
    })
}

async fn query_table_estimates(
    pool: &sqlx::Pool<sqlx::Postgres>,
) -> Result<Vec<TableEstimate>, sqlx::Error> {
    let rows = sqlx::query(
        r#"
        WITH RECURSIVE targets(schema_name, table_name) AS (
            VALUES
                ('core', 'events'),
                ('raw', 'source_material_registry'),
                ('sinex_schemas', 'dlq_events')
        ),
        target_oids AS (
            SELECT t.schema_name,
                   t.table_name,
                   c.oid AS target_oid
            FROM targets t
            JOIN pg_namespace n ON n.nspname = t.schema_name
            JOIN pg_class c ON c.relnamespace = n.oid AND c.relname = t.table_name
        ),
        members AS (
            SELECT target_oid, target_oid AS member_oid
            FROM target_oids
            UNION ALL
            SELECT m.target_oid, i.inhrelid AS member_oid
            FROM members m
            JOIN pg_inherits i ON i.inhparent = m.member_oid
        ),
        estimates AS (
            SELECT m.target_oid,
                   SUM(CASE WHEN c.reltuples < 0 THEN 0 ELSE c.reltuples END)::bigint AS reltuples,
                   SUM(COALESCE(s.n_live_tup, 0))::bigint AS live_tup
            FROM members m
            JOIN pg_class c ON c.oid = m.member_oid
            LEFT JOIN pg_stat_all_tables s ON s.relid = m.member_oid
            GROUP BY m.target_oid
        )
        SELECT t.schema_name || '.' || t.table_name AS relation,
               NULLIF(GREATEST(e.reltuples, e.live_tup), 0) AS estimated_rows
        FROM target_oids t
        JOIN estimates e ON e.target_oid = t.target_oid
        ORDER BY 1
        "#,
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| TableEstimate {
            relation: row.get("relation"),
            estimated_rows: row.get("estimated_rows"),
        })
        .collect())
}

async fn query_recent_event_mix(
    pool: &sqlx::Pool<sqlx::Postgres>,
    window_minutes: i64,
    top_limit: i64,
) -> Result<Vec<EventMixRow>, sqlx::Error> {
    let rows = sqlx::query(
        r#"
        SELECT event_type, COUNT(*)::bigint AS events
        FROM core.events
        WHERE ts_coided > NOW() - ($1::int * INTERVAL '1 minute')
        GROUP BY event_type
        ORDER BY events DESC, event_type
        LIMIT $2
        "#,
    )
    .bind(window_minutes)
    .bind(top_limit)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| EventMixRow {
            event_type: row.get("event_type"),
            events: row.get("events"),
        })
        .collect())
}

async fn query_recent_source_materials(
    pool: &sqlx::Pool<sqlx::Postgres>,
    window_minutes: i64,
    top_limit: i64,
) -> Result<Vec<SourceMaterialRollup>, sqlx::Error> {
    let rows = sqlx::query(
        r#"
        SELECT split_part(source_identifier, '#', 1) AS source_base,
               status,
               COUNT(*)::bigint AS materials,
               SUM(total_bytes)::bigint AS total_bytes,
               COALESCE(SUM(parsed_event_count), 0)::bigint AS parsed_events
        FROM raw.source_material_registry
        WHERE staged_at > NOW() - ($1::int * INTERVAL '1 minute')
        GROUP BY source_base, status
        ORDER BY materials DESC, parsed_events DESC, source_base
        LIMIT $2
        "#,
    )
    .bind(window_minutes)
    .bind(top_limit)
    .fetch_all(pool)
    .await?;

    Ok(rows.into_iter().map(source_material_rollup_from_row).collect())
}

async fn query_browser_history_materials(
    pool: &sqlx::Pool<sqlx::Postgres>,
) -> Result<Vec<SourceMaterialRollup>, sqlx::Error> {
    let rows = sqlx::query(
        r#"
        SELECT split_part(source_identifier, '#', 1) AS source_base,
               status,
               COUNT(*)::bigint AS materials,
               SUM(total_bytes)::bigint AS total_bytes,
               COALESCE(SUM(parsed_event_count), 0)::bigint AS parsed_events
        FROM raw.source_material_registry
        WHERE source_identifier LIKE 'browser.history#%'
        GROUP BY source_base, status
        ORDER BY materials DESC, parsed_events DESC, source_base
        "#,
    )
    .fetch_all(pool)
    .await?;

    Ok(rows.into_iter().map(source_material_rollup_from_row).collect())
}

fn source_material_rollup_from_row(row: sqlx::postgres::PgRow) -> SourceMaterialRollup {
    SourceMaterialRollup {
        source_base: row.get("source_base"),
        status: row.get("status"),
        materials: row.get("materials"),
        total_bytes: row.get("total_bytes"),
        parsed_events: row.get("parsed_events"),
    }
}

async fn query_dlq_summary(pool: &sqlx::Pool<sqlx::Postgres>) -> Result<DlqSummary, sqlx::Error> {
    let row = sqlx::query(
        r#"
        SELECT
            COUNT(*) FILTER (WHERE resolved_at IS NULL)::bigint AS unresolved,
            COUNT(*) FILTER (WHERE resolved_at IS NOT NULL)::bigint AS resolved
        FROM sinex_schemas.dlq_events
        "#,
    )
    .fetch_one(pool)
    .await?;

    Ok(DlqSummary {
        unresolved: row.get("unresolved"),
        resolved: row.get("resolved"),
    })
}

#[must_use]
pub fn assess_store(
    recent_event_mix: &[EventMixRow],
    browser_history_materials: &[SourceMaterialRollup],
    dlq: &DlqSummary,
    window_minutes: i64,
) -> StoreAssessment {
    let browser_history_materials_total = browser_history_materials
        .iter()
        .map(|row| row.materials)
        .sum::<i64>();
    let browser_history_parsed_events_total = browser_history_materials
        .iter()
        .map(|row| row.parsed_events)
        .sum::<i64>();
    let top_recent_event_type = recent_event_mix.first().map(|row| row.event_type.clone());
    let current_ingest_quiet = recent_event_mix.is_empty();
    let unresolved_dlq = dlq.unresolved;
    let mut warnings = Vec::new();

    if current_ingest_quiet {
        warnings.push(format!(
            "no events observed in the last {window_minutes} minute(s)"
        ));
    }
    if let Some(top) = recent_event_mix.first()
        && top.event_type == "page.visited"
        && top.events > 10_000
    {
        warnings.push(format!(
            "browser history dominates the recent window ({} page.visited events)",
            top.events
        ));
    }
    if browser_history_materials_total > 100 || browser_history_parsed_events_total > 10_000_000 {
        warnings.push(format!(
            "browser history material inventory is large ({browser_history_materials_total} materials, {browser_history_parsed_events_total} parsed events)"
        ));
    }
    if unresolved_dlq > 0 {
        warnings.push(format!("{unresolved_dlq} unresolved DLQ row(s)"));
    }

    StoreAssessment {
        current_ingest_quiet,
        top_recent_event_type,
        browser_history_materials_total,
        browser_history_parsed_events_total,
        unresolved_dlq,
        warnings,
    }
}

#[cfg(test)]
#[path = "runtime_store_test.rs"]
mod tests;
