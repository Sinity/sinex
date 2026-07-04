//! Cheap runtime-store and backlog snapshots for agent/operator triage.
//!
//! This intentionally avoids exact full-table counts. The command is for
//! deciding the next devloop move under live load, so bounded recent windows
//! and catalog estimates are more useful than scans that become part of the
//! problem.

use futures::StreamExt;
use serde::Serialize;
use sinex_primitives::environment::environment;
use sinex_primitives::nats::JetStreamTopology;
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
    pub active_source_materials: Vec<ActiveSourceMaterialRow>,
    pub browser_history_materials: Vec<SourceMaterialRollup>,
    pub dlq: DlqSummary,
    pub jetstream: JetStreamStoreSnapshot,
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
pub struct ActiveSourceMaterialRow {
    pub material_id: String,
    pub source_identifier: String,
    pub age_seconds: i64,
    pub parsed_events: i64,
    pub total_bytes: Option<i64>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct DlqSummary {
    pub unresolved: i64,
    pub resolved: i64,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct JetStreamStoreSnapshot {
    pub nats_url: String,
    pub available: bool,
    pub error: Option<String>,
    pub streams: Vec<JetStreamStreamSnapshot>,
    pub sql_dlq_unresolved: i64,
    pub jetstream_dlq_messages: Option<u64>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct JetStreamStreamSnapshot {
    pub role: String,
    pub stream: String,
    pub present: bool,
    pub messages: Option<u64>,
    pub bytes: Option<u64>,
    pub first_sequence: Option<u64>,
    pub last_sequence: Option<u64>,
    pub consumer_count: Option<usize>,
    pub consumers: Vec<JetStreamConsumerSnapshot>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct JetStreamConsumerSnapshot {
    pub name: String,
    pub durable_name: Option<String>,
    pub filter_subject: String,
    pub num_pending: u64,
    pub num_ack_pending: usize,
    pub num_redelivered: usize,
    pub num_waiting: usize,
    pub delivered_stream_sequence: u64,
    pub ack_floor_stream_sequence: u64,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct StoreAssessment {
    pub current_ingest_quiet: bool,
    pub top_recent_event_type: Option<String>,
    pub active_source_materials: i64,
    pub active_source_materials_over_60m: i64,
    pub browser_history_materials_total: i64,
    pub browser_history_parsed_events_total: i64,
    pub unresolved_dlq: i64,
    pub warnings: Vec<String>,
}

pub async fn query_runtime_store_snapshot(
    db_url: &str,
    nats_url: &str,
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
    let active_source_materials = query_active_source_materials(&pool, top_limit).await?;
    let browser_history_materials = query_browser_history_materials(&pool).await?;
    let dlq = query_dlq_summary(&pool).await?;
    pool.close().await;

    let jetstream = query_jetstream_store_snapshot(nats_url, &dlq).await;
    let mut assessment = assess_store(
        &recent_event_mix,
        &active_source_materials,
        &browser_history_materials,
        &dlq,
        window_minutes,
    );
    assessment.warnings.extend(jetstream.warnings.clone());

    Ok(RuntimeStoreSnapshot {
        window_minutes,
        top_limit,
        estimated_tables,
        recent_event_mix,
        recent_source_materials,
        active_source_materials,
        browser_history_materials,
        dlq,
        jetstream,
        assessment,
    })
}

async fn query_jetstream_store_snapshot(
    nats_url: &str,
    dlq: &DlqSummary,
) -> JetStreamStoreSnapshot {
    match tokio::time::timeout(
        Duration::from_millis(2_000),
        query_jetstream_store_snapshot_inner(nats_url, dlq),
    )
    .await
    {
        Ok(snapshot) => snapshot,
        Err(_) => JetStreamStoreSnapshot::unavailable(
            nats_url,
            dlq.unresolved,
            "timed out querying JetStream stream state",
        ),
    }
}

async fn query_jetstream_store_snapshot_inner(
    nats_url: &str,
    dlq: &DlqSummary,
) -> JetStreamStoreSnapshot {
    let client = match async_nats::connect(nats_url).await {
        Ok(client) => client,
        Err(err) => {
            return JetStreamStoreSnapshot::unavailable(
                nats_url,
                dlq.unresolved,
                format!("failed to connect to NATS: {err}"),
            );
        }
    };
    let js = async_nats::jetstream::new(client);
    let mut streams = Vec::new();
    for (role, stream_name) in jetstream_stream_targets() {
        streams.push(query_jetstream_stream(&js, role, &stream_name).await);
    }
    let mut snapshot = JetStreamStoreSnapshot {
        nats_url: nats_url.to_string(),
        available: true,
        error: None,
        jetstream_dlq_messages: streams
            .iter()
            .find(|stream| stream.role == "dlq")
            .and_then(|stream| stream.messages),
        streams,
        sql_dlq_unresolved: dlq.unresolved,
        warnings: Vec::new(),
    };
    snapshot.warnings = assess_jetstream(&snapshot);
    snapshot
}

async fn query_jetstream_stream(
    js: &async_nats::jetstream::Context,
    role: &str,
    stream_name: &str,
) -> JetStreamStreamSnapshot {
    let mut stream = match js.get_stream(stream_name).await {
        Ok(stream) => stream,
        Err(err) => {
            return JetStreamStreamSnapshot {
                role: role.to_string(),
                stream: stream_name.to_string(),
                present: false,
                messages: None,
                bytes: None,
                first_sequence: None,
                last_sequence: None,
                consumer_count: None,
                consumers: Vec::new(),
                error: Some(format!("{err}")),
            };
        }
    };
    let info = match stream.info().await {
        Ok(info) => info,
        Err(err) => {
            return JetStreamStreamSnapshot {
                role: role.to_string(),
                stream: stream_name.to_string(),
                present: true,
                messages: None,
                bytes: None,
                first_sequence: None,
                last_sequence: None,
                consumer_count: None,
                consumers: Vec::new(),
                error: Some(format!("failed to read stream info: {err}")),
            };
        }
    };
    let stream_display_name = info.config.name.clone();
    let messages = info.state.messages;
    let bytes = info.state.bytes;
    let first_sequence = info.state.first_sequence;
    let last_sequence = info.state.last_sequence;
    let consumer_count = info.state.consumer_count;
    let mut consumers = Vec::new();
    let mut consumer_list = stream.consumers();
    let mut consumer_error = None;
    while let Some(result) = consumer_list.next().await {
        match result {
            Ok(consumer) => consumers.push(JetStreamConsumerSnapshot {
                name: consumer.name,
                durable_name: consumer.config.durable_name,
                filter_subject: consumer.config.filter_subject,
                num_pending: consumer.num_pending,
                num_ack_pending: consumer.num_ack_pending,
                num_redelivered: consumer.num_redelivered,
                num_waiting: consumer.num_waiting,
                delivered_stream_sequence: consumer.delivered.stream_sequence,
                ack_floor_stream_sequence: consumer.ack_floor.stream_sequence,
            }),
            Err(err) => {
                consumer_error = Some(format!("failed to list consumers: {err}"));
                break;
            }
        }
    }
    consumers.sort_by(|left, right| left.name.cmp(&right.name));
    JetStreamStreamSnapshot {
        role: role.to_string(),
        stream: stream_display_name,
        present: true,
        messages: Some(messages),
        bytes: Some(bytes),
        first_sequence: Some(first_sequence),
        last_sequence: Some(last_sequence),
        consumer_count: Some(consumer_count),
        consumers,
        error: consumer_error,
    }
}

fn jetstream_stream_targets() -> Vec<(&'static str, String)> {
    let env = environment();
    let base_stream = env.nats_stream_name("SINEX_RAW_EVENTS");
    let topology = JetStreamTopology::new(&env, base_stream, "event-engine".to_string(), None);
    vec![
        ("raw", topology.events_stream.into_string()),
        (
            "confirmed-events",
            topology.confirmed_events_stream.into_string(),
        ),
        ("dlq", topology.dlq_stream.into_string()),
        (
            "processing-failures",
            topology.processing_failures_stream.into_string(),
        ),
        ("invalidations", topology.invalidation_stream.into_string()),
        ("source-material", env.nats_stream_name("SOURCE_MATERIAL")),
    ]
}

#[must_use]
pub fn assess_jetstream(snapshot: &JetStreamStoreSnapshot) -> Vec<String> {
    let mut warnings = Vec::new();
    if !snapshot.available {
        warnings.push(format!(
            "JetStream unavailable at {}: {}",
            snapshot.nats_url,
            snapshot.error.as_deref().unwrap_or("unknown error")
        ));
        return warnings;
    }
    if let Some(messages) = snapshot.jetstream_dlq_messages
        && messages > 0
    {
        warnings.push(format!(
            "JetStream DLQ has {messages} message(s); SQL unresolved DLQ rows: {}",
            snapshot.sql_dlq_unresolved
        ));
    }
    for stream in &snapshot.streams {
        let pending = stream
            .consumers
            .iter()
            .map(|consumer| consumer.num_pending)
            .sum::<u64>();
        if stream.role == "raw" && pending > 0 {
            warnings.push(format!(
                "raw JetStream consumer backlog: {pending} pending message(s) on {}",
                stream.stream
            ));
        }
        if stream.role == "source-material" && pending > 0 {
            warnings.push(format!(
                "source-material JetStream backlog: {pending} pending frame message(s)"
            ));
        }
        if stream.role == "source-material" {
            let ack_pending = stream
                .consumers
                .iter()
                .map(|consumer| consumer.num_ack_pending)
                .sum::<usize>();
            let redelivered = stream
                .consumers
                .iter()
                .map(|consumer| consumer.num_redelivered)
                .sum::<usize>();
            if ack_pending > 0 {
                warnings.push(format!(
                    "source-material JetStream has {ack_pending} ack-pending frame message(s)"
                ));
            }
            if redelivered > 0 {
                warnings.push(format!(
                    "source-material JetStream has {redelivered} redelivered frame message(s)"
                ));
            }
        }
    }
    warnings
}

impl JetStreamStoreSnapshot {
    fn unavailable(nats_url: &str, sql_dlq_unresolved: i64, error: impl Into<String>) -> Self {
        let error = error.into();
        let mut snapshot = Self {
            nats_url: nats_url.to_string(),
            available: false,
            error: Some(error),
            streams: Vec::new(),
            sql_dlq_unresolved,
            jetstream_dlq_messages: None,
            warnings: Vec::new(),
        };
        snapshot.warnings = assess_jetstream(&snapshot);
        snapshot
    }
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

    Ok(rows
        .into_iter()
        .map(source_material_rollup_from_row)
        .collect())
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

    Ok(rows
        .into_iter()
        .map(source_material_rollup_from_row)
        .collect())
}

async fn query_active_source_materials(
    pool: &sqlx::Pool<sqlx::Postgres>,
    top_limit: i64,
) -> Result<Vec<ActiveSourceMaterialRow>, sqlx::Error> {
    let rows = sqlx::query(
        r#"
        SELECT id::text AS material_id,
               source_identifier,
               EXTRACT(EPOCH FROM (NOW() - COALESCE(start_time, staged_at)))::bigint AS age_seconds,
               COALESCE(parsed_event_count, 0)::bigint AS parsed_events,
               total_bytes
        FROM raw.source_material_registry
        WHERE status = 'sensing'
        ORDER BY COALESCE(start_time, staged_at), id
        LIMIT $1
        "#,
    )
    .bind(top_limit)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| ActiveSourceMaterialRow {
            material_id: row.get("material_id"),
            source_identifier: row.get("source_identifier"),
            age_seconds: row.get("age_seconds"),
            parsed_events: row.get("parsed_events"),
            total_bytes: row.get("total_bytes"),
        })
        .collect())
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
    active_source_materials: &[ActiveSourceMaterialRow],
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
    let active_source_materials_total = active_source_materials.len() as i64;
    let active_source_materials_over_60m = active_source_materials
        .iter()
        .filter(|row| row.age_seconds >= 3600)
        .count() as i64;
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
    if active_source_materials_over_60m > 0 {
        warnings.push(format!(
            "{active_source_materials_over_60m} active source material(s) are older than 60 minutes"
        ));
    }
    if unresolved_dlq > 0 {
        warnings.push(format!("{unresolved_dlq} unresolved DLQ row(s)"));
    }

    StoreAssessment {
        current_ingest_quiet,
        top_recent_event_type,
        active_source_materials: active_source_materials_total,
        active_source_materials_over_60m,
        browser_history_materials_total,
        browser_history_parsed_events_total,
        unresolved_dlq,
        warnings,
    }
}

#[cfg(test)]
#[path = "runtime_store_test.rs"]
mod tests;
