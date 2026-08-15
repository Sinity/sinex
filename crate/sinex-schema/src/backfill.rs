use crate::apply::ApplyError;
use crate::primitives::Uuid;
use serde::Serialize;
use sqlx::{PgPool, Row};
use std::collections::HashMap;

#[cfg(test)]
#[path = "backfill_test.rs"]
mod backfill_test;

pub const PARSED_EVENT_COUNT_BACKFILL_KEY: &str = "parsed-event-count-v1";
pub const PARSED_EVENT_COUNT_BACKFILL_VERSION: i32 = 1;

const BACKFILL_LOCK_KEY: &str = "sinex.schema.backfill.parsed-event-count-v1";

/// The observable execution state of a registered schema backfill.
///
/// `running` is persisted because it records resumable progress. Whether that
/// persisted run is executing now is authoritative only while its advisory
/// lock is held, so status readers expose active and interrupted separately.
#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BackfillExecutionState {
    Registered,
    Active,
    Interrupted,
    Failed,
    Succeeded,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct BackfillRunStatus {
    pub backfill_key: String,
    pub version: i32,
    /// Persisted lifecycle value from `schema_backfill_runs`.
    pub status: String,
    /// Observed execution state, including active versus interrupted runs.
    pub execution_state: BackfillExecutionState,
    pub phase: String,
    pub cursor_event_id: Option<Uuid>,
    pub target_max_event_id: Option<Uuid>,
    pub scanned_events: i64,
    pub applied_materials: i64,
    pub error_message: Option<String>,
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
    pub updated_at: String,
}

#[derive(Debug, Clone)]
pub struct ParsedEventCountBackfillOptions {
    pub batch_size: i64,
    pub assume_quiescent: bool,
    pub restart: bool,
    pub stop_after_chunks: Option<usize>,
    #[cfg(test)]
    failpoint: Option<BackfillTestFailpoint>,
}

impl Default for ParsedEventCountBackfillOptions {
    fn default() -> Self {
        Self {
            batch_size: 50_000,
            assume_quiescent: false,
            restart: false,
            stop_after_chunks: None,
            #[cfg(test)]
            failpoint: None,
        }
    }
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BackfillTestFailpoint {
    DuringScan,
    DuringApply,
}

pub async fn ensure_backfill_schema(pool: &PgPool) -> Result<(), ApplyError> {
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS sinex_schemas.schema_backfill_runs (
            backfill_key text NOT NULL,
            version integer NOT NULL,
            status text NOT NULL DEFAULT 'registered'
                CHECK (status IN ('registered', 'running', 'succeeded', 'failed')),
            phase text NOT NULL DEFAULT 'registered',
            cursor_event_id uuid NULL,
            target_max_event_id uuid NULL,
            scanned_events bigint NOT NULL DEFAULT 0 CHECK (scanned_events >= 0),
            applied_materials bigint NOT NULL DEFAULT 0 CHECK (applied_materials >= 0),
            error_message text NULL,
            operation_id uuid NULL,
            started_at timestamptz NULL,
            finished_at timestamptz NULL,
            updated_at timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (backfill_key, version)
        )
        "#,
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS sinex_schemas.schema_backfill_material_counts (
            backfill_key text NOT NULL,
            version integer NOT NULL,
            source_material_id uuid NOT NULL,
            event_count bigint NOT NULL CHECK (event_count >= 0),
            updated_at timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (backfill_key, version, source_material_id)
        )
        "#,
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        INSERT INTO sinex_schemas.schema_backfill_runs (
            backfill_key, version, status, phase
        )
        VALUES ($1, $2, 'registered', 'registered')
        ON CONFLICT (backfill_key, version) DO NOTHING
        "#,
    )
    .bind(PARSED_EVENT_COUNT_BACKFILL_KEY)
    .bind(PARSED_EVENT_COUNT_BACKFILL_VERSION)
    .execute(pool)
    .await?;

    Ok(())
}

pub async fn list_backfill_runs(pool: &PgPool) -> Result<Vec<BackfillRunStatus>, ApplyError> {
    ensure_backfill_schema(pool).await?;

    let rows = sqlx::query(
        r#"
        SELECT
            backfill_key,
            version,
            status,
            phase,
            cursor_event_id,
            target_max_event_id,
            scanned_events,
            applied_materials,
            error_message,
            started_at::text AS started_at,
            finished_at::text AS finished_at,
            updated_at::text AS updated_at
        FROM sinex_schemas.schema_backfill_runs
        ORDER BY backfill_key, version
        "#,
    )
    .fetch_all(pool)
    .await?;

    let backfill_active = backfill_lock_is_held(pool).await?;
    rows.into_iter()
        .map(status_from_row)
        .map(|status| status.map(|status| classify_execution_state(status, backfill_active)))
        .collect()
}

pub async fn run_parsed_event_count_backfill(
    pool: &PgPool,
    options: ParsedEventCountBackfillOptions,
) -> Result<BackfillRunStatus, ApplyError> {
    if !options.assume_quiescent {
        return Err(ApplyError::Internal(
            "parsed-event-count-v1 requires an explicit quiescent-mode acknowledgement".to_string(),
        ));
    }
    if options.batch_size <= 0 {
        return Err(ApplyError::Internal(
            "schema backfill batch size must be greater than zero".to_string(),
        ));
    }

    ensure_backfill_schema(pool).await?;
    let mut lock_conn = pool.acquire().await?;
    let acquired =
        sqlx::query_scalar::<_, bool>("SELECT pg_try_advisory_lock(hashtext($1)::bigint)")
            .bind(BACKFILL_LOCK_KEY)
            .fetch_one(&mut *lock_conn)
            .await?;
    if !acquired {
        return Err(ApplyError::Internal(format!(
            "schema backfill lock is already held for {PARSED_EVENT_COUNT_BACKFILL_KEY}"
        )));
    }

    let result = async {
        let current = get_backfill_run(pool).await?;
        if current.status == "succeeded" && !options.restart {
            return Ok(current);
        }
        if current.status == "failed" && !options.restart {
            return Err(ApplyError::Internal(format!(
                "schema backfill {PARSED_EVENT_COUNT_BACKFILL_KEY} previously failed; rerun with --restart after correcting the cause"
            )));
        }

        ensure_event_writers_quiescent(pool).await?;
        match run_parsed_event_count_backfill_locked(pool, options).await {
            Ok(status) => Ok(status),
            Err(error) => {
                if let Err(persist_error) = persist_backfill_failure(pool, &error).await {
                    return Err(ApplyError::Internal(format!(
                        "schema backfill failed ({error}) and could not persist that failure ({persist_error})"
                    )));
                }
                Err(error)
            }
        }
    }
    .await;
    let unlock_result =
        sqlx::query_scalar::<_, bool>("SELECT pg_advisory_unlock(hashtext($1)::bigint)")
            .bind(BACKFILL_LOCK_KEY)
            .fetch_one(&mut *lock_conn)
            .await
            .map(|_| ())
            .map_err(ApplyError::from);
    match (result, unlock_result) {
        (Ok(status), Ok(())) => Ok(status),
        (Err(error), Ok(())) => Err(error),
        (Ok(_), Err(error)) | (Err(_), Err(error)) => Err(error),
    }
}

async fn run_parsed_event_count_backfill_locked(
    pool: &PgPool,
    options: ParsedEventCountBackfillOptions,
) -> Result<BackfillRunStatus, ApplyError> {
    if options.restart {
        reset_parsed_event_count_backfill(pool).await?;
    }

    let mut status = get_backfill_run(pool).await?;
    if status.target_max_event_id.is_none() {
        let target_max_event_id = sqlx::query_scalar::<_, Uuid>(
            r#"
            SELECT id
            FROM core.events
            WHERE source_material_id IS NOT NULL
            ORDER BY id DESC
            LIMIT 1
            "#,
        )
        .fetch_optional(pool)
        .await?;

        sqlx::query(
            r#"
            UPDATE sinex_schemas.schema_backfill_runs
            SET status = 'running',
                phase = 'scanning',
                target_max_event_id = $3,
                started_at = COALESCE(started_at, now()),
                finished_at = NULL,
                error_message = NULL,
                updated_at = now()
            WHERE backfill_key = $1 AND version = $2
            "#,
        )
        .bind(PARSED_EVENT_COUNT_BACKFILL_KEY)
        .bind(PARSED_EVENT_COUNT_BACKFILL_VERSION)
        .bind(target_max_event_id)
        .execute(pool)
        .await?;

        status = get_backfill_run(pool).await?;
    }

    let mut completed_chunks = 0usize;
    loop {
        let rows = next_material_event_chunk(
            pool,
            status.cursor_event_id,
            status.target_max_event_id,
            options.batch_size,
        )
        .await?;
        if rows.is_empty() {
            break;
        }

        #[cfg(test)]
        if options.failpoint == Some(BackfillTestFailpoint::DuringScan) {
            return Err(ApplyError::Internal(
                "schema backfill test failure during scan".to_string(),
            ));
        }

        persist_material_event_chunk(pool, &rows).await?;
        status = get_backfill_run(pool).await?;
        completed_chunks += 1;

        if options
            .stop_after_chunks
            .is_some_and(|limit| completed_chunks >= limit)
        {
            return Ok(status);
        }
    }

    apply_parsed_event_counts(
        pool,
        #[cfg(test)]
        options.failpoint,
    )
    .await?;
    get_backfill_run(pool).await
}

async fn ensure_event_writers_quiescent(pool: &PgPool) -> Result<(), ApplyError> {
    let writer_active = sqlx::query_scalar::<_, bool>(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM pg_locks lock
            WHERE lock.locktype = 'relation'
              AND lock.granted
              AND lock.pid <> pg_backend_pid()
              AND lock.relation = 'core.events'::regclass
              AND lock.mode IN (
                  'RowExclusiveLock',
                  'ShareRowExclusiveLock',
                  'ExclusiveLock',
                  'AccessExclusiveLock'
              )
        )
        "#,
    )
    .fetch_one(pool)
    .await?;

    if writer_active {
        return Err(ApplyError::Internal(
            "schema backfill cannot start while a core event writer transaction is active; stop replay or ingest and wait for its transaction to finish".to_string(),
        ));
    }

    Ok(())
}

async fn persist_backfill_failure(pool: &PgPool, error: &ApplyError) -> Result<(), ApplyError> {
    sqlx::query(
        r#"
        UPDATE sinex_schemas.schema_backfill_runs
        SET status = 'failed',
            error_message = $3,
            started_at = COALESCE(started_at, now()),
            finished_at = now(),
            updated_at = now()
        WHERE backfill_key = $1 AND version = $2
        "#,
    )
    .bind(PARSED_EVENT_COUNT_BACKFILL_KEY)
    .bind(PARSED_EVENT_COUNT_BACKFILL_VERSION)
    .bind(error.to_string())
    .execute(pool)
    .await?;

    Ok(())
}

async fn reset_parsed_event_count_backfill(pool: &PgPool) -> Result<(), ApplyError> {
    let mut tx = pool.begin().await?;
    sqlx::query(
        r#"
        DELETE FROM sinex_schemas.schema_backfill_material_counts
        WHERE backfill_key = $1 AND version = $2
        "#,
    )
    .bind(PARSED_EVENT_COUNT_BACKFILL_KEY)
    .bind(PARSED_EVENT_COUNT_BACKFILL_VERSION)
    .execute(&mut *tx)
    .await?;

    sqlx::query(
        r#"
        UPDATE sinex_schemas.schema_backfill_runs
        SET status = 'registered',
            phase = 'registered',
            cursor_event_id = NULL,
            target_max_event_id = NULL,
            scanned_events = 0,
            applied_materials = 0,
            error_message = NULL,
            started_at = NULL,
            finished_at = NULL,
            updated_at = now()
        WHERE backfill_key = $1 AND version = $2
        "#,
    )
    .bind(PARSED_EVENT_COUNT_BACKFILL_KEY)
    .bind(PARSED_EVENT_COUNT_BACKFILL_VERSION)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(())
}

async fn get_backfill_run(pool: &PgPool) -> Result<BackfillRunStatus, ApplyError> {
    let row = sqlx::query(
        r#"
        SELECT
            backfill_key,
            version,
            status,
            phase,
            cursor_event_id,
            target_max_event_id,
            scanned_events,
            applied_materials,
            error_message,
            started_at::text AS started_at,
            finished_at::text AS finished_at,
            updated_at::text AS updated_at
        FROM sinex_schemas.schema_backfill_runs
        WHERE backfill_key = $1 AND version = $2
        "#,
    )
    .bind(PARSED_EVENT_COUNT_BACKFILL_KEY)
    .bind(PARSED_EVENT_COUNT_BACKFILL_VERSION)
    .fetch_one(pool)
    .await?;

    let backfill_active = backfill_lock_is_held(pool).await?;
    status_from_row(row).map(|status| classify_execution_state(status, backfill_active))
}

async fn backfill_lock_is_held(pool: &PgPool) -> Result<bool, ApplyError> {
    let mut connection = pool.acquire().await?;
    let acquired =
        sqlx::query_scalar::<_, bool>("SELECT pg_try_advisory_lock(hashtext($1)::bigint)")
            .bind(BACKFILL_LOCK_KEY)
            .fetch_one(&mut *connection)
            .await?;
    if !acquired {
        return Ok(true);
    }

    let released = sqlx::query_scalar::<_, bool>("SELECT pg_advisory_unlock(hashtext($1)::bigint)")
        .bind(BACKFILL_LOCK_KEY)
        .fetch_one(&mut *connection)
        .await?;
    if !released {
        return Err(ApplyError::Internal(
            "schema backfill activity probe could not release its advisory lock".to_string(),
        ));
    }

    Ok(false)
}

async fn next_material_event_chunk(
    pool: &PgPool,
    cursor_event_id: Option<Uuid>,
    target_max_event_id: Option<Uuid>,
    batch_size: i64,
) -> Result<Vec<(Uuid, Uuid)>, ApplyError> {
    let rows = sqlx::query(
        r#"
        SELECT id, source_material_id
        FROM core.events
        WHERE source_material_id IS NOT NULL
          AND ($1::uuid IS NULL OR id > $1)
          AND ($2::uuid IS NULL OR id <= $2)
        ORDER BY id
        LIMIT $3
        "#,
    )
    .bind(cursor_event_id)
    .bind(target_max_event_id)
    .bind(batch_size)
    .fetch_all(pool)
    .await?;

    rows.into_iter()
        .map(|row| {
            let event_id: Uuid = row.try_get("id")?;
            let source_material_id: Uuid = row.try_get("source_material_id")?;
            Ok((event_id, source_material_id))
        })
        .collect::<Result<Vec<_>, sqlx::Error>>()
        .map_err(ApplyError::from)
}

async fn persist_material_event_chunk(
    pool: &PgPool,
    rows: &[(Uuid, Uuid)],
) -> Result<(), ApplyError> {
    let mut counts: HashMap<Uuid, i64> = HashMap::new();
    let mut last_event_id = None;
    for (event_id, source_material_id) in rows {
        *counts.entry(*source_material_id).or_default() += 1;
        last_event_id = Some(*event_id);
    }

    let mut tx = pool.begin().await?;
    for (source_material_id, event_count) in counts {
        sqlx::query(
            r#"
            INSERT INTO sinex_schemas.schema_backfill_material_counts (
                backfill_key, version, source_material_id, event_count
            )
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (backfill_key, version, source_material_id)
            DO UPDATE SET
                event_count = sinex_schemas.schema_backfill_material_counts.event_count
                    + EXCLUDED.event_count,
                updated_at = now()
            "#,
        )
        .bind(PARSED_EVENT_COUNT_BACKFILL_KEY)
        .bind(PARSED_EVENT_COUNT_BACKFILL_VERSION)
        .bind(source_material_id)
        .bind(event_count)
        .execute(&mut *tx)
        .await?;
    }

    sqlx::query(
        r#"
        UPDATE sinex_schemas.schema_backfill_runs
        SET status = 'running',
            phase = 'scanning',
            cursor_event_id = $3,
            scanned_events = scanned_events + $4,
            updated_at = now()
        WHERE backfill_key = $1 AND version = $2
        "#,
    )
    .bind(PARSED_EVENT_COUNT_BACKFILL_KEY)
    .bind(PARSED_EVENT_COUNT_BACKFILL_VERSION)
    .bind(last_event_id)
    .bind(i64::try_from(rows.len()).map_err(|_| {
        ApplyError::Internal("schema backfill chunk length does not fit in i64".to_string())
    })?)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(())
}

async fn apply_parsed_event_counts(
    pool: &PgPool,
    #[cfg(test)] failpoint: Option<BackfillTestFailpoint>,
) -> Result<(), ApplyError> {
    let mut tx = pool.begin().await?;

    sqlx::query(
        r#"
        UPDATE sinex_schemas.schema_backfill_runs
        SET phase = 'applying',
            updated_at = now()
        WHERE backfill_key = $1 AND version = $2
        "#,
    )
    .bind(PARSED_EVENT_COUNT_BACKFILL_KEY)
    .bind(PARSED_EVENT_COUNT_BACKFILL_VERSION)
    .execute(&mut *tx)
    .await?;

    #[cfg(test)]
    if failpoint == Some(BackfillTestFailpoint::DuringApply) {
        return Err(ApplyError::Internal(
            "schema backfill test failure during apply".to_string(),
        ));
    }

    sqlx::query("UPDATE raw.source_material_registry SET parsed_event_count = 0")
        .execute(&mut *tx)
        .await?;

    let update_result = sqlx::query(
        r#"
        UPDATE raw.source_material_registry sm
        SET parsed_event_count = counts.event_count
        FROM sinex_schemas.schema_backfill_material_counts counts
        WHERE counts.backfill_key = $1
          AND counts.version = $2
          AND counts.source_material_id = sm.id
        "#,
    )
    .bind(PARSED_EVENT_COUNT_BACKFILL_KEY)
    .bind(PARSED_EVENT_COUNT_BACKFILL_VERSION)
    .execute(&mut *tx)
    .await?;

    sqlx::query(
        r#"
        UPDATE sinex_schemas.schema_backfill_runs
        SET status = 'succeeded',
            phase = 'complete',
            applied_materials = $3,
            finished_at = now(),
            updated_at = now()
        WHERE backfill_key = $1 AND version = $2
        "#,
    )
    .bind(PARSED_EVENT_COUNT_BACKFILL_KEY)
    .bind(PARSED_EVENT_COUNT_BACKFILL_VERSION)
    .bind(i64::try_from(update_result.rows_affected()).map_err(|_| {
        ApplyError::Internal("schema backfill affected-row count does not fit in i64".to_string())
    })?)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(())
}

fn status_from_row(row: sqlx::postgres::PgRow) -> Result<BackfillRunStatus, ApplyError> {
    Ok(BackfillRunStatus {
        backfill_key: row.try_get("backfill_key")?,
        version: row.try_get("version")?,
        status: row.try_get("status")?,
        execution_state: BackfillExecutionState::Registered,
        phase: row.try_get("phase")?,
        cursor_event_id: row.try_get("cursor_event_id")?,
        target_max_event_id: row.try_get("target_max_event_id")?,
        scanned_events: row.try_get("scanned_events")?,
        applied_materials: row.try_get("applied_materials")?,
        error_message: row.try_get("error_message")?,
        started_at: row.try_get("started_at")?,
        finished_at: row.try_get("finished_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn classify_execution_state(
    mut status: BackfillRunStatus,
    backfill_active: bool,
) -> BackfillRunStatus {
    status.execution_state = match status.status.as_str() {
        "registered" => BackfillExecutionState::Registered,
        "running" if backfill_active => BackfillExecutionState::Active,
        "running" => BackfillExecutionState::Interrupted,
        "failed" => BackfillExecutionState::Failed,
        "succeeded" => BackfillExecutionState::Succeeded,
        _ => BackfillExecutionState::Failed,
    };
    status
}
