//! Storage for the analysis layer (spec `analysis.md` §14): the rows metrics
//! read, the evidence observation opportunity is computed from, maintained
//! metric values, and products (requests, attempts, results, dependencies,
//! status, findings). The analysis itself is pure code in `sinex-analysis`;
//! the daemon joins the two.
use crate::Result;
use serde_json::Value;
use sqlx::PgConnection;
use sqlx::postgres::types::PgRange;
use std::ops::Bound;
use time::OffsetDateTime;
use uuid::Uuid;

/// A live event of a relation, as a metric reads it.
#[derive(Clone, Debug)]
pub struct EventRow {
    pub id: Uuid,
    pub source: String,
    pub ts_orig: OffsetDateTime,
    pub payload: Value,
}

/// The live events of `relation` with a known time in `[from, to)`, as of a
/// commit position, with only the payload `fields` a metric reads (a journal
/// entry's payload holds every journal field; a count of errors needs one).
pub async fn relation_rows(
    conn: &mut PgConnection,
    relation: &str,
    from: OffsetDateTime,
    to: OffsetDateTime,
    as_of: i64,
    fields: &[String],
) -> Result<Vec<EventRow>> {
    let rows = sqlx::query!(
        r#"SELECT e.id, e.source, e.ts_orig AS "ts_orig!",
                  (SELECT coalesce(jsonb_object_agg(k, e.payload -> k), '{}'::jsonb)
                   FROM unnest($5::text[]) AS k WHERE e.payload ? k) AS "payload!"
           FROM core.events e
           WHERE e.event_type = $1 AND e.ts_orig >= $2 AND e.ts_orig < $3 AND e.commit_seq <= $4
             AND NOT EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id = e.id AND a.archived_seq <= $4)"#,
        relation,
        from,
        to,
        as_of,
        fields,
    )
    .fetch_all(&mut *conn)
    .await?;
    Ok(rows
        .into_iter()
        .map(|r| EventRow {
            id: r.id,
            source: r.source,
            ts_orig: r.ts_orig,
            payload: r.payload,
        })
        .collect())
}

/// What the store holds about one source's coverage: its earliest and latest
/// live record, and the runs that produced its events with when each began,
/// ended and last recorded.
#[derive(Clone, Debug, Default)]
pub struct SourceEvidence {
    pub records: Option<(OffsetDateTime, OffsetDateTime)>,
    pub runs: Vec<RunSpan>,
}

#[derive(Clone, Debug)]
pub struct RunSpan {
    pub started_at: OffsetDateTime,
    pub ended_at: Option<OffsetDateTime>,
    pub last_record: Option<OffsetDateTime>,
}

pub async fn source_evidence(
    conn: &mut PgConnection,
    source: &str,
    as_of: i64,
) -> Result<SourceEvidence> {
    let bounds = sqlx::query!(
        r#"SELECT min(e.ts_orig) AS first, max(e.ts_orig) AS last
           FROM core.events e
           WHERE e.source = $1 AND e.ts_orig IS NOT NULL AND e.commit_seq <= $2
             AND NOT EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id = e.id AND a.archived_seq <= $2)"#,
        source,
        as_of,
    )
    .fetch_one(&mut *conn)
    .await?;
    let runs = sqlx::query!(
        r#"SELECT r.started_at, r.ended_at, max(e.ts_orig) AS last_record
           FROM core.events e JOIN core.runs r ON r.id = e.run_id
           WHERE e.source = $1 AND e.commit_seq <= $2
           GROUP BY r.id, r.started_at, r.ended_at
           ORDER BY r.started_at"#,
        source,
        as_of,
    )
    .fetch_all(&mut *conn)
    .await?;
    Ok(SourceEvidence {
        records: bounds.first.zip(bounds.last),
        runs: runs
            .into_iter()
            .map(|r| RunSpan {
                started_at: r.started_at,
                ended_at: r.ended_at,
                last_record: r.last_record,
            })
            .collect(),
    })
}

/// The newest commit position.
pub async fn head(conn: &mut PgConnection) -> Result<i64> {
    Ok(
        sqlx::query_scalar!("SELECT value FROM core.counters WHERE name = 'commit_seq'")
            .fetch_optional(&mut *conn)
            .await?
            .unwrap_or(0),
    )
}

/// One maintained metric cell.
#[derive(Clone, Debug, PartialEq)]
pub struct MetricValue {
    pub metric: String,
    pub metric_version: i32,
    pub unit_start: OffsetDateTime,
    pub unit_end: OffsetDateTime,
    pub unit_label: String,
    pub dimension: String,
    pub dimension_value: String,
    pub state: String,
    pub reason: Option<String>,
    pub value: Option<f64>,
    pub observed_fraction: Option<f64>,
    pub numerator: Option<f64>,
    pub denominator: Option<f64>,
    pub n_rows: i64,
    pub observed_s: f64,
}

/// The cells of one metric version under one day setting, over units
/// starting in `[from, to)`.
pub struct MetricRange<'a> {
    pub metric: &'a str,
    pub metric_version: i32,
    pub day_setting: &'a str,
    pub from: OffsetDateTime,
    pub to: OffsetDateTime,
}

/// Replace the cells in `range` with `cells`: a breakdown value that no
/// longer occurs disappears rather than lingering.
pub async fn replace_metric_values(
    conn: &mut PgConnection,
    range: &MetricRange<'_>,
    cells: &[MetricValue],
    computed_seq: i64,
) -> Result<()> {
    let MetricRange {
        metric,
        metric_version,
        day_setting,
        from,
        to,
    } = *range;
    sqlx::query!(
        r#"DELETE FROM analysis.metric_values
           WHERE metric = $1 AND metric_version = $2 AND day_setting = $3 AND unit_start >= $4 AND unit_start < $5"#,
        metric,
        metric_version,
        day_setting,
        from,
        to,
    )
    .execute(&mut *conn)
    .await?;
    if cells.is_empty() {
        return Ok(());
    }
    let col = |f: fn(&MetricValue) -> String| cells.iter().map(f).collect::<Vec<String>>();
    let starts: Vec<OffsetDateTime> = cells.iter().map(|c| c.unit_start).collect();
    let ends: Vec<OffsetDateTime> = cells.iter().map(|c| c.unit_end).collect();
    let opt =
        |f: fn(&MetricValue) -> Option<f64>| cells.iter().map(f).collect::<Vec<Option<f64>>>();
    let reasons: Vec<Option<String>> = cells.iter().map(|c| c.reason.clone()).collect();
    let n_rows: Vec<i64> = cells.iter().map(|c| c.n_rows).collect();
    let observed_s: Vec<f64> = cells.iter().map(|c| c.observed_s).collect();
    sqlx::query!(
        r#"INSERT INTO analysis.metric_values
             (metric, metric_version, day_setting, unit_start, unit_end, unit_label, dimension, dimension_value,
              state, reason, value, observed_fraction, numerator, denominator, n_rows, observed_s, computed_seq)
           SELECT $1, $2, $3, u.unit_start, u.unit_end, u.unit_label, u.dimension, u.dimension_value,
                  u.state, u.reason, u.value, u.observed_fraction, u.numerator, u.denominator, u.n_rows, u.observed_s, $4
           FROM unnest($5::timestamptz[], $6::timestamptz[], $7::text[], $8::text[], $9::text[], $10::text[],
                       $11::text[], $12::float8[], $13::float8[], $14::float8[], $15::float8[], $16::int8[], $17::float8[])
             AS u(unit_start, unit_end, unit_label, dimension, dimension_value, state, reason, value,
                  observed_fraction, numerator, denominator, n_rows, observed_s)"#,
        metric,
        metric_version,
        day_setting,
        computed_seq,
        &starts,
        &ends,
        &col(|c| c.unit_label.clone()),
        &col(|c| c.dimension.clone()),
        &col(|c| c.dimension_value.clone()),
        &col(|c| c.state.clone()),
        &reasons as &[Option<String>],
        &opt(|c| c.value) as &[Option<f64>],
        &opt(|c| c.observed_fraction) as &[Option<f64>],
        &opt(|c| c.numerator) as &[Option<f64>],
        &opt(|c| c.denominator) as &[Option<f64>],
        &n_rows,
        &observed_s,
    )
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// The stored cells of one metric over units starting in `[from, to)`.
pub async fn metric_values(
    conn: &mut PgConnection,
    metric: &str,
    metric_version: i32,
    day_setting: &str,
    from: OffsetDateTime,
    to: OffsetDateTime,
) -> Result<Vec<MetricValue>> {
    let rows = sqlx::query_as!(
        MetricValue,
        r#"SELECT metric, metric_version, unit_start, unit_end, unit_label, dimension, dimension_value, state,
                  reason, value, observed_fraction, numerator, denominator, n_rows, observed_s
           FROM analysis.metric_values
           WHERE metric = $1 AND metric_version = $2 AND day_setting = $3 AND unit_start >= $4 AND unit_start < $5
           ORDER BY unit_start, dimension, dimension_value"#,
        metric,
        metric_version,
        day_setting,
        from,
        to,
    )
    .fetch_all(&mut *conn)
    .await?;
    Ok(rows)
}

/// Clear a metric unit's dirty mark unless it was marked again after `seq`.
pub async fn clear_dirty(
    conn: &mut PgConnection,
    derivation: &str,
    key: &Value,
    seq: i64,
) -> Result<()> {
    sqlx::query!(
        "DELETE FROM derivation.dirty WHERE derivation = $1 AND key = $2 AND last_dirty_seq <= $3",
        derivation,
        key,
        seq,
    )
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// A product to store: the request, its attempt and result, what the result
/// depends on, and its findings.
#[derive(Clone, Debug)]
pub struct NewResult {
    pub request_id: [u8; 32],
    pub request_body: Value,
    pub recipe: String,
    pub recipe_version: i32,
    pub intent: &'static str,
    pub family: Option<Value>,
    pub as_of: i64,
    /// The standing question this evaluates; a newer result supersedes older ones.
    pub standing: Option<String>,
    pub requested_by: Value,
    pub producer: String,
    pub started_at: OffsetDateTime,
    pub status: &'static str,
    pub reason: Option<String>,
    pub readsets: Value,
    pub tables: Value,
    pub diagnostics: Value,
    pub output_hash: [u8; 32],
    pub dependencies: Vec<(String, OffsetDateTime, OffsetDateTime)>,
    pub findings: Vec<NewFinding>,
}

#[derive(Clone, Debug)]
pub struct NewFinding {
    pub template: String,
    pub slots: Value,
    pub role: String,
    pub status: String,
    pub caveats: Value,
    pub evidence: Value,
    pub checks: Value,
}

/// A stored result and its state.
#[derive(Clone, Debug, PartialEq)]
pub struct StoredResult {
    pub id: Uuid,
    pub state: String,
}

/// The current result of an identical request, if one exists: asking the
/// same question at the same commit position returns the stored answer.
pub async fn current_result(
    conn: &mut PgConnection,
    request_id: &[u8; 32],
) -> Result<Option<Uuid>> {
    Ok(sqlx::query_scalar!(
        r#"SELECT r.id FROM analysis.results r JOIN analysis.result_status s ON s.result = r.id
           WHERE r.request = $1 AND s.state = 'current' ORDER BY r.created_at DESC LIMIT 1"#,
        &request_id[..],
    )
    .fetch_optional(&mut *conn)
    .await?)
}

/// Store a product in one transaction. A result whose dependency types were
/// written after its as-of position is stored `stale`, never `current`, so a
/// change racing the computation cannot be missed (`analysis.md` §7.3); a
/// current result of the same standing question is superseded.
pub async fn store_result(conn: &mut PgConnection, r: &NewResult) -> Result<StoredResult> {
    sqlx::query!(
        r#"INSERT INTO analysis.requests (id, body, recipe, recipe_version, intent, family, as_of, standing)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8) ON CONFLICT (id) DO NOTHING"#,
        &r.request_id[..],
        r.request_body,
        r.recipe,
        r.recipe_version,
        r.intent,
        r.family,
        r.as_of,
        r.standing,
    )
    .execute(&mut *conn)
    .await?;
    let attempt = Uuid::now_v7();
    sqlx::query!(
        r#"INSERT INTO analysis.attempts (id, request, requested_by, producer, started_at, finished_at, status)
           VALUES ($1, $2, $3, $4, $5, now(), 'succeeded')"#,
        attempt,
        &r.request_id[..],
        r.requested_by,
        r.producer,
        r.started_at,
    )
    .execute(&mut *conn)
    .await?;
    let id = Uuid::now_v7();
    sqlx::query!(
        r#"INSERT INTO analysis.results (id, attempt, request, status, reason, readsets, tables, diagnostics, output_hash)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)"#,
        id,
        attempt,
        &r.request_id[..],
        r.status,
        r.reason,
        r.readsets,
        r.tables,
        r.diagnostics,
        &r.output_hash[..],
    )
    .execute(&mut *conn)
    .await?;
    let relations: Vec<String> = r.dependencies.iter().map(|d| d.0.clone()).collect();
    let spans: Vec<PgRange<OffsetDateTime>> = r
        .dependencies
        .iter()
        .map(|d| PgRange {
            start: Bound::Included(d.1),
            end: Bound::Excluded(d.2),
        })
        .collect();
    sqlx::query!(
        r#"INSERT INTO analysis.result_dependencies (result, relation, span)
           SELECT $1, d.relation, d.span FROM unnest($2::text[], $3::tstzrange[]) AS d(relation, span)"#,
        id,
        &relations,
        &spans as &[PgRange<OffsetDateTime>],
    )
    .execute(&mut *conn)
    .await?;
    let head = head(conn).await?;
    let raced = sqlx::query_scalar!(
        r#"SELECT EXISTS (SELECT 1 FROM core.commit_log c
                          WHERE c.commit_seq > $1 AND c.event_type = ANY($2)) AS "raced!""#,
        r.as_of,
        &relations,
    )
    .fetch_one(&mut *conn)
    .await?;
    let state = if raced { "stale" } else { "current" };
    if let Some(standing) = &r.standing {
        sqlx::query!(
            r#"UPDATE analysis.result_status s SET state = 'superseded', reason = 'a newer result of the standing question',
                      changed_seq = $3, changed_at = now()
               FROM analysis.results r JOIN analysis.requests q ON q.id = r.request
               WHERE s.result = r.id AND q.standing = $1 AND r.id <> $2 AND s.state IN ('current', 'stale')"#,
            standing,
            id,
            head,
        )
        .execute(&mut *conn)
        .await?;
    }
    sqlx::query!(
        r#"INSERT INTO analysis.result_status (result, state, reason, changed_seq)
           VALUES ($1, $2, $3, $4)"#,
        id,
        state,
        raced.then_some("inputs changed after the as-of position"),
        head,
    )
    .execute(&mut *conn)
    .await?;
    for f in &r.findings {
        sqlx::query!(
            r#"INSERT INTO analysis.findings (id, result, template, slots, role, status, caveats, evidence, checks)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)"#,
            Uuid::now_v7(),
            id,
            f.template,
            f.slots,
            f.role,
            f.status,
            f.caveats,
            f.evidence,
            f.checks,
        )
        .execute(&mut *conn)
        .await?;
    }
    Ok(StoredResult {
        id,
        state: state.to_owned(),
    })
}

/// A stored result with its request, status and findings.
#[derive(Clone, Debug)]
pub struct ResultView {
    pub id: Uuid,
    pub request: Value,
    pub recipe: String,
    pub as_of: i64,
    pub status: String,
    pub reason: Option<String>,
    pub state: String,
    pub state_reason: Option<String>,
    pub readsets: Value,
    pub tables: Option<Value>,
    pub diagnostics: Value,
    pub created_at: OffsetDateTime,
    pub findings: Vec<Value>,
}

pub async fn result(conn: &mut PgConnection, id: Uuid) -> Result<Option<ResultView>> {
    let Some(row) = sqlx::query!(
        r#"SELECT r.id, q.body, q.recipe, q.as_of, r.status, r.reason, s.state, s.reason AS state_reason,
                  r.readsets, r.tables, r.diagnostics, r.created_at
           FROM analysis.results r
           JOIN analysis.requests q ON q.id = r.request
           JOIN analysis.result_status s ON s.result = r.id
           WHERE r.id = $1"#,
        id
    )
    .fetch_optional(&mut *conn)
    .await?
    else {
        return Ok(None);
    };
    let findings = sqlx::query_scalar!(
        r#"SELECT jsonb_build_object('template', template, 'slots', slots, 'role', role, 'status', status,
                                     'caveats', caveats, 'evidence', evidence, 'checks', checks) AS "f!"
           FROM analysis.findings WHERE result = $1 ORDER BY id"#,
        id
    )
    .fetch_all(&mut *conn)
    .await?;
    Ok(Some(ResultView {
        id: row.id,
        request: row.body,
        recipe: row.recipe,
        as_of: row.as_of,
        status: row.status,
        reason: row.reason,
        state: row.state,
        state_reason: row.state_reason,
        readsets: row.readsets,
        tables: row.tables,
        diagnostics: row.diagnostics,
        created_at: row.created_at,
        findings,
    }))
}

/// The newest results, optionally of one standing question.
pub async fn recent_results(
    conn: &mut PgConnection,
    standing: Option<&str>,
    limit: i64,
) -> Result<Vec<(Uuid, String, String, String, OffsetDateTime)>> {
    let rows = sqlx::query!(
        r#"SELECT r.id, q.recipe, r.status, s.state, r.created_at
           FROM analysis.results r
           JOIN analysis.requests q ON q.id = r.request
           JOIN analysis.result_status s ON s.result = r.id
           WHERE $1::text IS NULL OR q.standing = $1
           ORDER BY r.created_at DESC LIMIT $2"#,
        standing,
        limit,
    )
    .fetch_all(&mut *conn)
    .await?;
    Ok(rows
        .into_iter()
        .map(|r| (r.id, r.recipe, r.status, r.state, r.created_at))
        .collect())
}

/// Mark stale every current result that depends on the changed events'
/// types over a range their times fall in. Called by admission in the same
/// transaction that admits or archives the events, so nothing polls. One
/// range per event type covers the batch: a result between two changed days
/// may be marked stale needlessly, never left current wrongly.
pub(crate) async fn mark_stale(conn: &mut PgConnection, changed: &[Uuid], seq: i64) -> Result<u64> {
    if changed.is_empty() {
        return Ok(0);
    }
    let done = sqlx::query!(
        r#"WITH changed AS (
             SELECT e.event_type, min(e.ts_orig) AS lo, max(e.ts_orig) AS hi
             FROM core.events e WHERE e.id = ANY($1) AND e.ts_orig IS NOT NULL
             GROUP BY e.event_type)
           UPDATE analysis.result_status s
           SET state = 'stale', reason = 'inputs changed', changed_seq = $2, changed_at = now()
           FROM analysis.result_dependencies d, changed c
           WHERE s.result = d.result AND s.state = 'current'
             AND d.relation = c.event_type AND d.span && tstzrange(c.lo, c.hi, '[]')"#,
        changed,
        seq,
    )
    .execute(&mut *conn)
    .await?;
    Ok(done.rows_affected())
}
