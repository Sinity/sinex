//! The live intervals side table (`core.live_intervals`): for every live
//! event of an interval kind that lasts longer than an instant, where it
//! ends, so a window read finds the earlier intervals that reach it by index
//! instead of reading the kind's history. Admission and restore add rows,
//! archiving removes them, each in the transaction that changes liveness.
//! The store computes every end with `core.interval_end`, so the rows a
//! schema upgrade filled and the rows admission writes agree.
use crate::Result;
use crate::derivations::{Kind, Window};
use crate::event_row::EventRow;
use serde_json::Value;
use sinex_model::clock::{INTERVAL_KINDS, IntervalKind};
use sqlx::PgConnection;
use time::OffsetDateTime;
use uuid::Uuid;

/// The interval kinds as the parallel arrays the statements unnest.
struct Declared {
    event_types: Vec<String>,
    end_fields: Vec<String>,
    end_rules: Vec<String>,
    narrowed_by: Vec<Option<String>>,
}

fn declared() -> Declared {
    Declared {
        event_types: INTERVAL_KINDS
            .iter()
            .map(|k| k.event_type.to_owned())
            .collect(),
        end_fields: INTERVAL_KINDS
            .iter()
            .map(|k| k.end.field().to_owned())
            .collect(),
        end_rules: INTERVAL_KINDS
            .iter()
            .map(|k| k.end.rule().to_owned())
            .collect(),
        narrowed_by: INTERVAL_KINDS
            .iter()
            .map(|k| k.narrowed_by.map(str::to_owned))
            .collect(),
    }
}

/// Record the staged batch's intervals; every staged event is admitted live.
pub(crate) async fn admitted(conn: &mut PgConnection) -> Result<()> {
    let kinds = declared();
    sqlx::query!(
        r#"INSERT INTO core.live_intervals (event_id, event_type, narrowing, started_at, ended_at)
           SELECT s.id, s.event_type, s.payload->>k.narrowed_by, s.ts_orig, i.ended_at
           FROM admission_stage s
           JOIN unnest($1::text[], $2::text[], $3::text[], $4::text[])
             AS k(event_type, end_field, end_rule, narrowed_by) ON k.event_type = s.event_type
           CROSS JOIN LATERAL (
             SELECT core.interval_end(k.end_rule, s.ts_orig, s.payload->k.end_field) AS ended_at) i
           WHERE s.ts_orig IS NOT NULL AND (i.ended_at IS NULL OR i.ended_at > s.ts_orig)"#,
        &kinds.event_types,
        &kinds.end_fields,
        &kinds.end_rules,
        &kinds.narrowed_by as &[Option<String>],
    )
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// Record the intervals among events that just became live again.
pub(crate) async fn restored(conn: &mut PgConnection, events: &[Uuid]) -> Result<()> {
    if events.is_empty() {
        return Ok(());
    }
    let kinds = declared();
    sqlx::query!(
        r#"INSERT INTO core.live_intervals (event_id, event_type, narrowing, started_at, ended_at)
           SELECT e.id, e.event_type, e.payload->>k.narrowed_by, e.ts_orig, i.ended_at
           FROM core.events e
           JOIN unnest($1::text[], $2::text[], $3::text[], $4::text[])
             AS k(event_type, end_field, end_rule, narrowed_by) ON k.event_type = e.event_type
           CROSS JOIN LATERAL (
             SELECT core.interval_end(k.end_rule, e.ts_orig, e.payload->k.end_field) AS ended_at) i
           WHERE e.id = ANY($5) AND e.event_type = ANY($1)
             AND e.ts_orig IS NOT NULL AND (i.ended_at IS NULL OR i.ended_at > e.ts_orig)"#,
        &kinds.event_types,
        &kinds.end_fields,
        &kinds.end_rules,
        &kinds.narrowed_by as &[Option<String>],
        events,
    )
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// Forget the intervals among events that just left the live set.
pub(crate) async fn archived(conn: &mut PgConnection, events: &[Uuid]) -> Result<()> {
    sqlx::query!(
        "DELETE FROM core.live_intervals WHERE event_id = ANY($1)",
        events
    )
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// The value of an interval kind's narrowing field that a window kind fixes.
fn fixed<'a>(interval: &IntervalKind, kind: &'a Kind) -> Option<&'a str> {
    kind.contains.get(interval.narrowed_by?)?.as_str()
}

/// What the index is asked: each interval kind's event type with the
/// narrowing value a window kind fixes, or with none for the whole type.
fn asked(window: &Window) -> (Vec<String>, Vec<Option<String>>) {
    let mut asked: Vec<(&str, Option<&str>)> = Vec::new();
    for interval in &window.intervals {
        let mut values: Vec<Option<&str>> = window
            .kinds
            .iter()
            .filter(|kind| kind.event_type == interval.event_type)
            .map(|kind| fixed(interval, kind))
            .collect();
        // A kind that fixes no value reads the whole type, which covers the rest.
        if values.contains(&None) {
            values = vec![None];
        }
        for value in values {
            if !asked.contains(&(interval.event_type, value)) {
                asked.push((interval.event_type, value));
            }
        }
    }
    asked
        .into_iter()
        .map(|(event_type, value)| (event_type.to_owned(), value.map(str::to_owned)))
        .unzip()
}

/// How many selected events one row read covers, as for commit-order pages.
const ROWS_PER_READ: usize = 64;

/// The live events of the window's interval kinds that started before the
/// window and reach its start, or have no end. The index selects their
/// identities; their rows are then read in small groups bounded by ID, kind
/// and time, so compressed batches outside a group are skipped.
pub(crate) async fn reaching(
    conn: &mut PgConnection,
    window: &Window,
    start: OffsetDateTime,
) -> Result<Vec<EventRow>> {
    let (event_types, narrowings) = asked(window);
    if event_types.is_empty() {
        return Ok(Vec::new());
    }
    let mut selected = sqlx::query_file!(
        "src/derivations/sql/reaching.sql",
        &event_types,
        &narrowings as &[Option<String>],
        start,
        window.end,
    )
    .fetch_all(&mut *conn)
    .await?;
    selected.sort_by_key(|row| (row.started_at, row.event_id));
    let (kind_types, contains): (Vec<String>, Vec<Value>) = window
        .kinds
        .iter()
        .map(|k| (k.event_type.clone(), k.contains.clone()))
        .unzip();
    let mut rows = Vec::with_capacity(selected.len());
    for group in selected.chunks(ROWS_PER_READ) {
        let ids: Vec<Uuid> = group.iter().map(|row| row.event_id).collect();
        let mut types: Vec<String> = group.iter().map(|row| row.event_type.clone()).collect();
        types.sort();
        types.dedup();
        rows.extend(
            sqlx::query_file_as!(
                EventRow,
                "src/derivations/sql/reaching_rows.sql",
                &ids,
                ids.iter().min().copied(),
                ids.iter().max().copied(),
                &types,
                group.first().map(|row| row.started_at),
                group.last().map(|row| row.started_at),
                &kind_types,
                &contains,
            )
            .fetch_all(&mut *conn)
            .await?,
        );
    }
    rows.sort_by_key(|row| (row.ts_orig, row.commit_seq));
    Ok(rows)
}
