//! The time each live output of a keyed derivation covers
//! (`derivation.output_ranges`). An input can change a key whose stored
//! output covers part of the input's range. A derivation declares, per output
//! type, the payload fields that record what an output covers; admission
//! keeps that range beside each key's membership when it admits the output
//! and drops it when the output is archived, each in the transaction that
//! changes the output. The keys an input's range touches are then found
//! through two indexes, at the cost of the outputs that overlap and the keys
//! that have an open output, whatever the derivation has stored.
use crate::Result;
use crate::admission::{AffectedRange, DirtyKeys};
use serde_json::Value;
use sqlx::PgConnection;
use std::collections::{BTreeMap, HashSet};
use time::OffsetDateTime;
use uuid::Uuid;

/// Keep the ranges of the outputs a batch just made members of `derivation`'s
/// keys: each pair of `keys` and `outputs` is a new membership. An output the
/// batch admitted has its range read from its staged payload. One admitted
/// earlier, which the batch repeats under another key, keeps the range stored
/// with it. An output with no readable start covers nothing and gets no row.
pub(crate) async fn record(
    conn: &mut PgConnection,
    derivation: &str,
    declared: &dyn DirtyKeys,
    keys: Vec<Value>,
    outputs: Vec<Uuid>,
) -> Result<()> {
    let declared: Vec<_> = declared
        .output_ranges()
        .into_iter()
        .filter(|range| range.derivation == derivation)
        .collect();
    if declared.is_empty() || outputs.is_empty() {
        return Ok(());
    }
    let text = |field: fn(&crate::OutputRange) -> &'static str| -> Vec<String> {
        declared
            .iter()
            .map(|range| field(range).to_owned())
            .collect()
    };
    let optional = |field: fn(&crate::OutputRange) -> Option<&'static str>| -> Vec<Option<String>> {
        declared
            .iter()
            .map(|range| field(range).map(str::to_owned))
            .collect()
    };
    // The stage holds the events this batch admitted, and no other.
    let staged: HashSet<(Value, Uuid)> = sqlx::query!(
        r#"INSERT INTO derivation.output_ranges (derivation, key, event_id, event_type, started_at, ended_at)
           SELECT $1, f.key, s.id, s.event_type, r.started_at, r.ended_at
           FROM unnest($2::jsonb[], $3::uuid[]) AS f(key, event_id)
           JOIN admission_stage s ON s.id = f.event_id
           JOIN unnest($4::text[], $5::text[], $6::text[], $7::text[], $8::text[])
             AS d(output_type, start_field, end_field, open_field, fence_field)
             ON d.output_type = s.event_type
           CROSS JOIN LATERAL (
             SELECT core.payload_time(s.payload->>d.start_field) AS started_at,
                    CASE WHEN s.payload->d.open_field = 'true'::jsonb THEN NULL
                         ELSE coalesce(core.payload_time(s.payload->>d.end_field),
                                       core.payload_time(s.payload->>d.fence_field)) END AS ended_at) r
           WHERE r.started_at IS NOT NULL
           ON CONFLICT DO NOTHING
           RETURNING key, event_id"#,
        derivation,
        &keys,
        &outputs,
        &text(|range| range.output_type),
        &text(|range| range.start_field),
        &text(|range| range.end_field),
        &optional(|range| range.open_field) as &[Option<String>],
        &optional(|range| range.fence_field) as &[Option<String>],
    )
    .fetch_all(&mut *conn)
    .await?
    .into_iter()
    .map(|row| (row.key, row.event_id))
    .collect();
    let (keys, outputs): (Vec<Value>, Vec<Uuid>) = keys
        .into_iter()
        .zip(outputs)
        .filter(|pair| !staged.contains(pair))
        .unzip();
    if !outputs.is_empty() {
        sqlx::query!(
            r#"INSERT INTO derivation.output_ranges (derivation, key, event_id, event_type, started_at, ended_at)
               SELECT DISTINCT ON (f.key, x.event_id)
                      $1, f.key, x.event_id, x.event_type, x.started_at, x.ended_at
               FROM unnest($2::jsonb[], $3::uuid[]) AS f(key, event_id)
               JOIN derivation.output_ranges x ON x.event_id = f.event_id AND x.derivation = $1
               ON CONFLICT DO NOTHING"#,
            derivation,
            &keys,
            &outputs,
        )
        .execute(&mut *conn)
        .await?;
    }
    Ok(())
}

/// Forget the ranges of outputs that just left the live set.
pub(crate) async fn archived(conn: &mut PgConnection, events: &[Uuid]) -> Result<()> {
    sqlx::query!(
        "DELETE FROM derivation.output_ranges WHERE event_id = ANY($1)",
        events
    )
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// The keys with a live output that covers part of one of `ranges`: an output
/// that starts no later than the range ends and ends no earlier than it
/// starts, both bounds inclusive, or starts no later than the range ends and
/// is open.
pub(crate) async fn affected_keys(
    conn: &mut PgConnection,
    ranges: Vec<AffectedRange>,
) -> Result<HashSet<(String, Value)>> {
    let mut affected = HashSet::new();
    if ranges.is_empty() {
        return Ok(affected);
    }
    // One admission offers a range per input, most of them the same or
    // touching. Outputs are looked up once per distinct stretch of time.
    let mut asked: BTreeMap<(&str, &str), Vec<(OffsetDateTime, OffsetDateTime)>> = BTreeMap::new();
    for range in &ranges {
        asked
            .entry((range.derivation, range.output_type))
            .or_default()
            .push((range.start, range.end));
    }
    let (mut derivations, mut output_types, mut starts, mut ends) =
        (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    for (&(derivation, output_type), spans) in &mut asked {
        for (start, end) in joined(spans) {
            derivations.push(derivation.to_owned());
            output_types.push(output_type.to_owned());
            starts.push(start);
            ends.push(end);
        }
    }
    for row in sqlx::query_file!(
        "src/output_ranges/affected_keys.sql",
        &derivations,
        &output_types,
        &starts,
        &ends,
    )
    .fetch_all(&mut *conn)
    .await?
    {
        affected.insert((row.derivation, row.key));
    }
    // An open output is affected by any range that ends at or after its
    // start: the latest end asked decides for the key's earliest open output.
    for ((derivation, output_type), spans) in &asked {
        let Some(latest) = spans.iter().map(|(_, end)| *end).max() else {
            continue;
        };
        for key in sqlx::query_file_scalar!(
            "src/output_ranges/open_keys.sql",
            derivation,
            output_type,
            latest,
        )
        .fetch_all(&mut *conn)
        .await?
        {
            affected.insert(((*derivation).to_owned(), key));
        }
    }
    Ok(affected)
}

/// `spans` with those that overlap or touch joined: an output covers part of
/// the joined stretch exactly when it covers part of one of its members. A
/// span that ends before it starts is asked as it stands.
fn joined(
    spans: &mut Vec<(OffsetDateTime, OffsetDateTime)>,
) -> Vec<(OffsetDateTime, OffsetDateTime)> {
    spans.sort_unstable();
    spans.dedup();
    let mut out: Vec<(OffsetDateTime, OffsetDateTime)> = Vec::new();
    let mut last_forward: Option<usize> = None;
    for &(start, end) in spans.iter() {
        if end < start {
            out.push((start, end));
            continue;
        }
        match last_forward {
            Some(at) if start <= out[at].1 => out[at].1 = out[at].1.max(end),
            _ => {
                last_forward = Some(out.len());
                out.push((start, end));
            }
        }
    }
    out
}

#[cfg(test)]
mod tests;
