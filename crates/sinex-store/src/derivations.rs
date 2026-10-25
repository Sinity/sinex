//! Reads for the derivation driver: the live events a key reads, the keys
//! waiting to be recomputed, and committed events in commit order. Writes go
//! through admission (`DerivedScope`), never from here, except the registry
//! rows written at daemon start and the comparisons a recompute command keeps.
use crate::Result;
use serde_json::Value;
use sinex_model::{
    Class, EquivalenceKey, Event, EventId, MaterialId, Origin, ReadingOrder, RecordLocation,
    RecordState, ReplaceRule, TimeQuality, Timestamp, TsOrig, event_types,
};
use sqlx::PgConnection;
use std::collections::HashSet;
use std::sync::{Mutex, OnceLock};
use time::OffsetDateTime;
use uuid::Uuid;

/// A committed event and its place in commit order.
#[derive(Clone, Debug)]
pub struct Committed {
    pub event: Event,
    pub commit_seq: i64,
}

/// Payload filter: field equals a value; a `values` list means "one of".
#[derive(Clone, Debug, PartialEq)]
pub enum Match {
    Equals(&'static str, Value),
    OneOf(&'static str, Vec<String>),
}

/// The nearest events to a window edge.
#[derive(Clone, Debug, PartialEq)]
pub struct Nearest {
    pub event_types: Vec<String>,
    pub matching: Vec<Match>,
    /// One answer set per value of this payload field seen in the window.
    pub per: Option<&'static str>,
    pub count: i64,
}

/// Live events of some types in a `ts_orig` window, plus nearest neighbours.
#[derive(Clone, Debug, PartialEq)]
pub struct Window {
    pub event_types: Vec<String>,
    pub start: OffsetDateTime,
    pub end: OffsetDateTime,
    pub matching: Vec<Match>,
    pub before: Vec<Nearest>,
    pub after: Vec<Nearest>,
}

/// A dirty key: recompute it.
#[derive(Clone, Debug, PartialEq)]
pub struct DirtyKey {
    pub derivation: String,
    pub key: Value,
    pub last_dirty_seq: i64,
}

/// The type name as a `&'static str`: the registered one when known,
/// otherwise interned once (type names are a small, fixed set per build plus
/// whatever outside producers register).
pub fn static_type(name: &str) -> &'static str {
    if let Some(k) = event_types::find(name) {
        return k.event_type;
    }
    static NAMES: OnceLock<Mutex<HashSet<&'static str>>> = OnceLock::new();
    let mut names = NAMES
        .get_or_init(Default::default)
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(n) = names.get(name) {
        return n;
    }
    let leaked: &'static str = Box::leak(name.to_owned().into_boxed_str());
    names.insert(leaked);
    leaked
}

fn text_enum<T: serde::de::DeserializeOwned>(value: &str, column: &str) -> Result<T> {
    serde_json::from_value(Value::String(value.to_owned()))
        .map_err(|_| crate::Error::Invalid(format!("unknown {column} {value:?}")))
}

#[allow(clippy::too_many_arguments)]
fn event_from_row(
    id: Uuid,
    event_type: &str,
    source: String,
    material_id: Option<Uuid>,
    anchor: Option<i64>,
    ordinal: i16,
    parents: Option<Vec<Uuid>>,
    ts_orig: Option<OffsetDateTime>,
    ts_quality: &str,
    payload: Value,
    equivalence_key: Option<String>,
    reading_order: i64,
    record_hash: Vec<u8>,
    record_state: &str,
    class: &str,
    support: Option<Value>,
) -> Result<Event> {
    let event_type = static_type(event_type);
    let origin = match (material_id, anchor, parents) {
        (Some(material), Some(anchor), _) => Origin::Record {
            location: RecordLocation {
                material: MaterialId(material),
                anchor: u64::try_from(anchor)
                    .map_err(|_| crate::Error::Invalid("negative anchor".into()))?,
            },
            ordinal: u16::try_from(ordinal)
                .map_err(|_| crate::Error::Invalid("negative ordinal".into()))?,
        },
        (_, _, Some(parents)) => Origin::Derived {
            parents: parents.into_iter().map(EventId).collect(),
        },
        _ => return Err(crate::Error::Invalid(format!("event {id} has no origin"))),
    };
    let quality: TimeQuality = text_enum(ts_quality, "time quality")?;
    Ok(Event {
        id: EventId(id),
        event_type,
        source,
        origin,
        ts_orig: TsOrig::maybe(ts_orig.map(Timestamp), quality),
        payload,
        equivalence_key: equivalence_key.map(EquivalenceKey),
        replace: event_types::find(event_type).map_or(ReplaceRule::SuppressRepeat, |k| k.replace),
        reading_order: ReadingOrder(reading_order),
        record_hash: record_hash
            .try_into()
            .map_err(|_| crate::Error::Invalid("record hash is not 32 bytes".into()))?,
        record_state: text_enum::<RecordState>(record_state, "record state")?,
        class: text_enum::<Class>(class, "class")?,
        support,
        cites: Vec::new(),
    })
}

macro_rules! committed {
    ($r:expr) => {
        Committed {
            commit_seq: $r.commit_seq,
            event: event_from_row(
                $r.id,
                &$r.event_type,
                $r.source,
                $r.material_id,
                $r.anchor,
                $r.ordinal,
                $r.parents,
                $r.ts_orig,
                &$r.ts_quality,
                $r.payload,
                $r.equivalence_key,
                $r.reading_order,
                $r.record_hash,
                &$r.record_state,
                &$r.class,
                $r.support,
            )?,
        }
    };
}

/// The scalar equalities as one JSON object for `@>`, and the one-of filter.
fn split(matching: &[Match]) -> (Value, Option<String>, Vec<String>) {
    let mut object = serde_json::Map::new();
    let mut one_of: (Option<String>, Vec<String>) = (None, Vec::new());
    for m in matching {
        match m {
            Match::Equals(f, v) => {
                object.insert((*f).to_owned(), v.clone());
            }
            Match::OneOf(f, values) => one_of = (Some((*f).to_owned()), values.clone()),
        }
    }
    (Value::Object(object), one_of.0, one_of.1)
}

/// Live events in the window, sorted by `ts_orig` then commit order, with the
/// neighbours appended (also sorted, without repeats).
pub async fn read_window(conn: &mut PgConnection, w: &Window) -> Result<Vec<Committed>> {
    let (contains, one_of_field, one_of) = split(&w.matching);
    let rows = sqlx::query!(
        r#"SELECT e.id, e.event_type, e.source, e.material_id, e.anchor, e.ordinal, e.parents,
             e.ts_orig, e.ts_quality, e.payload, e.equivalence_key, e.reading_order, e.record_hash,
             e.record_state, e.class, e.support, e.commit_seq
           FROM core.events e
           WHERE e.event_type = ANY($1) AND e.ts_orig >= $2 AND e.ts_orig < $3
             AND e.payload @> $4
             AND ($5::text IS NULL OR e.payload->>$5 = ANY($6))
             AND NOT EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id = e.id)
           ORDER BY e.ts_orig, e.commit_seq"#,
        &w.event_types,
        w.start,
        w.end,
        contains,
        one_of_field,
        &one_of,
    )
    .fetch_all(&mut *conn)
    .await?;
    let mut out = Vec::with_capacity(rows.len());
    for r in rows {
        out.push(committed!(r));
    }
    let mut seen: HashSet<EventId> = out.iter().map(|c| c.event.id).collect();
    let mut extra = Vec::new();
    for (n, after) in w
        .before
        .iter()
        .map(|n| (n, false))
        .chain(w.after.iter().map(|n| (n, true)))
    {
        for c in nearest(conn, w, n, after, &out).await? {
            if seen.insert(c.event.id) {
                extra.push(c);
            }
        }
    }
    out.extend(extra);
    out.sort_by_key(|c| (c.event.ts_orig.at, c.commit_seq));
    Ok(out)
}

/// [`read_window`] for many windows, in their order. Windows with the same
/// types and filter and no neighbours, whose ranges touch or lie within one
/// window length of each other, are read with one query over their combined
/// range and split by time: one round trip for a run of adjacent keys
/// (hours, days) instead of one per key. Each slice is exactly what its own
/// query returns.
pub async fn read_windows(
    conn: &mut PgConnection,
    windows: &[Window],
) -> Result<Vec<Vec<Committed>>> {
    let mut out: Vec<Option<Vec<Committed>>> = vec![None; windows.len()];
    let mut plain: Vec<usize> = Vec::new();
    for (i, w) in windows.iter().enumerate() {
        if w.before.is_empty() && w.after.is_empty() {
            plain.push(i);
        } else {
            out[i] = Some(read_window(conn, w).await?);
        }
    }
    // Group by filter, then chain by start.
    plain.sort_by(|&a, &b| {
        let (wa, wb) = (&windows[a], &windows[b]);
        (format!("{:?}{:?}", wa.event_types, wa.matching), wa.start)
            .cmp(&(format!("{:?}{:?}", wb.event_types, wb.matching), wb.start))
    });
    let mut at = 0;
    while at < plain.len() {
        let first = &windows[plain[at]];
        let (mut end, mut len) = (first.end, first.end - first.start);
        let mut upto = at + 1;
        while upto < plain.len() {
            let w = &windows[plain[upto]];
            let same = w.event_types == first.event_types && w.matching == first.matching;
            if !same || w.start > end + len {
                break;
            }
            end = end.max(w.end);
            len = len.max(w.end - w.start);
            upto += 1;
        }
        let union = Window {
            start: first.start,
            end,
            ..first.clone()
        };
        let rows = read_window(conn, &union).await?;
        for &i in &plain[at..upto] {
            let w = &windows[i];
            let from = rows.partition_point(|c| c.event.ts_orig.at.is_some_and(|t| t.0 < w.start));
            let to = rows.partition_point(|c| c.event.ts_orig.at.is_some_and(|t| t.0 < w.end));
            out[i] = Some(rows[from..to].to_vec());
        }
        at = upto;
    }
    Ok(out.into_iter().map(Option::unwrap_or_default).collect())
}

async fn nearest(
    conn: &mut PgConnection,
    w: &Window,
    n: &Nearest,
    after: bool,
    window_events: &[Committed],
) -> Result<Vec<Committed>> {
    let (contains, one_of_field, one_of) = split(&n.matching);
    let edge = if after { w.end } else { w.start };
    let mut out = Vec::new();
    match n.per {
        None => {
            let rows = sqlx::query!(
                r#"SELECT e.id, e.event_type, e.source, e.material_id, e.anchor, e.ordinal, e.parents,
                     e.ts_orig, e.ts_quality, e.payload, e.equivalence_key, e.reading_order, e.record_hash,
                     e.record_state, e.class, e.support, e.commit_seq
                   FROM core.events e
                   WHERE e.event_type = ANY($1)
                     AND (($2 AND e.ts_orig >= $3) OR (NOT $2 AND e.ts_orig < $3))
                     AND e.payload @> $4
                     AND ($5::text IS NULL OR e.payload->>$5 = ANY($6))
                     AND NOT EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id = e.id)
                   ORDER BY CASE WHEN $2 THEN e.ts_orig END ASC, CASE WHEN NOT $2 THEN e.ts_orig END DESC
                   LIMIT $7"#,
                &n.event_types,
                after,
                edge,
                contains,
                one_of_field,
                &one_of,
                n.count,
            )
            .fetch_all(&mut *conn)
            .await?;
            for r in rows {
                out.push(committed!(r));
            }
        }
        Some(per) => {
            let mut values: Vec<String> = window_events
                .iter()
                .filter(|c| n.event_types.iter().any(|t| t == c.event.event_type))
                .filter_map(|c| match c.event.payload.get(per)? {
                    Value::String(s) => Some(s.clone()),
                    Value::Null => None,
                    other => Some(other.to_string()),
                })
                .collect();
            values.sort_unstable();
            values.dedup();
            if values.is_empty() {
                return Ok(out);
            }
            let rows = sqlx::query!(
                r#"SELECT x.id AS "id!", x.event_type AS "event_type!", x.source AS "source!",
                     x.material_id, x.anchor, x.ordinal AS "ordinal!", x.parents, x.ts_orig,
                     x.ts_quality AS "ts_quality!", x.payload AS "payload!", x.equivalence_key,
                     x.reading_order AS "reading_order!", x.record_hash AS "record_hash!",
                     x.record_state AS "record_state!", x.class AS "class!", x.support,
                     x.commit_seq AS "commit_seq!"
                   FROM unnest($7::text[]) AS v(val) CROSS JOIN LATERAL (
                     SELECT e.* FROM core.events e
                     WHERE e.event_type = ANY($1)
                       AND (($2 AND e.ts_orig >= $3) OR (NOT $2 AND e.ts_orig < $3))
                       AND e.payload->>$8 = v.val
                       AND e.payload @> $4
                       AND ($5::text IS NULL OR e.payload->>$5 = ANY($6))
                       AND NOT EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id = e.id)
                     ORDER BY CASE WHEN $2 THEN e.ts_orig END ASC, CASE WHEN NOT $2 THEN e.ts_orig END DESC
                     LIMIT $9) x"#,
                &n.event_types,
                after,
                edge,
                contains,
                one_of_field,
                &one_of,
                &values,
                per,
                n.count,
            )
            .fetch_all(&mut *conn)
            .await?;
            for r in rows {
                out.push(committed!(r));
            }
        }
    }
    Ok(out)
}

/// Live committed events of `event_types` after `after_seq`, in commit order.
pub async fn after_seq(
    conn: &mut PgConnection,
    event_types: &[String],
    after_seq: i64,
    limit: i64,
) -> Result<Vec<Committed>> {
    let rows = sqlx::query!(
        r#"SELECT e.id, e.event_type, e.source, e.material_id, e.anchor, e.ordinal, e.parents,
             e.ts_orig, e.ts_quality, e.payload, e.equivalence_key, e.reading_order, e.record_hash,
             e.record_state, e.class, e.support, e.commit_seq
           FROM core.commit_log l JOIN core.events e ON e.id = l.event_id
           WHERE l.commit_seq > $2 AND l.event_type = ANY($1)
             AND NOT EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id = e.id)
           ORDER BY l.commit_seq LIMIT $3"#,
        event_types,
        after_seq,
        limit,
    )
    .fetch_all(&mut *conn)
    .await?;
    let mut out = Vec::with_capacity(rows.len());
    for r in rows {
        out.push(committed!(r));
    }
    Ok(out)
}

/// The live readings at these record locations, of any event type and output
/// number.
pub async fn live_at(conn: &mut PgConnection, locations: &[RecordLocation]) -> Result<Vec<Event>> {
    let materials: Vec<Uuid> = locations.iter().map(|l| l.material.0).collect();
    let anchors: Vec<i64> = locations
        .iter()
        .map(|l| i64::try_from(l.anchor).unwrap_or(i64::MAX))
        .collect();
    let rows = sqlx::query!(
        r#"SELECT e.id, e.event_type, e.source, e.material_id, e.anchor, e.ordinal, e.parents,
             e.ts_orig, e.ts_quality, e.payload, e.equivalence_key, e.reading_order, e.record_hash,
             e.record_state, e.class, e.support, e.commit_seq
           FROM unnest($1::uuid[], $2::bigint[]) AS l(material_id, anchor)
           JOIN core.events e ON e.material_id = l.material_id AND e.anchor = l.anchor
           WHERE EXISTS (SELECT 1 FROM core.live_locations c WHERE c.event_id = e.id)
              OR EXISTS (SELECT 1 FROM core.live_equivalence q WHERE q.event_id = e.id)
           ORDER BY e.commit_seq"#,
        &materials,
        &anchors,
    )
    .fetch_all(&mut *conn)
    .await?;
    let mut out = Vec::with_capacity(rows.len());
    for r in rows {
        out.push(committed!(r).event);
    }
    Ok(out)
}

/// The highest commit sequence so far.
pub async fn head(conn: &mut PgConnection) -> Result<i64> {
    Ok(
        sqlx::query_scalar!("SELECT value FROM core.counters WHERE name = 'commit_seq'")
            .fetch_one(&mut *conn)
            .await?,
    )
}

/// Dirty keys of `derivations`, oldest dirt first.
pub async fn dirty(
    conn: &mut PgConnection,
    derivations: &[String],
    limit: i64,
) -> Result<Vec<DirtyKey>> {
    let rows = sqlx::query!(
        r#"SELECT derivation, key, last_dirty_seq FROM derivation.dirty
           WHERE derivation = ANY($1) ORDER BY first_dirty_seq, derivation LIMIT $2"#,
        derivations,
        limit,
    )
    .fetch_all(&mut *conn)
    .await?;
    Ok(rows
        .into_iter()
        .map(|r| DirtyKey {
            derivation: r.derivation,
            key: r.key,
            last_dirty_seq: r.last_dirty_seq,
        })
        .collect())
}

/// Keys of `derivation` waiting on evidence outside their window, with when
/// each started waiting.
pub async fn waiting(conn: &mut PgConnection, derivation: &str) -> Result<Vec<(Value, Timestamp)>> {
    Ok(sqlx::query!(
        "SELECT key, since FROM derivation.waiting WHERE derivation = $1 ORDER BY since",
        derivation
    )
    .fetch_all(&mut *conn)
    .await?
    .into_iter()
    .map(|r| (r.key, Timestamp(r.since)))
    .collect())
}

/// The live outputs a key last produced.
pub async fn outputs(conn: &mut PgConnection, derivation: &str, key: &Value) -> Result<Vec<Event>> {
    let rows = sqlx::query!(
        r#"SELECT e.id, e.event_type, e.source, e.material_id, e.anchor, e.ordinal, e.parents,
             e.ts_orig, e.ts_quality, e.payload, e.equivalence_key, e.reading_order, e.record_hash,
             e.record_state, e.class, e.support, e.commit_seq
           FROM derivation.outputs o JOIN core.events e ON e.id = o.event_id
           WHERE o.derivation = $1 AND o.key = $2
             AND NOT EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id = e.id)
           ORDER BY e.ts_orig, e.commit_seq"#,
        derivation,
        key,
    )
    .fetch_all(&mut *conn)
    .await?;
    let mut out = Vec::with_capacity(rows.len());
    for r in rows {
        out.push(committed!(r).event);
    }
    Ok(out)
}

/// Where a per-event derivation has read to.
pub async fn position(conn: &mut PgConnection, derivation: &str) -> Result<i64> {
    Ok(sqlx::query_scalar!(
        "SELECT commit_seq FROM derivation.positions WHERE derivation = $1",
        derivation
    )
    .fetch_optional(&mut *conn)
    .await?
    .unwrap_or(0))
}

/// Start a per-event derivation over from the first committed event (its
/// version changed). Written at daemon start, before the driver runs.
pub async fn reset_position(conn: &mut PgConnection, derivation: &str) -> Result<()> {
    sqlx::query!(
        "DELETE FROM derivation.positions WHERE derivation = $1",
        derivation
    )
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// A stored comparison: what a recompute of `derivation` at `version` would
/// change.
#[derive(Clone, Debug, PartialEq)]
pub struct StoredComparison {
    pub id: i64,
    pub derivation: String,
    pub version: i32,
    pub range: Option<(Option<Timestamp>, Option<Timestamp>)>,
    pub made_at: Timestamp,
    pub keys: i64,
    pub unchanged: i64,
    pub added: i64,
    pub changed: i64,
    pub removed: i64,
    pub examples: Value,
}

/// Keep a comparison. Comparisons are operations records, not events; they are
/// written by the command that made them.
pub async fn store_comparison(conn: &mut PgConnection, c: &StoredComparison) -> Result<i64> {
    let (from, until) = c.range.unwrap_or((None, None));
    Ok(sqlx::query_scalar!(
        r#"INSERT INTO derivation.comparisons
             (derivation, version, range_from, range_until, keys, unchanged, added, changed, removed, examples)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) RETURNING id"#,
        c.derivation,
        c.version,
        from.map(|t| t.0),
        until.map(|t| t.0),
        c.keys,
        c.unchanged,
        c.added,
        c.changed,
        c.removed,
        c.examples,
    )
    .fetch_one(&mut *conn)
    .await?)
}

/// Stored comparisons, newest first, optionally of one derivation.
pub async fn comparisons(
    conn: &mut PgConnection,
    derivation: Option<&str>,
    limit: i64,
) -> Result<Vec<StoredComparison>> {
    let rows = sqlx::query!(
        r#"SELECT id, derivation, version, range_from, range_until, made_at, keys, unchanged,
             added, changed, removed, examples
           FROM derivation.comparisons
           WHERE $1::text IS NULL OR derivation = $1
           ORDER BY id DESC LIMIT $2"#,
        derivation,
        limit,
    )
    .fetch_all(&mut *conn)
    .await?;
    Ok(rows
        .into_iter()
        .map(|r| StoredComparison {
            id: r.id,
            derivation: r.derivation,
            version: r.version,
            range: (r.range_from.is_some() || r.range_until.is_some())
                .then(|| (r.range_from.map(Timestamp), r.range_until.map(Timestamp))),
            made_at: Timestamp(r.made_at),
            keys: r.keys,
            unchanged: r.unchanged,
            added: r.added,
            changed: r.changed,
            removed: r.removed,
            examples: r.examples,
        })
        .collect())
}

/// A derivation as the registry records it.
#[derive(Clone, Debug, PartialEq)]
pub struct Registered {
    pub name: String,
    /// `per_event` or `keyed`.
    pub kind: &'static str,
    pub version: i32,
    pub inputs: Vec<String>,
    pub outputs: Vec<String>,
}

/// Write the registry rows from the Rust catalog at daemon start. Returns the
/// derivations whose version differs from the stored one (including new
/// ones), whose outputs must be recomputed.
pub async fn register(conn: &mut PgConnection, catalog: &[Registered]) -> Result<Vec<String>> {
    let mut changed = Vec::new();
    for d in catalog {
        let previous = sqlx::query_scalar!(
            "SELECT version FROM registry.derivations WHERE name = $1",
            d.name
        )
        .fetch_optional(&mut *conn)
        .await?;
        if previous != Some(d.version) {
            changed.push(d.name.clone());
        }
        sqlx::query!(
            r#"INSERT INTO registry.derivations (name, kind, version, inputs, outputs) VALUES ($1, $2, $3, $4, $5)
               ON CONFLICT (name) DO UPDATE SET kind = EXCLUDED.kind, version = EXCLUDED.version,
                 inputs = EXCLUDED.inputs, outputs = EXCLUDED.outputs"#,
            d.name,
            d.kind,
            d.version,
            &d.inputs,
            &d.outputs,
        )
        .execute(&mut *conn)
        .await?;
    }
    Ok(changed)
}
