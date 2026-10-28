//! Reads for the derivation runner: the live events a key reads, the keys
//! waiting to be recomputed, and committed events in commit order. Writes go
//! through admission (`DerivedScope`), never from here, except the registry
//! rows written at daemon start and the comparisons a recompute command keeps.
use crate::Result;
use serde_json::Value;
use sinex_model::{
    Class, EquivalenceKey, Event, EventId, MaterialId, Origin, ReadingOrder, RecordLocation,
    RecordState, ReplaceRule, TimeQuality, Timestamp, TsOrig,
};
use sqlx::PgConnection;
use std::collections::HashSet;
use time::OffsetDateTime;
use uuid::Uuid;

/// A committed event and its place in commit order.
#[derive(Clone, Debug)]
pub struct Committed {
    pub event: Event,
    pub commit_seq: i64,
}

/// Events of one type whose payload contains `contains`, a JSON object (`{}`
/// for every event of the type). An event is read when any kind admits it.
#[derive(Clone, Debug, PartialEq)]
pub struct Kind {
    pub event_type: String,
    pub contains: Value,
}

/// The parallel arrays the queries unnest: each kind's type and containment.
fn kind_arrays(kinds: &[Kind]) -> (Vec<String>, Vec<Value>) {
    kinds
        .iter()
        .map(|k| (k.event_type.clone(), k.contains.clone()))
        .unzip()
}

/// The nearest events to a window edge.
#[derive(Clone, Debug, PartialEq)]
pub struct Nearest {
    pub kinds: Vec<Kind>,
    /// One answer set per value of this payload field seen in the window.
    pub per: Option<&'static str>,
    pub count: i64,
}

/// Live events of some kinds in a `ts_orig` window, plus nearest neighbours.
#[derive(Clone, Debug, PartialEq)]
pub struct Window {
    pub range: Option<InputRange>,
    pub unknown_times: bool,
    pub kinds: Vec<Kind>,
    /// No start reads all earlier known times.
    pub start: Option<OffsetDateTime>,
    /// No end reads all later known times.
    pub end: Option<OffsetDateTime>,
    pub before: Vec<Nearest>,
    pub intervals: Vec<(&'static str, sinex_model::clock::IntervalEnd)>,
    pub after: Vec<Nearest>,
}

/// Time bounds resolved from live records within the reader's snapshot.
#[derive(Clone, Debug, PartialEq)]
pub enum InputRange {
    Connected {
        kinds: Vec<Kind>,
        at: OffsetDateTime,
        end_field: &'static str,
        gap_ns: i128,
    },
    Extent {
        kinds: Vec<Kind>,
    },
    UntilNext {
        start: OffsetDateTime,
        kinds: Vec<Kind>,
        field: &'static str,
        now: OffsetDateTime,
    },
}

/// A dirty key: recompute it.
#[derive(Clone, Debug, PartialEq)]
pub struct DirtyKey {
    pub derivation: String,
    pub key: Value,
    pub last_dirty_seq: i64,
}

fn text_enum<T: serde::de::DeserializeOwned>(value: &str, column: &str) -> Result<T> {
    serde_json::from_value(Value::String(value.to_owned()))
        .map_err(|_| crate::Error::Invalid(format!("unknown {column} {value:?}")))
}

#[allow(clippy::too_many_arguments)]
fn event_from_row(
    id: Uuid,
    event_type: &str,
    kind_version: i64,
    validation: &str,
    replace_rule: Option<&str>,
    source: String,
    material_id: Option<Uuid>,
    anchor: Option<i64>,
    output_index: i16,
    parents: Option<Vec<Uuid>>,
    ts_orig: Option<OffsetDateTime>,
    ts_quality: &str,
    payload: Value,
    equivalence_key: Option<String>,
    reading_order: i64,
    record_hash: Vec<u8>,
    record_state: &str,
    class: &str,
) -> Result<Event> {
    let origin = match (material_id, anchor, parents) {
        (Some(material), Some(anchor), _) => Origin::Record {
            location: RecordLocation {
                material: MaterialId(material),
                anchor: u64::try_from(anchor)
                    .map_err(|_| crate::Error::Invalid("negative anchor".into()))?,
            },
            output_index: u16::try_from(output_index)
                .map_err(|_| crate::Error::Invalid("negative output_index".into()))?,
        },
        (_, _, Some(parents)) => Origin::Derived {
            parents: parents.into_iter().map(EventId).collect(),
        },
        _ => return Err(crate::Error::Invalid(format!("event {id} has no origin"))),
    };
    let quality: TimeQuality = text_enum(ts_quality, "time quality")?;
    Ok(Event {
        id: EventId(id),
        event_type: event_type.into(),
        kind_version: u64::try_from(kind_version)
            .map_err(|_| crate::Error::Invalid("invalid kind version".into()))?,
        validation: text_enum(validation, "validation")?,
        source,
        origin,
        ts_orig: TsOrig::maybe(ts_orig.map(Timestamp), quality),
        payload,
        equivalence_key: equivalence_key.map(EquivalenceKey),
        replace: replace_rule
            .map(|r| text_enum::<ReplaceRule>(r, "replacement rule"))
            .transpose()?,
        reading_order: ReadingOrder(reading_order),
        record_hash: record_hash
            .try_into()
            .map_err(|_| crate::Error::Invalid("record hash is not 32 bytes".into()))?,
        record_state: text_enum::<RecordState>(record_state, "record state")?,
        class: text_enum::<Class>(class, "class")?,
        owned_blobs: Vec::new(),
    })
}

macro_rules! committed {
    ($r:expr) => {
        Committed {
            commit_seq: $r.commit_seq,
            event: event_from_row(
                $r.id,
                &$r.event_type,
                $r.kind_version,
                &$r.validation,
                $r.replace_rule.as_deref(),
                $r.source,
                $r.material_id,
                $r.anchor,
                $r.output_index,
                $r.parents,
                $r.ts_orig,
                &$r.ts_quality,
                $r.payload,
                $r.equivalence_key,
                $r.reading_order,
                $r.record_hash,
                &$r.record_state,
                &$r.class,
            )?,
        }
    };
}

async fn resolve_range(conn: &mut PgConnection, w: &Window) -> Result<Option<Window>> {
    let Some(range) = &w.range else {
        return Ok(Some(w.clone()));
    };
    let mut resolved = w.clone();
    resolved.range = None;
    match range {
        InputRange::Connected {
            kinds,
            at,
            end_field,
            gap_ns,
        } => {
            let (types, contains) = kind_arrays(kinds);
            let gap = gap_ns.to_string();
            let bounds = sqlx::query!(
                r#"WITH RECURSIVE chain(id, start, finish) AS (
                     SELECT e.id, e.ts_orig, coalesce((e.payload->>$3)::timestamptz, e.ts_orig)
                     FROM core.events e WHERE e.ts_orig=$4 AND e.event_type=ANY($1)
                       AND EXISTS (SELECT 1 FROM unnest($1::text[], $2::jsonb[]) AS k(event_type, contains)
                                   WHERE k.event_type=e.event_type AND e.payload @> k.contains)
                       AND NOT EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id=e.id)
                     UNION
                     SELECT n.id, n.ts_orig, coalesce((n.payload->>$3)::timestamptz, n.ts_orig)
                     FROM chain c CROSS JOIN LATERAL (
                       (SELECT e.id, e.ts_orig, e.payload FROM core.events e
                        WHERE e.event_type=ANY($1) AND e.ts_orig<c.start
                          AND EXISTS (SELECT 1 FROM unnest($1::text[], $2::jsonb[]) AS k(event_type, contains)
                                      WHERE k.event_type=e.event_type AND e.payload @> k.contains)
                          AND NOT EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id=e.id)
                        ORDER BY e.ts_orig DESC, e.id LIMIT 1)
                       UNION ALL
                       (SELECT e.id, e.ts_orig, e.payload FROM core.events e
                        WHERE e.event_type=ANY($1) AND e.ts_orig>c.start
                          AND EXISTS (SELECT 1 FROM unnest($1::text[], $2::jsonb[]) AS k(event_type, contains)
                                      WHERE k.event_type=e.event_type AND e.payload @> k.contains)
                          AND NOT EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id=e.id)
                        ORDER BY e.ts_orig, e.id LIMIT 1)
                     ) n WHERE extract(epoch FROM n.ts_orig-c.finish) * 1000000000 < $5::text::numeric
                           AND extract(epoch FROM c.start-coalesce((n.payload->>$3)::timestamptz, n.ts_orig)) * 1000000000 < $5::text::numeric)
                   SELECT min(start) AS start, max(finish) AS finish FROM chain"#,
                &types, &contains, *end_field, at, gap,
            ).fetch_one(&mut *conn).await?;
            let (Some(start), Some(end)) = (bounds.start, bounds.finish) else {
                return Ok(None);
            };
            resolved.start = Some(start);
            resolved.end = Timestamp(end)
                .unix_ns()
                .checked_add(*gap_ns)
                .and_then(|ns| Timestamp::from_unix_ns(ns).ok())
                .map(|at| at.0);
        }
        InputRange::Extent { kinds } => {
            let (types, contains) = kind_arrays(kinds);
            let bounds = sqlx::query!(
                r#"SELECT min(e.ts_orig) AS start, max(e.ts_orig) AS finish
                   FROM core.events e WHERE e.event_type = ANY($1) AND e.ts_orig IS NOT NULL
                     AND EXISTS (SELECT 1 FROM unnest($1::text[], $2::jsonb[]) AS k(event_type, contains)
                                 WHERE k.event_type = e.event_type AND e.payload @> k.contains)
                     AND NOT EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id = e.id)"#,
                &types, &contains,
            ).fetch_one(&mut *conn).await?;
            let (Some(start), Some(end)) = (bounds.start, bounds.finish) else {
                return Ok(None);
            };
            resolved.start = Some(start);
            resolved.end = Timestamp::from_unix_ns(Timestamp(end).unix_ns() + 1_000)
                .ok()
                .map(|at| at.0);
        }
        InputRange::UntilNext {
            start,
            kinds,
            field,
            now,
        } => {
            let (types, contains) = kind_arrays(kinds);
            let end = sqlx::query_scalar!(
                r#"SELECT min((e.payload->>$3)::timestamptz) AS finish FROM core.events e
                   WHERE e.event_type = ANY($1) AND (e.payload->>$3)::timestamptz > $4
                     AND EXISTS (SELECT 1 FROM unnest($1::text[], $2::jsonb[]) AS k(event_type, contains)
                                 WHERE k.event_type = e.event_type AND e.payload @> k.contains)
                     AND NOT EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id = e.id)"#,
                &types, &contains, *field, start,
            ).fetch_one(&mut *conn).await?;
            resolved.start = Some(*start);
            resolved.end = Some(end.unwrap_or(*now).min(*now));
        }
    }
    Ok(Some(resolved))
}

/// Live events in the window, sorted by `ts_orig` then commit order, with the
/// neighbours appended (also sorted, without repeats).
pub async fn read_window(conn: &mut PgConnection, w: &Window) -> Result<Vec<Committed>> {
    let Some(resolved) = resolve_range(conn, w).await? else {
        return Ok(Vec::new());
    };
    let w = &resolved;
    let (types, contains) = kind_arrays(&w.kinds);
    let mut interval_types = Vec::new();
    let mut end_fields = Vec::new();
    let mut durations = Vec::new();
    let mut open_ends = Vec::new();
    for &(kind, end) in &w.intervals {
        interval_types.push(kind.to_owned());
        let (field, duration, open) = match end {
            sinex_model::clock::IntervalEnd::At(field) => (field, false, true),
            sinex_model::clock::IntervalEnd::EndOrInstant(field) => (field, false, false),
            sinex_model::clock::IntervalEnd::AfterNs(field) => (field, true, false),
        };
        open_ends.push(open);
        end_fields.push(field.to_owned());
        durations.push(duration);
    }
    let rows = sqlx::query!(
        r#"SELECT e.id, e.event_type, e.source, e.material_id, e.anchor, e.output_index, e.parents,
             e.ts_orig, e.ts_quality, e.payload, e.equivalence_key, e.reading_order, e.record_hash,
             e.record_state, e.class, e.commit_seq, e.kind_version, e.validation,
             (SELECT replace_rule FROM registry.event_types k WHERE k.name=e.event_type AND k.version=e.kind_version) AS replace_rule
           FROM core.events e
           WHERE e.event_type = ANY($1) AND (e.ts_orig IS NOT NULL OR $8)
             AND ($3::timestamptz IS NULL OR e.ts_orig < $3)
             AND ($2::timestamptz IS NULL OR e.ts_orig >= $2 OR EXISTS (
               SELECT 1 FROM unnest($5::text[], $6::text[], $7::bool[], $9::bool[]) AS i(event_type, field, duration, open)
               WHERE i.event_type = e.event_type
                 AND CASE WHEN i.duration THEN e.ts_orig + interval '1 microsecond' *
                       ceil(greatest(coalesce((e.payload->>i.field)::numeric, 0), 0) / 1000)::double precision
                     ELSE coalesce((e.payload->>i.field)::timestamptz, CASE WHEN i.open THEN coalesce($3, 'infinity'::timestamptz) ELSE e.ts_orig END) END >= $2))
             AND EXISTS (SELECT 1 FROM unnest($1::text[], $4::jsonb[]) AS k(event_type, contains)
                         WHERE k.event_type = e.event_type AND e.payload @> k.contains)
             AND NOT EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id = e.id)
           ORDER BY e.ts_orig, e.commit_seq"#,
        &types,
        w.start,
        w.end,
        &contains,
        &interval_types,
        &end_fields,
        &durations,
        w.unknown_times,
        &open_ends,
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

/// One window page: live events of a window without neighbours after
/// `after` in (`ts_orig`, commit order), at most `limit`, ending early once
/// their stored payloads pass `bytes` (the first is always read). Also how
/// many matched before the size bound.
async fn window_page(
    conn: &mut PgConnection,
    w: &Window,
    after: Option<(OffsetDateTime, i64)>,
    limit: i64,
    bytes: i64,
) -> Result<(Vec<Committed>, i64)> {
    let (types, contains) = kind_arrays(&w.kinds);
    let (after_ts, after_seq) = after.unzip();
    let rows = sqlx::query!(
        r#"SELECT e.id, e.event_type, e.source, e.material_id, e.anchor, e.output_index, e.parents,
             e.ts_orig, e.ts_quality, e.payload, e.equivalence_key, e.reading_order, e.record_hash,
             e.record_state, e.class, e.commit_seq, e.kind_version, e.validation,
             (SELECT replace_rule FROM registry.event_types k WHERE k.name=e.event_type AND k.version=e.kind_version) AS replace_rule, page.matched AS "matched!"
           FROM core.events e JOIN (
             SELECT p.id, p.matched FROM (
               SELECT q.id, count(*) OVER () AS matched,
                      (sum(q.size) OVER (ORDER BY q.ts_orig, q.commit_seq) - q.size)::bigint AS before
               FROM (
                 SELECT s.id, s.ts_orig, s.commit_seq, pg_column_size(s.payload)::bigint AS size
                 FROM core.events s
                 WHERE s.event_type = ANY($1) AND s.ts_orig IS NOT NULL
                   AND ($2::timestamptz IS NULL OR s.ts_orig >= $2)
                   AND ($3::timestamptz IS NULL OR s.ts_orig < $3)
                   AND EXISTS (SELECT 1 FROM unnest($1::text[], $4::jsonb[]) AS k(event_type, contains)
                               WHERE k.event_type = s.event_type AND s.payload @> k.contains)
                   AND ($5::timestamptz IS NULL OR (s.ts_orig, s.commit_seq) > ($5, $6::bigint))
                   AND NOT EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id = s.id)
                 ORDER BY s.ts_orig, s.commit_seq
                 LIMIT $7
               ) q
             ) p WHERE p.before < $8::bigint
           ) page ON page.id = e.id
           ORDER BY e.ts_orig, e.commit_seq"#,
        &types,
        w.start,
        w.end,
        &contains,
        after_ts,
        after_seq,
        limit,
        bytes,
    )
    .fetch_all(&mut *conn)
    .await?;
    let matched = rows.first().map_or(0, |r| r.matched);
    let mut out = Vec::with_capacity(rows.len());
    for r in rows {
        out.push(committed!(r));
    }
    Ok((out, matched))
}

/// Windows read page by page, at most `page` rows and about `bytes` stored
/// payload bytes per query. Windows with the same
/// kinds and no neighbours, whose ranges touch or lie within one
/// window length of each other, share one paged query over their combined
/// range: one round trip per page for a run of adjacent keys (hours, days)
/// instead of one per key. Each
/// window is handed out as soon as the pages have passed its end, with exactly
/// what [`read_window`] returns for it, and its rows are then dropped, so a
/// reader holds about one page and the windows still open, never the whole
/// range. A window with neighbours is read alone.
pub struct Windows {
    windows: Vec<Window>,
    page: i64,
    bytes: i64,
    alone: Vec<usize>,
    values: std::collections::VecDeque<ValueGroup>,
    groups: std::collections::VecDeque<Group>,
}

/// Adjacent windows read through one paged query.
struct Group {
    union: Window,
    /// Windows not yet handed out, by start.
    open: Vec<usize>,
    /// Rows read that an open window may still need, in reading order.
    rows: std::collections::VecDeque<Committed>,
    after: Option<(OffsetDateTime, i64)>,
    finished: bool,
}

/// Windows with the same range and types, selecting string values of one
/// field, read all selected values in one paged query.
type ValueGroupKey = (Vec<String>, String, OffsetDateTime, OffsetDateTime);

struct ValueGroup {
    kinds: Vec<String>,
    field: String,
    start: OffsetDateTime,
    end: OffsetDateTime,
    open: std::collections::BTreeMap<String, Vec<usize>>,
    rows: std::collections::BTreeMap<String, Vec<Committed>>,
    after: Option<(String, OffsetDateTime, i64)>,
}

fn field_value(window: &Window) -> Option<(&str, &str)> {
    let first = window.kinds.first()?.contains.as_object()?;
    if first.len() != 1
        || window
            .kinds
            .iter()
            .any(|kind| kind.contains != window.kinds[0].contains)
    {
        return None;
    }
    let (field, value) = first.iter().next()?;
    Some((field, value.as_str()?))
}

impl ValueGroup {
    async fn next(
        &mut self,
        conn: &mut PgConnection,
        page: i64,
        bytes: i64,
    ) -> Result<Vec<(usize, Vec<Committed>)>> {
        let values: Vec<_> = self.open.keys().cloned().collect();
        let (after_value, after_ts, after_seq) = self
            .after
            .clone()
            .map(|(value, ts, seq)| (Some(value), Some(ts), Some(seq)))
            .unwrap_or_default();
        let rows = sqlx::query!(
            r#"SELECT e.id, e.event_type, e.source, e.material_id, e.anchor, e.output_index, e.parents,
                 e.ts_orig, e.ts_quality, e.payload, e.equivalence_key, e.reading_order, e.record_hash,
                 e.record_state, e.class, e.commit_seq, e.kind_version, e.validation,
                 (SELECT replace_rule FROM registry.event_types t WHERE t.name=e.event_type AND t.version=e.kind_version) AS replace_rule, page.value AS "value!", page.matched AS "matched!"
               FROM core.events e JOIN (
                 SELECT p.id, p.value, p.matched FROM (
                   SELECT q.id, q.value, count(*) OVER () AS matched,
                          (sum(q.size) OVER (ORDER BY q.value, q.ts_orig, q.commit_seq) - q.size)::bigint AS before
                   FROM (
                     SELECT s.id, v.value COLLATE "C" AS value, s.ts_orig, s.commit_seq, pg_column_size(s.payload)::bigint AS size
                     FROM unnest($1::text[]) v(value)
                     JOIN core.events s ON s.payload->>$2 = v.value AND jsonb_typeof(s.payload->$2) = 'string'
                     WHERE s.event_type = ANY($3) AND s.ts_orig >= $4 AND s.ts_orig < $5
                       AND ($6::text IS NULL OR (v.value COLLATE "C", s.ts_orig, s.commit_seq) > ($6 COLLATE "C", $7::timestamptz, $8::bigint))
                       AND NOT EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id = s.id)
                     ORDER BY v.value COLLATE "C", s.ts_orig, s.commit_seq
                     LIMIT $9
                   ) q
                 ) p WHERE p.before < $10::bigint
               ) page ON page.id = e.id
               ORDER BY page.value, e.ts_orig, e.commit_seq"#,
            &values, self.field, &self.kinds, self.start, self.end,
            after_value, after_ts, after_seq, page, bytes,
        ).fetch_all(&mut *conn).await?;
        let matched = rows.first().map_or(0, |row| row.matched);
        let finished = matched < page && rows.len() as i64 == matched;
        for row in rows {
            self.after = Some((
                row.value.clone(),
                row.ts_orig.expect("window time"),
                row.commit_seq,
            ));
            self.rows
                .entry(row.value.clone())
                .or_default()
                .push(committed!(row));
        }
        let complete: Vec<_> = self
            .open
            .keys()
            .filter(|value| {
                finished
                    || self
                        .after
                        .as_ref()
                        .is_some_and(|(last, _, _)| *value < last)
            })
            .cloned()
            .collect();
        let mut done = Vec::new();
        for value in complete {
            let rows = self.rows.remove(&value).unwrap_or_default();
            for index in self.open.remove(&value).expect("open value") {
                done.push((index, rows.clone()));
            }
        }
        Ok(done)
    }
}

impl Windows {
    pub fn new(windows: Vec<Window>, page: i64, bytes: i64) -> Self {
        let (alone, mut plain): (Vec<usize>, Vec<usize>) = (0..windows.len()).partition(|&i| {
            windows[i].unknown_times
                || windows[i].range.is_some()
                || !windows[i].before.is_empty()
                || !windows[i].after.is_empty()
                || !windows[i].intervals.is_empty()
        });
        let mut value_groups: std::collections::BTreeMap<ValueGroupKey, ValueGroup> =
            Default::default();
        plain.retain(|&index| {
            let window = &windows[index];
            let (Some(start), Some(end)) = (window.start, window.end) else {
                return true;
            };
            let Some((field, value)) = field_value(window) else {
                return true;
            };
            let mut kinds: Vec<_> = window
                .kinds
                .iter()
                .map(|kind| kind.event_type.clone())
                .collect();
            kinds.sort();
            kinds.dedup();
            let group = value_groups
                .entry((kinds.clone(), field.to_owned(), start, end))
                .or_insert_with(|| ValueGroup {
                    kinds,
                    field: field.to_owned(),
                    start,
                    end,
                    open: Default::default(),
                    rows: Default::default(),
                    after: None,
                });
            group.open.entry(value.to_owned()).or_default().push(index);
            false
        });
        // Group by kinds, then chain by start.
        plain.sort_by(|&a, &b| {
            let (wa, wb) = (&windows[a], &windows[b]);
            (format!("{:?}", wa.kinds), wa.start).cmp(&(format!("{:?}", wb.kinds), wb.start))
        });
        let mut groups = std::collections::VecDeque::new();
        let mut at = 0;
        while at < plain.len() {
            let first = &windows[plain[at]];
            let (Some(start), Some(mut end)) = (first.start, first.end) else {
                groups.push_back(Group {
                    union: first.clone(),
                    open: vec![plain[at]],
                    rows: std::collections::VecDeque::new(),
                    after: None,
                    finished: false,
                });
                at += 1;
                continue;
            };
            let mut len = end - start;
            let mut upto = at + 1;
            while upto < plain.len() {
                let w = &windows[plain[upto]];
                let (Some(next_start), Some(next_end)) = (w.start, w.end) else {
                    break;
                };
                let same = w.kinds == first.kinds;
                if !same || next_start > end + len {
                    break;
                }
                end = end.max(next_end);
                len = len.max(next_end - next_start);
                upto += 1;
            }
            groups.push_back(Group {
                union: Window {
                    start: first.start,
                    end: Some(end),
                    ..first.clone()
                },
                open: plain[at..upto].to_vec(),
                rows: std::collections::VecDeque::new(),
                after: None,
                finished: false,
            });
            at = upto;
        }
        Self {
            windows,
            page: page.max(1),
            bytes,
            alone,
            values: value_groups.into_values().collect(),
            groups,
        }
    }

    /// The next windows whose rows are complete, by their index in the list
    /// given to [`Windows::new`]; `None` once every window was handed out.
    pub async fn next(
        &mut self,
        conn: &mut PgConnection,
    ) -> Result<Option<Vec<(usize, Vec<Committed>)>>> {
        if let Some(i) = self.alone.pop() {
            return Ok(Some(vec![(i, read_window(conn, &self.windows[i]).await?)]));
        }
        while let Some(group) = self.values.front_mut() {
            if group.open.is_empty() {
                self.values.pop_front();
                continue;
            }
            let done = group.next(conn, self.page, self.bytes).await?;
            if !done.is_empty() {
                return Ok(Some(done));
            }
        }
        while let Some(group) = self.groups.front_mut() {
            if group.open.is_empty() {
                self.groups.pop_front();
                continue;
            }
            let (page, matched) =
                window_page(conn, &group.union, group.after, self.page, self.bytes).await?;
            // Finished only when the count bound was not reached and the size
            // bound cut nothing off.
            group.finished = matched < self.page && page.len() as i64 == matched;
            if let Some(last) = page.last() {
                group.after = last.event.ts_orig.at.map(|t| (t.0, last.commit_seq));
            }
            group.rows.extend(page);
            // Rows come in time order, so a window ending at or before the
            // last row read has all of its rows.
            let reached = group.rows.back().and_then(|c| c.event.ts_orig.at);
            let complete = |w: &Window| {
                group.finished || w.end.is_some_and(|end| reached.is_some_and(|t| end <= t.0))
            };
            let mut done = Vec::new();
            let windows = &self.windows;
            group.open.retain(|&i| {
                let w = &windows[i];
                if !complete(w) {
                    return true;
                }
                let inside = |c: &&Committed| {
                    c.event.ts_orig.at.is_some_and(|t| {
                        w.start.is_none_or(|start| t.0 >= start)
                            && w.end.is_none_or(|end| t.0 < end)
                    })
                };
                done.push((i, group.rows.iter().filter(inside).cloned().collect()));
                false
            });
            if let Some(start) = group.open.iter().map(|&i| windows[i].start).min().flatten() {
                while group
                    .rows
                    .front()
                    .is_some_and(|c| c.event.ts_orig.at.is_none_or(|t| t.0 < start))
                {
                    group.rows.pop_front();
                }
            } else if group.open.is_empty() {
                group.rows.clear();
            }
            if !done.is_empty() {
                return Ok(Some(done));
            }
        }
        Ok(None)
    }
}

async fn nearest(
    conn: &mut PgConnection,
    w: &Window,
    n: &Nearest,
    after: bool,
    window_events: &[Committed],
) -> Result<Vec<Committed>> {
    let (types, contains) = kind_arrays(&n.kinds);
    let Some(edge) = (if after { w.end } else { w.start }) else {
        return Ok(vec![]);
    };
    let mut out = Vec::new();
    match n.per {
        None => {
            let rows = sqlx::query!(
                r#"SELECT e.id, e.event_type, e.source, e.material_id, e.anchor, e.output_index, e.parents,
                     e.ts_orig, e.ts_quality, e.payload, e.equivalence_key, e.reading_order, e.record_hash,
                     e.record_state, e.class, e.commit_seq, e.kind_version, e.validation,
             (SELECT replace_rule FROM registry.event_types k WHERE k.name=e.event_type AND k.version=e.kind_version) AS replace_rule
                   FROM core.events e
                   WHERE e.event_type = ANY($1)
                     AND (($2 AND e.ts_orig >= $3) OR (NOT $2 AND e.ts_orig < $3))
                     AND EXISTS (SELECT 1 FROM unnest($1::text[], $4::jsonb[]) AS k(event_type, contains)
                                 WHERE k.event_type = e.event_type AND e.payload @> k.contains)
                     AND NOT EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id = e.id)
                   ORDER BY CASE WHEN $2 THEN e.ts_orig END ASC, CASE WHEN NOT $2 THEN e.ts_orig END DESC
                   LIMIT $5"#,
                &types,
                after,
                edge,
                &contains,
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
                .filter(|c| n.kinds.iter().any(|k| k.event_type == c.event.event_type))
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
                     x.material_id, x.anchor, x.output_index AS "output_index!", x.parents, x.ts_orig,
                     x.ts_quality AS "ts_quality!", x.payload AS "payload!", x.equivalence_key,
                     x.reading_order AS "reading_order!", x.record_hash AS "record_hash!",
                     x.record_state AS "record_state!", x.class AS "class!",
                     x.kind_version AS "kind_version!", x.validation AS "validation!", x.replace_rule,
                     x.commit_seq AS "commit_seq!"
                   FROM unnest($5::text[]) AS v(val) CROSS JOIN LATERAL (
                     SELECT e.*, (SELECT replace_rule FROM registry.event_types k WHERE k.name=e.event_type AND k.version=e.kind_version) AS replace_rule FROM core.events e
                     WHERE e.event_type = ANY($1)
                       AND (($2 AND e.ts_orig >= $3) OR (NOT $2 AND e.ts_orig < $3))
                       AND e.payload->>$6 = v.val
                       AND EXISTS (SELECT 1 FROM unnest($1::text[], $4::jsonb[]) AS k(event_type, contains)
                                   WHERE k.event_type = e.event_type AND e.payload @> k.contains)
                       AND NOT EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id = e.id)
                     ORDER BY CASE WHEN $2 THEN e.ts_orig END ASC, CASE WHEN NOT $2 THEN e.ts_orig END DESC
                     LIMIT $7) x"#,
                &types,
                after,
                edge,
                &contains,
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

/// Live committed and unhidden events after `after_seq`, in writer order.
pub async fn after_seq(
    conn: &mut PgConnection,
    event_types: &[String],
    after_seq: i64,
    limit: i64,
    bytes: i64,
) -> Result<Vec<Committed>> {
    let rows = sqlx::query!(
        r#"SELECT e.id, e.event_type, e.source, e.material_id, e.anchor, e.output_index, e.parents,
             e.ts_orig, e.ts_quality, e.payload, e.equivalence_key, e.reading_order, e.record_hash,
             e.record_state, e.class, page.seq AS "commit_seq!", e.kind_version, e.validation,
             (SELECT replace_rule FROM registry.event_types k WHERE k.name=e.event_type AND k.version=e.kind_version) AS replace_rule, e.kind_version, e.validation,
             (SELECT replace_rule FROM registry.event_types k WHERE k.name=e.event_type AND k.version=e.kind_version) AS replace_rule
           FROM core.events e JOIN (
             SELECT p.id, p.seq FROM (
               SELECT q.id, q.seq, (sum(q.size) OVER (ORDER BY q.seq) - q.size)::bigint AS before
               FROM (
                 SELECT s.id, s.commit_seq AS seq, pg_column_size(s.payload)::bigint AS size
                 FROM core.events s
                 WHERE s.commit_seq > $2 AND s.event_type = ANY($1)
                   AND NOT EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id = s.id)
                 UNION ALL
                 SELECT c.event_id AS id, c.commit_seq AS seq, pg_column_size(s.payload)::bigint AS size
                 FROM audit.archive_changes c JOIN core.events s ON s.id = c.event_id
                 WHERE c.commit_seq > $2 AND NOT c.archived AND s.event_type = ANY($1)
                   AND NOT EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id = s.id)
                 ORDER BY seq LIMIT $3
               ) q
             ) p WHERE p.before < $4::bigint
           ) page ON page.id = e.id
           ORDER BY page.seq"#,
        event_types,
        after_seq,
        limit,
        bytes,
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
        r#"SELECT e.id, e.event_type, e.source, e.material_id, e.anchor, e.output_index, e.parents,
             e.ts_orig, e.ts_quality, e.payload, e.equivalence_key, e.reading_order, e.record_hash,
             e.record_state, e.class, e.commit_seq, e.kind_version, e.validation,
             (SELECT replace_rule FROM registry.event_types k WHERE k.name=e.event_type AND k.version=e.kind_version) AS replace_rule
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

/// The largest stored reading order of this producer, before a new run.
pub async fn latest_reading_order(conn: &mut PgConnection, producer: &str) -> Result<i64> {
    Ok(sqlx::query_scalar!(
        r#"SELECT coalesce(max(e.reading_order), 0) AS "order!"
           FROM core.events e JOIN core.runs r ON r.id = e.run_id WHERE r.name = $1"#,
        producer,
    )
    .fetch_one(conn)
    .await?)
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
pub async fn waiting(
    conn: &mut PgConnection,
    derivation: &str,
    due: Option<time::OffsetDateTime>,
) -> Result<Vec<(Value, Timestamp)>> {
    Ok(sqlx::query!(
        "SELECT key, since FROM derivation.waiting WHERE derivation = $1 AND ($2::timestamptz IS NULL OR refresh_at <= $2) ORDER BY since",
        derivation, due,
    )
    .fetch_all(&mut *conn)
    .await?
    .into_iter()
    .map(|r| (r.key, Timestamp(r.since)))
    .collect())
}

/// Every key `derivation` has outputs under, so a recompute also reaches
/// keys its current key function no longer produces (a new version that
/// keys its windows differently).
pub async fn output_keys(conn: &mut PgConnection, derivation: &str) -> Result<Vec<Value>> {
    Ok(sqlx::query_scalar!(
        r#"SELECT DISTINCT key AS "key!" FROM derivation.outputs WHERE derivation = $1"#,
        derivation
    )
    .fetch_all(&mut *conn)
    .await?)
}

/// The live outputs a key last produced.
pub async fn outputs(conn: &mut PgConnection, derivation: &str, key: &Value) -> Result<Vec<Event>> {
    let rows = sqlx::query!(
        r#"SELECT e.id, e.event_type, e.source, e.material_id, e.anchor, e.output_index, e.parents,
             e.ts_orig, e.ts_quality, e.payload, e.equivalence_key, e.reading_order, e.record_hash,
             e.record_state, e.class, e.commit_seq, e.kind_version, e.validation,
             (SELECT replace_rule FROM registry.event_types k WHERE k.name=e.event_type AND k.version=e.kind_version) AS replace_rule
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
/// version changed). Written at daemon start, before the runner runs.
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

#[cfg(test)]
mod tests;
