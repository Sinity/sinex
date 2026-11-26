//! Read only the prior boundaries and the forward transitions a state day
//! needs. A transition that names no scope opens no interval, so it selects
//! no scope here: it has no prior state and no closer to look for.
use super::*;
use sinex_model::state_transition::{self, SubjectField};
use std::collections::{BTreeMap, HashMap, HashSet};

const ROWS_PER_QUERY: i64 = 256;

struct Row {
    id: Uuid,
    event_type: String,
    source: String,
    material_id: Option<Uuid>,
    record_offset: Option<i64>,
    output_index: i64,
    at: OffsetDateTime,
    reading_order: i64,
    commit_seq: i64,
    equivalence_key: Option<String>,
    payload: Value,
}
impl Row {
    fn stable(&self) -> Result<String> {
        let location = match (self.material_id, self.record_offset) {
            (Some(material), Some(offset)) => Some((
                material.to_string(),
                u64::try_from(offset)
                    .map_err(|_| crate::Error::Invalid("negative record offset".into()))?,
                u64::try_from(self.output_index)
                    .map_err(|_| crate::Error::Invalid("negative output_index".into()))?,
            )),
            _ => None,
        };
        Ok(sinex_model::record::stable_from_parts(
            &self.event_type,
            &self.source,
            &self.payload,
            location,
            self.equivalence_key.as_deref(),
            &self.id.to_string(),
        ))
    }
    fn scope(&self) -> Option<String> {
        state_transition::scope(&self.payload)
    }
}

/// Payload values a wanted record holds, each as a one-field object.
#[derive(Default)]
struct Candidates {
    payloads: BTreeMap<String, Value>,
}
impl Candidates {
    fn field(&mut self, field: &str, value: &Value) {
        let candidate = serde_json::json!({(field): value});
        self.payloads.insert(candidate.to_string(), candidate);
    }
    /// Fetch a superset; the shared Rust interpretation chooses the scope.
    fn scope(&mut self, scope: &str) {
        for field in state_transition::SCOPE_FIELDS {
            self.field(field, &Value::String(scope.to_owned()));
        }
    }
}

#[derive(Default)]
struct Scope {
    previous: Option<OffsetDateTime>,
    prior: Vec<Uuid>,
    latest: Option<Observation>,
    closer: Option<OffsetDateTime>,
    refresh: Option<OffsetDateTime>,
    field_values: HashMap<&'static str, HashSet<String>>,
}
struct Observation {
    order: (OffsetDateTime, i64, String),
    subject: Option<String>,
    stop: bool,
}

pub(super) struct Boundaries {
    #[cfg(test)]
    pub from: OffsetDateTime,
    pub until: OffsetDateTime,
    pub prior: Vec<Uuid>,
    #[cfg(test)]
    pub visited: usize,
}

async fn page(
    conn: &mut PgConnection,
    kinds: &[Kind],
    fields: &[String],
    start: Option<OffsetDateTime>,
    end: Option<OffsetDateTime>,
    cursor: Option<(OffsetDateTime, i64)>,
    prior: Option<&Candidates>,
) -> Result<Vec<Row>> {
    let (types, contains) = kind_arrays(kinds);
    let (at, seq) = cursor.map_or((None, None), |(at, seq)| (Some(at), Some(seq)));
    Ok(if let Some(candidates) = prior {
        let payloads: Vec<_> = candidates.payloads.values().cloned().collect();
        sqlx::query_file_as!(
            Row,
            "src/derivations/sql/state_transition_prior.sql",
            &types,
            &contains,
            start,
            end,
            fields,
            at,
            seq,
            ROWS_PER_QUERY,
            &payloads,
        )
        .fetch_all(conn)
        .await?
    } else {
        sqlx::query_file_as!(
            Row,
            "src/derivations/sql/state_transition_rows.sql",
            &types,
            &contains,
            start,
            end,
            fields,
            at,
            seq,
            ROWS_PER_QUERY,
        )
        .fetch_all(conn)
        .await?
    })
}

fn remember(scope: &mut Scope, row: &Row, fields: &[&'static str]) {
    for field in fields {
        if let Some(value) = row.payload.get(*field).and_then(Value::as_str) {
            scope
                .field_values
                .entry(*field)
                .or_default()
                .insert(value.to_owned());
        }
    }
}
fn observe(
    scope: &mut Scope,
    row: &Row,
    stable: &str,
    stop: bool,
    start: OffsetDateTime,
    end: OffsetDateTime,
    subjects: &[SubjectField],
) {
    if row.at < start && scope.previous.is_none_or(|prior| row.at >= prior) {
        if scope.previous != Some(row.at) {
            scope.prior.clear();
        }
        scope.previous = Some(row.at);
        scope.prior.push(row.id);
    }
    if row.at < end {
        let order = (row.at, row.reading_order, stable.to_owned());
        if scope.latest.as_ref().is_none_or(|last| order >= last.order) {
            scope.latest = Some(Observation {
                order,
                subject: state_transition::subject(subjects, &row.payload),
                stop,
            });
        }
    } else if scope.closer.is_none()
        && let Some(last) = &scope.latest
        && !last.stop
    {
        let subject = state_transition::subject(subjects, &row.payload);
        if stop || last.subject.is_none() || last.subject != subject {
            scope.closer = Some(row.at);
        } else {
            scope.refresh = Some(row.at);
        }
    }
}

pub(super) async fn resolve(
    conn: &mut PgConnection,
    kinds: &[Kind],
    start: OffsetDateTime,
    end: OffsetDateTime,
    subjects: &[SubjectField],
    interruptions: &[Nearest],
) -> Result<Boundaries> {
    let mut fields: Vec<String> = state_transition::payload_fields(subjects)
        .into_iter()
        .map(str::to_owned)
        .collect();
    let interruption_fields: Vec<_> = interruptions.iter().filter_map(|n| n.per).collect();
    let mut selected = kinds.to_vec();
    selected.extend(interruptions.iter().flat_map(|n| n.kinds.iter().cloned()));
    for field in &interruption_fields {
        if !fields.iter().any(|selected| selected == *field) {
            fields.push((*field).into());
        }
    }
    for kind in &selected {
        if let Some(equal) = kind.contains.as_object() {
            for field in equal.keys() {
                if !fields.contains(field) {
                    fields.push(field.clone());
                }
            }
        }
    }
    let mut scopes: HashMap<String, Scope> = HashMap::new();
    let mut candidates = Candidates::default();
    #[cfg(test)]
    let mut visited = 0;
    let mut cursor = None;
    loop {
        let rows = page(conn, kinds, &fields, Some(start), Some(end), cursor, None).await?;
        if rows.is_empty() {
            break;
        }
        for row in &rows {
            #[cfg(test)]
            {
                visited += 1;
            }
            if kinds
                .iter()
                .any(|kind| kind.admits(&row.event_type, &row.payload))
                && let Some(scope) = row.scope()
            {
                candidates.scope(&scope);
                remember(scopes.entry(scope).or_default(), row, &interruption_fields);
            }
        }
        cursor = rows.last().map(|row| (row.at, row.commit_seq));
    }
    cursor = None;
    while !scopes.is_empty() {
        let rows = page(
            conn,
            kinds,
            &fields,
            None,
            Some(start),
            cursor,
            Some(&candidates),
        )
        .await?;
        if rows.is_empty() {
            break;
        }
        for row in &rows {
            #[cfg(test)]
            {
                visited += 1;
            }
            if !kinds
                .iter()
                .any(|kind| kind.admits(&row.event_type, &row.payload))
            {
                continue;
            }
            if let Some(scope) = row.scope().and_then(|name| scopes.get_mut(&name))
                && scope.previous.is_none_or(|at| at == row.at)
            {
                remember(scope, row, &interruption_fields);
                observe(scope, row, &row.stable()?, false, start, end, subjects);
            }
        }
        let row = rows.last().unwrap();
        cursor = Some((row.at, row.commit_seq));
        // Finish the entire timestamp, so recorded/stable ties remain visible.
        if scopes
            .values()
            .all(|scope| scope.previous.is_some_and(|at| at > row.at))
        {
            break;
        }
    }
    // A stop after the selected prior state must also reach the fold. Its
    // candidate fields come from the day and the selected prior observations.
    let mut stops = Candidates::default();
    for (name, scope) in &scopes {
        stops.scope(name);
        for (field, values) in &scope.field_values {
            for value in values {
                stops.field(field, &Value::String(value.clone()));
            }
        }
    }
    let stop_kinds: Vec<_> = interruptions
        .iter()
        .flat_map(|stop| stop.kinds.iter().cloned())
        .collect();
    if let Some(from) = scopes.values().filter_map(|scope| scope.previous).min() {
        cursor = None;
        loop {
            let rows = page(
                conn,
                &stop_kinds,
                &fields,
                Some(from),
                Some(start),
                cursor,
                Some(&stops),
            )
            .await?;
            if rows.is_empty() {
                break;
            }
            for row in &rows {
                #[cfg(test)]
                {
                    visited += 1;
                }
                apply(
                    &mut scopes,
                    row,
                    kinds,
                    subjects,
                    interruptions,
                    &interruption_fields,
                    start,
                    end,
                )?;
            }
            let row = rows.last().unwrap();
            cursor = Some((row.at, row.commit_seq));
        }
    }
    cursor = None;
    while !scopes.is_empty() {
        let rows = page(conn, &selected, &fields, Some(start), None, cursor, None).await?;
        if rows.is_empty() {
            break;
        }
        for row in &rows {
            #[cfg(test)]
            {
                visited += 1;
            }
            apply(
                &mut scopes,
                row,
                kinds,
                subjects,
                interruptions,
                &interruption_fields,
                start,
                end,
            )?;
        }
        let row = rows.last().unwrap();
        cursor = Some((row.at, row.commit_seq));
        if row.at >= end
            && scopes.values().all(|scope| {
                scope.closer.is_some_and(|at| at < row.at)
                    || scope.latest.as_ref().is_some_and(|last| last.stop)
            })
        {
            break;
        }
    }
    #[cfg(test)]
    let from = scopes
        .values()
        .filter_map(|scope| scope.previous)
        .min()
        .map_or(start, |at| start.min(at));
    let until = scopes
        .values()
        .filter_map(|scope| scope.closer.or(scope.refresh))
        .max()
        .map_or(end, |at| end.max(at));
    let until = Timestamp::from_unix_ns(Timestamp(until).unix_ns() + 1_000)
        .map_err(|error| crate::Error::Invalid(error.to_string()))?
        .0;
    Ok(Boundaries {
        #[cfg(test)]
        from,
        until,
        prior: scopes.into_values().flat_map(|scope| scope.prior).collect(),
        #[cfg(test)]
        visited,
    })
}

#[allow(clippy::too_many_arguments)]
fn apply(
    scopes: &mut HashMap<String, Scope>,
    row: &Row,
    kinds: &[Kind],
    subjects: &[SubjectField],
    interruptions: &[Nearest],
    fields: &[&'static str],
    start: OffsetDateTime,
    end: OffsetDateTime,
) -> Result<()> {
    let stops: Vec<_> = interruptions
        .iter()
        .filter(|n| {
            n.kinds
                .iter()
                .any(|kind| kind.admits(&row.event_type, &row.payload))
        })
        .collect();
    let state = kinds
        .iter()
        .any(|kind| kind.admits(&row.event_type, &row.payload));
    if !state && stops.is_empty() {
        return Ok(());
    }
    let name = row.scope();
    let stable = row.stable()?;
    if stops.is_empty() {
        if let Some(scope) = name.and_then(|name| scopes.get_mut(&name)) {
            remember(scope, row, fields);
            observe(scope, row, &stable, false, start, end, subjects);
        }
        return Ok(());
    }
    if state && let Some(scope) = name.as_ref().and_then(|name| scopes.get_mut(name)) {
        remember(scope, row, fields);
    }
    for (scope_name, scope) in scopes {
        let matching = stops.iter().any(|n| {
            n.per.is_some_and(|field| {
                row.payload
                    .get(field)
                    .and_then(Value::as_str)
                    .is_some_and(|value| {
                        scope
                            .field_values
                            .get(field)
                            .is_some_and(|values| values.contains(value))
                    })
            })
        });
        if name.as_ref() == Some(scope_name) || matching {
            observe(
                scope,
                row,
                &stable,
                !stops.is_empty() && !state,
                start,
                end,
                subjects,
            );
        }
    }
    Ok(())
}

#[cfg(test)]
async fn bounds(
    conn: &mut PgConnection,
    kinds: &[Kind],
    start: OffsetDateTime,
    end: OffsetDateTime,
    subjects: &[SubjectField],
    interruptions: &[Nearest],
) -> Result<(OffsetDateTime, OffsetDateTime)> {
    let result = resolve(conn, kinds, start, end, subjects, interruptions).await?;
    Ok((result.from, result.until))
}

#[cfg(test)]
mod tests;
