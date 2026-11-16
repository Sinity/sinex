//! Read transition keys with the same interpretation as the pure fold.
use super::*;
use futures::TryStreamExt;
use sinex_model::state_transition::{self, SubjectField};
use std::collections::{HashMap, HashSet};

#[derive(Default)]
struct Scope {
    previous: Option<OffsetDateTime>,
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

pub(super) async fn bounds(
    conn: &mut PgConnection,
    kinds: &[Kind],
    start: OffsetDateTime,
    end: OffsetDateTime,
    subjects: &[SubjectField],
    interruptions: &[Nearest],
) -> Result<(OffsetDateTime, OffsetDateTime)> {
    let mut fields: Vec<String> = state_transition::payload_fields(subjects)
        .into_iter()
        .map(str::to_owned)
        .collect();
    let interruption_fields: Vec<_> = interruptions.iter().filter_map(|n| n.per).collect();
    for field in &interruption_fields {
        if !fields.iter().any(|selected| selected.as_str() == *field) {
            fields.push((*field).to_owned());
        }
    }
    for kind in kinds
        .iter()
        .chain(interruptions.iter().flat_map(|n| &n.kinds))
    {
        if let Some(equal) = kind.contains.as_object() {
            for field in equal.keys() {
                if !fields.contains(field) {
                    fields.push(field.clone());
                }
            }
        }
    }
    let (state_types, state_contains) = kind_arrays(kinds);
    let mut selected = kinds.to_vec();
    selected.extend(interruptions.iter().flat_map(|n| n.kinds.iter().cloned()));
    let mut scopes: HashMap<String, Scope> = HashMap::new();
    // The bounded first pass selects touched scopes. History is streamed once,
    // keeping one boundary state per touched scope rather than every event.
    for day_only in [true, false] {
        let (types, contains) = if day_only {
            kind_arrays(kinds)
        } else {
            kind_arrays(&selected)
        };
        let mut rows = sqlx::query_file!(
            "src/derivations/sql/state_transition_rows.sql",
            &types,
            &contains,
            start,
            end,
            &fields,
            &state_types,
            &state_contains,
            day_only,
        )
        .fetch(&mut *conn);
        while let Some(row) = rows.try_next().await? {
            let stops: Vec<_> = interruptions
                .iter()
                .filter(|n| {
                    n.kinds
                        .iter()
                        .any(|kind| kind.admits(&row.event_type, &row.payload))
                })
                .collect();
            let stop = !stops.is_empty();
            let state = kinds
                .iter()
                .any(|kind| kind.admits(&row.event_type, &row.payload));
            if !state && !stop {
                continue;
            }
            let location = match (row.material_id, row.record_offset) {
                (Some(material), Some(record_offset)) => Some((
                    material.to_string(),
                    u64::try_from(record_offset)
                        .map_err(|_| crate::Error::Invalid("negative record offset".into()))?,
                    u64::try_from(row.output_index)
                        .map_err(|_| crate::Error::Invalid("negative output_index".into()))?,
                )),
                _ => None,
            };
            let stable = || {
                sinex_model::record::stable_from_parts(
                    &row.event_type,
                    &row.source,
                    &row.payload,
                    location.clone(),
                    row.equivalence_key.as_deref(),
                    &row.id.to_string(),
                )
            };
            let stable = stable();
            let scope_name = state_transition::scope(&row.payload, || stable.clone());
            let remember_fields = |scope: &mut Scope| {
                for field in &interruption_fields {
                    if let Some(value) = row.payload.get(*field).and_then(Value::as_str) {
                        scope
                            .field_values
                            .entry(*field)
                            .or_default()
                            .insert(value.to_owned());
                    }
                }
            };
            if day_only {
                if state {
                    remember_fields(scopes.entry(scope_name).or_default());
                }
                continue;
            }
            if state && let Some(scope) = scopes.get_mut(&scope_name) {
                remember_fields(scope);
            }
            let observe = |scope: &mut Scope| {
                if row.at < start {
                    scope.previous = Some(row.at);
                }
                if row.at < end {
                    let order = (row.at, row.reading_order, stable.clone());
                    if scope.latest.as_ref().is_none_or(|last| order >= last.order) {
                        scope.latest = Some(Observation {
                            order,
                            subject: state_transition::subject(subjects, &row.payload),
                            stop: stop && !state,
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
            };
            if stop {
                for (name, scope) in &mut scopes {
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
                    if name == &scope_name || matching {
                        observe(scope);
                    }
                }
            } else if let Some(scope) = scopes.get_mut(&scope_name) {
                observe(scope);
            }
        }
        if scopes.is_empty() {
            break;
        }
    }
    let from = scopes
        .values()
        .filter_map(|scope| scope.previous)
        .min()
        .map_or(start, |prior| start.min(prior));
    let until = scopes
        .values()
        .filter_map(|scope| scope.closer.or(scope.refresh))
        .max()
        .map_or(end, |last| end.max(last));
    // Include all records at the closing time; the fold orders them by their
    // recorded sequence and stable record identity, never by arrival.
    let until = Timestamp::from_unix_ns(Timestamp(until).unix_ns() + 1_000)
        .map_err(|error| crate::Error::Invalid(error.to_string()))?
        .0;
    Ok((from, until))
}

#[cfg(test)]
mod tests;
