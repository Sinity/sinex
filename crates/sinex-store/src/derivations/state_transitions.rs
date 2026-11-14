//! Read transition keys with the same interpretation as the pure fold.
use super::*;
use futures::TryStreamExt;
use sinex_model::state_transition::{self, SubjectField};
use std::collections::HashMap;

#[derive(Default)]
struct Scope {
    previous: Option<OffsetDateTime>,
    latest: Option<Observation>,
    closer: Option<OffsetDateTime>,
    refresh: Option<OffsetDateTime>,
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
    interruptions: &[Kind],
) -> Result<(OffsetDateTime, OffsetDateTime)> {
    let mut fields: Vec<String> = state_transition::payload_fields(subjects)
        .into_iter()
        .map(str::to_owned)
        .collect();
    for kind in kinds.iter().chain(interruptions) {
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
    selected.extend_from_slice(interruptions);
    let mut scopes: HashMap<String, Scope> = HashMap::new();
    // The bounded first pass selects touched scopes. History is streamed once,
    // retaining one boundary state per touched scope rather than every event.
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
            let stop = interruptions
                .iter()
                .any(|kind| kind.admits(&row.event_type, &row.payload));
            let state = kinds
                .iter()
                .any(|kind| kind.admits(&row.event_type, &row.payload));
            if !state && !stop {
                continue;
            }
            let location = match (row.material_id, row.anchor) {
                (Some(material), Some(anchor)) => Some((
                    material.to_string(),
                    u64::try_from(anchor)
                        .map_err(|_| crate::Error::Invalid("negative anchor".into()))?,
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
            let scope = state_transition::scope(&row.payload, &stable);
            if day_only {
                if state {
                    scopes.entry(scope).or_default();
                }
                continue;
            }
            let Some(scope) = scopes.get_mut(&scope) else {
                continue;
            };
            if row.at < start {
                scope.previous = Some(row.at);
            }
            if row.at < end {
                let order = (row.at, row.reading_order, stable());
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
