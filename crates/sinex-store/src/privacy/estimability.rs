//! Required-input checks consume the trace from the single rule execution.
use super::{AffectedPath, projection};
use crate::{Error, Result};
use sinex_model::{Event, InputRequirement, InputUnavailableReason, KindRef, UnavailableInput};
use sqlx::PgConnection;

fn components(pointer: &str) -> Option<Vec<String>> {
    if pointer.is_empty() {
        return Some(vec![]);
    }
    if !pointer.starts_with('/') {
        return None;
    }
    pointer[1..]
        .split('/')
        .map(|component| {
            let mut out = String::new();
            let mut chars = component.chars();
            while let Some(c) = chars.next() {
                if c == '~' {
                    out.push(match chars.next()? {
                        '0' => '~',
                        '1' => '/',
                        _ => return None,
                    })
                } else {
                    out.push(c)
                }
            }
            Some(out)
        })
        .collect()
}
fn overlaps(required: &str, affected: &str) -> bool {
    let (Some(required), Some(affected)) = (components(required), components(affected)) else {
        return true;
    };
    required
        .iter()
        .zip(&affected)
        .all(|(r, a)| r == "*" || r == a)
}
fn valid(requirements: &[InputRequirement]) -> bool {
    requirements.iter().all(|r| {
        r.kind.database_version().is_ok()
            && r.paths.iter().all(|p| components(p).is_some())
            && r.when.iter().all(|(p, _)| {
                components(p).is_some_and(|parts| !parts.iter().any(|part| part == "*"))
            })
    })
}
/// Classification retains estimability. A transforming action is evidence
/// even when it produced the same visible value as the original.
pub fn reason(
    event: &Event,
    trace: &[AffectedPath],
    requirements: Option<&[InputRequirement]>,
) -> Option<InputUnavailableReason> {
    let affected: Vec<_> = trace.iter().filter(|p| p.action.transforms()).collect();
    if affected.is_empty() {
        return None;
    }
    let Some(requirements) = requirements.filter(|r| valid(r)) else {
        return Some(InputUnavailableReason::UndeclaredAffectedInput);
    };
    let same: Vec<_> = requirements
        .iter()
        .filter(|r| r.kind.name == event.event_type && r.kind.version == event.kind_version)
        .collect();
    if same.is_empty() {
        return Some(InputUnavailableReason::UndeclaredAffectedInput);
    }
    if same
        .iter()
        .flat_map(|r| &r.when)
        .any(|(p, _)| affected.iter().any(|a| overlaps(p, &a.path)))
    {
        return Some(InputUnavailableReason::RequiredPathTransformed);
    }
    let applicable: Vec<_> = same
        .into_iter()
        .filter(|r| {
            r.when
                .iter()
                .all(|(p, v)| event.payload.pointer(p) == Some(v))
        })
        .collect();
    if applicable.is_empty() {
        return Some(InputUnavailableReason::UndeclaredAffectedInput);
    }
    applicable
        .iter()
        .flat_map(|r| &r.paths)
        .any(|p| affected.iter().any(|a| overlaps(p, &a.path)))
        .then_some(InputUnavailableReason::RequiredPathTransformed)
}
/// The caller holds the input-read snapshot. Missing/stale projections never
/// become an empty trace, and the public issue omits affected paths and values.
pub async fn unavailable(
    conn: &mut PgConnection,
    events: &[Event],
    requirements: Option<&[InputRequirement]>,
) -> Result<Vec<UnavailableInput>> {
    let state = projection::epoch(conn).await?;
    if !state.ready() {
        return Err(Error::Invalid("privacy_projection_stale".into()));
    }
    let mut issues = Vec::new();
    for page in events.chunks(512) {
        let ids: Vec<_> = page.iter().map(|e| e.id.0).collect();
        let rows: Vec<(uuid::Uuid, i64, String, serde_json::Value)> = sqlx::query_as(
            "SELECT event_id,epoch,state,trace FROM privacy.event_outputs WHERE event_id=ANY($1)",
        )
        .bind(ids)
        .fetch_all(&mut *conn)
        .await?;
        for event in page {
            let reason = if sinex_sealing::is_sealed(&event.payload) {
                Some(InputUnavailableReason::ProjectionUnavailable)
            } else {
                match rows.iter().find(|r| r.0 == event.id.0) {
                    Some((_, epoch, status, trace))
                        if *epoch == state.active && status == "complete" =>
                    {
                        match serde_json::from_value::<Vec<AffectedPath>>(trace.clone()) {
                            Ok(trace) => reason(event, &trace, requirements),
                            Err(_) => Some(InputUnavailableReason::ProjectionUnavailable),
                        }
                    }
                    _ => Some(InputUnavailableReason::ProjectionUnavailable),
                }
            };
            if let Some(reason) = reason {
                issues.push(UnavailableInput {
                    event: event.id,
                    kind: KindRef {
                        name: event.event_type.clone(),
                        version: event.kind_version,
                    },
                    reason,
                })
            }
        }
    }
    Ok(issues)
}
/// A build's computational footprint is immutable like its producer version.
pub async fn register(
    conn: &mut PgConnection,
    name: &str,
    version: u32,
    build: &str,
    requirements: Option<&[InputRequirement]>,
) -> Result<()> {
    if requirements.is_some_and(|r| !valid(r)) {
        return Err(Error::Invalid(
            "invalid_derivation_input_requirements".into(),
        ));
    }
    let requirements =
        serde_json::to_value(requirements).map_err(|e| Error::Invalid(e.to_string()))?;
    let saved:serde_json::Value=sqlx::query_scalar("INSERT INTO privacy.derivation_contracts(derivation,version,build,requirements) VALUES($1,$2,$3,$4) ON CONFLICT(derivation,version,build) DO UPDATE SET requirements=privacy.derivation_contracts.requirements RETURNING requirements")
        .bind(name).bind(i64::from(version)).bind(build).bind(&requirements).fetch_one(conn).await?;
    if saved != requirements {
        return Err(Error::Invalid(
            "derivation_input_contract_changed_for_build".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
