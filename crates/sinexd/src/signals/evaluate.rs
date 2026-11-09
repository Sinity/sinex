//! Sinex's rules read through the ordinary query engine and feed the shared
//! evaluator; the evaluator alone decides what fires.
use super::*;
use crate::Result;
use sinex_model::event_types::signal::RuleWritten;
use sinex_store::signals::Checkpoint;
use std::collections::{BTreeMap, BTreeSet};

/// Adopt the current authored rules and observe each one's complete answer at
/// `at`, read in the snapshot `conn` holds at commit position `seq`.
pub(super) async fn evaluate(
    api: &Api,
    conn: &mut sqlx::PgConnection,
    state: &mut Checkpoint,
    at: Timestamp,
    seq: i64,
    read: sinex_store::ReadMark,
    calendar: &::signals::DayCalendar,
) -> Result<()> {
    let now = millis(at)?;
    sinex_store::query::set_grants(conn, "guarded").await?;
    let records = sinex_store::signals::rule_rows(conn, RuleWritten::EVENT_TYPE, seq).await?;
    let mut active = BTreeMap::new();
    for (id, payload) in records {
        if payload.get("withheld").is_some() {
            continue;
        }
        let Ok(written) = serde_json::from_value::<RuleWritten>(payload) else {
            continue;
        };
        if !written.withdrawn {
            active.insert(written.name.clone(), (id, written));
        }
    }
    let wanted: BTreeSet<_> = active
        .values()
        .map(|(_, written)| written.active.version.id.clone())
        .collect();
    let removed: Vec<_> = state
        .rules
        .iter()
        .filter(|(id, _)| !wanted.contains(*id))
        .map(|(id, rule)| (id.clone(), rule.active.version.clone()))
        .collect();
    for (id, version) in removed {
        state.evaluator.withdraw(&version);
        state.rules.remove(&id);
        state.rows.remove(&id);
        state.inputs.remove(&id);
    }
    for (_, (event, written)) in active {
        let rule = written.active;
        let key = rule.version.id.clone();
        match state.rules.get_mut(&key) {
            // A replayed rule keeps its revision under a new reading.
            Some(prior) if prior.active.version == rule.version => prior.event = event,
            prior => {
                // An archived newest revision leaves an older one current: it
                // is adopted afresh, since the evaluator only moves forward
                // within one rule's revisions.
                if let Some(prior) = prior
                    && prior.active.version.revision > rule.version.revision
                {
                    state.evaluator.withdraw(&prior.active.version);
                }
                state.evaluator.replace_rule(rule.clone(), now, calendar)?;
                state.rows.remove(&key);
                state.inputs.remove(&key);
                state.rules.insert(
                    key.clone(),
                    sinex_store::signals::Rule {
                        active: rule.clone(),
                        event,
                        class: Class::Open,
                    },
                );
            }
        }
        let author = api
            .caller(match rule.author {
                ::signals::Author::Operator | ::signals::Author::Shipped => {
                    sinex_protocol::Access::Operator
                }
                _ => sinex_protocol::Access::Agent,
            })
            .await;
        // A rule reads only what its author could read when it was written
        // and can still read now.
        let mut granted = Vec::new();
        let mut readable = true;
        for required in &written.grants {
            let grant = match required {
                Class::Open => continue,
                Class::Guarded => "guarded",
                Class::Sealed => "sealed",
            };
            if author.grants.split(',').any(|current| current == grant) {
                granted.push(grant);
            } else {
                readable = false;
            }
        }
        sinex_store::query::set_grants(conn, &granted.join(",")).await?;
        // An unreadable or failing query leaves the rule incomplete: it never
        // fires and never reports a change until it reads completely again.
        let answer = if readable {
            match query(conn, &rule.rule.query, &api.context_at(at)?, seq).await {
                Ok(answer) => Some(answer).filter(|answer| answer.complete),
                Err(reason) => {
                    eprintln!("sinexd: signals: rule {}: {reason}", rule.rule.name);
                    None
                }
            }
        } else {
            None
        };
        let mut rows = Vec::new();
        let mut value = None;
        let mut complete = answer.is_some();
        if let Some(answer) = answer {
            let state_rule = state.rules.get_mut(&key).expect("adopted above");
            state_rule.class = state_rule.class.max(answer.class);
            let rule_reference = rule_reference(&rule.rule.name);
            let prior = state.rows.get(&key);
            let inputs = state.inputs.entry(key.clone()).or_default();
            for row in answer.rows {
                inputs.entry(row.identity.clone()).or_insert_with(|| {
                    sinex_store::signals::Inputs {
                        parents: row
                            .event
                            .map_or_else(|| answer.parents.clone(), |event| vec![event]),
                        read,
                    }
                });
                let old = prior.and_then(|rows| rows.get(&row.identity));
                let start = row
                    .start
                    .map(millis)
                    .transpose()?
                    .or_else(|| old.map(|row| row.start))
                    .unwrap_or(now);
                let observed = row
                    .observed
                    .map(millis)
                    .transpose()?
                    .or_else(|| old.map(|row| row.observed))
                    .unwrap_or(now)
                    .min(now);
                rows.push(::signals::Row {
                    occurrence: ::signals::Occurrence {
                        id: row.identity.clone(),
                        // Native keys and record coordinates already tell
                        // occurrences apart; a reread clock is not a new fact.
                        period: "native".into(),
                    },
                    reference: row.reference.unwrap_or_else(|| rule_reference.clone()),
                    observed,
                    start,
                    end: row.end.map(millis).transpose()?,
                    origin: ::signals::Origin::Source,
                    evidence: row.result,
                });
            }
            if let [row] = rows.as_slice() {
                let numbers: Vec<f64> = row
                    .evidence
                    .as_object()
                    .into_iter()
                    .flat_map(|object| object.values())
                    .filter_map(serde_json::Value::as_f64)
                    .collect();
                if let [number] = numbers.as_slice() {
                    value = Some(*number);
                }
            }
            // A threshold reads one number; any other answer shape leaves the
            // rule incomplete rather than guessing a sample.
            if matches!(rule.rule.fires, ::signals::Fires::OnThreshold { .. }) && value.is_none() {
                complete = false;
            }
            state.rows.insert(
                key.clone(),
                rows.iter()
                    .cloned()
                    .map(|row| (row.occurrence.id.clone(), row))
                    .collect(),
            );
        }
        state.evaluator.observe(::signals::Evaluation {
            rule: rule.version,
            at: now,
            complete,
            rows,
            value,
        })?;
    }
    Ok(())
}

/// The citation of a rule by its source record key, for rows that name no
/// single reading (groups and aggregates). It follows the rule's revisions.
pub(super) fn rule_reference(name: &str) -> String {
    let key = sinex_model::EquivalenceKey::of(
        RuleWritten::EVENT_TYPE,
        &["name"],
        &serde_json::json!({ "name": name }),
    );
    sinex_model::query::citation(
        "key",
        &format!(
            "{}/{}/{}",
            sinex_sources::sources::signals::SPEC.name,
            RuleWritten::EVENT_TYPE,
            key.0
        ),
    )
}
