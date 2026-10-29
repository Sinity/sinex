//! Product query evidence feeds the shared evaluator without a second matcher.
use super::*;
use sinex_model::event_types::signal::{EvaluationRecorded, RuleWritten};
use sinex_model::{Event, EventId, EventPayload, Origin, ReadingOrder};
use sinex_store::signals::Checkpoint;
use std::collections::{BTreeMap, BTreeSet};

pub(super) async fn evaluate(
    api: &Api,
    conn: &mut sqlx::PgConnection,
    state: &mut Checkpoint,
    at: Timestamp,
    seq: i64,
    calendar: &::signals::DayCalendar,
) -> Result<(Vec<Event>, Class), String> {
    state.current_inputs.clear();
    sinex_store::query::set_grants(conn, "guarded")
        .await
        .map_err(error)?;
    let records = sinex_store::signals::rule_rows(conn, RuleWritten::EVENT_TYPE, seq)
        .await
        .map_err(error)?;
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
        .map(|(id, rule)| (id.clone(), rule.version.clone()))
        .collect();
    for (id, version) in removed {
        state.evaluator.withdraw(&version);
        state.rules.remove(&id);
        state.rows.remove(&id);
        state.rule_events.remove(&id);
    }
    let mut products = Vec::new();
    let mut class = Class::Open;
    for (_, (rule_event, written)) in active {
        let rule = written.active;
        let key = rule.version.id.clone();
        let new = state
            .rules
            .get(&key)
            .is_none_or(|prior| prior.version != rule.version);
        if new {
            state
                .evaluator
                .replace_rule(rule.clone(), millis(at)?, calendar)
                .map_err(error)?;
            state.rows.remove(&key);
        }
        state.rules.insert(key.clone(), rule.clone());
        state.rule_events.insert(key.clone(), rule_event);
        let current = api
            .caller(
                if matches!(
                    rule.author,
                    ::signals::Author::Operator | ::signals::Author::Shipped
                ) {
                    sinex_protocol::Access::Operator
                } else {
                    sinex_protocol::Access::Agent
                },
            )
            .await;
        let mut granted = Vec::new();
        let mut complete = true;
        for required in &written.grants {
            if *required == Class::Open {
                continue;
            }
            let grant = match required {
                Class::Guarded => "guarded",
                Class::Sealed => "sealed",
                Class::Open => unreachable!(),
            };
            if current.grants.split(',').any(|current| current == grant) {
                granted.push(grant);
            } else {
                complete = false;
            }
        }
        sinex_store::query::set_grants(conn, &granted.join(","))
            .await
            .map_err(error)?;
        let (answer, unavailable) = if complete {
            match crate::signals::query(conn, &rule.rule.query, &api.context_at(at)?, seq).await {
                Ok(answer) if answer.complete => (Some(answer), None),
                Ok(answer) => (
                    None,
                    answer
                        .reason
                        .or_else(|| Some("query_evidence_incomplete".into())),
                ),
                Err(reason) => (None, Some(reason)),
            }
        } else {
            (None, Some("rule_grants_unavailable".into()))
        };
        let mut rows = Vec::new();
        let mut value = None;
        if let Some(answer) = answer.filter(|answer| answer.complete) {
            class = class.max(answer.class);
            let prior = state.rows.get(&key);
            let results = serde_json::to_value(&answer.rows).map_err(error)?;
            let mut parents = answer.parents;
            parents.push(rule_event);
            parents.sort();
            parents.dedup();
            state.current_inputs.insert(key.clone(), parents.clone());
            let product = EvaluationRecorded {
                rule: key.clone(),
                row_identity: "answer".into(),
                occurrence: ::signals::Occurrence {
                    id: key.clone(),
                    episode: rule.version.revision.to_string(),
                },
                query: rule.rule.query.clone(),
                read_as: answer.read_as,
                settings_identity: state.settings_identity.clone(),
                day_setting: calendar.setting().wire(),
                result: results,
                complete: true,
                reason: None,
                observed_at: at,
            };
            let product = Event::build(
                &product,
                "sinex.signals",
                Origin::Derived { parents },
                ReadingOrder(i64::try_from(at.unix_ns()).map_err(error)?),
                *blake3::hash(&serde_json::to_vec(&product).map_err(error)?).as_bytes(),
                answer.class,
            )
            .map_err(error)?;
            let reference = format!("sinex:ev/{}", product.id);
            let now = millis(at)?;
            for row in answer.rows {
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
                        // Native keys/record coordinates already distinguish
                        // episodes; a reinterpreted clock is not a new fact.
                        episode: "native".into(),
                    },
                    reference: row.reference.unwrap_or_else(|| reference.clone()),
                    observed,
                    start,
                    end: row.end.map(millis).transpose()?,
                    origin: ::signals::Origin::Source,
                    evidence: row.result,
                });
            }
            if rows.len() == 1 {
                let numbers: Vec<f64> = rows[0]
                    .evidence
                    .as_object()
                    .into_iter()
                    .flat_map(|object| object.values())
                    .filter_map(serde_json::Value::as_f64)
                    .collect();
                if numbers.len() == 1 {
                    value = Some(numbers[0]);
                }
            }
            state.rows.insert(
                key.clone(),
                rows.iter()
                    .cloned()
                    .map(|row| (row.occurrence.id.clone(), row))
                    .collect(),
            );
            let mut product = product;
            if matches!(rule.rule.fires, ::signals::Fires::OnThreshold { .. }) && value.is_none() {
                complete = false;
                product.payload["complete"] = serde_json::json!(false);
                product.payload["reason"] =
                    serde_json::json!("threshold_requires_one_numeric_cell");
                product.record_hash =
                    *blake3::hash(&serde_json::to_vec(&product.payload).map_err(error)?).as_bytes();
            }
            products.push(product);
        } else {
            complete = false;
            let product = EvaluationRecorded {
                rule: key.clone(),
                row_identity: "answer".into(),
                occurrence: ::signals::Occurrence {
                    id: key.clone(),
                    episode: rule.version.revision.to_string(),
                },
                query: rule.rule.query.clone(),
                read_as: seq,
                settings_identity: state.settings_identity.clone(),
                day_setting: calendar.setting().wire(),
                result: serde_json::Value::Null,
                complete: false,
                reason: unavailable,
                observed_at: at,
            };
            products.push(
                Event::build(
                    &product,
                    "sinex.signals",
                    Origin::Derived {
                        parents: vec![rule_event],
                    },
                    ReadingOrder(i64::try_from(at.unix_ns()).map_err(error)?),
                    *blake3::hash(&serde_json::to_vec(&product).map_err(error)?).as_bytes(),
                    Class::Guarded,
                )
                .map_err(error)?,
            );
            class = class.max(Class::Guarded);
        }
        state
            .evaluator
            .observe(::signals::Evaluation {
                rule: rule.version,
                at: millis(at)?,
                complete,
                rows,
                value,
            })
            .map_err(error)?;
    }
    Ok((products, class))
}
