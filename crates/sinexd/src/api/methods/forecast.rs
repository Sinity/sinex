//! Forecast command adapters. Reads share ordinary event visibility.
use super::*;
use sinex_model::Timestamp;
use sinex_model::event_types::forecast::{ForecastRecord, Issued, Resolved};
use sqlx::{Connection, PgConnection};
use std::collections::BTreeMap;

fn error(e: impl std::fmt::Display) -> String {
    e.to_string()
}

async fn visible(conn: &mut PgConnection, reference: &str) -> Result<Value, String> {
    let reference = query::parse_reference(reference).map_err(error)?;
    let event = sinex_store::query::show(conn, &reference)
        .await
        .map_err(error)?;
    if event["state"] != "live"
        || (!event["payload"].is_object() || event["payload"].get("withheld").is_some())
    {
        return Err("reference needs an available live event".into());
    }
    if event["class"] == "sealed" {
        return Err("forecasts cannot cite sealed evidence".into());
    }
    Ok(event)
}

async fn references(
    conn: &mut PgConnection,
    refs: &[String],
    cutoff: Option<Timestamp>,
) -> Result<Vec<String>, String> {
    let mut ids = Vec::new();
    for reference in refs {
        let event = visible(conn, reference).await?;
        if let Some(cutoff) = cutoff {
            let written: Timestamp =
                serde_json::from_value(event["written"].clone()).map_err(error)?;
            if written > cutoff {
                return Err("information was recorded after the declared cutoff".into());
            }
        }
        ids.push(event["id"].as_str().ok_or("event has no ID")?.to_owned());
    }
    ids.sort();
    ids.dedup();
    Ok(ids)
}

async fn available_evidence(conn: &mut PgConnection, refs: &[String]) -> Result<bool, String> {
    for reference in refs {
        let id = uuid::Uuid::parse_str(reference)
            .map_err(|_| "resolution has an invalid evidence ID")?;
        let row = sinex_store::live::class_and_payload(conn, id)
            .await
            .map_err(error)?;
        if !row.is_some_and(|(class, payload)| {
            class != "sealed"
                && payload.is_some_and(|p| p.is_object() && p.get("withheld").is_none())
        }) {
            return Ok(false);
        }
    }
    Ok(true)
}

impl Api {
    pub(super) async fn forecast(
        &self,
        request: Request,
        caller: &Caller,
    ) -> Result<Value, String> {
        let mut conn = self.acquire(caller).await.map_err(error)?;
        let mut tx = conn.begin().await.map_err(error)?;
        sinex_store::query::read_one_snapshot(&mut tx)
            .await
            .map_err(error)?;
        if matches!(request, Request::ForecastReport) {
            let answer = report(&mut tx).await?;
            tx.commit().await.map_err(error)?;
            return Ok(answer);
        }
        if !caller.grants.split(',').any(|g| g == "guarded") {
            return Err("forecast authoring requires the guarded grant".into());
        }
        let now = Timestamp::now();
        let (record, forecast) = match request {
            Request::ForecastIssue {
                target,
                probability,
                criteria,
                horizon,
                cutoff,
                information,
            } => {
                let p: f64 = probability
                    .parse()
                    .map_err(|_| "probability must be a number")?;
                if !p.is_finite() || !(0.0..=1.0).contains(&p) {
                    return Err("probability must be finite and between 0 and 1".into());
                }
                if target.trim().is_empty() || criteria.trim().is_empty() {
                    return Err("target and criteria must be nonempty".into());
                }
                let horizon = timestamp(&horizon)?;
                let cutoff = timestamp(&cutoff)?;
                if horizon <= now || cutoff > now {
                    return Err("horizon must be future and cutoff must not be future".into());
                }
                let information = references(&mut tx, &information, Some(cutoff)).await?;
                let position = sinex_store::query::current_seq(&mut tx)
                    .await
                    .map_err(error)?;
                let id = uuid::Uuid::now_v7().to_string();
                (
                    ForecastRecord::Issued(Issued {
                        forecast: id.clone(),
                        target,
                        probability: p,
                        criteria,
                        horizon,
                        information_cutoff: cutoff,
                        information,
                        information_position: position,
                        by: caller.actor.into(),
                        at: now,
                    }),
                    id,
                )
            }
            Request::ForecastResolve {
                forecast,
                outcome,
                at,
                evidence,
            } => {
                let event = visible(&mut tx, &forecast).await?;
                if event["kind"] != "forecast.issued" {
                    return Err("forecast must cite a forecast.issued event".into());
                }
                let issued: Issued =
                    serde_json::from_value(event["payload"].clone()).map_err(error)?;
                let outcome = match outcome.as_str() {
                    "true" => Some(true),
                    "false" => Some(false),
                    "unknown" => None,
                    _ => return Err("outcome must be true, false or unknown".into()),
                };
                let outcome_at = at.as_deref().map(timestamp).transpose()?;
                if outcome_at.is_some_and(|at| at < issued.at || at > now) {
                    return Err("outcome time must be between issuance and recording".into());
                }
                if evidence.is_empty() {
                    return Err("resolution needs evidence".into());
                }
                let evidence = references(&mut tx, &evidence, None).await?;
                if outcome.is_some() {
                    let resolutions =
                        sinex_store::live::of_types(&mut tx, &["forecast.resolved".into()])
                            .await
                            .map_err(error)?;
                    if resolutions.iter().any(|r| {
                        r.payload.as_ref().is_some_and(|p| {
                            p["forecast"] == issued.forecast.as_str() && p["outcome"].is_boolean()
                        })
                    }) {
                        return Err("forecast already has a visible known resolution".into());
                    }
                }
                let id = issued.forecast;
                (
                    ForecastRecord::Resolved(Resolved {
                        forecast: id.clone(),
                        outcome,
                        outcome_at,
                        evidence,
                        by: caller.actor.into(),
                        at: now,
                    }),
                    id,
                )
            }
            _ => unreachable!("forecast commands only"),
        };
        tx.commit().await.map_err(error)?;
        let writer = self
            .writer
            .as_ref()
            .ok_or("forecast writes require sinexd run")?;
        let binding = format!("forecast.request:{}@{}", caller.actor, self.host);
        let acquired = sinex_sources::sources::forecast::request_material(
            record,
            caller.actor,
            &self.host,
            now,
        );
        let done = crate::authored::submit(
            writer,
            &self.blobs,
            std::sync::Arc::new(sinex_sources::sources::forecast::request_source()),
            &binding,
            acquired,
        )
        .await?;
        if done.outcome.admitted != 1 || done.outcome.failures != 0 {
            return Err(format!(
                "the forecast was not admitted as one event (admitted {}, failures {})",
                done.outcome.admitted, done.outcome.failures
            ));
        }
        let material = done.material.ok_or("forecast admission has no material")?;
        let id = sinex_store::live::at(
            &mut conn,
            sinex_model::RecordLocation {
                material,
                anchor: 0,
            },
            0,
        )
        .await
        .map_err(error)?;
        let id = id.ok_or("forecast event was withdrawn before its response")?;
        Ok(
            json!({"forecast":forecast, "event":id, "citation":format!("sinex:ev/{id}"), "material":material}),
        )
    }
}

async fn report(conn: &mut PgConnection) -> Result<Value, String> {
    let position = sinex_store::query::current_seq(conn).await.map_err(error)?;
    let rows = sinex_store::live::of_types(
        conn,
        &["forecast.issued".into(), "forecast.resolved".into()],
    )
    .await
    .map_err(error)?;
    let mut issued = BTreeMap::new();
    let mut resolutions: BTreeMap<String, Vec<(String, Resolved)>> = BTreeMap::new();
    let mut withheld = 0;
    for sinex_store::live::LiveEvent {
        id,
        event_type: kind,
        payload,
    } in rows
    {
        let Some(payload) = payload.filter(|p| p.is_object() && p.get("withheld").is_none()) else {
            withheld += 1;
            continue;
        };
        if kind == "forecast.issued" {
            let value: Issued = serde_json::from_value(payload).map_err(error)?;
            if issued
                .insert(value.forecast.clone(), (id.to_string(), value))
                .is_some()
            {
                return Err("multiple visible issuances share a forecast identity".into());
            }
        } else {
            let value: Resolved = serde_json::from_value(payload).map_err(error)?;
            resolutions
                .entry(value.forecast.clone())
                .or_default()
                .push((id.to_string(), value));
        }
    }
    let mut pairs = Vec::new();
    let mut inputs = Vec::new();
    let mut unavailable_evidence = 0;
    for (forecast, (id, issue)) in issued {
        inputs.push(id);
        let mut outcomes = Vec::new();
        for (id, resolution) in resolutions.remove(&forecast).unwrap_or_default() {
            if resolution.at < issue.at
                || resolution
                    .outcome_at
                    .is_some_and(|t| t < issue.at || t > resolution.at)
            {
                return Err("resolution time precedes issuance or follows recording".into());
            }
            if resolution.evidence.is_empty() {
                return Err("resolution has no evidence".into());
            }
            if !available_evidence(conn, &resolution.evidence).await? {
                unavailable_evidence += 1;
                continue;
            }
            inputs.push(id);
            if let Some(outcome) = resolution.outcome {
                outcomes.push(outcome);
            }
        }
        if outcomes.len() > 1 {
            return Err(
                "multiple visible known resolutions; hide an incorrect resolution before scoring"
                    .into(),
            );
        }
        pairs.push(sinex_analysis::forecast::Pair {
            forecast,
            probability: issue.probability,
            outcome: outcomes.pop(),
        });
    }
    let report = sinex_analysis::forecast::score(pairs)?;
    Ok(
        json!({"report":report,"commit_seq":position,"inputs":inputs,"withheld_events":withheld,"unavailable_evidence":unavailable_evidence}),
    )
}
