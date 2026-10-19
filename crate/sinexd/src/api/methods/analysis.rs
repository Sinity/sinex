//! The analysis commands.
use super::*;

impl Api {
    /// The analysis calls. Results carry their status, as-of position, caveats
    /// and citations; findings are rendered from their slots, so every surface
    /// reads the same text.
    pub(super) async fn analysis(&self, caller: &Caller, request: Request) -> Reply {
        use crate::analysis::{self, Recipe};
        if let Request::AnalysisCatalog = request {
            let metrics: Vec<Value> = sinex_analysis::catalog::METRICS
                .iter()
                .map(|m| {
                    json!({"id": m.id, "version": m.version, "description": m.description,
                           "relation": m.relation, "sources": m.coverage.sources(),
                           "dimensions": m.dimensions, "role": m.role})
                })
                .collect();
            let methods: Vec<Value> = sinex_analysis::methods::catalog()
                .into_iter()
                .map(|(id, version, summary)| json!({"id": id, "version": version, "summary": summary}))
                .collect();
            return data(json!({
                "metrics": metrics, "methods": methods,
                "recipes": ["correlations", "trends", "deviations"],
            }));
        }
        let settings = match analysis::Settings::from_api(&self.zone, self.day_start) {
            Ok(s) => s,
            Err(e) => return refused(e),
        };
        let mut conn = match self.acquire(caller).await {
            Ok(c) => c,
            Err(e) => return refused(e.to_string()),
        };
        let outcome: Result<Value, String> = async {
            match request {
                Request::AnalysisRun {
                    recipe,
                    from,
                    to,
                    standing,
                    partial,
                } => {
                    let recipe = Recipe::parse(&recipe)?;
                    let min_observed_fraction = partial
                        .map(|p| {
                            p.parse::<f64>()
                                .ok()
                                .filter(|f| (0.0..=1.0).contains(f))
                                .ok_or_else(|| format!("`{p}` is not a fraction from 0 to 1"))
                        })
                        .transpose()?;
                    let eligibility = sinex_analysis::Eligibility {
                        min_observed_fraction,
                        admit_recorded: false,
                    };
                    let run = analysis::run(
                        &mut conn,
                        &settings,
                        recipe,
                        analysis::parse_day(&from)?,
                        analysis::parse_day(&to)?,
                        eligibility,
                        standing.unwrap_or(false),
                    )
                    .await?;
                    Ok(json!({
                        "result": run.result.id, "state": run.result.state, "reused": run.reused,
                        "tables": run.tables,
                        "findings": run.findings.iter().map(|f| json!({"text": f.markdown(), "finding": f})).collect::<Vec<_>>(),
                    }))
                }
                Request::AnalysisPanel {
                    from,
                    to,
                    metric,
                    by,
                } => {
                    let defs = metric_defs(&metric.unwrap_or_default())?;
                    let (from, to) = (analysis::parse_day(&from)?, analysis::parse_day(&to)?);
                    let as_of = sinex_store::analysis::head(&mut conn)
                        .await
                        .map_err(|e| e.to_string())?;
                    let mut sources: Vec<&str> = defs
                        .iter()
                        .flat_map(|d| d.coverage.sources().iter().copied())
                        .collect();
                    sources.sort_unstable();
                    sources.dedup();
                    let coverage = analysis::coverage(&mut conn, &sources, as_of).await?;
                    let range = settings.day_span(from, to)?;
                    let p = analysis::panel(&mut conn, &settings, &defs, range, as_of, &coverage)
                        .await?;
                    Ok(json!({
                        "text": analysis::table(&p, &defs, by.as_deref()),
                        "as_of": as_of, "day_setting": settings.label,
                        "coverage": coverage, "cells": p.cells,
                    }))
                }
                Request::AnalysisWhy { metric, day } => {
                    let def = metric_defs(&[metric])?[0];
                    let text =
                        analysis::why_not(&mut conn, &settings, def, analysis::parse_day(&day)?)
                            .await?;
                    Ok(json!({ "text": text }))
                }
                Request::AnalysisResults { limit } => {
                    let limit = limit.map_or(20, |l| i64::try_from(l).unwrap_or(i64::MAX));
                    let rows = sinex_store::analysis::recent_results(&mut conn, None, limit)
                        .await
                        .map_err(|e| e.to_string())?;
                    let results: Vec<Value> = rows
                        .into_iter()
                        .map(|(id, recipe, status, state, at)| {
                            json!({"result": id, "recipe": recipe, "status": status,
                                   "state": state,
                                   "created_at": at.format(&time::format_description::well_known::Rfc3339).unwrap_or_default()})
                        })
                        .collect();
                    Ok(json!({ "results": results }))
                }
                Request::AnalysisShow { result } => {
                    let id: uuid::Uuid = result
                        .parse()
                        .map_err(|e| format!("`{result}` is not a result ID: {e}"))?;
                    let view = sinex_store::analysis::result(&mut conn, id)
                        .await
                        .map_err(|e| e.to_string())?
                        .ok_or_else(|| format!("no result {id}"))?;
                    let findings: Vec<Value> = view
                        .findings
                        .iter()
                        .filter_map(analysis::stored_finding)
                        .map(|f| json!({"text": f.markdown(), "finding": f}))
                        .collect();
                    Ok(json!({
                        "result": view.id, "recipe": view.recipe, "as_of": view.as_of,
                        "status": view.status, "reason": view.reason, "state": view.state,
                        "state_reason": view.state_reason, "inputs_read": view.inputs_read,
                        "tables": view.tables, "findings": findings,
                    }))
                }
                Request::Brief { day, budget } => {
                    let day = match day {
                        Some(d) => analysis::parse_day(&d)?,
                        None => settings
                            .civil
                            .day(sinex_model::Timestamp::now())
                            .map_err(|e| e.to_string())?,
                    };
                    let budget = budget.map_or(4000, |b| usize::try_from(b).unwrap_or(usize::MAX));
                    let p = analysis::brief(&mut conn, &settings, day, budget).await?;
                    Ok(json!({"text": p.markdown, "presentation": p}))
                }
                _ => Err("not an analysis call".into()),
            }
        }
        .await;
        match outcome {
            Ok(v) => data(v),
            Err(e) => refused(e),
        }
    }
}

/// The metrics named by `ids`, or every shipped metric when none is named.
fn metric_defs(ids: &[String]) -> Result<Vec<&'static sinex_analysis::MetricDef>, String> {
    if ids.is_empty() {
        return Ok(sinex_analysis::catalog::METRICS.iter().collect());
    }
    ids.iter()
        .map(|id| {
            sinex_analysis::catalog::metric(id).ok_or_else(|| {
                let known: Vec<&str> = sinex_analysis::catalog::METRICS
                    .iter()
                    .map(|m| m.id)
                    .collect();
                format!("no metric {id}; known: {}", known.join(", "))
            })
        })
        .collect()
}
