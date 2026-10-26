//! The analysis commands.
use super::*;

impl Api {
    /// The analysis calls. Each reads the daily metric figures with the
    /// caller's grants and answers at one commit position; findings are
    /// rendered from their slots, so every surface reads the same text.
    pub(super) async fn analysis(&self, caller: &Caller, request: Request) -> Reply {
        use crate::analysis::{self, Recipe};
        use sinex_derivations::figures::{Law, METRICS};
        if let Request::AnalysisCatalog = request {
            let metrics: Vec<Value> = METRICS
                .iter()
                .map(|m| {
                    let counts: Vec<Value> = m
                        .kinds()
                        .iter()
                        .map(|k| {
                            let only: serde_json::Map<String, Value> = k
                                .equal
                                .iter()
                                .map(|(f, v)| ((*f).to_owned(), v.clone()))
                                .collect();
                            json!({"event_type": k.event_type, "only": only})
                        })
                        .collect();
                    let law = match m.law {
                        Law::Count => "count",
                        Law::Seconds { .. } => "seconds",
                        Law::Ratio { .. } => "ratio",
                    };
                    json!({"id": m.name, "description": m.description, "counts": counts,
                           "law": law, "sources": m.sources, "dimensions": m.dimensions,
                           "tracked": m.tracked})
                })
                .collect();
            let methods: Vec<Value> = sinex_analysis::methods::catalog()
                .into_iter()
                .map(|(id, version, summary)| json!({"id": id, "version": version, "summary": summary}))
                .collect();
            let recipes: Vec<&str> = Recipe::ALL.iter().map(|r| r.id()).collect();
            return data(json!({"metrics": metrics, "methods": methods, "recipes": recipes}));
        }
        let settings = analysis::Settings::operator_default();
        let ctx = self.context();
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
                        &ctx,
                        &settings,
                        recipe,
                        analysis::parse_day(&from)?,
                        analysis::parse_day(&to)?,
                        eligibility,
                    )
                    .await?;
                    Ok(json!({
                        "recipe": recipe.id(), "as_of": run.as_of, "day_setting": settings.label,
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
                    let metrics = metrics(&metric.unwrap_or_default())?;
                    let p = analysis::panel(
                        &mut conn,
                        &ctx,
                        &settings,
                        &metrics,
                        analysis::parse_day(&from)?,
                        analysis::parse_day(&to)?,
                    )
                    .await?;
                    Ok(json!({
                        "text": analysis::table(&p, &metrics, by.as_deref()),
                        "as_of": p.as_of, "day_setting": settings.label,
                        "coverage": p.coverage, "cells": p.cells,
                    }))
                }
                Request::AnalysisWhy { metric, day } => {
                    let metric = metrics(&[metric])?[0];
                    let text = analysis::why_not(
                        &mut conn,
                        &ctx,
                        &settings,
                        metric,
                        analysis::parse_day(&day)?,
                    )
                    .await?;
                    Ok(json!({ "text": text }))
                }
                Request::Brief { day, budget } => {
                    let day = match day {
                        Some(d) => analysis::parse_day(&d)?,
                        None => settings.today()?,
                    };
                    let budget = budget.map_or(4000, |b| usize::try_from(b).unwrap_or(usize::MAX));
                    let p = analysis::brief(&mut conn, &ctx, &settings, day, budget).await?;
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

/// The metrics named by `names`, or every metric when none is named.
fn metrics(names: &[String]) -> Result<Vec<&'static sinex_derivations::figures::Metric>, String> {
    use sinex_derivations::figures::{METRICS, metric};
    if names.is_empty() {
        return Ok(METRICS.iter().collect());
    }
    names
        .iter()
        .map(|name| {
            metric(name).ok_or_else(|| {
                let known: Vec<&str> = METRICS.iter().map(|m| m.name).collect();
                format!("no metric {name}; known: {}", known.join(", "))
            })
        })
        .collect()
}
