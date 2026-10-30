//! The analysis commands.
use super::*;

impl Api {
    /// The analysis calls. Each reads the daily metric figures with the
    /// caller's grants and answers at one commit position; findings are
    /// rendered from their slots, so every surface reads the same text.
    pub(super) async fn analysis(&self, caller: &Caller, request: Request) -> Reply {
        use crate::analysis::{self, Recipe};
        use sinex_derivations::figures::{Calculation, METRICS};
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
                    let calculation = match m.calculation {
                        Calculation::Count => "count",
                        Calculation::Seconds { .. } => "seconds",
                        Calculation::Ratio { .. } => "ratio",
                    };
                    json!({"id": m.name, "description": m.description, "counts": counts,
                           "calculation": calculation, "sources": m.sources, "dimensions": m.dimensions,
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
        let ctx = self.context();
        let time_settings =
            sinex_model::time_settings::TimeSettings::from_parts(ctx.zone.clone(), ctx.day_start);
        let settings = analysis::Settings {
            label: time_settings.label(),
            time_settings,
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
                    let question = analysis::question(&mut conn, &ctx, &settings, "recipe", Some(recipe.id()),
                        &METRICS.iter().filter(|m| m.tracked).collect::<Vec<_>>(),
                        analysis::parse_day(&from)?, analysis::parse_day(&to)?, eligibility).await?;
                    analysis::materialize(&mut conn, self.writer.as_ref().ok_or("analysis requests require sinexd run")?, &self.blobs,
                        question, caller.actor, &self.host).await?;
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
                        "tables": run.tables, "definition": run.definition, "producer_versions": run.producer_versions, "citation": run.citation,
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
                    let question = analysis::question(&mut conn, &ctx, &settings, "panel", None, &metrics,
                        analysis::parse_day(&from)?, analysis::parse_day(&to)?, Default::default()).await?;
                    analysis::materialize(&mut conn, self.writer.as_ref().ok_or("analysis requests require sinexd run")?, &self.blobs,
                        question, caller.actor, &self.host).await?;
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
                        "coverage": p.coverage, "coverage_evidence": p.evidence, "cells": p.cells, "producer_versions": p.producer_versions, "citation": p.citation,
                    }))
                }
                Request::Gaps { from, to, source } => {
                    let gaps = analysis::gaps(
                        &mut conn,
                        &settings,
                        source.as_deref(),
                        analysis::parse_day(&from)?,
                        analysis::parse_day(&to)?,
                    )
                    .await?;
                    Ok(json!({ "day_setting": settings.label, "gaps": gaps }))
                }
                Request::AnalysisWhy { metric, day } => {
                    let metric = metrics(&[metric])?[0];
                    let question = analysis::question(&mut conn, &ctx, &settings, "panel", None, &[metric],
                        analysis::parse_day(&day)?, analysis::parse_day(&day)?, Default::default()).await?;
                    analysis::materialize(&mut conn, self.writer.as_ref().ok_or("analysis requests require sinexd run")?, &self.blobs,
                        question, caller.actor, &self.host).await?;
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
                    let budget = budget.map_or(4000, |b| usize::try_from(b).unwrap_or(usize::MAX));
                    if let Some(reference) = day.as_deref().filter(|value| value.starts_with("sinex:ev/") || value.starts_with("query:")) {
                        let (p, citation) = analysis::render_pack(&mut conn, reference, budget).await?;
                        let machine = json!({"context_pack": citation, "read_as": p.context.as_of, "sections": p.sections.iter().filter(|section| section.title == "Machine").collect::<Vec<_>>()});
                        return Ok(json!({"text": p.markdown, "presentation": p, "context_pack": citation, "machine": machine}));
                    }
                    let day = match day {
                        Some(d) => analysis::parse_day(&d)?,
                        None => ctx.logical_date(ctx.now),
                    };
                    let from = day.checked_sub(jiff::Span::new().days(56)).map_err(|e| e.to_string())?;
                    let all_metrics: Vec<_> = METRICS.iter().collect();
                    let question = analysis::question(&mut conn, &ctx, &settings, "brief", None, &all_metrics,
                        from, day, Default::default()).await?;
                    let pack_request = analysis::materialize(&mut conn, self.writer.as_ref().ok_or("analysis requests require sinexd run")?, &self.blobs,
                        question, caller.actor, &self.host).await?;
                    let (p, citation) = analysis::render_pack(&mut conn, &format!("query:{pack_request}"), budget).await?;
                    let machine = json!({"context_pack": citation, "read_as": p.context.as_of, "sections": p.sections.iter().filter(|section| section.title == "Machine").collect::<Vec<_>>()});
                    Ok(json!({"text": p.markdown, "presentation": p, "context_pack": citation, "machine": machine}))
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
