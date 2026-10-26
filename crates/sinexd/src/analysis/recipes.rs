//! The shipped recipes, each run over logical days when asked for:
//!
//! - `correlations`: the declared family over the tracked metrics, with
//!   Benjamini-Hochberg q-values (Lynchpin's `lifestyle_correlations`);
//! - `trends`: Mann-Kendall trend and change points per tracked metric
//!   (Lynchpin's `life_phase`, without snapping boundaries to events);
//! - `deviations`: each day against its own prior-only robust baseline
//!   (Lynchpin's `anomaly_crossref`, prior-only instead of window-relative).
//!
//! A recipe reads the panel at one commit position, so the same question
//! asked again at the same position gives the same answer.
use super::Settings;
use super::panel::{Panel, panel};
use serde_json::{Value, json};
use sinex_analysis::Eligibility;
use sinex_analysis::finding::{self, Caveat, Finding, Template};
use sinex_analysis::methods::correlation::family;
use sinex_analysis::methods::fdr::bh_qvalues;
use sinex_analysis::methods::{self, MethodInput};
use sinex_derivations::figures::{METRICS, Metric};
use sinex_model::query::Context;
use sqlx::PgConnection;

/// A shipped recipe.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Recipe {
    Correlations,
    Trends,
    Deviations,
}

impl Recipe {
    pub(crate) const ALL: [Recipe; 3] = [Recipe::Correlations, Recipe::Trends, Recipe::Deviations];

    pub(crate) fn parse(name: &str) -> Result<Self, String> {
        Self::ALL
            .into_iter()
            .find(|r| r.id() == name)
            .ok_or_else(|| format!("no recipe {name}; recipes: correlations, trends, deviations"))
    }

    pub(crate) fn id(self) -> &'static str {
        match self {
            Recipe::Correlations => "correlations",
            Recipe::Trends => "trends",
            Recipe::Deviations => "deviations",
        }
    }
}

/// A recipe's answer: what each method estimated or why it refused, the
/// findings worth reading, and the commit position it was read at.
pub(crate) struct RecipeRun {
    pub tables: Value,
    pub findings: Vec<Finding>,
    pub as_of: i64,
}

pub(super) fn tracked() -> Vec<&'static Metric> {
    METRICS.iter().filter(|m| m.tracked).collect()
}

/// The tracked metrics' series from a panel, for the methods.
pub(super) fn input(p: &Panel, eligibility: Eligibility) -> MethodInput {
    let mut input = MethodInput {
        eligibility,
        ..Default::default()
    };
    for m in tracked() {
        input.series.insert(m.name.to_owned(), p.series(m.name));
    }
    input
}

/// Run a recipe over the logical days `from..=to`.
pub(crate) async fn run(
    conn: &mut PgConnection,
    ctx: &Context,
    settings: &Settings,
    recipe: Recipe,
    from: jiff::civil::Date,
    to: jiff::civil::Date,
    eligibility: Eligibility,
) -> Result<RecipeRun, String> {
    let p = panel(conn, ctx, settings, &tracked(), from, to).await?;
    let input = input(&p, eligibility);
    let names: Vec<&str> = tracked().iter().map(|m| m.name).collect();
    let (tables, findings) = match recipe {
        Recipe::Correlations => correlations(&input, &family(&names))?,
        Recipe::Trends => trends(&input, &names)?,
        Recipe::Deviations => deviations(&input, &names)?,
    };
    Ok(RecipeRun {
        tables,
        findings,
        as_of: p.as_of,
    })
}

type Outputs = (Value, Vec<Finding>);

fn method_row(r: &Result<methods::MethodResult, methods::NotEstimable>) -> Value {
    match r {
        Ok(m) => json!({"status": "estimated", "method": m.method, "output": m.output, "n": m.n,
                        "n_excluded": m.n_excluded, "diagnostics": m.diagnostics, "estimand": m.estimand}),
        Err(e) => {
            json!({"status": "not_estimable", "reason": e.reason, "n": e.n, "n_excluded": e.n_excluded})
        }
    }
}

fn run_method(
    id: &str,
    params: Value,
    input: &MethodInput,
) -> Result<Result<methods::MethodResult, methods::NotEstimable>, String> {
    methods::run(id, &params, input).map_err(|e| e.to_string())
}

pub(super) fn correlations(
    input: &MethodInput,
    family: &[(&str, &str, i32)],
) -> Result<Outputs, String> {
    let results: Vec<_> = family
        .iter()
        .map(|(x, y, lag)| {
            run_method(
                "lagged_correlation",
                json!({"x": x, "y": y, "lag": lag}),
                input,
            )
        })
        .collect::<Result<_, _>>()?;
    let p: Vec<Option<f64>> = results
        .iter()
        .map(|r| r.as_ref().ok().and_then(|m| m.output["p"].as_f64()))
        .collect();
    let q = bh_qvalues(&p);
    let mut findings = Vec::new();
    let mut rows = Vec::new();
    for ((cell, r), q) in family.iter().zip(&results).zip(&q) {
        let mut row = method_row(r);
        row["cell"] = json!(cell);
        row["q"] = json!(q);
        rows.push(row);
        if let (Ok(m), Some(q)) = (r, q)
            && *q <= 0.1
            && let Some(f) = finding::from_correlation(m, Some(*q), family.len())
        {
            findings.push(f);
        }
    }
    Ok((
        json!({"family_size": family.len(), "cells": rows}),
        findings,
    ))
}

fn trends(input: &MethodInput, metrics: &[&str]) -> Result<Outputs, String> {
    let mut rows = Vec::new();
    let mut findings = Vec::new();
    for m in metrics {
        let t = run_method("trend", json!({"metric": m}), input)?;
        let c = run_method("change_points", json!({"metric": m}), input)?;
        if let Ok(r) = &t
            && let (Some(slope), Some(p)) = (
                r.output["sen_slope_per_unit"].as_f64(),
                r.output["p"].as_f64(),
            )
            && p < 0.05
        {
            findings.push(statistic(Template::Trend {
                metric: (*m).to_owned(),
                slope_per_unit: slope,
                p,
                from: r.output["first_unit"]
                    .as_str()
                    .unwrap_or_default()
                    .to_owned(),
                to: r.output["last_unit"]
                    .as_str()
                    .unwrap_or_default()
                    .to_owned(),
                units: r.n,
            }));
        }
        if let Ok(r) = &c {
            for cp in r.output["change_points"].as_array().into_iter().flatten() {
                findings.push(statistic(Template::LevelShift {
                    metric: (*m).to_owned(),
                    boundary: cp["unit"].as_str().unwrap_or_default().to_owned(),
                    before: cp["before_mean"].as_f64().unwrap_or(f64::NAN),
                    after: cp["after_mean"].as_f64().unwrap_or(f64::NAN),
                    method: "binary segmentation, BIC".into(),
                }));
            }
        }
        rows.push(json!({"metric": m, "trend": method_row(&t), "change_points": method_row(&c)}));
    }
    Ok((json!({"metrics": rows}), findings))
}

pub(super) fn deviations(input: &MethodInput, metrics: &[&str]) -> Result<Outputs, String> {
    let mut rows = Vec::new();
    let mut findings = Vec::new();
    for m in metrics {
        let r = run_method("anomaly", json!({"metric": m}), input)?;
        if let Ok(res) = &r {
            for u in res.output["units"]
                .as_array()
                .into_iter()
                .flatten()
                .filter(|u| u["flagged"] == true)
            {
                findings.push(statistic(Template::Deviation {
                    metric: (*m).to_owned(),
                    unit: u["unit"].as_str().unwrap_or_default().to_owned(),
                    value: u["value"].as_f64().unwrap_or(f64::NAN),
                    median: u["median"].as_f64().unwrap_or(f64::NAN),
                    distance: u["distance"].as_f64().unwrap_or(f64::NAN),
                }));
            }
        }
        rows.push(json!({"metric": m, "anomaly": method_row(&r)}));
    }
    Ok((json!({"metrics": rows}), findings))
}

fn statistic(template: Template) -> Finding {
    Finding {
        template,
        caveats: vec![Caveat::Exploratory],
        evidence: Vec::new(),
    }
}
