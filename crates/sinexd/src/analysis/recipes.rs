//! The shipped recipes, each run over logical days and stored as a result:
//!
//! - `correlations`: the declared family over the deviation-tracked metrics,
//!   with Benjamini-Hochberg q-values (Lynchpin's `lifestyle_correlations`);
//! - `trends`: Mann-Kendall trend and change points per tracked metric
//!   (Lynchpin's `life_phase`, without snapping boundaries to events);
//! - `deviations`: each day against its own prior-only robust baseline
//!   (Lynchpin's `anomaly_crossref`, prior-only instead of window-relative).
use super::cells::{coverage, panel};
use super::{Settings, odt, rfc3339};
use serde_json::{Value, json};
use sinex_analysis::Eligibility;
use sinex_analysis::catalog;
use sinex_analysis::finding::{self, Caveat, Check, Checks, Finding, Role, Support, Template};
use sinex_analysis::methods::fdr::bh_qvalues;
use sinex_analysis::methods::{self, MethodInput};
use sinex_analysis::metric::MetricDef;
use sinex_analysis::request::{Intent, ReadPlan, Request, output_hash, sorted};
use sinex_store::analysis as store;
use sqlx::{Connection, PgConnection};
use time::OffsetDateTime;

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

/// A recipe's outcome as stored.
pub(crate) struct RecipeRun {
    pub result: store::StoredResult,
    pub findings: Vec<Finding>,
    pub tables: Value,
    pub reused: bool,
}

pub(super) fn tracked() -> Vec<&'static MetricDef> {
    catalog::METRICS.iter().filter(|m| m.deviation).collect()
}

/// Run a recipe over the logical days `from..=to` at the current commit
/// position, and store the result (or return the stored one for an
/// identical request).
pub(crate) async fn run(
    conn: &mut PgConnection,
    settings: &Settings,
    recipe: Recipe,
    from: jiff::civil::Date,
    to: jiff::civil::Date,
    eligibility: Eligibility,
    standing: bool,
) -> Result<RecipeRun, String> {
    let started_at = OffsetDateTime::now_utc();
    let as_of = store::head(conn).await.map_err(|e| e.to_string())?;
    let metrics = tracked();
    let range = settings.day_span(from, to)?;
    let family: Vec<(&str, &str, i32)> = if recipe == Recipe::Correlations {
        catalog::correlation_family()
    } else {
        Vec::new()
    };
    let request = Request {
        recipe: recipe.id().to_owned(),
        recipe_version: 1,
        params: json!({"from": from.to_string(), "to": to.to_string(), "eligibility": eligibility}),
        methods: match recipe {
            Recipe::Correlations => vec![("lagged_correlation".into(), 1)],
            Recipe::Trends => vec![("trend".into(), 1), ("change_points".into(), 1)],
            Recipe::Deviations => vec![("anomaly".into(), 1)],
        },
        plans: metrics
            .iter()
            .map(|m| ReadPlan {
                relation: m.relation.to_owned(),
                from: rfc3339(range.start),
                to: rfc3339(range.end),
            })
            .collect(),
        as_of,
        day_setting: settings.label.clone(),
        family: (!family.is_empty())
            .then(|| family.iter().map(|(x, y, l)| json!([x, y, l])).collect()),
        intent: Intent::Exploratory,
    };
    let request_id = request.id();
    if let Some(id) = store::current_result(conn, &request_id)
        .await
        .map_err(|e| e.to_string())?
    {
        let view = store::result(conn, id)
            .await
            .map_err(|e| e.to_string())?
            .ok_or("result vanished")?;
        let findings = view.findings.iter().filter_map(stored_finding).collect();
        return Ok(RecipeRun {
            result: store::StoredResult {
                id,
                state: view.state,
            },
            findings,
            tables: view.tables.unwrap_or(Value::Null),
            reused: true,
        });
    }
    let covered = coverage(conn, &catalog::coverage_sources(), as_of).await?;
    let p = panel(conn, settings, &metrics, range, as_of, &covered).await?;
    let mut input = MethodInput {
        eligibility,
        ..Default::default()
    };
    for m in &metrics {
        input.series.insert(m.id.to_owned(), p.series(m.id));
    }
    let result_ref = request.id_hex();
    let (tables, findings, any_estimated) = match recipe {
        Recipe::Correlations => correlations(&input, &family, &result_ref)?,
        Recipe::Trends => trends(&input, &metrics, &result_ref)?,
        Recipe::Deviations => deviations(&input, &metrics, &result_ref)?,
    };
    let tables = sorted(tables);
    let stored = store::NewResult {
        request_id,
        request_body: serde_json::to_value(&request).map_err(|e| e.to_string())?,
        recipe: recipe.id().to_owned(),
        recipe_version: 1,
        intent: "exploratory",
        family: request.family.clone().map(Value::Array),
        as_of,
        standing: standing.then(|| recipe.id().to_owned()),
        requested_by: json!({"kind": "operator", "via": "analysis-run"}),
        producer: format!("sinexd {}", env!("CARGO_PKG_VERSION")),
        started_at,
        status: if any_estimated {
            "estimated"
        } else {
            "not_estimable"
        },
        reason: (!any_estimated)
            .then(|| "no method in the recipe could estimate over this range".into()),
        inputs_read: serde_json::to_value(&p.inputs_read).map_err(|e| e.to_string())?,
        tables: tables.clone(),
        diagnostics: json!({"units": p.units.len(), "coverage_rows": covered.len()}),
        output_hash: output_hash(&tables),
        dependencies: metrics
            .iter()
            .map(|m| (m.relation.to_owned(), odt(range.start), odt(range.end)))
            .collect(),
        findings: findings.iter().map(new_finding).collect(),
    };
    let mut tx = conn.begin().await.map_err(|e| e.to_string())?;
    let result = store::store_result(&mut tx, &stored)
        .await
        .map_err(|e| e.to_string())?;
    tx.commit().await.map_err(|e| e.to_string())?;
    Ok(RecipeRun {
        result,
        findings,
        tables,
        reused: false,
    })
}

type Outputs = (Value, Vec<Finding>, bool);

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

fn correlations(
    input: &MethodInput,
    family: &[(&str, &str, i32)],
    result_ref: &str,
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
            && let Some(f) = finding::from_correlation(m, Some(*q), family.len(), result_ref)
        {
            findings.push(f);
        }
    }
    let any = results.iter().any(Result::is_ok);
    Ok((
        json!({"family_size": family.len(), "cells": rows}),
        findings,
        any,
    ))
}

fn trends(
    input: &MethodInput,
    metrics: &[&MetricDef],
    result_ref: &str,
) -> Result<Outputs, String> {
    let mut rows = Vec::new();
    let mut findings = Vec::new();
    let mut any = false;
    for m in metrics {
        let t = run_method("trend", json!({"metric": m.id}), input)?;
        let c = run_method("change_points", json!({"metric": m.id}), input)?;
        any |= t.is_ok() || c.is_ok();
        if let Ok(r) = &t
            && let (Some(slope), Some(p)) = (
                r.output["sen_slope_per_unit"].as_f64(),
                r.output["p"].as_f64(),
            )
            && p < 0.05
        {
            findings.push(statistic(
                Template::Trend {
                    metric: m.id.to_owned(),
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
                },
                result_ref,
            ));
        }
        if let Ok(r) = &c {
            for cp in r.output["change_points"].as_array().into_iter().flatten() {
                findings.push(statistic(
                    Template::LevelShift {
                        metric: m.id.to_owned(),
                        boundary: cp["unit"].as_str().unwrap_or_default().to_owned(),
                        before: cp["before_mean"].as_f64().unwrap_or(f64::NAN),
                        after: cp["after_mean"].as_f64().unwrap_or(f64::NAN),
                        method: "binary segmentation, BIC".into(),
                    },
                    result_ref,
                ));
            }
        }
        rows.push(
            json!({"metric": m.id, "trend": method_row(&t), "change_points": method_row(&c)}),
        );
    }
    Ok((json!({"metrics": rows}), findings, any))
}

pub(super) fn deviations(
    input: &MethodInput,
    metrics: &[&MetricDef],
    result_ref: &str,
) -> Result<Outputs, String> {
    let mut rows = Vec::new();
    let mut findings = Vec::new();
    let mut any = false;
    for m in metrics {
        let r = run_method("anomaly", json!({"metric": m.id}), input)?;
        any |= r.is_ok();
        if let Ok(res) = &r {
            for u in res.output["units"]
                .as_array()
                .into_iter()
                .flatten()
                .filter(|u| u["flagged"] == true)
            {
                findings.push(statistic(
                    Template::Deviation {
                        metric: m.id.to_owned(),
                        unit: u["unit"].as_str().unwrap_or_default().to_owned(),
                        value: u["value"].as_f64().unwrap_or(f64::NAN),
                        median: u["median"].as_f64().unwrap_or(f64::NAN),
                        distance: u["distance"].as_f64().unwrap_or(f64::NAN),
                    },
                    result_ref,
                ));
            }
        }
        rows.push(json!({"metric": m.id, "anomaly": method_row(&r)}));
    }
    Ok((json!({"metrics": rows}), findings, any))
}

fn statistic(template: Template, result_ref: &str) -> Finding {
    Finding {
        template,
        role: Role::DerivedStatistic,
        status: Support::Supported,
        caveats: vec![Caveat::Exploratory],
        evidence: vec![format!("sinex:result/{result_ref}")],
        checks: Checks {
            references: Check::Passed,
            calculation: Check::Passed,
            coverage: Check::Passed,
            semantics: Check::ByConstruction,
        },
    }
}

fn new_finding(f: &Finding) -> store::NewFinding {
    let template = serde_json::to_value(&f.template).unwrap_or(Value::Null);
    store::NewFinding {
        template: template["template"].as_str().unwrap_or_default().to_owned(),
        slots: template,
        role: serde_json::to_value(f.role)
            .ok()
            .and_then(|v| v.as_str().map(str::to_owned))
            .unwrap_or_default(),
        status: serde_json::to_value(f.status)
            .ok()
            .and_then(|v| v.as_str().map(str::to_owned))
            .unwrap_or_default(),
        caveats: serde_json::to_value(&f.caveats).unwrap_or(Value::Null),
        evidence: serde_json::to_value(&f.evidence).unwrap_or(Value::Null),
        checks: serde_json::to_value(f.checks).unwrap_or(Value::Null),
    }
}

/// A finding read back from its stored row.
pub(crate) fn stored_finding(row: &Value) -> Option<Finding> {
    Some(Finding {
        template: serde_json::from_value(row["slots"].clone()).ok()?,
        role: serde_json::from_value(row["role"].clone()).ok()?,
        status: serde_json::from_value(row["status"].clone()).ok()?,
        caveats: serde_json::from_value(row["caveats"].clone()).ok()?,
        evidence: serde_json::from_value(row["evidence"].clone()).ok()?,
        checks: serde_json::from_value(row["checks"].clone()).ok()?,
    })
}
