//! The analysis layer in the daemon (architecture §6.5): reads relations and
//! coverage evidence from the store, runs the pure code in `sinex-analysis`,
//! and stores maintained metric values and products. Shipped recipes:
//!
//! - `correlations`: the declared family over the deviation-tracked metrics,
//!   with Benjamini-Hochberg q-values (Lynchpin's `lifestyle_correlations`);
//! - `trends`: Mann-Kendall trend and change points per tracked metric
//!   (Lynchpin's `life_phase`, without snapping boundaries to events);
//! - `deviations`: each day against its own prior-only robust baseline
//!   (Lynchpin's `anomaly_crossref`, prior-only instead of window-relative).
use serde_json::{Value, json};
use sinex_analysis::catalog;
use sinex_analysis::finding::{self, Caveat, Check, Checks, Finding, Role, Support, Template};
use sinex_analysis::methods::fdr::bh_qvalues;
use sinex_analysis::methods::{self, MethodInput, Point};
use sinex_analysis::metric::{MetricDef, Row, UnitGrain};
use sinex_analysis::opportunity::{self, Evidence};
use sinex_analysis::panel::{self, Opportunity, PanelCell, Unit};
use sinex_analysis::present::{self, Context, Presentation, Section};
use sinex_analysis::request::{Coverage, Intent, Plan, Readset, Request, output_hash, sorted};
use sinex_analysis::{Cell, Eligibility, Span};
use sinex_model::Timestamp;
use sinex_model::civil::Civil;
use sinex_store::analysis as store;
use sqlx::{Connection, PgConnection};
use std::collections::BTreeMap;
use time::OffsetDateTime;

const MICROS_PER_DAY: i64 = 86_400_000_000;

/// The day setting every day-grain value is computed under.
#[derive(Clone)]
pub struct Settings {
    pub civil: Civil,
    /// `04:00 Europe/Warsaw`: recorded with every stored value.
    pub label: String,
}

impl Settings {
    pub fn new(zone: &str, day_start: &str) -> Result<Self, String> {
        let (h, m) = day_start
            .split_once(':')
            .and_then(|(h, m)| Some((h.parse::<u8>().ok()?, m.parse::<u8>().ok()?)))
            .ok_or_else(|| format!("day start `{day_start}` is not a clock time like 04:00"))?;
        let civil = Civil::new(zone, h, m).map_err(|e| e.to_string())?;
        Ok(Self {
            civil,
            label: format!("{h:02}:{m:02} {zone}"),
        })
    }

    /// The API's day setting. A zone without an IANA name (a bare offset)
    /// cannot be stored as a setting, so it is refused rather than guessed.
    pub fn from_api(
        zone: &jiff::tz::TimeZone,
        day_start: jiff::civil::Time,
    ) -> Result<Self, String> {
        let name = zone
            .iana_name()
            .ok_or("the daemon's zone has no IANA name; start it with --zone")?;
        Self::new(
            name,
            &format!("{:02}:{:02}", day_start.hour(), day_start.minute()),
        )
    }

    pub fn operator_default() -> Self {
        Self::new("Europe/Warsaw", "04:00").expect("the operator's default day setting is valid")
    }

    fn day_span(
        &self,
        from: jiff::civil::Date,
        to_inclusive: jiff::civil::Date,
    ) -> Result<Span, String> {
        let start = self.civil.day_start(from).map_err(|e| e.to_string())?;
        let end = self
            .civil
            .day_end(to_inclusive)
            .map_err(|e| e.to_string())?;
        Ok(Span::new(micros(start.0), micros(end.0)))
    }

    fn today(&self) -> Result<jiff::civil::Date, String> {
        self.civil.day(Timestamp::now()).map_err(|e| e.to_string())
    }
}

pub fn parse_day(s: &str) -> Result<jiff::civil::Date, String> {
    s.parse()
        .map_err(|e| format!("`{s}` is not a date like 2026-10-18: {e}"))
}

fn micros(t: OffsetDateTime) -> i64 {
    i64::try_from(t.unix_timestamp_nanos() / 1000).unwrap_or(i64::MAX)
}

fn odt(us: i64) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp_nanos(i128::from(us) * 1000)
        .unwrap_or(OffsetDateTime::UNIX_EPOCH)
}

fn rfc3339(us: i64) -> String {
    odt(us)
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_default()
}

/// Observation opportunity for `sources`, as of a commit position.
pub async fn opportunity(
    conn: &mut PgConnection,
    sources: &[&str],
    as_of: i64,
) -> Result<Vec<Opportunity>, String> {
    let mut out = Vec::new();
    for source in sources {
        let e = store::source_evidence(conn, source, as_of)
            .await
            .map_err(|e| e.to_string())?;
        let last_read = e
            .runs
            .iter()
            .map(|r| micros(r.ended_at.or(r.last_record).unwrap_or(r.started_at)))
            .max();
        let evidence = Evidence {
            records: e.records.map(|(a, b)| Span::new(micros(a), micros(b))),
            last_read,
            runs: e
                .runs
                .iter()
                .map(|r| {
                    Span::new(
                        micros(r.started_at),
                        micros(r.ended_at.or(r.last_record).unwrap_or(r.started_at)),
                    )
                })
                .collect(),
        };
        out.extend(opportunity::opportunity(
            source,
            catalog::basis(source),
            catalog::completeness(source),
            &evidence,
        ));
    }
    Ok(out)
}

/// A computed panel: its units, cells, and the readsets it read.
pub struct Panel {
    pub units: Vec<Unit>,
    pub cells: Vec<PanelCell>,
    pub readsets: Vec<Readset>,
    pub range: Span,
}

impl Panel {
    /// The total series of one metric, in unit order.
    pub fn series(&self, metric: &str) -> Vec<Point> {
        self.cells
            .iter()
            .filter(|c| c.metric == metric && c.dimension.is_empty())
            .map(|c| Point {
                unit: c.unit.label.clone(),
                cell: c.cell.clone(),
            })
            .collect()
    }
}

/// Compute `metrics` over the logical days of `range` from the events live at
/// `as_of`, with the given opportunity.
pub async fn panel(
    conn: &mut PgConnection,
    settings: &Settings,
    metrics: &[&MetricDef],
    range: Span,
    as_of: i64,
    opportunity: &[Opportunity],
) -> Result<Panel, String> {
    let units = panel::units(&settings.civil, UnitGrain::Day, range).map_err(|e| e.to_string())?;
    let Some(span) = units
        .first()
        .zip(units.last())
        .map(|(a, b)| Span::new(a.span.start, b.span.end))
    else {
        return Ok(Panel {
            units,
            cells: Vec::new(),
            readsets: Vec::new(),
            range,
        });
    };
    let mut cells = Vec::new();
    let mut readsets = Vec::new();
    let mut fetched: BTreeMap<(&str, bool), Vec<store::EventRow>> = BTreeMap::new();
    // Each relation is read once, with the fields every metric over it needs.
    let mut fields: BTreeMap<&str, Vec<String>> = BTreeMap::new();
    for def in metrics {
        let f = fields.entry(def.relation).or_default();
        f.extend(def.fields().into_iter().map(str::to_owned));
        f.sort_unstable();
        f.dedup();
    }
    for def in metrics {
        // An interval that started the day before can still reach into the range.
        let lookback = if def.interval.is_some() {
            MICROS_PER_DAY
        } else {
            0
        };
        let key = (def.relation, lookback > 0);
        if let std::collections::btree_map::Entry::Vacant(slot) = fetched.entry(key) {
            let rows = store::relation_rows(
                conn,
                def.relation,
                odt(span.start - lookback),
                odt(span.end),
                as_of,
                &fields[def.relation],
            )
            .await
            .map_err(|e| e.to_string())?;
            slot.insert(rows);
        }
        let events = &fetched[&key];
        let rows: Vec<Row<'_>> = events
            .iter()
            .map(|e| Row {
                source: &e.source,
                at: micros(e.ts_orig),
                payload: &e.payload,
            })
            .collect();
        cells.extend(panel::compute(
            def,
            &units,
            &rows,
            opportunity,
            &Default::default(),
        ));
        let plan = Plan {
            relation: def.relation.to_owned(),
            from: rfc3339(span.start - lookback),
            to: rfc3339(span.end),
        };
        let coverage = opportunity
            .iter()
            .filter(|o| def.opportunity.sources().contains(&o.source.as_str()))
            .map(|o| Coverage {
                source: o.source.clone(),
                span: o.span,
                completeness: o.completeness,
            })
            .collect();
        let ids = events
            .iter()
            .filter(|e| {
                def.row_matches(&Row {
                    source: &e.source,
                    at: 0,
                    payload: &e.payload,
                })
            })
            .map(|e| e.id.to_string())
            .collect();
        readsets.push(Readset::new(&plan, as_of, ids, coverage));
    }
    Ok(Panel {
        units,
        cells,
        readsets,
        range: span,
    })
}

fn stored_cells(cells: &[PanelCell]) -> Vec<store::MetricValue> {
    cells
        .iter()
        .map(|c| {
            let (state, reason, value, fraction) = match &c.cell {
                Cell::Observed { value } => ("observed", None, Some(*value), Some(1.0)),
                Cell::Partial {
                    value,
                    observed_fraction,
                } => ("partial", None, Some(*value), Some(*observed_fraction)),
                Cell::Recorded { value } => ("recorded", None, Some(*value), None),
                Cell::Missing { reason } => (
                    c.cell.state_name(),
                    serde_json::to_value(reason)
                        .ok()
                        .and_then(|v| v.as_str().map(str::to_owned)),
                    None,
                    None,
                ),
            };
            store::MetricValue {
                metric: c.metric.to_owned(),
                metric_version: i32::try_from(c.metric_version).unwrap_or(i32::MAX),
                unit_start: odt(c.unit.span.start),
                unit_end: odt(c.unit.span.end),
                unit_label: c.unit.label.clone(),
                dimension: c.dimension.clone(),
                dimension_value: c.dimension_value.clone(),
                state: state.to_owned(),
                reason,
                value,
                observed_fraction: fraction,
                numerator: c.numerator,
                denominator: c.denominator,
                n_rows: i64::try_from(c.n_rows).unwrap_or(i64::MAX),
                observed_s: c.observed_seconds,
            }
        })
        .collect()
}

/// Store a panel's cells as the maintained metric values of its days.
pub async fn keep(
    conn: &mut PgConnection,
    settings: &Settings,
    panel: &Panel,
    as_of: i64,
) -> Result<(), String> {
    let mut tx = conn.begin().await.map_err(|e| e.to_string())?;
    for def in catalog::METRICS {
        let cells: Vec<PanelCell> = panel
            .cells
            .iter()
            .filter(|c| c.metric == def.id)
            .cloned()
            .collect();
        if cells.is_empty() {
            continue;
        }
        let range = store::MetricRange {
            metric: def.id,
            metric_version: i32::try_from(def.version).unwrap_or(i32::MAX),
            day_setting: &settings.label,
            from: odt(panel.range.start),
            to: odt(panel.range.end),
        };
        store::replace_metric_values(&mut tx, &range, &stored_cells(&cells), as_of)
            .await
            .map_err(|e| e.to_string())?;
    }
    tx.commit().await.map_err(|e| e.to_string())
}

/// The dirty-key name admission marks for a metric's changed days.
pub fn dirty_name(metric: &str) -> String {
    format!("metric:{metric}")
}

/// Recompute the maintained cells of every day admission marked dirty, plus
/// the trailing `recent_days` (new opportunity, such as a poll that found
/// nothing new, also changes a day's state). Returns the days recomputed.
pub async fn maintain(
    conn: &mut PgConnection,
    settings: &Settings,
    recent_days: i64,
) -> Result<usize, String> {
    let names: Vec<String> = catalog::METRICS.iter().map(|m| dirty_name(m.id)).collect();
    let dirty = sinex_store::derived::dirty(conn, &names, 10_000)
        .await
        .map_err(|e| e.to_string())?;
    let mut days: BTreeMap<String, Vec<(String, Value, i64)>> = BTreeMap::new();
    for d in dirty {
        if let Some(day) = d.key.get("day").and_then(Value::as_str) {
            days.entry(day.to_owned()).or_default().push((
                d.derivation,
                d.key.clone(),
                d.last_dirty_seq,
            ));
        }
    }
    let today = settings.today()?;
    for back in 0..recent_days {
        let day = today
            .checked_sub(jiff::Span::new().days(back))
            .map_err(|e| e.to_string())?;
        days.entry(day.to_string()).or_default();
    }
    if days.is_empty() {
        return Ok(0);
    }
    let as_of = store::head(conn).await.map_err(|e| e.to_string())?;
    let opp = opportunity(conn, &catalog::opportunity_sources(), as_of).await?;
    let metrics: Vec<&MetricDef> = catalog::METRICS.iter().collect();
    let n = days.len();
    for (day, marks) in days {
        let date = parse_day(&day)?;
        let p = panel(
            conn,
            settings,
            &metrics,
            settings.day_span(date, date)?,
            as_of,
            &opp,
        )
        .await?;
        keep(conn, settings, &p, as_of).await?;
        for (name, key, seq) in marks {
            store::clear_dirty(conn, &name, &key, seq)
                .await
                .map_err(|e| e.to_string())?;
        }
    }
    Ok(n)
}

/// Metric values as a maintained derivation of the driver: the days
/// admission marked dirty whenever it runs, plus the trailing `recent_days`
/// every `recent_every` (a poll that found nothing new still changes a day's
/// observation state).
pub struct MetricValues {
    pub settings: Settings,
    pub recent_days: i64,
    pub recent_every: std::time::Duration,
    last_recent: Option<std::time::Instant>,
}

impl MetricValues {
    pub fn new(settings: Settings, recent_days: i64, recent_every: std::time::Duration) -> Self {
        Self {
            settings,
            recent_days,
            recent_every,
            last_recent: None,
        }
    }
}

impl crate::derive::Maintained for MetricValues {
    fn name(&self) -> &'static str {
        "metric values"
    }

    fn maintain<'a>(
        &'a mut self,
        conn: &'a mut PgConnection,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<usize, String>> + Send + 'a>>
    {
        Box::pin(async move {
            let due = self
                .last_recent
                .is_none_or(|t| t.elapsed() >= self.recent_every);
            let recent = if due { self.recent_days } else { 0 };
            let n = maintain(conn, &self.settings, recent).await?;
            if due {
                self.last_recent = Some(std::time::Instant::now());
            }
            Ok(n)
        })
    }
}

/// A shipped recipe.
#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum Recipe {
    Correlations,
    Trends,
    Deviations,
}

impl Recipe {
    pub fn id(self) -> &'static str {
        match self {
            Recipe::Correlations => "correlations",
            Recipe::Trends => "trends",
            Recipe::Deviations => "deviations",
        }
    }
}

/// A recipe's outcome as stored.
pub struct RecipeRun {
    pub result: store::StoredResult,
    pub findings: Vec<Finding>,
    pub tables: Value,
    pub reused: bool,
}

fn tracked() -> Vec<&'static MetricDef> {
    catalog::METRICS.iter().filter(|m| m.deviation).collect()
}

/// Run a recipe over the logical days `from..=to` at the current commit
/// position, and store the result (or return the stored one for an
/// identical request).
pub async fn run(
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
            .map(|m| Plan {
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
    let opp = opportunity(conn, &catalog::opportunity_sources(), as_of).await?;
    let p = panel(conn, settings, &metrics, range, as_of, &opp).await?;
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
        requested_by: json!({"kind": "operator", "via": "sinexd analysis run"}),
        producer: format!("sinexd {}", env!("CARGO_PKG_VERSION")),
        started_at,
        status: if any_estimated {
            "estimated"
        } else {
            "not_estimable"
        },
        reason: (!any_estimated)
            .then(|| "no method in the recipe could estimate over this range".into()),
        readsets: serde_json::to_value(&p.readsets).map_err(|e| e.to_string())?,
        tables: tables.clone(),
        diagnostics: json!({"units": p.units.len(), "opportunity_rows": opp.len()}),
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

fn deviations(
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
pub fn stored_finding(row: &Value) -> Option<Finding> {
    Some(Finding {
        template: serde_json::from_value(row["slots"].clone()).ok()?,
        role: serde_json::from_value(row["role"].clone()).ok()?,
        status: serde_json::from_value(row["status"].clone()).ok()?,
        caveats: serde_json::from_value(row["caveats"].clone()).ok()?,
        evidence: serde_json::from_value(row["evidence"].clone()).ok()?,
        checks: serde_json::from_value(row["checks"].clone()).ok()?,
    })
}

/// The daily brief for one logical day (`analysis.md` §12): data quality
/// first, then what the day's metrics show, departures from each metric's
/// recent level, the current associations, and the machine.
pub async fn brief(
    conn: &mut PgConnection,
    settings: &Settings,
    day: jiff::civil::Date,
    budget: usize,
) -> Result<Presentation, String> {
    let as_of = store::head(conn).await.map_err(|e| e.to_string())?;
    let sources = catalog::opportunity_sources();
    let opp = opportunity(conn, &sources, as_of).await?;
    let day_span = settings.day_span(day, day)?;
    let provisional = settings.today()? <= day;

    let mut quality = Vec::new();
    for source in &sources {
        let covered = sinex_analysis::SpanSet::from_spans(
            opp.iter().filter(|o| o.source == *source).map(|o| o.span),
        )
        .clip(day_span);
        let fraction = covered.len() as f64 / day_span.len().max(1) as f64;
        let first = opp.iter().find(|o| o.source == *source);
        quality.push(Finding {
            template: Template::Coverage {
                source: (*source).to_owned(),
                unit: day.to_string(),
                observed_fraction: (fraction * 1000.0).round() / 1000.0,
                completeness: first
                    .map(|o| {
                        serde_json::to_value(o.completeness)
                            .ok()
                            .and_then(|v| v.as_str().map(str::to_owned))
                            .unwrap_or_default()
                    })
                    .unwrap_or_else(|| "no records".into()),
                basis: first
                    .map(|o| o.basis.clone())
                    .unwrap_or_else(|| "nothing stored".into()),
            },
            role: Role::MeasuredFact,
            status: if fraction >= 0.999 {
                Support::Supported
            } else if fraction > 0.0 {
                Support::PartiallySupported
            } else {
                Support::Unknown
            },
            caveats: if provisional {
                vec![Caveat::Provisional]
            } else {
                vec![]
            },
            evidence: vec![format!("sinex:opportunity/{source}/{day}")],
            checks: Checks {
                references: Check::Passed,
                calculation: Check::Passed,
                coverage: Check::Passed,
                semantics: Check::ByConstruction,
            },
        });
    }

    let metrics: Vec<&MetricDef> = catalog::METRICS.iter().collect();
    let today_panel = panel(conn, settings, &metrics, day_span, as_of, &opp).await?;
    let mut activity = Vec::new();
    let mut machine = Vec::new();
    for def in &metrics {
        let totals: Vec<&PanelCell> = today_panel
            .cells
            .iter()
            .filter(|c| c.metric == def.id && c.dimension.is_empty())
            .collect();
        for c in totals {
            let f = finding::from_cell(c, def.description, def.opportunity.sources(), provisional);
            if def.relation == "journal.entry" {
                machine.push(f)
            } else {
                activity.push(f)
            }
        }
        // The three largest values of each breakdown.
        for dim in def.dimensions {
            let mut parts: Vec<&PanelCell> = today_panel
                .cells
                .iter()
                .filter(|c| {
                    c.metric == def.id
                        && c.dimension == *dim
                        && c.cell.value().is_some_and(|v| *v > 0.0)
                })
                .collect();
            parts.sort_by(|a, b| {
                b.cell
                    .value()
                    .unwrap_or(&0.0)
                    .total_cmp(a.cell.value().unwrap_or(&0.0))
            });
            for c in parts.into_iter().take(3) {
                let description = format!("{}, {dim} {}", def.description, c.dimension_value);
                let f = finding::from_cell(c, &description, def.opportunity.sources(), provisional);
                if def.relation == "journal.entry" {
                    machine.push(f)
                } else {
                    activity.push(f)
                }
            }
        }
    }

    // Departures: each tracked metric's day against its prior 28 days.
    let window_start = day
        .checked_sub(jiff::Span::new().days(56))
        .map_err(|e| e.to_string())?;
    let tracked = tracked();
    let history = panel(
        conn,
        settings,
        &tracked,
        settings.day_span(window_start, day)?,
        as_of,
        &opp,
    )
    .await?;
    let mut input = MethodInput::default();
    for m in &tracked {
        input.series.insert(m.id.to_owned(), history.series(m.id));
    }
    let (_, flagged, _) = deviations(&input, &tracked, "brief")?;
    let label = day.to_string();
    let departures: Vec<Finding> = flagged
        .into_iter()
        .filter(|f| matches!(&f.template, Template::Deviation { unit, .. } if *unit == label))
        .collect();

    let latest = store::recent_results(conn, Some(Recipe::Correlations.id()), 1)
        .await
        .map_err(|e| e.to_string())?;
    let (associations, absent) = match latest.first() {
        Some((id, _, _, state, _)) => {
            let view = store::result(conn, *id).await.map_err(|e| e.to_string())?.ok_or("result vanished")?;
            let mut fs: Vec<Finding> = view.findings.iter().filter_map(stored_finding).collect();
            if state != "current" {
                for f in &mut fs {
                    f.caveats.push(Caveat::Provisional);
                    f.status = Support::PartiallySupported;
                }
            }
            (fs, None)
        }
        None => (Vec::new(), Some("no stored correlation result; `sinexd analysis run correlations --standing` makes one".to_owned())),
    };

    let sections = vec![
        Section {
            title: "Data quality".into(),
            findings: quality,
            absent: None,
        },
        Section {
            title: "The day".into(),
            findings: activity,
            absent: None,
        },
        Section {
            title: "Departures from the recent level".into(),
            findings: departures,
            absent: None,
        },
        Section {
            title: "Associations".into(),
            findings: associations,
            absent,
        },
        Section {
            title: "Machine".into(),
            findings: machine,
            absent: None,
        },
    ];
    let ctx = Context {
        title: format!(
            "Brief for {day}{}",
            if provisional {
                " (the day is not over)"
            } else {
                ""
            }
        ),
        evaluated_at: OffsetDateTime::now_utc()
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap_or_default(),
        as_of,
        day_setting: settings.label.clone(),
    };
    Ok(present::compile(ctx, sections, budget))
}

/// Why a metric's cell for a day is what it is: its state and the
/// opportunity each of its sources had (`analysis.md` §13.2).
pub async fn why_not(
    conn: &mut PgConnection,
    settings: &Settings,
    def: &MetricDef,
    day: jiff::civil::Date,
) -> Result<String, String> {
    let as_of = store::head(conn).await.map_err(|e| e.to_string())?;
    let sources: Vec<&str> = def.opportunity.sources().to_vec();
    let opp = opportunity(conn, &sources, as_of).await?;
    let span = settings.day_span(day, day)?;
    let p = panel(conn, settings, &[def], span, as_of, &opp).await?;
    let mut out = String::new();
    for c in p.cells.iter().filter(|c| c.dimension.is_empty()) {
        out.push_str(&format!(
            "{} on {}: {} ({} rows)\n",
            def.id,
            c.unit.label,
            serde_json::to_string(&c.cell).unwrap_or_default(),
            c.n_rows
        ));
    }
    let rule = match def.opportunity {
        sinex_analysis::metric::OpportunityRule::AnyOf(_) => "any of",
        sinex_analysis::metric::OpportunityRule::Sources(_) => "all of",
    };
    out.push_str(&format!("observed when {rule}: {}\n", sources.join(", ")));
    for s in &sources {
        let rows: Vec<&Opportunity> = opp.iter().filter(|o| o.source == *s).collect();
        if rows.is_empty() {
            out.push_str(&format!(
                "  {s}: nothing stored, so no opportunity anywhere\n"
            ));
        }
        for o in rows {
            let inside = o.span.intersect(&span).map_or(0, |x| x.len());
            out.push_str(&format!(
                "  {s}: {} .. {} ({}, {:?}) covers {:.0}% of the day\n",
                rfc3339(o.span.start),
                rfc3339(o.span.end),
                o.basis,
                o.completeness,
                100.0 * inside as f64 / span.len().max(1) as f64
            ));
        }
    }
    Ok(out)
}

/// Print a panel as a table: one row per day, one column per metric.
pub fn table(p: &Panel, metrics: &[&MetricDef], by: Option<&str>) -> String {
    let mut out = String::new();
    let cell_text = |c: &Cell<f64>| match c {
        Cell::Observed { value } => fmt_num(*value),
        Cell::Partial {
            value,
            observed_fraction,
        } => format!("{}~{:.0}%", fmt_num(*value), observed_fraction * 100.0),
        Cell::Recorded { value } => format!(">={}", fmt_num(*value)),
        Cell::Missing { reason } => format!(
            "({})",
            serde_json::to_value(reason)
                .ok()
                .and_then(|v| v.as_str().map(str::to_owned))
                .unwrap_or_default()
        ),
    };
    match by {
        None => {
            out.push_str(&format!("{:<12}", "day"));
            for m in metrics {
                out.push_str(&format!(" {:>24}", m.id));
            }
            out.push('\n');
            for u in &p.units {
                out.push_str(&format!("{:<12}", u.label));
                for m in metrics {
                    let c = p
                        .cells
                        .iter()
                        .find(|c| c.metric == m.id && c.dimension.is_empty() && c.unit == *u);
                    out.push_str(&format!(
                        " {:>24}",
                        c.map(|c| cell_text(&c.cell)).unwrap_or_default()
                    ));
                }
                out.push('\n');
            }
        }
        Some(dim) => {
            for c in p.cells.iter().filter(|c| c.dimension == dim) {
                out.push_str(&format!(
                    "{:<12} {:<24} {:<40} {}\n",
                    c.unit.label,
                    c.metric,
                    c.dimension_value,
                    cell_text(&c.cell)
                ));
            }
        }
    }
    out
}

fn fmt_num(v: f64) -> String {
    if v.fract() == 0.0 {
        format!("{}", v as i64)
    } else {
        format!("{v:.2}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn day_settings_parse_and_label() {
        let s = Settings::new("Europe/Warsaw", "04:00").unwrap();
        assert_eq!(s.label, "04:00 Europe/Warsaw");
        assert!(Settings::new("Europe/Warsaw", "4am").is_err());
        assert!(Settings::new("Nowhere/Else", "04:00").is_err());
        let d = parse_day("2026-10-18").unwrap();
        let span = s.day_span(d, d).unwrap();
        assert_eq!(span.len(), MICROS_PER_DAY);
    }
}

#[cfg(test)]
#[path = "analysis_tests.rs"]
mod db_tests;
