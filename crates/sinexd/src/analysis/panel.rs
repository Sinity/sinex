//! Panels: the daily figures of some metrics, read through the query language
//! with the caller's grants, the coverage of their sources at the same commit
//! position, and the cells built from the two.
use super::{Settings, micros, rfc3339};
use serde_json::Value;
use sinex_analysis::coverage::{self, Coverage, CoverageEvidence};
use sinex_analysis::methods::Point;
use sinex_analysis::panel::{self, DayFigure, PanelCell, Unit};
use sinex_analysis::{Cell, Span};
use sinex_derivations::figures::Metric;
use sinex_model::Timestamp;
use sinex_model::query::Context;
use sinex_store::query::Page;
use sqlx::PgConnection;

/// Rows per query page while reading figures.
const FIGURES_PER_PAGE: u64 = 5_000;

/// The coverage of `sources`, as of a commit position.
async fn coverage(
    conn: &mut PgConnection,
    sources: &[&str],
    as_of: i64,
) -> Result<Vec<Coverage>, String> {
    let ns = |t: time::OffsetDateTime| micros(Timestamp(t));
    let mut out = Vec::new();
    for source in sources {
        let e = sinex_store::coverage::source_evidence(conn, source, as_of)
            .await
            .map_err(|e| e.to_string())?;
        let run_end = |r: &sinex_store::coverage::RunSpan| {
            ns(r.ended_at.or(r.last_record).unwrap_or(r.started_at))
        };
        let evidence = CoverageEvidence {
            records: e.records.map(|(a, b)| Span::new(ns(a), ns(b))),
            last_read: e.runs.iter().map(run_end).max(),
            runs: e
                .runs
                .iter()
                .map(|r| Span::new(ns(r.started_at), run_end(r)))
                .collect(),
        };
        out.extend(coverage::coverage(
            source,
            coverage::capture_mode(source),
            coverage::completeness(source),
            &evidence,
        ));
    }
    Ok(out)
}

/// The query that reads the daily figures of `metrics` on `days`.
fn figures_query(metrics: &[&Metric], days: &[Unit]) -> String {
    let list = |items: Vec<String>| items.join(", ");
    format!(
        "figures where grain = \"day\" and metric = [{}] and day = [{}] \
         fields id, metric, day, dimension, dimension_value, value, numerator, denominator, rows",
        list(
            metrics
                .iter()
                .map(|m| serde_json::to_string(m.name).expect("strings serialize"))
                .collect()
        ),
        list(
            days.iter()
                .map(|d| serde_json::to_string(&d.label).expect("strings serialize"))
                .collect()
        ),
    )
}

fn figure(row: &Value) -> Option<DayFigure> {
    let text = |f: &str| row.get(f).and_then(Value::as_str).map(str::to_owned);
    let number = |f: &str| row.get(f).and_then(Value::as_f64);
    Some(DayFigure {
        metric: text("metric")?,
        day: text("day")?,
        dimension: text("dimension")?,
        dimension_value: text("dimension_value")?,
        value: number("value"),
        numerator: number("numerator"),
        denominator: number("denominator"),
        rows: number("rows").map_or(0, |r| r as u64),
        reference: format!("sinex:ev/{}", text("id")?),
    })
}

/// The daily figures of `metrics` on `days`, and the commit position they
/// were read at.
async fn figures(
    conn: &mut PgConnection,
    ctx: &Context,
    metrics: &[&Metric],
    days: &[Unit],
) -> Result<(Vec<DayFigure>, i64), String> {
    let text = figures_query(metrics, days);
    let query = sinex_model::query::bind(&text, ctx).map_err(|e| e.to_string())?;
    let mut page = Page {
        size: Some(FIGURES_PER_PAGE),
        cursor: None,
    };
    let mut out = Vec::new();
    loop {
        let answer = sinex_store::query::run(conn, &text, &query, ctx, &page)
            .await
            .map_err(|e| e.to_string())?;
        out.extend(answer.rows.iter().filter_map(figure));
        match answer.page.cursor {
            Some(cursor) => page.cursor = Some(cursor),
            None => return Ok((out, answer.resolved.commit_seq)),
        }
    }
}

/// A computed panel: its days, its cells, the coverage behind them and the
/// commit position everything was read at.
pub(crate) struct Panel {
    pub units: Vec<Unit>,
    pub cells: Vec<PanelCell>,
    pub coverage: Vec<Coverage>,
    pub as_of: i64,
}

impl Panel {
    /// The total series of one metric, in day order.
    pub(crate) fn series(&self, metric: &str) -> Vec<Point> {
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

/// The cells of `metrics` over the logical days `from..=to`.
pub(crate) async fn panel(
    conn: &mut PgConnection,
    ctx: &Context,
    settings: &Settings,
    metrics: &[&'static Metric],
    from: jiff::civil::Date,
    to: jiff::civil::Date,
) -> Result<Panel, String> {
    let units = panel::days(&settings.civil, from, to).map_err(|e| e.to_string())?;
    let (figures, as_of) = figures(conn, ctx, metrics, &units).await?;
    let mut sources: Vec<&str> = metrics
        .iter()
        .flat_map(|m| m.sources.iter().copied())
        .collect();
    sources.sort_unstable();
    sources.dedup();
    let coverage = coverage(conn, &sources, as_of).await?;
    let cells = metrics
        .iter()
        .flat_map(|m| panel::cells(m, &units, &figures, &coverage))
        .collect();
    Ok(Panel {
        units,
        cells,
        coverage,
        as_of,
    })
}

fn cell_text(c: &Cell<f64>) -> String {
    match c {
        Cell::Observed { value } => number(*value),
        Cell::Partial {
            value,
            observed_fraction,
        } => format!("{}~{:.0}%", number(*value), observed_fraction * 100.0),
        Cell::Recorded { value } => format!(">={}", number(*value)),
        Cell::Missing { reason } => format!(
            "({})",
            serde_json::to_value(reason)
                .ok()
                .and_then(|v| v.as_str().map(str::to_owned))
                .unwrap_or_default()
        ),
    }
}

/// A panel as a table: one row per day, one column per metric, or one line
/// per breakdown value.
pub(crate) fn table(p: &Panel, metrics: &[&Metric], by: Option<&str>) -> String {
    let mut out = String::new();
    match by {
        None => {
            out.push_str(&format!("{:<12}", "day"));
            for m in metrics {
                out.push_str(&format!(" {:>24}", m.name));
            }
            out.push('\n');
            for u in &p.units {
                out.push_str(&format!("{:<12}", u.label));
                for m in metrics {
                    let c = p
                        .cells
                        .iter()
                        .find(|c| c.metric == m.name && c.dimension.is_empty() && c.unit == *u);
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

fn number(v: f64) -> String {
    if v.fract() == 0.0 {
        format!("{}", v as i64)
    } else {
        format!("{v:.2}")
    }
}

/// The span of a coverage row, as text.
pub(super) fn span_text(span: Span) -> (String, String) {
    (rfc3339(span.start), rfc3339(span.end))
}
