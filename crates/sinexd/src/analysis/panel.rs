//! Panels: the daily figures of some metrics, read through the query language
//! with the caller's grants, the coverage of their sources at the same commit
//! position, and the cells built from the two.
use super::{Settings, rfc3339};
use serde_json::Value;
use sinex_analysis::coverage::{self, Coverage};
use sinex_analysis::methods::Point;
use sinex_analysis::panel::{PanelCell, Unit};
use sinex_analysis::{Cell, Span};
use sinex_derivations::figures::Metric;
use sinex_model::query::Context;
use sqlx::PgConnection;

/// Coverage reads use the admitted source observations that can parent products.
async fn coverage(
    conn: &mut PgConnection,
    sources: &[&str],
    _as_of: i64,
) -> Result<Vec<Coverage>, String> {
    let events = sinex_store::live::of_types(conn, &["source.coverage".into()])
        .await
        .map_err(|e| e.to_string())?;
    let mut out = Vec::new();
    for event in events {
        let Some(payload) = event.payload else {
            continue;
        };
        let Ok(row) =
            serde_json::from_value::<sinex_model::event_types::coverage::SourceCoverage>(payload)
        else {
            continue;
        };
        if sources.contains(&row.observed_source.as_str())
            && let Some(coverage) = coverage::from_observation(&row)
        {
            out.push(coverage);
        }
    }
    Ok(out)
}

/// A computed panel: its days, its cells, the coverage behind them and the
/// commit position everything was read at.
pub(crate) struct Panel {
    pub citation: Option<String>,
    pub producer_versions: Option<Value>,
    pub evidence: Vec<Value>,
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
    let question = super::access::question(
        conn,
        ctx,
        settings,
        "panel",
        None,
        metrics,
        from,
        to,
        Default::default(),
    )
    .await?;
    let payload = super::access::find(conn, "analysis.panel", &question.request)
        .await?
        .ok_or("analysis panel is not materialized; submit its canonical request first")?;
    from_payload(&payload)
}

pub(super) fn from_payload(payload: &Value) -> Result<Panel, String> {
    let decode = |field: &str| {
        payload
            .get(field)
            .cloned()
            .ok_or_else(|| format!("stored panel has no {field}"))
    };
    let units: Vec<Unit> = serde_json::from_value(decode("units")?).map_err(|e| e.to_string())?;
    let coverage: Vec<Coverage> =
        serde_json::from_value(decode("coverage")?).map_err(|e| e.to_string())?;
    let mut cells = Vec::new();
    for row in payload["cells"]
        .as_array()
        .ok_or("stored panel cells are not an array")?
    {
        let metric = row["metric"]
            .as_str()
            .and_then(sinex_derivations::figures::metric)
            .ok_or("stored panel has an unknown metric")?;
        cells.push(PanelCell {
            metric: metric.name,
            unit: serde_json::from_value(row["unit"].clone()).map_err(|e| e.to_string())?,
            dimension: row["dimension"]
                .as_str()
                .ok_or("stored panel dimension is absent")?
                .into(),
            dimension_value: row["dimension_value"]
                .as_str()
                .ok_or("stored panel dimension value is absent")?
                .into(),
            cell: serde_json::from_value(row["cell"].clone()).map_err(|e| e.to_string())?,
            numerator: row["numerator"].as_f64(),
            denominator: row["denominator"].as_f64(),
            rows: row["rows"]
                .as_u64()
                .ok_or("stored panel row count is absent")?,
            observed_seconds: row["observed_seconds"]
                .as_f64()
                .ok_or("stored panel observed seconds are absent")?,
            reference: row["reference"].as_str().map(str::to_owned),
        });
    }
    Ok(Panel {
        citation: payload["_citation"].as_str().map(str::to_owned),
        producer_versions: payload.get("producer_versions").cloned(),
        evidence: payload["coverage_evidence"]
            .as_array()
            .cloned()
            .unwrap_or_default(),
        units,
        coverage,
        cells,
        as_of: payload["read_as"]
            .as_i64()
            .ok_or("stored panel has no selected read context")?,
    })
}

fn cell_text(c: &Cell<f64>) -> String {
    match c {
        Cell::Observed { value } => number(*value),
        Cell::Partial {
            value,
            observed_fraction,
        } => format!("{}~{}", number(*value), coverage::percent_label(*observed_fraction)),
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
    let mut out = p.citation.as_ref().map_or_else(String::new, |citation| format!("Panel {citation}, as of commit {}.\n", p.as_of));
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

/// One stretch a source could not have recorded in.
#[derive(Clone, Debug, PartialEq, serde::Serialize)]
pub(crate) struct Gap {
    pub source: String,
    pub from: String,
    pub to: String,
    /// `gap`: outside every stretch the source covered; `unknown`: Sinex holds
    /// nothing that bounds the source's coverage.
    pub state: &'static str,
}

/// Where each of `sources` (every source with events when `None`) could not
/// have recorded between the logical days `from` and `to`, at the current
/// commit position.
pub(crate) async fn gaps(
    conn: &mut PgConnection,
    settings: &Settings,
    sources: Option<&[String]>,
    from: jiff::civil::Date,
    to: jiff::civil::Date,
) -> Result<Vec<Gap>, String> {
    let range = settings.day_span(from, to)?;
    let as_of = sinex_store::query::current_seq(conn)
        .await
        .map_err(|e| e.to_string())?;
    let names: Vec<String> = match sources {
        Some(s) => s.to_vec(),
        None => sinex_store::coverage::sources(conn)
            .await
            .map_err(|e| e.to_string())?,
    };
    let mut out = Vec::new();
    for name in &names {
        let covered = coverage(conn, &[name.as_str()], as_of).await?;
        let (uncovered, state) = if covered.is_empty() {
            (vec![range], "unknown")
        } else {
            let covered = sinex_analysis::SpanSet::from_spans(
                covered
                    .iter()
                    .filter(|c| c.completeness != coverage::Completeness::Unknown)
                    .map(|c| c.span),
            );
            let all = sinex_analysis::SpanSet::from_spans([range]);
            (all.subtract(&covered).spans().to_vec(), "gap")
        };
        out.extend(uncovered.into_iter().map(|s| Gap {
            source: name.clone(),
            from: rfc3339(s.start),
            to: rfc3339(s.end),
            state,
        }));
    }
    Ok(out)
}
