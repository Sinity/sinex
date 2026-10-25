//! Coverage, panels and the maintained metric values: the cells of each
//! metric per logical day, kept current as admission marks days dirty.
use super::{MICROS_PER_DAY, Settings, micros, odt, parse_day, rfc3339};
use serde_json::Value;
use sinex_analysis::catalog;
use sinex_analysis::coverage::{self, Coverage, CoverageEvidence};
use sinex_analysis::methods::Point;
use sinex_analysis::metric::{MetricDef, Row, UnitGrain};
use sinex_analysis::panel::{self, PanelCell, Unit};
use sinex_analysis::request::{InputsRead, ReadPlan};
use sinex_analysis::{Cell, Span};
use sinex_store::analysis as store;
use sqlx::{Connection, PgConnection};
use std::collections::BTreeMap;

/// The coverage of `sources`, as of a commit position.
pub(crate) async fn coverage(
    conn: &mut PgConnection,
    sources: &[&str],
    as_of: i64,
) -> Result<Vec<Coverage>, String> {
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
        let evidence = CoverageEvidence {
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
        out.extend(coverage::coverage(
            source,
            catalog::capture_mode(source),
            catalog::completeness(source),
            &evidence,
        ));
    }
    Ok(out)
}

/// A computed panel: its units, its cells, and the inputs it read.
pub(crate) struct Panel {
    pub units: Vec<Unit>,
    pub cells: Vec<PanelCell>,
    pub inputs_read: Vec<InputsRead>,
    pub range: Span,
}

impl Panel {
    /// The total series of one metric, in unit order.
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

/// Compute `metrics` over the logical days of `range` from the events live at
/// `as_of`, with the given coverage.
pub(crate) async fn panel(
    conn: &mut PgConnection,
    settings: &Settings,
    metrics: &[&MetricDef],
    range: Span,
    as_of: i64,
    coverage: &[Coverage],
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
            inputs_read: Vec::new(),
            range,
        });
    };
    let mut cells = Vec::new();
    let mut inputs_read = Vec::new();
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
            coverage,
            &Default::default(),
        ));
        let plan = ReadPlan {
            relation: def.relation.to_owned(),
            from: rfc3339(span.start - lookback),
            to: rfc3339(span.end),
        };
        let relied_on = coverage
            .iter()
            .filter(|c| def.coverage.sources().contains(&c.source.as_str()))
            .cloned()
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
        inputs_read.push(InputsRead::new(&plan, as_of, ids, relied_on));
    }
    Ok(Panel {
        units,
        cells,
        inputs_read,
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
pub(crate) async fn keep(
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
pub(crate) fn dirty_name(metric: &str) -> String {
    format!("metric:{metric}")
}

/// Recompute the maintained cells of every day admission marked dirty, plus
/// the trailing `recent_days` (more coverage, such as a poll that found
/// nothing new, also changes a day's state). Returns the days recomputed.
pub(crate) async fn maintain(
    conn: &mut PgConnection,
    settings: &Settings,
    recent_days: i64,
) -> Result<usize, String> {
    let names: Vec<String> = catalog::METRICS.iter().map(|m| dirty_name(m.id)).collect();
    let dirty = sinex_store::derivations::dirty(conn, &names, 10_000)
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
    let covered = coverage(conn, &catalog::coverage_sources(), as_of).await?;
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
            &covered,
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
pub(crate) struct MetricValues {
    pub settings: Settings,
    pub recent_days: i64,
    pub recent_every: std::time::Duration,
    last_recent: Option<std::time::Instant>,
}

impl MetricValues {
    pub(crate) fn new(
        settings: Settings,
        recent_days: i64,
        recent_every: std::time::Duration,
    ) -> Self {
        Self {
            settings,
            recent_days,
            recent_every,
            last_recent: None,
        }
    }
}

impl crate::derivation_runner::Maintained for MetricValues {
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

/// Print a panel as a table: one row per day, one column per metric.
pub(crate) fn table(p: &Panel, metrics: &[&MetricDef], by: Option<&str>) -> String {
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
