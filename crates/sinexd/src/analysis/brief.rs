//! The daily brief and the account of why one cell is what it is.
use super::Settings;
use super::panel::{panel, span_text};
use super::recipes::{correlations, deviations, input, tracked};
use sinex_analysis::Eligibility;
use sinex_analysis::SpanSet;
use sinex_analysis::coverage::{Completeness, Coverage};
use sinex_analysis::finding::{self, Caveat, Finding, Template};
use sinex_analysis::methods::correlation::family;
use sinex_analysis::panel::PanelCell;
use sinex_analysis::presentation::{self, Context as Presented, Presentation, Section};
use sinex_derivations::figures::{METRICS, Metric};
use sinex_model::query::Context;
use sqlx::PgConnection;

/// Days before the brief's day the departures and associations look back.
const HISTORY_DAYS: i64 = 56;

/// The daily brief for one logical day: data quality first, then what the
/// day's metrics show, departures from each tracked metric's recent level,
/// the associations among the tracked metrics over the days before, and the
/// machine.
pub(crate) async fn brief(
    conn: &mut PgConnection,
    ctx: &Context,
    settings: &Settings,
    day: jiff::civil::Date,
    budget: usize,
) -> Result<Presentation, String> {
    let metrics: Vec<&'static Metric> = METRICS.iter().collect();
    let today = panel(conn, ctx, settings, &metrics, day, day).await?;
    let day_span = settings.day_span(day, day)?;
    let provisional = settings.today()? <= day;
    let label = day.to_string();

    let mut sources: Vec<&str> = METRICS
        .iter()
        .flat_map(|m| m.sources.iter().copied())
        .collect();
    sources.sort_unstable();
    sources.dedup();
    let mut quality = Vec::new();
    for source in &sources {
        let rows: Vec<&Coverage> = today
            .coverage
            .iter()
            .filter(|c| c.source == *source)
            .collect();
        let covered = SpanSet::from_spans(rows.iter().map(|c| c.span)).clip(day_span);
        let fraction = covered.len() as f64 / day_span.len().max(1) as f64;
        let first = rows.first();
        quality.push(Finding {
            template: Template::Coverage {
                source: (*source).to_owned(),
                unit: label.clone(),
                observed_fraction: (fraction * 1000.0).round() / 1000.0,
                completeness: first
                    .map(|c| completeness_name(c.completeness))
                    .unwrap_or_else(|| "no records".into()),
                bounded_by: first
                    .map(|c| c.bounded_by.clone())
                    .unwrap_or_else(|| "nothing stored".into()),
            },
            caveats: if provisional {
                vec![Caveat::Provisional]
            } else {
                vec![]
            },
            evidence: vec![],
        });
    }

    let mut activity = Vec::new();
    let mut machine = Vec::new();
    for m in &metrics {
        let mine: Vec<&PanelCell> = today.cells.iter().filter(|c| c.metric == m.name).collect();
        let mut findings: Vec<Finding> = mine
            .iter()
            .filter(|c| c.dimension.is_empty())
            .map(|c| finding::from_cell(c, m.description, m.sources, provisional))
            .collect();
        // The three largest values of each breakdown.
        for dim in m.dimensions {
            let mut parts: Vec<&&PanelCell> = mine
                .iter()
                .filter(|c| c.dimension == *dim && c.cell.value().is_some_and(|v| *v > 0.0))
                .collect();
            parts.sort_by(|a, b| {
                b.cell
                    .value()
                    .unwrap_or(&0.0)
                    .total_cmp(a.cell.value().unwrap_or(&0.0))
            });
            for c in parts.into_iter().take(3) {
                let description = format!("{}, {dim} {}", m.description, c.dimension_value);
                findings.push(finding::from_cell(c, &description, m.sources, provisional));
            }
        }
        if m.name.starts_with("machine.") {
            machine.extend(findings);
        } else {
            activity.extend(findings);
        }
    }

    // Departures and associations: the tracked metrics over the days before.
    let window_start = day
        .checked_sub(jiff::Span::new().days(HISTORY_DAYS))
        .map_err(|e| e.to_string())?;
    let history = panel(conn, ctx, settings, &tracked(), window_start, day).await?;
    let series = input(&history, Eligibility::default());
    let names: Vec<&str> = tracked().iter().map(|m| m.name).collect();
    let (_, flagged) = deviations(&series, &names)?;
    let departures: Vec<Finding> = flagged
        .into_iter()
        .filter(|f| matches!(&f.template, Template::Deviation { unit, .. } if *unit == label))
        .collect();
    let (_, associations) = correlations(&series, &family(&names))?;

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
            title: format!("Associations over the {HISTORY_DAYS} days before"),
            findings: associations,
            absent: None,
        },
        Section {
            title: "Machine".into(),
            findings: machine,
            absent: None,
        },
    ];
    let context = Presented {
        title: format!(
            "Brief for {day}{}",
            if provisional {
                " (the day is not over)"
            } else {
                ""
            }
        ),
        evaluated_at: ctx.now.to_string(),
        as_of: today.as_of,
        day_setting: settings.label.clone(),
    };
    Ok(presentation::compile(context, sections, budget))
}

fn completeness_name(c: Completeness) -> String {
    serde_json::to_value(c)
        .ok()
        .and_then(|v| v.as_str().map(str::to_owned))
        .unwrap_or_default()
}

/// Why a metric's cell for a day is what it is: its state and the coverage
/// each of its sources had.
pub(crate) async fn why_not(
    conn: &mut PgConnection,
    ctx: &Context,
    settings: &Settings,
    metric: &'static Metric,
    day: jiff::civil::Date,
) -> Result<String, String> {
    let p = panel(conn, ctx, settings, &[metric], day, day).await?;
    let span = settings.day_span(day, day)?;
    let mut out = String::new();
    for c in p.cells.iter().filter(|c| c.dimension.is_empty()) {
        out.push_str(&format!(
            "{} on {}: {} ({} events counted{})\n",
            metric.name,
            c.unit.label,
            serde_json::to_string(&c.cell).unwrap_or_default(),
            c.rows,
            c.reference
                .as_deref()
                .map(|r| format!(", {r}"))
                .unwrap_or_default()
        ));
    }
    out.push_str(&format!(
        "observed while any of: {}\n",
        metric.sources.join(", ")
    ));
    for s in metric.sources {
        let mine: Vec<&Coverage> = p.coverage.iter().filter(|o| o.source == *s).collect();
        if mine.is_empty() {
            out.push_str(&format!("  {s}: nothing stored, so no coverage anywhere\n"));
        }
        for o in mine {
            let inside = o.span.intersect(&span).map_or(0, |x| x.len());
            let (from, to) = span_text(o.span);
            out.push_str(&format!(
                "  {s}: {from} .. {to} ({}, {}) covers {:.0}% of the day\n",
                o.bounded_by,
                completeness_name(o.completeness),
                100.0 * inside as f64 / span.len().max(1) as f64
            ));
        }
    }
    Ok(out)
}
