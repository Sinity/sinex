//! The daily brief and the account of why one cell is what it is.
use super::cells::{coverage, panel};
use super::recipes::{Recipe, deviations, stored_finding, tracked};
use super::{Settings, rfc3339};
use sinex_analysis::catalog;
use sinex_analysis::coverage::Coverage;
use sinex_analysis::finding::{self, Caveat, Check, Checks, Finding, Role, Support, Template};
use sinex_analysis::methods::MethodInput;
use sinex_analysis::metric::MetricDef;
use sinex_analysis::panel::PanelCell;
use sinex_analysis::presentation::{self, Context, Presentation, Section};
use sinex_store::analysis as store;
use sqlx::PgConnection;
use time::OffsetDateTime;

/// The daily brief for one logical day: data quality first, then what the day's
/// metrics show, departures from each metric's recent level, the current
/// associations, and the machine.
pub(crate) async fn brief(
    conn: &mut PgConnection,
    settings: &Settings,
    day: jiff::civil::Date,
    budget: usize,
) -> Result<Presentation, String> {
    let as_of = store::head(conn).await.map_err(|e| e.to_string())?;
    let sources = catalog::coverage_sources();
    let source_coverage = coverage(conn, &sources, as_of).await?;
    let day_span = settings.day_span(day, day)?;
    let provisional = settings.today()? <= day;

    let mut quality = Vec::new();
    for source in &sources {
        let covered = sinex_analysis::SpanSet::from_spans(
            source_coverage
                .iter()
                .filter(|o| o.source == *source)
                .map(|o| o.span),
        )
        .clip(day_span);
        let fraction = covered.len() as f64 / day_span.len().max(1) as f64;
        let first = source_coverage.iter().find(|o| o.source == *source);
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
                bounded_by: first
                    .map(|o| o.bounded_by.clone())
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
            evidence: vec![format!("sinex:coverage/{source}/{day}")],
            checks: Checks {
                references: Check::Passed,
                calculation: Check::Passed,
                coverage: Check::Passed,
                semantics: Check::ByConstruction,
            },
        });
    }

    let metrics: Vec<&MetricDef> = catalog::METRICS.iter().collect();
    let today_panel = panel(conn, settings, &metrics, day_span, as_of, &source_coverage).await?;
    let mut activity = Vec::new();
    let mut machine = Vec::new();
    for def in &metrics {
        let totals: Vec<&PanelCell> = today_panel
            .cells
            .iter()
            .filter(|c| c.metric == def.id && c.dimension.is_empty())
            .collect();
        for c in totals {
            let f = finding::from_cell(c, def.description, def.coverage.sources(), provisional);
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
                let f = finding::from_cell(c, &description, def.coverage.sources(), provisional);
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
        &source_coverage,
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
        None => (Vec::new(), Some("no stored correlation result; `sinexctl analysis-run correlations FROM TO --standing` makes one".to_owned())),
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
    Ok(presentation::compile(ctx, sections, budget))
}

/// Why a metric's cell for a day is what it is: its state and the coverage
/// each of its sources had.
pub(crate) async fn why_not(
    conn: &mut PgConnection,
    settings: &Settings,
    def: &MetricDef,
    day: jiff::civil::Date,
) -> Result<String, String> {
    let as_of = store::head(conn).await.map_err(|e| e.to_string())?;
    let sources: Vec<&str> = def.coverage.sources().to_vec();
    let covered = coverage(conn, &sources, as_of).await?;
    let span = settings.day_span(day, day)?;
    let p = panel(conn, settings, &[def], span, as_of, &covered).await?;
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
    let rule = match def.coverage {
        sinex_analysis::metric::CoverageRule::AnyOf(_) => "any of",
        sinex_analysis::metric::CoverageRule::Sources(_) => "all of",
    };
    out.push_str(&format!("observed when {rule}: {}\n", sources.join(", ")));
    for s in &sources {
        let rows: Vec<&Coverage> = covered.iter().filter(|o| o.source == *s).collect();
        if rows.is_empty() {
            out.push_str(&format!("  {s}: nothing stored, so no coverage anywhere\n"));
        }
        for o in rows {
            let inside = o.span.intersect(&span).map_or(0, |x| x.len());
            out.push_str(&format!(
                "  {s}: {} .. {} ({}, {:?}) covers {:.0}% of the day\n",
                rfc3339(o.span.start),
                rfc3339(o.span.end),
                o.bounded_by,
                o.completeness,
                100.0 * inside as f64 / span.len().max(1) as f64
            ));
        }
    }
    Ok(out)
}
