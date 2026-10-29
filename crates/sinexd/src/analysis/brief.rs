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

pub(crate) async fn render_pack(
    conn: &mut PgConnection,
    reference: &str,
    budget: usize,
) -> Result<(Presentation, String), String> {
    let pack = super::access::load_pack(conn, reference)
        .await?
        .ok_or("no visible stored context pack for this reference")?;
    compile_pack(pack, budget)
}

fn compile_pack(
    pack: super::access::StoredPack,
    budget: usize,
) -> Result<(Presentation, String), String> {
    let content = &pack.payload["analysis"];
    let sections: Vec<Section> = if content.is_object() {
        serde_json::from_value(content["sections"].clone()).map_err(|error| error.to_string())?
    } else {
        resume_sections(&pack.payload)?
    };
    let context = Presented {
        citation: Some(pack.citation.clone()),
        producer_versions: pack.payload.get("producer_versions").cloned(),
        title: content["title"]
            .as_str()
            .map(str::to_owned)
            .unwrap_or_else(|| {
                content["day"].as_str().map_or_else(
                    || {
                        pack.payload["episode"]
                            .as_str()
                            .filter(|episode| !episode.is_empty())
                            .map_or_else(
                                || "Recall".into(),
                                |episode| format!("Resume card for {episode}"),
                            )
                    },
                    |day| format!("Brief for {day}"),
                )
            }),
        evaluated_at: pack.payload["evaluated_at"]
            .as_str()
            .unwrap_or("evaluation time not recorded")
            .into(),
        as_of: pack.payload["read_as"]
            .as_i64()
            .ok_or("stored context pack has no selected read context")?,
        day_setting: pack.payload["day_setting"]
            .as_str()
            .ok_or("stored context pack has no logical-day setting")?
            .into(),
    };
    Ok((
        presentation::compile(context, sections, budget),
        pack.citation,
    ))
}

fn resume_sections(payload: &serde_json::Value) -> Result<Vec<Section>, String> {
    use sinex_model::event_types::{activity::ActivityEvidence, work::ContextPack};
    let pack: ContextPack =
        serde_json::from_value(payload.clone()).map_err(|error| error.to_string())?;
    let reference = |evidence: &ActivityEvidence, description: String| Finding {
        template: Template::Observation {
            event_type: evidence.event_type.clone(),
            source: evidence.source.clone(),
            when: None,
            description,
        },
        caveats: Vec::new(),
        evidence: vec![format!("sinex:ev/{}", evidence.event)],
    };
    let goal = pack
        .stated_goal
        .as_ref()
        .map(|evidence| reference(evidence, "Stated goal reference".into()))
        .into_iter()
        .collect();
    let trail = pack
        .trail
        .iter()
        .enumerate()
        .map(|(index, evidence)| reference(evidence, format!("Recorded work act {}", index + 1)))
        .collect();
    let mut machine: Vec<_> = pack
        .last_edit
        .as_ref()
        .map(|evidence| {
            reference(
                evidence,
                pack.last_edit_path.as_ref().map_or_else(
                    || "Last edit reference; path not recorded".into(),
                    |path| format!("Last recorded edit: {path}"),
                ),
            )
        })
        .into_iter()
        .collect();
    machine.extend(
        pack.open_failures
            .iter()
            .map(|evidence| reference(evidence, "Unresolved recorded check failure".into())),
    );
    let quality = pack
        .coverage
        .iter()
        .map(|description| Finding {
            template: Template::Observation {
                event_type: "context.pack".into(),
                source: "context.packs".into(),
                when: None,
                description: description.clone(),
            },
            caveats: Vec::new(),
            evidence: Vec::new(),
        })
        .collect();
    Ok(vec![
        Section {
            title: "Data quality".into(),
            findings: quality,
            absent: None,
        },
        Section {
            title: "Resume card".into(),
            findings: goal,
            absent: pack
                .stated_goal
                .is_none()
                .then(|| "stated goal was not recorded".into()),
        },
        Section {
            title: "Trail".into(),
            findings: trail,
            absent: pack
                .trail
                .is_empty()
                .then(|| "work acts were not recorded".into()),
        },
        Section {
            title: "Machine".into(),
            absent: machine
                .is_empty()
                .then(|| "edit and unresolved-check references were not recorded".into()),
            findings: machine,
        },
    ])
}

pub(super) fn sections(
    today: &super::panel::Panel,
    history: &super::panel::Panel,
    settings: &Settings,
    day: jiff::civil::Date,
    provisional: bool,
) -> Result<Vec<Section>, String> {
    let metrics: Vec<&'static Metric> = METRICS.iter().collect();
    let day_span = settings.day_span(day, day)?;
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
        let covered = SpanSet::from_spans(
            rows.iter()
                .filter(|c| c.completeness != Completeness::Unknown)
                .map(|c| c.span),
        )
        .clip(day_span);
        let fraction = covered.len() as f64 / day_span.len().max(1) as f64;
        let first = rows.first();
        quality.push(Finding {
            template: Template::Coverage {
                source: (*source).to_owned(),
                unit: label.clone(),
                observed_fraction: fraction,
                completeness: first
                    .map(|c| completeness_name(c.completeness))
                    .unwrap_or_else(|| "coverage not established".into()),
                bounded_by: first
                    .map(|c| c.bounded_by.clone())
                    .unwrap_or_else(|| "no declared observation interval".into()),
            },
            caveats: if provisional {
                vec![Caveat::Provisional]
            } else {
                vec![]
            },
            evidence: vec![],
        });
    }

    let mut read_outcomes = source_read_findings(today, settings)?;
    read_outcomes.extend(quality);
    let quality = read_outcomes;

    let mut activity = Vec::new();
    let mut machine = Vec::new();
    for m in &metrics {
        let mine: Vec<&PanelCell> = today.cells.iter().filter(|c| c.metric == m.name).collect();
        let mut findings: Vec<Finding> = mine
            .iter()
            .filter(|c| c.dimension.is_empty())
            .map(|c| finding::from_cell(c, m.description, m.sources, provisional))
            .collect();
        // Presentation budgets account for omitted findings after storage.
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
            for c in parts {
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
    Ok(sections)
}

fn source_read_findings(
    panel: &super::panel::Panel,
    settings: &Settings,
) -> Result<Vec<Finding>, String> {
    use sinex_model::event_types::coverage::{CoverageState, SourceCoverage};
    let mut observations = panel
        .evidence
        .iter()
        .map(|evidence| {
            let row: SourceCoverage = serde_json::from_value(evidence["observation"].clone())
                .map_err(|error| error.to_string())?;
            let citation = evidence["citation"]
                .as_str()
                .ok_or("coverage evidence has no citation")?
                .to_owned();
            Ok((row, citation))
        })
        .collect::<Result<Vec<_>, String>>()?;
    let rank = |state: CoverageState| match state {
        CoverageState::Missing => 0,
        CoverageState::Partial => 1,
        CoverageState::RecordedUnknownCoverage => 2,
        CoverageState::Observed => 3,
    };
    observations.sort_by(|(a, _), (b, _)| {
        (
            rank(a.state),
            &a.observed_source,
            &a.binding,
            &a.host,
            a.ended_at,
            &a.observation,
        )
            .cmp(&(
                rank(b.state),
                &b.observed_source,
                &b.binding,
                &b.host,
                b.ended_at,
                &b.observation,
            ))
    });
    let zone = jiff::tz::TimeZone::get(settings.time_settings.zone_name())
        .map_err(|error| error.to_string())?;
    observations
        .into_iter()
        .filter(|(row, _)| row.state != CoverageState::Observed)
        .map(|(row, citation)| {
            let state = match row.state {
                CoverageState::Observed => "observed",
                CoverageState::Partial => "partial",
                CoverageState::RecordedUnknownCoverage => "recorded_unknown_coverage",
                CoverageState::Missing => "missing",
            };
            let when = jiff::Timestamp::from_nanosecond(row.ended_at.unix_ns())
                .map_err(|error| error.to_string())?
                .to_zoned(zone.clone())
                .to_string();
            Ok(Finding {
                template: Template::Observation {
                    event_type: "source.coverage".into(),
                    source: row.observed_source.clone(),
                    when: Some(when),
                    description: format!(
                        "{} read outcome {} ({}, {}){}",
                        row.observed_source,
                        state,
                        row.binding,
                        row.host,
                        row.reason
                            .as_ref()
                            .map(|reason| format!(": {reason}"))
                            .unwrap_or_default()
                    ),
                },
                caveats: Vec::new(),
                evidence: vec![citation],
            })
        })
        .collect()
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
            out.push_str(&format!("  {s}: no declared temporal coverage stored\n"));
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
    for evidence in &p.evidence {
        let row = &evidence["observation"];
        out.push_str(&format!(
            "  {}: {}{} ({})\n",
            row["observed_source"].as_str().unwrap_or_default(),
            row["state"].as_str().unwrap_or_default(),
            row["reason"]
                .as_str()
                .map(|reason| format!(": {reason}"))
                .unwrap_or_default(),
            evidence["citation"].as_str().unwrap_or_default()
        ));
    }
    if let Some(citation) = &p.citation {
        out.push_str(&format!("Stored panel: {citation}\n"));
    }
    Ok(out)
}
