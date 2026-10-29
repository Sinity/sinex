//! Questions are admitted once; this keyed derivation maintains their complete
//! answer set as figures and source observations change.
use super::{
    Settings,
    panel::Panel,
    recipes::{self, Recipe},
};
use serde_json::{Value, json};
use sinex_analysis::Eligibility;
use sinex_analysis::coverage::Coverage;
use sinex_analysis::panel::{self, DayFigure};
use sinex_derivations::derived_event::derived;
use sinex_derivations::figures::{METRICS, Metric};
use sinex_derivations::{AffectedRange, Computed, Env, InputQuery, KeyState, KeyView, Keyed, Kind};
use sinex_model::Event;
use sinex_model::event_types::analysis::*;
use sinex_model::event_types::coverage::{CoverageState, SourceCoverage};
use sinex_model::event_types::work::ContextPack;
use sinex_model::time_settings::TimeSettings;

pub(crate) struct StoredProducts;
impl Keyed for StoredProducts {
    const NAME: &'static str = "analysis.products";
    const VERSION: u32 = 1;
    const INPUTS: &'static [&'static str] =
        &["analysis.request", "metric.figure", "source.coverage"];
    const OUTPUTS: &'static [&'static str] = &[
        "analysis.panel",
        "recipe.answer",
        "analysis.finding",
        "context.pack",
    ];
    fn version(&self) -> u32 {
        let mut hash = blake3::Hasher::new();
        hash.update(&Self::VERSION.to_le_bytes());
        for (name, version, _) in sinex_analysis::methods::catalog() {
            hash.update(name.as_bytes());
            hash.update(&version.to_le_bytes());
        }
        for recipe in Recipe::ALL {
            hash.update(recipe.id().as_bytes());
            hash.update(&recipe.version().to_le_bytes());
        }
        1 + (u32::from_le_bytes(
            hash.finalize().as_bytes()[..4]
                .try_into()
                .expect("four bytes"),
        ) >> 2)
    }
    fn keys_for(&self, input: &KeyView<'_>, _: &Env) -> Vec<Value> {
        if input.event_type == "analysis.request" && input.payload["operation"] != "recall" {
            vec![input.payload.clone()]
        } else {
            vec![]
        }
    }
    fn inputs(&self, key: &Value, _: &Env) -> Option<InputQuery> {
        let request: AnalysisRequest = serde_json::from_value(key.clone()).ok()?;
        let metrics = metrics(&request);
        let mut kinds = vec![Kind {
            event_type: "analysis.request".into(),
            equal: vec![("request".into(), json!(request.request))],
        }];
        let from = super::parse_day(&request.from).ok()?;
        let to = super::parse_day(&request.to).ok()?;
        let settings = settings(&request).ok()?;
        let days = panel::days(&settings.time_settings, from, to).ok()?;
        for metric in &metrics {
            for day in &days {
                kinds.push(Kind {
                    event_type: "metric.figure".into(),
                    equal: vec![
                        ("grain".into(), json!("day")),
                        ("metric".into(), json!(metric.name)),
                        ("day".into(), json!(day.label)),
                    ],
                });
            }
        }
        let mut sources: Vec<_> = metrics
            .iter()
            .flat_map(|m| m.sources.iter().copied())
            .collect();
        sources.sort_unstable();
        sources.dedup();
        for source in sources {
            kinds.push(Kind {
                event_type: "source.coverage".into(),
                equal: vec![("observed_source".into(), json!(source))],
            });
        }
        // Reader observations can arrive long after the historical records
        // they describe. Their acquisition time cannot clip a record window.
        Some(InputQuery::all(kinds))
    }
    fn affected_ranges(&self, input: &KeyView<'_>, _: &Env) -> Vec<AffectedRange> {
        let Some(start) = input.ts_orig else {
            return vec![];
        };
        let end = input
            .payload
            .get("period_end")
            .or_else(|| input.payload.get("ended_at"))
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or(start);
        vec![AffectedRange {
            open_field: None,
            output_type: "analysis.panel",
            start_field: "started_at",
            end_field: "ended_at",
            start,
            end,
        }]
    }
    fn nearby_inputs(&self, input: &KeyView<'_>, _: &Env) -> Option<InputQuery> {
        if input.event_type == "analysis.request" {
            return None;
        }
        // All stored questions are eligible for new acquisition evidence;
        // compute selects exact periods and sources, without a history cap.
        Some(InputQuery::all(vec![Kind::all("analysis.request")]))
    }
    fn refresh_at(&self, outputs: &[Event], env: &Env) -> Option<sinex_model::Timestamp> {
        outputs
            .iter()
            .filter(|event| event.event_type == "analysis.panel")
            .filter_map(|event| {
                event.payload.get("ended_at").and_then(|value| {
                    serde_json::from_value::<sinex_model::Timestamp>(value.clone()).ok()
                })
            })
            .filter(|end| *end > env.now)
            .min()
    }
    fn key_state(&self, _: &Value, _: &Env) -> KeyState {
        KeyState::Closed
    }
    fn compute(&self, key: &Value, inputs: &[Event], env: &Env) -> Computed {
        match compute(key, inputs, env) {
            Ok(outputs) => Computed {
                outputs,
                ..Default::default()
            },
            Err(error) => Computed {
                error: Some(sinex_model::BuildError::Payload(
                    "analysis.request".into(),
                    error,
                )),
                ..Default::default()
            },
        }
    }
}

fn metrics(request: &AnalysisRequest) -> Vec<&'static Metric> {
    METRICS
        .iter()
        .filter(|m| request.metrics.is_empty() || request.metrics.iter().any(|name| name == m.name))
        .collect()
}
fn visible(event: &Event, request: &AnalysisRequest) -> bool {
    match event.class {
        sinex_model::Class::Open => true,
        sinex_model::Class::Guarded => request.grants.iter().any(|g| g == "guarded"),
        sinex_model::Class::Sealed => false,
    }
}
fn settings(request: &AnalysisRequest) -> Result<Settings, String> {
    let start = request
        .day_start
        .parse()
        .map_err(|e| format!("day start: {e}"))?;
    let zone = jiff::tz::TimeZone::get(&request.zone).map_err(|e| e.to_string())?;
    let time_settings = TimeSettings::from_parts(zone, start);
    Ok(Settings {
        label: time_settings.label(),
        time_settings,
    })
}
fn coverage(inputs: &[&Event]) -> Vec<Coverage> {
    let mut rows: Vec<_> = inputs
        .iter()
        .filter_map(|event| {
            sinex_analysis::coverage::from_observation(&event.payload_as::<SourceCoverage>()?)
        })
        .collect();
    rows.sort_by(|a, b| {
        (
            &a.source,
            a.span.start,
            a.span.end,
            a.completeness,
            &a.bounded_by,
        )
            .cmp(&(
                &b.source,
                b.span.start,
                b.span.end,
                b.completeness,
                &b.bounded_by,
            ))
    });
    rows.dedup();
    rows
}

fn panel(
    request: &AnalysisRequest,
    settings: &Settings,
    inputs: &[&Event],
    from: jiff::civil::Date,
    to: jiff::civil::Date,
) -> Result<Panel, String> {
    let units = panel::days(&settings.time_settings, from, to).map_err(|e| e.to_string())?;
    let bounds: std::collections::BTreeMap<_, _> = units
        .iter()
        .map(|unit| {
            let day = super::parse_day(&unit.label)?;
            Ok((
                unit.label.clone(),
                (
                    settings
                        .time_settings
                        .day_start(day)
                        .map_err(|error| error.to_string())?,
                    settings
                        .time_settings
                        .day_end(day)
                        .map_err(|error| error.to_string())?,
                ),
            ))
        })
        .collect::<Result<_, String>>()?;
    let figures: Vec<_> = inputs
        .iter()
        .filter(|e| e.event_type == "metric.figure" && e.payload["grain"] == "day")
        .filter(|event| {
            event.payload["day"]
                .as_str()
                .and_then(|day| bounds.get(day))
                .is_some_and(|(start, end)| {
                    event.ts_orig.at == Some(*start)
                        && event.payload.get("period_end").and_then(|value| {
                            serde_json::from_value::<sinex_model::Timestamp>(value.clone()).ok()
                        }) == Some(*end)
                })
        })
        .filter_map(|event| {
            Some(DayFigure {
                metric: event.payload["metric"].as_str()?.into(),
                day: event.payload["day"].as_str()?.into(),
                dimension: event.payload["dimension"].as_str()?.into(),
                dimension_value: event.payload["dimension_value"].as_str()?.into(),
                value: event.payload["value"].as_f64(),
                numerator: event.payload["numerator"].as_f64(),
                denominator: event.payload["denominator"].as_f64(),
                rows: event.payload["rows"].as_u64()?,
                reference: format!("sinex:ev/{}", event.id),
            })
        })
        .collect();
    let coverage = coverage(inputs);
    let mut cells: Vec<_> = metrics(request)
        .into_iter()
        .flat_map(|m| panel::cells(m, &units, &figures, &coverage))
        .collect();
    let compatible: std::collections::HashSet<_> = figures
        .iter()
        .map(|figure| figure.reference.as_str())
        .collect();
    let incompatible: std::collections::BTreeSet<_> = inputs
        .iter()
        .filter(|event| {
            event.event_type == "metric.figure"
                && event.payload["grain"] == "day"
                && !compatible.contains(format!("sinex:ev/{}", event.id).as_str())
        })
        .filter_map(|event| {
            Some((
                event.payload["metric"].as_str()?,
                event.payload["day"].as_str()?,
            ))
        })
        .collect();
    for cell in &mut cells {
        if cell.reference.is_none()
            && incompatible.contains(&(cell.metric, cell.unit.label.as_str()))
        {
            cell.cell = sinex_analysis::Cell::Missing {
                reason: sinex_analysis::cell::Missing::IncomparablePeriod,
            };
        }
    }
    let mut evidence: Vec<_> = inputs
        .iter()
        .filter(|event| event.event_type == "source.coverage")
        .map(
            |event| json!({"citation":format!("sinex:ev/{}",event.id),"observation":event.payload}),
        )
        .collect();
    evidence.sort_by(|a, b| a["citation"].as_str().cmp(&b["citation"].as_str()));
    Ok(Panel {
        citation: None,
        producer_versions: None,
        evidence,
        units,
        cells,
        coverage,
        as_of: 0,
    })
}
fn compute(key: &Value, inputs: &[Event], env: &Env) -> Result<Vec<Event>, String> {
    let request: AnalysisRequest =
        serde_json::from_value(key.clone()).map_err(|e| e.to_string())?;
    let Some(request_event) = inputs
        .iter()
        .find(|e| e.event_type == "analysis.request" && e.payload["request"] == request.request)
    else {
        return Ok(vec![]);
    };
    let settings = settings(&request)?;
    let from = super::parse_day(&request.from)?;
    let to = super::parse_day(&request.to)?;
    let requested_days: std::collections::BTreeSet<_> =
        panel::days(&settings.time_settings, from, to)
            .map_err(|error| error.to_string())?
            .into_iter()
            .map(|day| day.label)
            .collect();
    let visible: Vec<_> = inputs
        .iter()
        .filter(|event| visible(event, &request))
        .collect();
    let mut latest = std::collections::BTreeMap::new();
    for event in visible
        .iter()
        .copied()
        .filter(|e| e.event_type == "source.coverage")
    {
        let Some(row) = event.payload_as::<SourceCoverage>() else {
            continue;
        };
        let order = (row.ended_at, row.started_at, row.observation);
        let entry = latest
            .entry((row.observed_source, row.binding, row.host))
            .or_insert((order.clone(), event.id));
        if order > entry.0 {
            *entry = (order, event.id);
        }
    }
    let selected: Vec<_> = visible
        .into_iter()
        .filter(|event| match event.event_type.as_str() {
            "metric.figure" => event.payload["day"]
                .as_str()
                .is_some_and(|day| requested_days.contains(day)),
            "source.coverage" => event.payload_as::<SourceCoverage>().is_some_and(|row| {
                latest
                    .get(&(
                        row.observed_source.clone(),
                        row.binding.clone(),
                        row.host.clone(),
                    ))
                    .is_some_and(|(_, id)| *id == event.id)
                    || (matches!(row.state, CoverageState::Observed | CoverageState::Partial)
                        && row.started_at < request.ended_at
                        && row.ended_at > request.started_at)
            }),
            _ => true,
        })
        .collect();
    let mut parents = selected.clone();
    if !parents.iter().any(|e| e.id == request_event.id) {
        parents.push(request_event);
    }
    let p = panel(&request, &settings, &selected, from, to)?;
    let coverage_evidence = p.evidence.clone();
    let evidence: Vec<String> = parents
        .iter()
        .map(|event| format!("sinex:ev/{}", event.id))
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();
    let mut outputs = vec![derived(
        &AnalysisPanel {
            producer_versions: None,
            coverage_evidence: json!(coverage_evidence),
            read_as: None,
            request: request.request.clone(),
            started_at: request.started_at,
            ended_at: request.ended_at,
            day_setting: settings.label.clone(),
            units: json!(p.units),
            cells: json!(p.cells),
            coverage: json!(p.coverage),
        },
        StoredProducts::NAME,
        parents.iter().copied(),
    )];
    if let Some(recipe) = &request.recipe {
        let definition = Recipe::parse(recipe)?;
        let mut result = recipes::evaluate(
            &p,
            definition,
            Eligibility {
                min_observed_fraction: request.min_observed_fraction,
                admit_recorded: false,
            },
        )?;
        for finding in &mut result.findings {
            if finding.evidence.is_empty() {
                finding.evidence = evidence.clone();
            }
        }
        outputs.push(derived(
            &RecipeAnswer {
                recipe_version: definition.version(),
                method_versions: json!(sinex_analysis::methods::catalog().into_iter().filter(|(name, _, _)| definition.methods().contains(name)).map(|(name, version, _)| (name, version)).collect::<std::collections::BTreeMap<_, _>>()),
                parameters: json!({"from":request.from,"to":request.to,"metrics":request.metrics,"min_observed_fraction":request.min_observed_fraction,"admit_recorded":false}),
                day_setting: settings.label.clone(),
                coverage_hash: blake3::hash(serde_json::to_string(&coverage_evidence).expect("coverage serializes").as_bytes()).to_hex().to_string(),
                producer_versions: None,
                coverage_evidence: json!(coverage_evidence),
                read_as: None,
                request: request.request.clone(),
                started_at: request.started_at,
                ended_at: request.ended_at,
                recipe: recipe.clone(),
                tables: result.tables,
                findings: json!(result.findings),
                coverage: json!(p.coverage),
            },
            StoredProducts::NAME,
            parents.iter().copied(),
        ));
        for (index, finding) in result.findings.into_iter().enumerate() {
            outputs.push(derived(
                &Finding {
                    producer_versions: None,
                    read_as: None,
                    request: request.request.clone(),
                    index: index as u64,
                    started_at: request.started_at,
                    finding: json!(finding),
                },
                StoredProducts::NAME,
                parents.iter().copied(),
            ));
        }
    }
    if request.operation == "brief" {
        let today = panel(&request, &settings, &selected, to, to)?;
        let day_end = settings
            .time_settings
            .day_end(to)
            .map_err(|e| e.to_string())?;
        let mut sections = super::brief::sections(&today, &p, &settings, to, env.now < day_end)?;
        for finding in sections
            .iter_mut()
            .flat_map(|section| &mut section.findings)
        {
            if finding.evidence.is_empty() {
                finding.evidence = evidence.clone();
            }
        }
        for (index, finding) in sections
            .iter()
            .flat_map(|section| &section.findings)
            .enumerate()
        {
            outputs.push(derived(
                &Finding {
                    producer_versions: None,
                    read_as: None,
                    request: request.request.clone(),
                    index: index as u64,
                    started_at: request.started_at,
                    finding: json!(finding),
                },
                StoredProducts::NAME,
                parents.iter().copied(),
            ));
        }
        outputs.push(derived(&ContextPack { evaluated_at: Some(env.now), day_setting: Some(settings.label.clone()), producer_versions: None, read_as: None,
            analysis: Some(json!({"request": request.request, "day": request.to, "sections": sections, "panel": {"cells": today.cells, "coverage": today.coverage}, "coverage_evidence": coverage_evidence})),
            pack: format!("query:{}", request.request), episode: String::new(), at: request.started_at,
            stated_goal: None, trail: vec![], last_edit: None, last_edit_path: None, open_failures: vec![],
            coverage: parents.iter().filter(|e| e.event_type == "source.coverage").map(|e| format!("sinex:ev/{}", e.id)).collect::<std::collections::BTreeSet<_>>().into_iter().collect(), profile: "analysis/v1".into(),
        }, StoredProducts::NAME, parents.iter().copied()));
    }
    Ok(outputs)
}

#[cfg(test)]
mod tests;
