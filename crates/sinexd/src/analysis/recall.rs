//! Recall is a maintained query-backed context pack, presented from stored rows.
use serde_json::{Value, json};
use sinex_analysis::finding::{Finding, Template};
use sinex_analysis::presentation::Section;
use sinex_derivations::selected::SelectedInputs;
use sinex_derivations::{AnyKeyed, Computed, Env, InputQuery, KeyState, KeyView, Kind};
use sinex_model::event_types::analysis::{AnalysisRequest, QuerySelection};
use sinex_model::event_types::work::ContextPack;
use sinex_model::{Class, Event, EventId, Origin, ReadingOrder};
use sqlx::PgConnection;
use std::sync::OnceLock;

pub(crate) struct RecallProducts;
fn input_type(kind: &str) -> bool {
    !matches!(
        kind,
        "context.pack" | "analysis.panel" | "recipe.answer" | "analysis.finding"
    )
}
impl AnyKeyed for RecallProducts {
    fn name(&self) -> &'static str {
        "recall.products"
    }
    fn version(&self) -> u32 {
        2
    }
    fn inputs_types(&self) -> &'static [&'static str] {
        static TYPES: OnceLock<Vec<&'static str>> = OnceLock::new();
        TYPES.get_or_init(|| {
            sinex_model::event_types::ALL
                .iter()
                .map(|kind| kind.event_type)
                .filter(|kind| input_type(kind))
                .collect()
        })
    }
    fn output_types(&self) -> &'static [&'static str] {
        &["context.pack"]
    }
    fn reads_type(&self, kind: &str) -> bool {
        input_type(kind)
    }
    fn keys_for(&self, input: &KeyView<'_>, _: &Env) -> Vec<Value> {
        if input.event_type == "analysis.request" && input.payload["operation"] == "recall" {
            input.payload["request"]
                .as_str()
                .map(key)
                .into_iter()
                .collect()
        } else {
            vec![]
        }
    }
    fn inputs(&self, key: &Value, _: &Env) -> Option<InputQuery> {
        Some(InputQuery::all(vec![Kind {
            event_type: "analysis.request".into(),
            equal: vec![("request".into(), key.get("request")?.clone())],
        }]))
    }
    fn selected_query(&self, key: &Value, inputs: &[Event], _: &Env) -> Option<QuerySelection> {
        request_input(key, inputs).ok()?.1.selection
    }
    fn nearby_inputs(&self, input: &KeyView<'_>, _: &Env) -> Option<InputQuery> {
        (input.event_type != "analysis.request").then(|| {
            InputQuery::all(vec![Kind::only(
                "analysis.request",
                &[("operation", "recall")],
            )])
        })
    }
    fn key_state(&self, _: &Value, _: &Env) -> KeyState {
        KeyState::Closed
    }
    fn compute(&self, _: &Value, _: &[Event], _: &Env) -> Computed {
        Computed {
            error: Some(sinex_model::BuildError::Payload(
                "analysis.request".into(),
                "recall requires its selected query inputs".into(),
            )),
            ..Default::default()
        }
    }
    fn compute_selected(
        &self,
        key: &Value,
        inputs: &[Event],
        selected: &SelectedInputs,
        _: &Env,
    ) -> Computed {
        match compute(key, inputs, selected) {
            Ok(event) => Computed {
                outputs: vec![event],
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

fn key(request: &str) -> Value {
    json!({"request":request})
}

fn request_input<'a>(
    key: &Value,
    inputs: &'a [Event],
) -> Result<(&'a Event, AnalysisRequest), String> {
    let id = key["request"]
        .as_str()
        .ok_or("recall key has no canonical request identity")?;
    let event = inputs
        .iter()
        .find(|event| {
            event.event_type == "analysis.request"
                && event.payload["request"].as_str() == Some(id)
                && event.payload["operation"] == "recall"
        })
        .ok_or("recall request event is unavailable")?;
    let request =
        serde_json::from_value(event.payload.clone()).map_err(|error| error.to_string())?;
    Ok((event, request))
}

fn compute(key: &Value, inputs: &[Event], selected: &SelectedInputs) -> Result<Event, String> {
    let (request_event, request) = request_input(key, inputs)?;
    let selection = request
        .selection
        .as_ref()
        .ok_or("recall request has no selected context")?;
    let mut parents: Vec<EventId> = inputs
        .iter()
        .map(|event| event.id)
        .chain(selected.parents.iter().copied())
        .collect();
    parents.sort_unstable();
    parents.dedup();
    if parents.is_empty() {
        return Err("recall has no admitted request parent".into());
    }
    let question = request_event.id;
    let sections = sections(&selected.value, &parents, Some(question));
    let mut current_selection = selection.clone();
    current_selection.selected["_fingerprint"] = json!(selected.signature);
    let payload = ContextPack {
        evaluated_at: Some(selection.now),
        day_setting: Some(format!("{} {}", selection.day_start, selection.zone)),
        producer_versions: None,
        read_as: Some(selected.read_as),
        analysis: Some(
            json!({"kind":"recall", "title":"Recorded context", "answer":selected.value,
            "sections":sections,"input_signature":selected.signature,
            "selected":current_selection}),
        ),
        pack: format!("query:{}", request.request),
        episode: String::new(),
        at: request.started_at,
        stated_goal: None,
        trail: vec![],
        last_edit: None,
        last_edit_path: None,
        open_failures: vec![],
        coverage: vec!["source coverage remains as recorded in the selected answer".into()],
        profile: "recall/v1".into(),
    };
    let value = serde_json::to_value(&payload).map_err(|error| error.to_string())?;
    let hash = sinex_derivations::derived_event::payload_hash(&value, &parents);
    let class = inputs
        .iter()
        .map(|event| event.class)
        .chain([selected.class])
        .max()
        .unwrap_or(Class::Open);
    Event::build(
        &payload,
        "recall.products",
        Origin::Derived { parents },
        ReadingOrder(0),
        hash,
        class,
    )
    .map_err(|error| error.to_string())
}

fn description(row: &Value) -> String {
    let payload = row.get("payload").unwrap_or(row);
    let text = |name| payload.get(name).and_then(Value::as_str);
    let mut fields = Vec::new();
    match row["event_type"].as_str() {
        Some("work.episode") => {
            if let Some(label) = text("label") {
                fields.push(label.to_owned());
            }
            if let Some(reference) = payload
                .pointer("/objective/reference")
                .and_then(Value::as_str)
            {
                fields.push(format!("objective {reference}"));
            }
            if let Some(repository) = payload
                .pointer("/objective/repository")
                .and_then(Value::as_str)
            {
                fields.push(format!("repository {repository}"));
            }
            if let Some(state) = text("state") {
                fields.push(format!("state {state}"));
            }
        }
        Some("work.act") => {
            if let Some(action) = text("action") {
                fields.push(action.to_owned());
            }
            if let Some(label) = text("label") {
                fields.push(label.to_owned());
            }
            if let Some(path) = text("path") {
                fields.push(path.to_owned());
            }
            if let Some(check) = text("check_name") {
                let result = match payload.get("check_passed").and_then(Value::as_bool) {
                    Some(true) => "passed",
                    Some(false) => "failed",
                    None => "result not recorded",
                };
                fields.push(format!("check {check}: {result}"));
            }
            if let Some(revision) = text("revision") {
                fields.push(format!("revision {revision}"));
            }
            if payload["failed"] == true
                && payload
                    .get("check_passed")
                    .and_then(Value::as_bool)
                    .is_none()
            {
                fields.push("recorded failure".into());
            }
        }
        Some("source.coverage") => {
            if let Some(source) = text("observed_source") {
                fields.push(source.to_owned());
            }
            if let Some(state) = text("state") {
                fields.push(format!("state {state}"));
            }
            if let Some(reason) = text("reason") {
                fields.push(reason.to_owned());
            }
        }
        Some("shell.session") => {
            if let Some(host) = text("host") {
                fields.push(format!("host {host}"));
            }
            if let Some(commands) = payload.get("commands").and_then(Value::as_u64) {
                fields.push(format!("{commands} recorded commands"));
            }
            if let Some(ended_at) = text("ended_at") {
                fields.push(format!("observed through {ended_at}"));
            }
            if let Some(cwds) = payload.get("cwds").and_then(Value::as_array) {
                fields.push(format!(
                    "directories {}",
                    cwds.iter()
                        .filter_map(Value::as_str)
                        .collect::<Vec<_>>()
                        .join(", ")
                ));
            }
        }
        _ => {}
    }
    if !fields.is_empty() {
        return fields.join("; ");
    }
    for name in [
        "command",
        "text",
        "title",
        "path",
        "message",
        "subject",
        "description",
    ] {
        if let Some(text) = row
            .get(name)
            .or_else(|| row.get("payload").and_then(|payload| payload.get(name)))
            .and_then(Value::as_str)
        {
            return text.into();
        }
    }
    row.to_string()
}
fn evidence(row: &Value, parents: &[EventId]) -> Vec<String> {
    row["id"]
        .as_str()
        .map(|id| vec![format!("sinex:ev/{id}")])
        .unwrap_or_else(|| parents.iter().map(|id| format!("sinex:ev/{id}")).collect())
}
pub(crate) fn sections(
    answer: &Value,
    parents: &[EventId],
    question: Option<EventId>,
) -> Vec<Section> {
    let mut records = Vec::new();
    let mut machine = Vec::new();
    let zone = answer["resolved"]["zone"]
        .as_str()
        .and_then(|name| jiff::tz::TimeZone::get(name).ok());
    for row in answer["rows"].as_array().into_iter().flatten() {
        let event_type = row["event_type"].as_str().unwrap_or("query.result");
        let finding = Finding {
            template: Template::Observation {
                event_type: event_type.into(),
                source: row["source"].as_str().unwrap_or("selected query").into(),
                when: row["when"].as_str().map(|text| {
                    match (&zone, text.parse::<jiff::Timestamp>()) {
                        (Some(zone), Ok(at)) => at.to_zoned(zone.clone()).to_string(),
                        _ => text.to_owned(),
                    }
                }),
                description: description(row),
            },
            caveats: vec![],
            evidence: evidence(row, parents),
        };
        if event_type.starts_with("command.")
            || event_type.starts_with("shell.")
            || event_type.starts_with("machine.")
            || event_type.starts_with("code.")
            || event_type.starts_with("file.")
            || event_type.starts_with("work.")
            || event_type == "source.coverage"
        {
            machine.push(finding);
        } else {
            records.push(finding);
        }
    }
    let absent = if records.is_empty() && machine.is_empty() {
        Some(
            answer["recall"]["reason"]
                .as_str()
                .unwrap_or("no recorded events in the selected context")
                .into(),
        )
    } else {
        None
    };
    let quality = answer["recall"]["no_data"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(|source| Finding {
            template: Template::NotFound {
                what: format!("records from {source}"),
                scope: "the selected context; coverage is unknown".into(),
            },
            caveats: vec![],
            evidence: question
                .map(|question| vec![format!("sinex:ev/{question}")])
                .unwrap_or_else(|| {
                    answer["recall"]["citation"]
                        .as_str()
                        .map(|citation| vec![citation.into()])
                        .unwrap_or_default()
                }),
        })
        .collect();
    vec![
        Section {
            title: "Data quality".into(),
            findings: quality,
            absent: None,
        },
        Section {
            title: "Recorded activity".into(),
            findings: records,
            absent,
        },
        Section {
            title: "Machine".into(),
            findings: machine,
            absent: None,
        },
    ]
}

/// Admit only the canonical selector. The runner reads and stores its full answer.
pub(crate) async fn materialize(
    conn: &mut PgConnection,
    writer: &crate::writer::WriterHandle,
    blobs: &sinex_store::Blobs,
    mut request: AnalysisRequest,
    actor: &str,
    host: &str,
) -> Result<String, String> {
    let zone = jiff::tz::TimeZone::get(&request.zone).map_err(|error| error.to_string())?;
    let day_start = request
        .day_start
        .parse()
        .map_err(|error: jiff::Error| error.to_string())?;
    let time_settings = sinex_model::time_settings::TimeSettings::from_parts(zone, day_start);
    request.by = actor.into();
    let id = request.request.clone();
    let existing = sinex_store::query::live_payload(
        conn,
        <AnalysisRequest as sinex_model::EventPayload>::EVENT_TYPE,
        "request",
        &id,
    )
    .await
    .map_err(|error| error.to_string())?
    .map(|(_, payload)| payload);
    let signature = request
        .selection
        .as_ref()
        .and_then(|selection| selection.selected["_fingerprint"].as_str())
        .ok_or("recall selection has no input fingerprint")?
        .to_owned();
    if let Some(pack) = super::load_pack(conn, &format!("query:{id}")).await? {
        if pack.payload["analysis"]["input_signature"].as_str() == Some(signature.as_str()) {
            return Ok(pack.citation);
        }
    }
    if existing.is_none() {
        let acquired = sinex_sources::sources::analysis::request_material(
            request.clone(),
            actor,
            host,
            sinex_model::Timestamp::now(),
        );
        let mut source = sinex_sources::sources::analysis::request_source();
        if request
            .grants
            .iter()
            .any(|grant| grant == "guarded" || grant == "sealed")
        {
            source.spec.class = Class::Guarded;
        }
        let done = crate::intake::request(
            conn,
            writer,
            blobs,
            std::sync::Arc::new(source),
            &format!("analysis.request:{actor}@{host}"),
            host,
            acquired,
        )
        .await?;
        if done.outcome.failures != 0 {
            return Err("recall request was not admitted".into());
        }
    }
    crate::derivation_runner::compute_now(
        conn,
        writer,
        &RecallProducts,
        vec![key(&id)],
        host,
        time_settings,
    )
    .await?;
    Ok(format!("query:{id}"))
}

#[cfg(test)]
mod tests;
