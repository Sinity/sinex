//! Recall answers are read at one commit position and presented from the rows
//! of that read; an ordinary interactive recall stores nothing.
use serde_json::Value;
use sinex_analysis::finding::{Finding, Template};
use sinex_analysis::presentation::Section;
use sinex_model::EventId;

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
pub(crate) fn sections(answer: &Value, parents: &[EventId]) -> Vec<Section> {
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
            evidence: answer["recall"]["citation"]
                .as_str()
                .map(|citation| vec![citation.into()])
                .unwrap_or_default(),
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

#[cfg(test)]
mod tests;
