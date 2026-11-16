//! AgentCTL's event log (`events.jsonl` in its state directory), on
//! `AppendFile`: one JSON line per thing AgentCTL recorded, most of them a
//! job queued, started or finished, with the job's project, operation, pool,
//! outcome and checkout. Sinex only reads the log; AgentCTL and pueue keep
//! owning the jobs.
//!
//! Lines written over AgentCTL's history differ in shape; every line is an
//! `agentctl.event` with the fields it has. A finished job's execution
//! receipt and evidence (manifests of the checkout's files, up to megabytes)
//! stay in the stored line; the event carries the checkout's HEAD at the
//! job's start and end from them.
use crate::sources::{fields, input_format};
use serde_json::{Map, Value};
use sinex_model::{event_types::work::AgentctlEvent, json::JsonExt};
use sinex_source_core::{
    Expectation, Framer, Origin, RecordContext, RecordError, Shape, SourceSpec, Stateless,
    StatelessParser, binding,
    discovery::{DiscoverySpec, Location},
    drivers::append_file::AppendFile,
};

pub const SPEC: SourceSpec = SourceSpec {
    name: "work.agentctl",
    origin: Origin::Input(Shape::AppendFile),
    expect: Expectation::continuous_days(2),
    purpose: "the builds, tests and agent jobs run on this host, and how they ended",
    parser: "agentctl-events",
    event_types: &["agentctl.event"],
    framer: Framer::LINES,
    ..SourceSpec::DEFAULT
};

/// Fields read into the event's own; the rest go to `extra`.
const KNOWN: &[&str] = &[
    "kind",
    "phase",
    "job_id",
    "project",
    "operation",
    "pool",
    "attempt",
    "outcome",
    "exit_code",
    "working_directory",
    "emitted_at",
    "completed_at",
    "execution_receipt",
    "execution_evidence",
];

/// The fields that can say when a line was written, in preference order.
const TIMES: &[&str] = &["emitted_at", "completed_at", "at", "ended_at"];

pub struct AgentctlParser;

impl StatelessParser for AgentctlParser {
    type Record<'a> = Map<String, Value>;
    type Payload = AgentctlEvent;

    fn parse(
        &self,
        line: &Map<String, Value>,
        _context: &RecordContext<'_>,
    ) -> Result<Vec<AgentctlEvent>, RecordError> {
        let head = |side: &str| {
            line.get("execution_receipt")
                .and_then(|r| r.pointer(&format!("/{side}/head")))
                .and_then(Value::as_str)
                .map(str::to_owned)
        };
        let at = TIMES
            .iter()
            .find_map(|name| fields::optional_text(line, name))
            .map(|t| fields::rfc3339(&t))
            .transpose()?;
        Ok(vec![AgentctlEvent {
            record_kind: fields::text(line, "kind")?,
            phase: fields::optional_text(line, "phase"),
            job_id: fields::optional_text(line, "job_id"),
            project: fields::optional_text(line, "project"),
            operation: fields::optional_text(line, "operation"),
            pool: fields::optional_text(line, "pool"),
            attempt: line.u64("attempt"),
            outcome: fields::optional_text(line, "outcome"),
            exit_code: line.i64("exit_code"),
            working_directory: fields::optional_text(line, "working_directory"),
            start_head: head("start"),
            end_head: head("end"),
            at,
            extra: fields::extra(line, KNOWN),
        }])
    }
}

/// `stage --parser agentctl PATH`: AgentCTL's event log.
input_format! { pub FORMAT = "agentctl" {
    specs: [SPEC], parser: |_b| Stateless(AgentctlParser),
    discovery: DiscoverySpec::system(&[Location::new("input", "agentctl")]),
    plan: single, reader: |b| Ok(binding::appended(AppendFile { path: b.input.clone(), host: b.settings.host.clone(), framer: Framer::LINES })),
} }

#[cfg(test)]
mod tests;
