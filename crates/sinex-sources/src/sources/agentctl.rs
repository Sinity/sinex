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
use crate::sources::fields;
use serde_json::{Map, Value};
use sinex_model::Class;
use sinex_model::event_types::work::AgentctlEvent;
use sinex_source_core::drivers::append_file::AppendFile;
use sinex_source_core::registry::{DeclaredSource, Driver, SourceSpec};
use sinex_source_core::{Parser, RecordError};
use std::path::PathBuf;

pub const SPEC: SourceSpec = SourceSpec {
    name: "work.agentctl",
    driver: Driver::AppendFile,
    excisable: false,
    purpose: "the builds, tests and agent jobs run on this host, and how they ended",
    class: Class::Open,
    parser: "agentctl-events",
    parser_version: 1,
    event_types: &["agentctl.event"],
    framer: sinex_source_core::Framer::incremental(sinex_source_core::drivers::append_file::lines),
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

impl Parser for AgentctlParser {
    type Record<'a> = Map<String, Value>;
    type Carry = ();
    type Payload = AgentctlEvent;

    fn parse(
        &self,
        line: &Map<String, Value>,
        _: &(),
        _context: &sinex_source_core::RecordContext<'_>,
    ) -> Result<(Vec<AgentctlEvent>, ()), RecordError> {
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
        Ok((
            vec![AgentctlEvent {
                record_kind: fields::text(line, "kind")?,
                phase: fields::optional_text(line, "phase"),
                job_id: fields::optional_text(line, "job_id"),
                project: fields::optional_text(line, "project"),
                operation: fields::optional_text(line, "operation"),
                pool: fields::optional_text(line, "pool"),
                attempt: line.get("attempt").and_then(Value::as_u64),
                outcome: fields::optional_text(line, "outcome"),
                exit_code: line.get("exit_code").and_then(Value::as_i64),
                working_directory: fields::optional_text(line, "working_directory"),
                start_head: head("start"),
                end_head: head("end"),
                at,
                extra: fields::extra(line, KNOWN),
            }],
            (),
        ))
    }
}

pub fn source() -> DeclaredSource<AgentctlParser> {
    DeclaredSource {
        spec: SPEC,
        parser: AgentctlParser,
    }
}

pub fn driver(path: PathBuf, host: String) -> AppendFile {
    AppendFile {
        path,
        host,
        framer: sinex_source_core::Framer::incremental(
            sinex_source_core::drivers::append_file::lines,
        ),
    }
}

/// `--read agentctl=PATH`: AgentCTL's event log.
pub const FORMAT: sinex_source_core::binding::InputFormat =
    sinex_source_core::binding::InputFormat {
        name: "agentctl",
        discovery: sinex_source_core::discovery::DiscoverySpec {
            owner: sinex_source_core::discovery::Owner::System,
            locations: &[sinex_source_core::discovery::Location {
                root: "input",
                path: "agentctl",
            }],
            capabilities: &[],
            credentials: &[],
            streams: &[],
            requests: &[],
            request_only: false,
        },
        discover: crate::discovery::discover_format,
        specs: || vec![&SPEC],
        plan: Some(|input, settings| {
            Ok(sinex_source_core::binding::single(&SPEC, input, settings))
        }),
        source: |_| Ok(Box::new(source())),
        reader: Some(|b| {
            Ok(sinex_source_core::binding::appended(driver(
                b.input.clone(),
                b.settings.host.clone(),
            )))
        }),
        capture: None,
    };

#[cfg(test)]
mod tests;
