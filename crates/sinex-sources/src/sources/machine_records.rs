//! Records Sinnix keeps about the machine at the machine root
//! (`/realm/device/<host>`): one source per file, each read on `AppendFile`
//! one JSON object per line, and the experiment runs, read on `DirSnapshot`.
//!
//! - `generations.jsonl`: NixOS activations.
//! - `borg_status.jsonl`: backup checks, several shapes told apart by `type`.
//! - `borg_drill.jsonl`, `borg_beads_drill.jsonl`: restore drills of the Borg
//!   repositories and of the task store.
//! - `sandbox-audit.jsonl`: findings of the unit and capture-lane audit.
//! - `usage-census.jsonl`: one item per line, or a whole census per line with
//!   its items in `rows`.
//! - `config-drift.jsonl`: rewritten in place with the current results; its
//!   records carry no time.
//! - `experiment/<run>/started.json` and `manifest.json`, some runs nested
//!   under a benchmark's `runs/`.
use super::fields::FieldReader;
use crate::drivers::append_file::{AppendFile, lines};
use crate::drivers::dir_snapshot::DirSnapshot;
use crate::registry::{DeclaredSource, Driver, SourceSpec};
use crate::{Parser, RecordError};
use serde_json::{Map, Value};
use sinex_model::event_types::records::{
    BackupChecked, BackupDrilled, ConfigDriftChecked, ExperimentRun, ExperimentStarted,
    GenerationActivated, SandboxAuditFinding, TaskStoreDrilled, UsageAssessed, UsageCensusTaken,
};
use sinex_model::{Class, Payloads};
use std::path::{Path, PathBuf};

const fn line_spec(
    name: &'static str,
    parser: &'static str,
    purpose: &'static str,
    event_types: &'static [&'static str],
) -> SourceSpec {
    SourceSpec {
        name,
        driver: Driver::AppendFile,
        excisable: false,
        purpose,
        class: Class::Open,
        parser,
        parser_version: 1,
        event_types,
        framer: crate::Framer::incremental(lines),
    }
}

pub const GENERATIONS: SourceSpec = line_spec(
    "machine.generations",
    "machine-generations",
    "which system configuration ran when, for attributing changes in behavior",
    &["generation.activated"],
);
pub const BACKUP_CHECKS: SourceSpec = line_spec(
    "machine.backup-checks",
    "machine-backup-checks",
    "backup health over time",
    &["backup.checked"],
);
pub const RESTORE_DRILLS: SourceSpec = line_spec(
    "machine.restore-drills",
    "machine-restore-drills",
    "evidence that the backups restore",
    &["backup.drilled"],
);
pub const TASK_STORE_DRILLS: SourceSpec = line_spec(
    "machine.task-store-drills",
    "machine-task-store-drills",
    "evidence that the task store restores from the backups",
    &["backup.task_store_drilled"],
);
pub const SANDBOX_AUDIT: SourceSpec = line_spec(
    "machine.sandbox-audit",
    "machine-sandbox-audit",
    "unit and capture-lane health over time",
    &["sandbox_audit.finding"],
);
pub const USAGE_CENSUS: SourceSpec = line_spec(
    "machine.usage-census",
    "machine-usage-census",
    "deciding what in the machine configuration is used and what to remove",
    &["usage.assessed", "usage_census.taken"],
);
pub const CONFIG_DRIFT: SourceSpec = line_spec(
    "machine.config-drift",
    "machine-config-drift",
    "whether the live machine matches its configuration",
    &["config_drift.checked"],
);
pub const EXPERIMENTS: SourceSpec = SourceSpec {
    name: "machine.experiments",
    driver: Driver::DirSnapshot,
    excisable: false,
    purpose: "machine experiment results with the conditions they ran under",
    class: Class::Open,
    parser: "machine-experiments",
    parser_version: 1,
    event_types: &["experiment.started", "experiment.run"],
    framer: crate::Framer::export(
        crate::drivers::export_archive::frame_whole,
        crate::drivers::export_archive::Framing::Whole,
    ),
};

/// Each source and the name it has under the machine root.
const FILES: [(&SourceSpec, &str); 8] = [
    (&GENERATIONS, "generations.jsonl"),
    (&BACKUP_CHECKS, "borg_status.jsonl"),
    (&RESTORE_DRILLS, "borg_drill.jsonl"),
    (&TASK_STORE_DRILLS, "borg_beads_drill.jsonl"),
    (&SANDBOX_AUDIT, "sandbox-audit.jsonl"),
    (&USAGE_CENSUS, "usage-census.jsonl"),
    (&CONFIG_DRIFT, "config-drift.jsonl"),
    (&EXPERIMENTS, "experiment"),
];

/// A file of one JSON object per line, each read into its events by `read`.
pub struct JsonLines<P> {
    read: fn(FieldReader) -> Result<Vec<P>, RecordError>,
}

impl<P: Payloads> Parser for JsonLines<P> {
    type Record<'a> = Map<String, Value>;
    type Carry = ();
    type Payload = P;

    fn parse(
        &self,
        record: &Map<String, Value>,
        _: &(),
        _context: &crate::RecordContext<'_>,
    ) -> Result<(Vec<P>, ()), RecordError> {
        Ok(((self.read)(FieldReader::new(record.clone()))?, ()))
    }
}

fn missing(name: &str) -> RecordError {
    RecordError::new(format!("{name} is missing"))
}

fn generation(mut f: FieldReader) -> Result<Vec<GenerationActivated>, RecordError> {
    let host = f.required_text("host")?;
    let generation = f
        .unsigned("generation")?
        .ok_or_else(|| missing("generation"))?;
    let activated_at = f
        .time("activated_at")?
        .ok_or_else(|| missing("activated_at"))?;
    let (revision, dirty) = match f.text("sinnix_revision")? {
        None => (None, None),
        Some(r) if r == "unknown" => (None, None),
        Some(r) => match r.strip_suffix("-dirty") {
            Some(commit) => (Some(commit.to_owned()), Some(true)),
            None => (Some(r), Some(false)),
        },
    };
    Ok(vec![GenerationActivated {
        host,
        generation,
        activated_at,
        store_path: f.text("store_path")?,
        nixos_label: f.text("nixos_label")?,
        revision,
        dirty,
        extra: f.rest(),
    }])
}

fn backup_check(mut f: FieldReader) -> Result<Vec<BackupChecked>, RecordError> {
    // The check writes -1 for an age it could not read, and a zero deadline
    // and empty run for no planned integrity check.
    let age_sec = f.int("age_sec")?.filter(|age| *age != -1);
    let deadline = match f.int("deadline_epoch")? {
        None | Some(0) => None,
        Some(secs) => Some(
            sinex_model::Timestamp::from_unix_secs(secs)
                .map_err(|_| RecordError::new(format!("deadline_epoch {secs} is out of range")))?,
        ),
    };
    Ok(vec![BackupChecked {
        checked_at: f.time("ts")?.ok_or_else(|| missing("ts"))?,
        check: f.required_text("type")?,
        label: f.text("label")?,
        ok: f.boolean("ok")?,
        state: f.text("state")?,
        message: f.text("message")?,
        age_sec,
        max_age_sec: f.int("max_age_sec")?,
        count: f.unsigned("count")?,
        oldest_age_sec: f.int("oldest_age_sec")?,
        run_id: f.text("run_id")?,
        deadline,
        unit: f.text("unit")?,
        result: f.text("result")?,
        subvolume: f.text("subvolume")?,
        attempts: f.unsigned("attempts")?,
        extra: f.rest(),
    }])
}

fn restore_drill(mut f: FieldReader) -> Result<Vec<BackupDrilled>, RecordError> {
    Ok(vec![BackupDrilled {
        repo: f.required_text("repo")?,
        archive: f.text("archive")?,
        started_at: f.time("started_at")?.ok_or_else(|| missing("started_at"))?,
        ended_at: f.time("ended_at")?,
        duration_s: f.unsigned("duration_s")?,
        exit_code: f.int("exit_code")?,
        status: f.required_text("status")?,
        mode: f.text("mode")?,
        stderr_tail: f.text("stderr_tail")?,
        within_days: f.unsigned("within_days")?,
        max_duration_s: f.unsigned("max_duration_s")?,
        extra: f.rest(),
    }])
}

fn task_store_drill(mut f: FieldReader) -> Result<Vec<TaskStoreDrilled>, RecordError> {
    Ok(vec![TaskStoreDrilled {
        drilled_at: f.time("ts")?.ok_or_else(|| missing("ts"))?,
        archive: f.required_text("archive")?,
        ok: f.boolean("ok")?,
        dolt_commit: f.text("dolt_commit")?,
        authority_path: f.text("authority_path")?,
        issue: f.value("issue"),
        source_git_head: f.text("source_git_head")?,
        issues_jsonl_sha256: f.text("issues_jsonl_sha256")?,
        extra: f.rest(),
    }])
}

fn sandbox_finding(mut f: FieldReader) -> Result<Vec<SandboxAuditFinding>, RecordError> {
    Ok(vec![SandboxAuditFinding {
        found_at: f.time("timestamp")?.ok_or_else(|| missing("timestamp"))?,
        kind: f.required_text("kind")?,
        unit: f.text("unit")?,
        // A lane no unit owns is written with manager `-`.
        manager: f.text("manager")?.filter(|m| m != "-"),
        path: f.text("path")?,
        detail: f.text("detail")?,
        extra: f.rest(),
    }])
}

sinex_model::payload_enum! {
    #[derive(Clone, Debug, PartialEq)]
    pub enum Census {
        Assessed(UsageAssessed),
        Taken(UsageCensusTaken),
    }
}

/// One item, written alone with its own time and window, or as a row of a
/// census, which supplies both.
fn assessed(
    mut f: FieldReader,
    census: Option<(&str, sinex_model::Timestamp, Option<u64>)>,
) -> Result<UsageAssessed, RecordError> {
    let (snapshot_id, assessed_at, window_days) = match census {
        Some((id, at, window)) => (Some(id.to_owned()), at, window),
        None => (
            None,
            f.unix_time("ts")?.ok_or_else(|| missing("ts"))?,
            f.unsigned("window_days")?,
        ),
    };
    Ok(UsageAssessed {
        assessed_at,
        snapshot_id,
        window_days,
        class: f.required_text("class")?,
        name: f.required_text("name")?,
        verdict: f.text("verdict")?,
        declared_in: f.text("declared_in")?,
        static_refs: f.texts("static_refs")?,
        used_via_edges: f.boolean("used_via_edges")?,
        evidence: f.value("evidence"),
        extra: f.rest(),
    })
}

fn census(mut f: FieldReader) -> Result<Vec<Census>, RecordError> {
    let Some(rows) = f.value("rows") else {
        return Ok(vec![Census::Assessed(assessed(f, None)?)]);
    };
    let Value::Array(rows) = rows else {
        return Err(RecordError::new("rows is not a list"));
    };
    let taken = UsageCensusTaken {
        snapshot_id: f.required_text("snapshot_id")?,
        taken_at: f.unix_time("ts")?.ok_or_else(|| missing("ts"))?,
        collected_at: f.unix_time("collected_at")?,
        since: f.unix_time("since")?,
        until: f.unix_time("until")?,
        window_days: f.unsigned("window_days")?,
        complete: f.boolean("complete")?,
        row_count: f.unsigned("row_count")?,
        classes: f.texts("classes")?,
        schema: f.text("schema")?,
        extra: f.rest(),
    };
    let mut out = Vec::with_capacity(rows.len() + 1);
    for (i, row) in rows.into_iter().enumerate() {
        let Value::Object(row) = row else {
            return Err(RecordError::new(format!("rows/{i} is not an object")));
        };
        let census = (
            taken.snapshot_id.as_str(),
            taken.taken_at,
            taken.window_days,
        );
        out.push(Census::Assessed(
            assessed(FieldReader::new(row), Some(census))
                .map_err(|e| RecordError::new(format!("rows/{i}: {e}")))?,
        ));
    }
    out.insert(0, Census::Taken(taken));
    Ok(out)
}

fn config_drift(mut f: FieldReader) -> Result<Vec<ConfigDriftChecked>, RecordError> {
    Ok(vec![ConfigDriftChecked {
        check: f.required_text("check")?,
        status: f.text("status")?,
        severity: f.text("severity")?,
        matches: f.boolean("match")?,
        intended: f.value("intended"),
        live: f.value("live"),
        reboot_required: f.boolean("reboot_required")?,
        evidence: f.text("evidence")?,
        schema: f.text("schema")?,
        extra: f.rest(),
    }])
}

sinex_model::payload_enum! {
    #[derive(Clone, Debug, PartialEq)]
    pub enum Experiment {
        Started(ExperimentStarted),
        Run(ExperimentRun),
    }
}

/// One file of an experiment run: `started.json` when it starts, then
/// `manifest.json`. A file removed from the directory says nothing about the
/// run, so it is read without events.
pub struct ExperimentParser;

impl Parser for ExperimentParser {
    type Record<'a> = &'a [u8];
    type Carry = ();
    type Payload = Experiment;

    fn parse(
        &self,
        record: &&[u8],
        _: &(),
        context: &crate::RecordContext<'_>,
    ) -> Result<(Vec<Experiment>, ()), RecordError> {
        let member = context
            .member
            .ok_or_else(|| RecordError::new("an experiment file needs its directory member"))?;
        if member.details.contains_key("removed_at_ns") {
            return Ok((vec![], ()));
        }
        let object: Map<String, Value> = serde_json::from_slice(record)
            .map_err(|e| RecordError::new(format!("{} is not a JSON object: {e}", member.path)))?;
        let started = member.path == "started.json" || member.path.ends_with("/started.json");
        Ok((vec![experiment(FieldReader::new(object), started)?], ()))
    }
}

fn experiment(mut f: FieldReader, started: bool) -> Result<Experiment, RecordError> {
    let host = f.required_text("host")?;
    let run_id = f.required_text("run_id")?;
    let started_at = f.time("started_at")?;
    let command = f.texts("command")?;
    let cwd = f.text("cwd")?;
    let workload = f.text("workload")?;
    let cache_profile = f.text("cache_profile")?;
    let service_profile = f.text("service_profile")?;
    let git = f.value("git");
    let planned_treatment = f.value("planned_treatment");
    let pre_state = f.value("pre_state");
    let notes = f.texts("notes")?;
    let schema = f.text("schema")?;
    if started {
        return Ok(Experiment::Started(ExperimentStarted {
            host,
            run_id,
            started_at,
            command,
            cwd,
            workload,
            cache_profile,
            service_profile,
            git,
            planned_treatment,
            pre_state,
            notes,
            schema,
            extra: f.rest(),
        }));
    }
    Ok(Experiment::Run(ExperimentRun {
        host,
        run_id,
        started_at,
        ended_at: f.time("ended_at")?,
        exit_status: f.int("exit_status")?,
        command,
        cwd,
        workload,
        cache_profile,
        service_profile,
        git,
        planned_treatment,
        pre_state,
        post_state: f.value("post_state"),
        notes,
        schema,
        extra: f.rest(),
    }))
}

/// The experiment files read; templates, plans and logs beside them are not
/// runs.
pub fn experiment_file(path: &str) -> bool {
    let name = path.rsplit('/').next().unwrap_or(path);
    name == "started.json" || name == "manifest.json"
}

fn lines_source<P: Payloads>(
    spec: &SourceSpec,
    read: fn(FieldReader) -> Result<Vec<P>, RecordError>,
) -> Box<dyn crate::Source> {
    Box::new(DeclaredSource {
        spec: spec.clone(),
        parser: JsonLines { read },
    })
}

/// The parser of the source declared by `spec`.
pub fn source(spec: &SourceSpec) -> Result<Box<dyn crate::Source>, String> {
    Ok(match spec.parser {
        p if p == GENERATIONS.parser => lines_source(spec, generation),
        p if p == BACKUP_CHECKS.parser => lines_source(spec, backup_check),
        p if p == RESTORE_DRILLS.parser => lines_source(spec, restore_drill),
        p if p == TASK_STORE_DRILLS.parser => lines_source(spec, task_store_drill),
        p if p == SANDBOX_AUDIT.parser => lines_source(spec, sandbox_finding),
        p if p == USAGE_CENSUS.parser => lines_source(spec, census),
        p if p == CONFIG_DRIFT.parser => lines_source(spec, config_drift),
        p if p == EXPERIMENTS.parser => Box::new(DeclaredSource {
            spec: spec.clone(),
            parser: ExperimentParser,
        }),
        other => return Err(format!("{other}: not a machine record")),
    })
}

fn spec_of(parser: &str) -> Result<&'static SourceSpec, String> {
    FILES
        .iter()
        .map(|(spec, _)| *spec)
        .find(|spec| spec.parser == parser)
        .ok_or_else(|| format!("{parser}: not a machine record"))
}

/// The bindings under `input`: the machine root (each known file present), or
/// one of its files or the experiment directory named directly.
fn plan(input: &Path, settings: &crate::Settings) -> Vec<crate::Binding> {
    let named = input.file_name().and_then(|n| n.to_str());
    if let Some((spec, _)) = FILES.iter().find(|(_, name)| Some(*name) == named) {
        return vec![crate::Binding::of(spec, input, settings.clone())];
    }
    FILES
        .iter()
        .map(|(spec, name)| (spec, input.join(name)))
        .filter(|(spec, path)| {
            if spec.driver == Driver::DirSnapshot {
                path.is_dir()
            } else {
                path.is_file()
            }
        })
        .map(|(spec, path)| crate::Binding::of(spec, &path, settings.clone()))
        .collect()
}

pub fn line_driver(path: PathBuf, host: String) -> AppendFile {
    AppendFile {
        path,
        host,
        framer: crate::Framer::incremental(lines),
    }
}

pub fn experiment_driver(root: PathBuf, host: String) -> DirSnapshot {
    DirSnapshot {
        root,
        host,
        removed_at_source: false,
        select: experiment_file,
        part_bytes: 64 << 20,
    }
}

/// `--read machine-records=PATH`: the machine root's record files and
/// experiment runs, one binding each.
pub const FORMAT: crate::binding::InputFormat = crate::binding::InputFormat {
    name: "machine-records",
    specs: || FILES.iter().map(|(spec, _)| *spec).collect(),
    plan: Some(|input, settings| Ok(plan(input, settings))),
    source: |b| source(spec_of(&b.parser)?),
    reader: Some(|b| {
        let spec = spec_of(&b.parser)?;
        Ok(if spec.driver == Driver::DirSnapshot {
            crate::binding::walked(experiment_driver(b.input.clone(), b.settings.host.clone()))
        } else {
            crate::binding::appended(line_driver(b.input.clone(), b.settings.host.clone()))
        })
    }),
    capture: None,
};

#[cfg(test)]
mod tests;
