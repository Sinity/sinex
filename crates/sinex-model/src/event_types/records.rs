//! Records Sinnix keeps about the machine itself, at the machine root: NixOS
//! activations, backup checks and restore drills, the sandbox audit, the
//! usage census, configuration drift and experiment runs. Each file has one
//! shape; fields a type does not know are kept by name in `extra`.
use crate::Timestamp;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// One NixOS activation. A generation activated again (a re-activation, a
/// rollback) is its own record; the same activation read again is a repeat.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(
    type = "generation.activated",
    key = ["host", "generation", "activated_at"],
    replace = repeat,
    time = activated_at,
    quality = intrinsic
)]
pub struct GenerationActivated {
    pub host: String,
    pub generation: u64,
    pub activated_at: Timestamp,
    pub store_path: Option<String>,
    pub nixos_label: Option<String>,
    /// The Sinnix commit the system was built from; unknown when the
    /// activation did not record one.
    pub revision: Option<String>,
    /// Whether that checkout had uncommitted changes; unknown with the
    /// revision.
    pub dirty: Option<bool>,
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub extra: Map<String, Value>,
}

/// One result of the periodic backup check: an archive's age, the snapshot
/// queue, a failed backup unit, a subvolume's coverage.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "backup.checked", key = location, replace = repeat, time = checked_at, quality = intrinsic)]
pub struct BackupChecked {
    pub checked_at: Timestamp,
    /// What was checked (`archive_freshness`, `snapshot_queue`,
    /// `service_failure`, `nested_subvolume_coverage`, ...).
    pub check: String,
    /// The backup set (`persist`, `realm`).
    pub label: Option<String>,
    pub ok: Option<bool>,
    /// `healthy`, `red` or `maintenance`.
    pub state: Option<String>,
    pub message: Option<String>,
    /// Seconds since the newest archive; unknown when the check could not
    /// read the repository.
    pub age_sec: Option<i64>,
    pub max_age_sec: Option<i64>,
    /// Snapshots waiting to be archived, and the oldest one's age.
    pub count: Option<u64>,
    pub oldest_age_sec: Option<i64>,
    /// The planned integrity check this result waits for, and when it is due.
    pub run_id: Option<String>,
    pub deadline: Option<Timestamp>,
    pub unit: Option<String>,
    pub result: Option<String>,
    pub subvolume: Option<String>,
    pub attempts: Option<u64>,
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub extra: Map<String, Value>,
}

/// One restore drill: an archive extracted from a Borg repository to show it
/// can be restored.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "backup.drilled", key = location, replace = repeat, time = started_at, quality = intrinsic)]
pub struct BackupDrilled {
    pub repo: String,
    pub archive: Option<String>,
    pub started_at: Timestamp,
    pub ended_at: Option<Timestamp>,
    pub duration_s: Option<u64>,
    pub exit_code: Option<i64>,
    /// `ok`, `no_archives`, `skipped_busy`, ...
    pub status: String,
    /// How much was extracted (`repository_partial`); unknown before drills
    /// recorded it.
    pub mode: Option<String>,
    pub stderr_tail: Option<String>,
    /// How recent an archive the drill looked for.
    pub within_days: Option<u64>,
    pub max_duration_s: Option<u64>,
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub extra: Map<String, Value>,
}

/// One restore drill of the task store: the Beads database restored from an
/// archive and checked.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "backup.task_store_drilled", key = location, replace = repeat, time = drilled_at, quality = intrinsic)]
pub struct TaskStoreDrilled {
    pub drilled_at: Timestamp,
    pub archive: String,
    pub ok: Option<bool>,
    /// The restored database's head commit.
    pub dolt_commit: Option<String>,
    /// Where the store lives in the archive, and an issue read back from it.
    pub authority_path: Option<String>,
    pub issue: Option<Value>,
    pub source_git_head: Option<String>,
    pub issues_jsonl_sha256: Option<String>,
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub extra: Map<String, Value>,
}

/// One finding of the hourly capture and sandbox audit: a unit that never
/// succeeded, a capture lane that is missing, empty or abandoned.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "sandbox_audit.finding", key = location, replace = repeat, time = found_at, quality = intrinsic)]
pub struct SandboxAuditFinding {
    pub found_at: Timestamp,
    /// `never-succeeded`, `lane-missing`, `lane-empty`, `lane-abandoned`.
    pub kind: String,
    pub unit: Option<String>,
    /// `system` or `user`; unknown for a lane no unit owns.
    pub manager: Option<String>,
    pub path: Option<String>,
    pub detail: Option<String>,
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub extra: Map<String, Value>,
}

/// One item of the usage census: a script, service, skill or other declared
/// thing, with the evidence of its use and the census's verdict.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "usage.assessed", key = location, replace = repeat, time = assessed_at, quality = intrinsic)]
pub struct UsageAssessed {
    pub assessed_at: Timestamp,
    /// The census it is a row of; unknown for items written one per line.
    pub snapshot_id: Option<String>,
    pub window_days: Option<u64>,
    pub class: String,
    pub name: String,
    pub verdict: Option<String>,
    pub declared_in: Option<String>,
    pub static_refs: Vec<String>,
    pub used_via_edges: Option<bool>,
    /// Per evidence source (shell history, the gateway, the journal, ...),
    /// what it showed.
    pub evidence: Option<Value>,
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub extra: Map<String, Value>,
}

/// One whole usage census: its window, completeness and the sources it
/// declared from. Its items are `usage.assessed` events of the same record.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "usage_census.taken", key = ["snapshot_id"], replace = repeat, time = taken_at, quality = intrinsic)]
pub struct UsageCensusTaken {
    pub snapshot_id: String,
    pub taken_at: Timestamp,
    pub collected_at: Option<Timestamp>,
    pub since: Option<Timestamp>,
    pub until: Option<Timestamp>,
    pub window_days: Option<u64>,
    pub complete: Option<bool>,
    pub row_count: Option<u64>,
    pub classes: Vec<String>,
    pub schema: Option<String>,
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub extra: Map<String, Value>,
}

/// One configuration check: whether the live system matches what the
/// configuration declares. The file is rewritten with the current results and
/// records no time, so when it was checked is unknown.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "config_drift.checked", key = location, replace = repeat)]
pub struct ConfigDriftChecked {
    pub check: String,
    /// `matched`, `drifted`, ...
    pub status: Option<String>,
    pub severity: Option<String>,
    /// Whether the live value matches the declared one.
    pub matches: Option<bool>,
    pub intended: Option<Value>,
    pub live: Option<Value>,
    pub reboot_required: Option<bool>,
    pub evidence: Option<String>,
    pub schema: Option<String>,
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub extra: Map<String, Value>,
}

/// A machine experiment run as it starts (`started.json`). The run is open
/// until its `experiment.run` arrives.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "experiment.started", key = ["host", "run_id"], replace = newer, time = started_at, quality = intrinsic)]
pub struct ExperimentStarted {
    pub host: String,
    pub run_id: String,
    pub started_at: Option<Timestamp>,
    pub command: Vec<String>,
    pub cwd: Option<String>,
    pub workload: Option<String>,
    pub cache_profile: Option<String>,
    pub service_profile: Option<String>,
    pub git: Option<Value>,
    pub planned_treatment: Option<Value>,
    /// The machine's state before the run (boot, kernel, services).
    pub pre_state: Option<Value>,
    pub notes: Vec<String>,
    pub schema: Option<String>,
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub extra: Map<String, Value>,
}

/// A machine experiment run's manifest (`manifest.json`): what ran, under
/// which conditions, and how it ended. Runs backfilled from older captures
/// have no end or exit status.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "experiment.run", key = ["host", "run_id"], replace = newer, time = started_at, quality = intrinsic)]
pub struct ExperimentRun {
    pub host: String,
    pub run_id: String,
    pub started_at: Option<Timestamp>,
    pub ended_at: Option<Timestamp>,
    pub exit_status: Option<i64>,
    pub command: Vec<String>,
    pub cwd: Option<String>,
    pub workload: Option<String>,
    pub cache_profile: Option<String>,
    pub service_profile: Option<String>,
    pub git: Option<Value>,
    pub planned_treatment: Option<Value>,
    pub pre_state: Option<Value>,
    pub post_state: Option<Value>,
    pub notes: Vec<String>,
    pub schema: Option<String>,
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub extra: Map<String, Value>,
}
