//! Objective-linked work and its chronological resumption trail.
use super::activity::{ActivityEvidence, Agency};
use crate::Timestamp;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct Objective {
    /// `branch` is exact only together with the recorded repository.
    pub kind: String,
    pub reference: String,
    pub repository: String,
}

/// Repository facts observed beside a cwd, never resolved during derivation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct GitIdentity {
    pub common_dir: String,
    /// Multiple unrelated roots can be reachable after an unrelated-history merge.
    pub root_commits: Vec<String>,
    pub head: Option<String>,
    pub branch: Option<String>,
}

/// A terminal adapter's exact identity, scoped to its captured host and process.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct TerminalIdentity {
    pub host: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub boot_id: Option<String>,
    pub adapter: String,
    pub process_id: u32,
    pub window_id: String,
    pub parent_process_id: Option<u32>,
}

/// Whether an atomic work snapshot can establish its candidate acts.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum WorkCoverageState {
    Complete,
    Partial,
    Unavailable,
}

/// Exact snapshot coverage; unknown source times never become zero activity.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(version = 2, type = "work.coverage", key = ["session"], replace = newer)]
pub struct WorkCoverage {
    pub session: String,
    pub snapshot: ActivityEvidence,
    pub reducer_version: Option<u32>,
    pub state: WorkCoverageState,
    pub eligible_entries: Option<u64>,
    pub timed_entries: Option<u64>,
    pub untimed_entries: Option<u64>,
    pub reasons: std::collections::BTreeMap<String, u64>,
}

/// Source clock evidence stays separate from its timestamp.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct WorkTimeEvidence {
    pub quality: crate::TimeQuality,
    pub precision: Option<String>,
    pub until: Option<Timestamp>,
}
impl Default for WorkTimeEvidence {
    fn default() -> Self {
        Self {
            quality: crate::TimeQuality::Unknown,
            precision: None,
            until: None,
        }
    }
}
impl WorkTimeEvidence {
    pub fn instant(&self, at: Option<Timestamp>) -> Option<Timestamp> {
        (self.quality != crate::TimeQuality::Unknown
            && self.precision.as_deref() == Some("instant")
            && self.until.is_none())
        .then_some(at)
        .flatten()
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct WorkAct {
    pub act: String,
    pub session: String,
    pub episode: Option<String>,
    pub objective: Option<Objective>,
    pub label: Option<String>,
    pub label_source: ActivityEvidence,
    pub machine: Option<String>,
    pub at: Timestamp,
    #[serde(default)]
    pub at_evidence: WorkTimeEvidence,
    pub ended_at: Option<Timestamp>,
    #[serde(default)]
    pub ended_at_evidence: Option<WorkTimeEvidence>,
    pub evidence: ActivityEvidence,
    pub agency: Agency,
    pub action: String,
    pub path: Option<String>,
    /// Unknown until native outcome or check evidence establishes it.
    #[serde(default)]
    pub failed: Option<bool>,
    #[serde(default)]
    pub revision: Option<String>,
    #[serde(default)]
    pub check_name: Option<String>,
    #[serde(default)]
    pub check_passed: Option<bool>,
    #[serde(default)]
    pub cwd: Option<String>,
    #[serde(default)]
    pub git_identity: Option<GitIdentity>,
    #[serde(default)]
    pub terminal: Option<TerminalIdentity>,
}

impl crate::EventPayload for WorkAct {
    const VERSION: u64 = 2;
    const EVENT_TYPE: &'static str = "work.act";
    const KEY: crate::KeyKind = crate::KeyKind::Fields(&["act"]);
    const REPLACE: crate::ReplaceRule = crate::ReplaceRule::ReplaceWhenNewer;
    fn ts_orig(&self) -> crate::TsOrig {
        crate::TsOrig::maybe(
            self.at_evidence.instant(Some(self.at)),
            self.at_evidence.quality,
        )
    }
    fn payload_schema() -> Value {
        serde_json::to_value(schemars::schema_for!(Self)).expect("work act schema serializes")
    }
    fn declaration() -> crate::KindDeclaration {
        let mut declaration = crate::KindDeclaration::schemaless(
            crate::KindRef {
                name: Self::EVENT_TYPE.into(),
                version: Self::VERSION,
            },
            "sinex".into(),
        );
        declaration.normalization = crate::kinds::Normalization::Rust;
        declaration.payload_schema = Some(Self::payload_schema());
        declaration.key_rule = Some(crate::kinds::KeyRule::Fields {
            fields: vec!["act".into()],
        });
        declaration.replace_rule = Some(Self::REPLACE);
        declaration.time_rule = Some(crate::kinds::TimeRule::InstantEvidence {
            field: "at".into(),
            evidence_field: "at_evidence".into(),
        });
        declaration.search = Self::SEARCH;
        declaration
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(version = 2, type = "work.episode", key = ["episode"], replace = newer, time = started_at, quality = intrinsic)]
pub struct WorkEpisode {
    pub episode: String,
    pub objective: Objective,
    pub label: String,
    pub label_source: ActivityEvidence,
    pub state: String,
    pub started_at: Timestamp,
    pub last_activity: Timestamp,
    /// Instants without a recorded duration contribute no invented seconds.
    pub operator_s: Option<f64>,
    pub agent_s: Option<f64>,
    pub untimed_agent_acts: u64,
    pub trail: Vec<ActivityEvidence>,
    pub suspensions: Vec<String>,
    pub resumptions: Vec<String>,
    pub absorbed_excursions: u64,
    pub profile: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(version = 2, type = "work.suspension", key = ["suspension"], replace = newer, time = suspended_at, quality = intrinsic)]
pub struct WorkSuspension {
    pub suspension: String,
    pub episode: String,
    pub suspended_at: Timestamp,
    pub returned_at: Option<Timestamp>,
    pub interruption_source: String,
    pub away_s: Option<f64>,
    pub intervening_episodes: Vec<String>,
    pub absorbed_excursions: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(version = 2, type = "work.resumption", key = ["resumption"], replace = newer, time = resumed_at, quality = intrinsic)]
pub struct WorkResumption {
    pub resumption: String,
    pub episode: String,
    pub suspension: String,
    pub resumed_at: Timestamp,
    /// Unknown without a recorded return of focus to this exact objective.
    pub lag_s: Option<f64>,
    pub act: ActivityEvidence,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(version = 2, type = "context.pack", key = ["pack"], replace = newer, time = at, quality = intrinsic)]
pub struct ContextPack {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evaluated_at: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub day_setting: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub producer_versions: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read_as: Option<i64>,
    /// Stored analysis sections and their selected panel at one read context.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub analysis: Option<serde_json::Value>,
    pub pack: String,
    pub episode: String,
    pub at: Timestamp,
    pub stated_goal: Option<ActivityEvidence>,
    pub trail: Vec<ActivityEvidence>,
    pub last_edit: Option<ActivityEvidence>,
    pub last_edit_path: Option<String>,
    pub open_failures: Vec<ActivityEvidence>,
    pub coverage: Vec<String>,
    pub profile: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(version = 2, type = "activity.day", key = ["day_id"], replace = newer, time = started_at, quality = intrinsic)]
pub struct ActivityDay {
    pub day_id: String,
    pub started_at: Timestamp,
    pub ended_at: Timestamp,
    pub boundary_method: String,
    pub provisional: bool,
    pub agency_seconds: std::collections::BTreeMap<String, f64>,
    /// Recorded delegated time; unknown without timed delegated evidence.
    pub agent_s: Option<f64>,
    pub profile: String,
}

/// One line of AgentCTL's event log: a job queued, started or finished, or
/// something else AgentCTL recorded (a pool paused under memory pressure, a
/// campaign's step). The line is its place in the log.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(version = 2, type = "agentctl.event", key = location, replace = repeat, time = at, quality = intrinsic)]
pub struct AgentctlEvent {
    /// What the line is about, as AgentCTL names it (`queue-task`,
    /// `declared-operation`, `backpressure`, `campaign`, ...).
    pub record_kind: String,
    /// `queued`, `started`, `finished`, `succeeded`, `failed`, ...
    pub phase: Option<String>,
    pub job_id: Option<String>,
    pub project: Option<String>,
    pub operation: Option<String>,
    pub pool: Option<String>,
    pub attempt: Option<u64>,
    /// How a finished job ended (`success`, `failed`, `timeout`, `cancelled`).
    pub outcome: Option<String>,
    pub exit_code: Option<i64>,
    /// The checkout the job ran in.
    pub working_directory: Option<String>,
    /// The checkout's HEAD when the job started and when it ended, as
    /// AgentCTL recorded them.
    pub start_head: Option<String>,
    pub end_head: Option<String>,
    /// When AgentCTL wrote the line (`emitted_at`, or `completed_at` in the
    /// oldest lines).
    pub at: Option<Timestamp>,
    /// The line's other fields. The execution receipt and evidence (file
    /// manifests of the checkout) are left in the stored line.
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub extra: Map<String, Value>,
}

/// Exact rational allocation; unresolved time is carried separately.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ProjectShare {
    pub project: String,
    pub numerator: u64,
    pub denominator: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(version = 2, type = "activity.project_span", key = ["project_span"], replace = newer, time = started_at, quality = intrinsic)]
pub struct ProjectSpan {
    pub project_span: String,
    pub scope: String,
    pub started_at: Timestamp,
    pub ended_at: Timestamp,
    pub agency: Agency,
    pub activity_spans: Vec<ActivityEvidence>,
    pub shares: Vec<ProjectShare>,
    pub unallocated_numerator: u64,
    pub denominator: u64,
    pub ladder_rung: Option<u8>,
    pub reason: String,
    pub conflicting_projects: Vec<String>,
    pub supported_seconds: f64,
    pub profile: String,
}
