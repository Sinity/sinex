//! Objective-linked work and its chronological resumption trail.
use super::activity::{ActivityEvidence, Agency};
use crate::Timestamp;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct Objective {
    /// `branch` is exact only together with the recorded repository.
    pub kind: String,
    pub reference: String,
    pub repository: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "work.act", key = ["act"], replace = newer, time = at, quality = intrinsic)]
pub struct WorkAct {
    pub act: String,
    pub session: String,
    pub episode: Option<String>,
    pub objective: Option<Objective>,
    pub label: Option<String>,
    pub label_source: ActivityEvidence,
    pub machine: Option<String>,
    pub at: Timestamp,
    pub ended_at: Option<Timestamp>,
    pub evidence: ActivityEvidence,
    pub agency: Agency,
    pub action: String,
    pub path: Option<String>,
    pub failed: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "work.episode", key = ["episode"], replace = newer, time = started_at, quality = intrinsic)]
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
#[event(type = "work.suspension", key = ["suspension"], replace = newer, time = suspended_at, quality = intrinsic)]
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
#[event(type = "work.resumption", key = ["resumption"], replace = newer, time = resumed_at, quality = intrinsic)]
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
#[event(type = "context.pack", key = ["pack"], replace = newer, time = at, quality = intrinsic)]
pub struct ContextPack {
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
#[event(type = "activity.day", key = ["day_id"], replace = newer, time = started_at, quality = intrinsic)]
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
