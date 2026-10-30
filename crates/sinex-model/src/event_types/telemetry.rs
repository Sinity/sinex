//! Machine telemetry: the rows a host's telemetry collector writes to its
//! SQLite database. Every row is one event. The collector numbers each
//! table's rows itself and carries the numbering over when it starts a new
//! database file, so a row's table, ID and host identify it in every file
//! that holds it.
use crate::Timestamp;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// One row of a sample table (`metric_sample`, `service_state`,
/// `process_memory_sample` and the rest): what the collector measured at one
/// moment.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(version = 2,
    type = "resource.sampled",
    key = ["table", "row_id", "host"],
    replace = repeat,
    time = observed_at,
    quality = intrinsic
)]
pub struct ResourceSampled {
    /// The collector's table the row is from.
    pub table: String,
    /// The row's ID in that table.
    pub row_id: i64,
    /// Unknown when the row's `observed_at` is not a time with an offset.
    pub observed_at: Option<Timestamp>,
    pub host: Option<String>,
    pub boot_id: Option<String>,
    /// The row's other columns by name. A column holding JSON text
    /// (`*_json`) is kept as that JSON. Columns that are null are left out:
    /// their value is unknown.
    pub values: Map<String, Value>,
}

/// A process the kernel's or a userspace OOM killer killed, as the collector
/// read it from the journal (`kill_event`).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(version = 2,
    type = "process.killed",
    key = ["row_id", "host"],
    replace = repeat,
    time = observed_at,
    quality = intrinsic
)]
pub struct ProcessKilled {
    /// The row's ID in `kill_event`.
    pub row_id: i64,
    pub observed_at: Option<Timestamp>,
    pub host: Option<String>,
    pub boot_id: Option<String>,
    /// What killed it, as the collector names it (`earlyoom`, `memcg-oom`).
    pub killer: String,
    /// The killed process's command name.
    pub victim_comm: Option<String>,
    pub victim_pid: Option<i64>,
    pub victim_rss_mib: Option<i64>,
    pub cgroup_path: Option<String>,
    pub oom_score: Option<i64>,
    /// The journal line the kill was read from.
    pub raw_line: String,
    /// The journal's cursor of that line.
    pub journal_cursor: Option<String>,
    /// Columns not named above (`schema_version`), left out when null.
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub values: Map<String, Value>,
}

/// The host's hardware as the collector captured it (`hardware_state`):
/// CPU frequency driver and limits, kernel, sensor and power-meter paths.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(version = 2,
    type = "hardware.state",
    key = ["row_id", "host"],
    replace = repeat,
    time = captured_at,
    quality = intrinsic
)]
pub struct HardwareState {
    /// The row's ID in `hardware_state`.
    pub row_id: i64,
    pub captured_at: Option<Timestamp>,
    pub host: Option<String>,
    pub boot_id: Option<String>,
    /// The captured state, from the row's `payload_json`.
    pub state: Value,
    /// Columns not named above (`schema_version`), left out when null.
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub values: Map<String, Value>,
}

use std::collections::BTreeMap;

/// One sample of one part of Sinex: the writer, the publisher, a derivation, a
/// source. `measures` are the part's numbers at `sampled_at` (`behind`,
/// `dirty_keys`, `events`), each under a plain name.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(version = 2,
    type = "sinex.telemetry",
    key = ["component", "name", "sampled_at"],
    replace = repeat,
    time = sampled_at,
    quality = captured
)]
pub struct SinexTelemetry {
    /// `writer`, `publisher`, `search index`, `derivation` or `source`.
    pub component: String,
    /// The derivation or source sampled; empty for the daemon's own parts.
    pub name: String,
    pub sampled_at: Timestamp,
    pub measures: BTreeMap<String, f64>,
}

/// Daemon self-observation events, separate from machine resource samples.
pub const TYPES: &[&str] = &["sinex.telemetry"];
