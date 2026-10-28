//! Sinex's own sampled state, admitted and queried as ordinary events.
use crate::Timestamp;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// One sample of one part of Sinex: the writer, the publisher, a derivation, a
/// source. `measures` are the part's numbers at `sampled_at` (`behind`,
/// `dirty_keys`, `events`), each under a plain name.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(
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

/// Event types describing Sinex itself.
pub const TYPES: &[&str] = &["sinex.telemetry"];
