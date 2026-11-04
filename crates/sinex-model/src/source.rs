//! Owner-installed native source declarations. Transport credentials authorize
//! their use; source labels in an event payload never confer that authority.
use crate::{KindDeclaration, SourceRef};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceDeclaration {
    pub source: SourceRef,
    pub family: String,
    /// Full immutable emitted declarations bind both version and schema.
    pub kinds: Vec<KindDeclaration>,
}

impl SourceDeclaration {
    pub fn fingerprint(&self) -> String {
        let mut normalized = self.clone();
        normalized.kinds.sort_by(|a, b| a.kind.cmp(&b.kind));
        crate::json_identity::hash(
            &serde_json::to_value(normalized).expect("source declarations serialize"),
        )
    }

    pub fn allows(&self, kind: &crate::KindRef, hash: &str) -> bool {
        self.kinds
            .iter()
            .any(|decl| &decl.kind == kind && decl.definition_hash() == hash)
    }
}

/// Owner-installed cache projection contract. Its exact kind definitions are
/// read requirements, independently of any credential's write authority.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectionCatalog {
    pub name: String,
    pub version: u32,
    pub kinds: Vec<KindDeclaration>,
}

impl ProjectionCatalog {
    pub fn fingerprint(&self) -> String {
        let mut normalized = self.clone();
        normalized.kinds.sort_by(|a, b| a.kind.cmp(&b.kind));
        crate::json_identity::hash(
            &serde_json::to_value(normalized).expect("projection catalogs serialize"),
        )
    }
}

/// When a source should have something, so a quiet source can be told from a
/// dead one. Declared per source as a fact about its provider, never per host.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Expectation {
    /// Records arrive as they happen; a source with no new record for longer
    /// than `silent_after` is suspicious.
    Continuous { silent_after: Duration },
    /// Sinex reads the input on a schedule and a read may find nothing new;
    /// a source not read for longer than `every` is suspicious.
    Cadence { every: Duration },
    /// Records arrive in exports the operator takes. An export covers its
    /// period and is never late; only its age is shown.
    Export,
    /// Records exist only when the operator or an agent makes them; silence
    /// means nothing.
    OnDemand,
}

const HOUR: u64 = 3600;
const DAY: u64 = 24 * HOUR;

impl Expectation {
    pub const fn continuous_hours(hours: u64) -> Self {
        Self::Continuous {
            silent_after: Duration::from_secs(hours * HOUR),
        }
    }

    pub const fn continuous_days(days: u64) -> Self {
        Self::Continuous {
            silent_after: Duration::from_secs(days * DAY),
        }
    }

    pub const fn cadence_hours(hours: u64) -> Self {
        Self::Cadence {
            every: Duration::from_secs(hours * HOUR),
        }
    }

    pub const fn cadence_days(days: u64) -> Self {
        Self::Cadence {
            every: Duration::from_secs(days * DAY),
        }
    }

    /// The silence after which a continuous source is suspicious.
    pub fn silent_after(self) -> Option<Duration> {
        match self {
            Expectation::Continuous { silent_after } => Some(silent_after),
            _ => None,
        }
    }

    /// The longest time a source read on a schedule may go unread.
    pub fn every(self) -> Option<Duration> {
        match self {
            Expectation::Cadence { every } => Some(every),
            _ => None,
        }
    }

    /// Every expectation's name.
    pub const NAMES: &'static [&'static str] = &["continuous", "cadence", "export", "on_demand"];

    /// The expectation's name in the catalog, the registry and status.
    pub fn name(self) -> &'static str {
        match self {
            Expectation::Continuous { .. } => "continuous",
            Expectation::Cadence { .. } => "cadence",
            Expectation::Export => "export",
            Expectation::OnDemand => "on_demand",
        }
    }
}

/// A whole number of days, for `expires_after`.
pub const fn days(days: u64) -> Duration {
    Duration::from_secs(days * DAY)
}
