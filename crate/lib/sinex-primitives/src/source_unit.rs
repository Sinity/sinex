//! Source-unit declaration & promotion contract (issue #690).
//!
//! A `SourceUnitDescriptor` is the typed promise an ingestor makes about
//! itself: identity, what it emits, how it captures, what privacy tier it
//! occupies, what proof obligations gate its merge, what horizons it serves,
//! and what its retention policy is. Descriptors are pure data — collected
//! through `inventory` and inspectable by tooling, without affecting the
//! runtime path.
//!
//! This module is the executable form of CONTRIBUTING.md's "completion
//! stewardship" principle: a new ingestor is not promotable until its
//! descriptor is filled in and its declared proof obligations pass.
//!
//! Registered via [`register_source_unit!`] (a thin wrapper around
//! `inventory::submit!`). All registered descriptors are walkable through
//! [`all_source_units`].
//!
//! Folds in `#691` (horizons), `#699` (retention), and `#700`
//! (`CheckpointFamily::MutableSnapshot`).
//!
//! # Example
//!
//! ```ignore
//! use sinex_primitives::source_unit::*;
//! use sinex_primitives::register_source_unit;
//!
//! register_source_unit! {
//!     SourceUnitDescriptor {
//!         id: "terminal",
//!         namespace: "shell",
//!         checkpoint_family: CheckpointFamily::MutableSnapshot {
//!             backing_store_kind: "sqlite",
//!             occurrence_anchor: "row_id",
//!         },
//!         event_types: &[("shell.atuin", "command.executed")],
//!         privacy_tier: PrivacyTier::Sensitive,
//!         runtime_shape: RuntimeShape::Continuous,
//!         horizons: &[Horizon::Continuous, Horizon::Historical],
//!         retention: RetentionPolicy::Forever,
//!         proof_obligations: &["terminal_smoke", "terminal_replay"],
//!         occurrence_identity: OccurrenceIdentity::Uuid5From("(source_unit, row_id)"),
//!     }
//! }
//! ```

use serde::Serialize;

/// How the source's checkpoint state is shaped.
///
/// Determines what the SDK's checkpoint adapter must support and what
/// idempotency/replay strategy the source uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CheckpointFamily {
    /// Append-only stream (filesystem watcher, journald tail, browser export).
    /// Resumed from byte/sequence offset.
    AppendStream,
    /// Mutable backing store snapshotted with row-level occurrence anchors.
    /// Each row has a stable natural key inside the snapshot. Examples: Atuin
    /// SQLite, Fish history, ActivityWatch buckets, Reddit/Spotify exports.
    /// (Issue #700.)
    MutableSnapshot {
        /// Identifier of the backing store kind (`"sqlite"`, `"json"`, ...).
        backing_store_kind: &'static str,
        /// Identifier of the row-level occurrence anchor (e.g. `"row_id"`,
        /// `"export_index"`).
        occurrence_anchor: &'static str,
    },
    /// Journal/log API consumed by sequential cursor (e.g. systemd journal).
    Journal,
    /// Source has no native cursor; ingestor polls and diffs.
    Polling,
    /// Internal in-memory state observed at intervals (e.g. Hyprland focus).
    LiveObservation,
}

/// Privacy classification of the source's payloads.
///
/// Placeholder pending the privacy-tier vocabulary in #455/#460. The variants
/// are coarse on purpose — refinement is tracked there, not here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PrivacyTier {
    /// Public-by-default content (e.g. published RSS).
    Public,
    /// Personal but not secret (commands, window titles, history).
    Sensitive,
    /// Contains secrets that must never leave the host (clipboard, files in
    /// home dirs, document contents).
    Secret,
}

/// Time horizons the source serves on the *normal* runtime plane.
///
/// Issue #691: historical and continuous are two horizons of one source-unit
/// contract, not two persistence planes. Both flow through the same NATS →
/// ingestd → DB pipeline. There is no historical persistence backdoor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Horizon {
    /// Live tail of incoming events (default for capture sources).
    Continuous,
    /// Bounded import of pre-existing data (export archives, historical scans).
    /// Browser-history backfill (#320) is the canonical example.
    Historical,
}

/// How the source is invoked at runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeShape {
    /// Long-running daemon (most ingestors).
    Continuous,
    /// Invoked once per scan via `sinexctl` / control plane.
    OnDemand,
    /// Run on a timer (e.g. periodic export pulls).
    Scheduled,
}

/// Retention policy for events emitted by this source unit.
///
/// Issue #699: retention belongs on the source-unit descriptor — the policy is
/// declared by the source, evaluated by the maintenance runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RetentionPolicy {
    /// Default for personal-history sources: never archive automatically.
    Forever,
    /// Archive events older than `days`.
    Days { days: u32 },
    /// Tiered: `hot_days` in primary storage, then `warm_days` in cold tier,
    /// then archive.
    Tiered { hot_days: u32, warm_days: u32 },
}

/// How the source identifies real-world occurrences (the `(material_id,
/// anchor_byte)` substrate).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum OccurrenceIdentity {
    /// UUIDv5 derived from the named composite key (described in plain text
    /// for documentation; the actual derivation lives in the ingestor).
    Uuid5From(&'static str),
    /// Source provides a natural key the ingestor uses verbatim.
    Natural,
    /// Identity is anchor-byte position only (append-stream sources).
    Anchor,
}

/// The typed declaration every ingestor fills in.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct SourceUnitDescriptor {
    /// Canonical short name used as `EventSource` (`"terminal"`, `"fs"`).
    pub id: &'static str,
    /// Logical namespace this source belongs to (`"shell"`, `"filesystem"`,
    /// `"desktop"`).
    pub namespace: &'static str,
    /// Checkpoint shape — which SDK adapter family this source uses.
    pub checkpoint_family: CheckpointFamily,
    /// `(source, event_type)` pairs this source promises to emit. These match
    /// `(EventPayload::SOURCE, EventPayload::EVENT_TYPE)` constants on the
    /// ingestor's payload structs. A single ingestor binary often emits
    /// multiple `EventSource` values (e.g. `terminal` emits `shell.atuin`,
    /// `shell.history`, `shell.history.fish`), so the contract is on pairs,
    /// not on the binary's `id`. Verified by tooling against the live
    /// `PayloadInfo` registry.
    pub event_types: &'static [(&'static str, &'static str)],
    /// Privacy tier of payload contents.
    pub privacy_tier: PrivacyTier,
    /// Runtime invocation shape.
    pub runtime_shape: RuntimeShape,
    /// Time horizons this source serves (issue #691).
    pub horizons: &'static [Horizon],
    /// Retention policy for emitted events (issue #699).
    pub retention: RetentionPolicy,
    /// Names of proof scenarios that must pass for this source to be
    /// considered promoted. Cross-referenced against the proof catalog by
    /// tooling.
    pub proof_obligations: &'static [&'static str],
    /// How the source identifies real-world occurrences.
    pub occurrence_identity: OccurrenceIdentity,
}

inventory::collect!(SourceUnitDescriptor);

/// Iterate over every registered source-unit descriptor in the binary.
///
/// Tooling uses this to verify (a) every shipped ingestor has a descriptor,
/// (b) each declared `event_type` resolves to a real `PayloadInfo`, and
/// (c) each declared `proof_obligation` resolves to a real proof claim.
pub fn all_source_units() -> impl Iterator<Item = &'static SourceUnitDescriptor> {
    inventory::iter::<SourceUnitDescriptor>()
}

/// Find a source-unit descriptor by `id`.
#[must_use]
pub fn find_source_unit(id: &str) -> Option<&'static SourceUnitDescriptor> {
    all_source_units().find(|descriptor| descriptor.id == id)
}

/// Re-exported `inventory` for consumers of [`register_source_unit!`]. Not
/// stable public API — kept as a leaf so ingestor crates don't need a direct
/// dependency on `inventory` to register their descriptor.
#[doc(hidden)]
pub mod __register {
    pub use inventory;
}

/// Register a source-unit descriptor with the binary's inventory.
///
/// Thin wrapper over `inventory::submit!` — kept as a macro so the registration
/// surface is greppable (`register_source_unit!`) and so future evolution of the
/// registration mechanism (validation, attribute-style, etc.) does not require
/// every ingestor to change.
#[macro_export]
macro_rules! register_source_unit {
    ($descriptor:expr $(,)?) => {
        $crate::source_unit::__register::inventory::submit! { $descriptor }
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    register_source_unit! {
        SourceUnitDescriptor {
            id: "test-source-unit",
            namespace: "test",
            checkpoint_family: CheckpointFamily::AppendStream,
            event_types: &[("test", "test.event")],
            privacy_tier: PrivacyTier::Public,
            runtime_shape: RuntimeShape::OnDemand,
            horizons: &[Horizon::Continuous],
            retention: RetentionPolicy::Forever,
            proof_obligations: &["test_smoke"],
            occurrence_identity: OccurrenceIdentity::Anchor,
        }
    }

    #[test]
    fn registered_descriptor_is_findable() {
        let descriptor = find_source_unit("test-source-unit")
            .expect("test descriptor must be registered via inventory");
        assert_eq!(descriptor.namespace, "test");
        assert_eq!(descriptor.event_types, &[("test", "test.event")]);
        assert!(matches!(descriptor.privacy_tier, PrivacyTier::Public));
        assert_eq!(descriptor.horizons, &[Horizon::Continuous]);
        assert!(matches!(descriptor.retention, RetentionPolicy::Forever));
    }

    #[test]
    fn mutable_snapshot_carries_anchor_metadata() {
        let descriptor = SourceUnitDescriptor {
            id: "mutable",
            namespace: "test",
            checkpoint_family: CheckpointFamily::MutableSnapshot {
                backing_store_kind: "sqlite",
                occurrence_anchor: "row_id",
            },
            event_types: &[],
            privacy_tier: PrivacyTier::Sensitive,
            runtime_shape: RuntimeShape::Continuous,
            horizons: &[Horizon::Continuous, Horizon::Historical],
            retention: RetentionPolicy::Days { days: 90 },
            proof_obligations: &[],
            occurrence_identity: OccurrenceIdentity::Uuid5From("(source, row_id)"),
        };
        match descriptor.checkpoint_family {
            CheckpointFamily::MutableSnapshot {
                backing_store_kind,
                occurrence_anchor,
            } => {
                assert_eq!(backing_store_kind, "sqlite");
                assert_eq!(occurrence_anchor, "row_id");
            }
            _ => panic!("expected MutableSnapshot"),
        }
    }
}
