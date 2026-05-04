#![doc = include_str!("../../../lib/sinex-node-sdk/docs/overview.md")]

//! Browser history ingestor that captures historical dump files and browser
//! history `SQLite` databases through the normal node/runtime plane.

mod history_formats;
mod sqlite_sources;
mod unified_node;
mod visit;

pub use sqlite_sources::{BrowserSqliteFormat, BrowserSqliteSourceConfig};
pub use unified_node::{BrowserIngestorConfig, BrowserNode};

use sinex_primitives::register_source_unit;
use sinex_primitives::proof::{
    CheckpointFamily as SuCheckpointFamily, Horizon as SuHorizon,
    OccurrenceIdentity as SuOccurrenceIdentity, PrivacyTier as SuPrivacyTier,
    RetentionPolicy as SuRetentionPolicy, RuntimeShape as SuRuntimeShape, SourceUnitDescriptor,
};

// Source-unit descriptor (issue #690 / #734). Browser history backing stores
// are SQLite databases (Firefox `places.sqlite`, Chromium `History`); the
// occurrence anchor is the row's stable visit_id within the snapshot.
register_source_unit! {
    SourceUnitDescriptor {
        id: "browser.history",
        namespace: "web",
        runner_pack: "browser",
        checkpoint_family: SuCheckpointFamily::MutableSnapshot {
            backing_store_kind: "sqlite",
            occurrence_anchor: "visit_id",
        },
        event_types: &[
            ("webhistory", "page.visited"),
        ],
        // URLs and titles routinely contain auth tokens, search queries.
        privacy_tier: SuPrivacyTier::Secret,
        runtime_shape: SuRuntimeShape::Continuous,
        horizons: &[SuHorizon::Continuous, SuHorizon::Historical],
        retention: SuRetentionPolicy::Forever,
        proof_obligations: &[],
        occurrence_identity: SuOccurrenceIdentity::Uuid5From(
            "(source_unit, browser_profile, visit_id)",
        ),
        access_policy: "target_home_read:browser_history",
        package_impact: "no_new_output",
        implementation_mode: "rust_in_pack:browser",
        build_impact: sinex_primitives::proof::SourceUnitBuildImpact::ZERO,
    }
}
