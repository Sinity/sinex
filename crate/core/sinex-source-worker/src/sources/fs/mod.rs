//! Filesystem source unit (`fs`).
//!
//! Moved from the legacy `sinex-fs-ingestor` crate during Wave B. The
//! imperative [`FilesystemNode`] implementation is too large and too kernel-
//! adjacent (inotify watcher ownership, watch-budget planning, dual-shape
//! content/observation material) to slot into the SDK adapter framework, so
//! `fs` follows the "honest exception" pattern established by
//! [`crate::noop::NoopSourceUnit`], `terminal.monitor`, and `system.monitor`:
//! a raw [`sinex_node_sdk::IngestorNode`] registered via
//! [`register_node_factory!`].
//!
//! A follow-up issue tracks extending `FileDropAdapter` (or introducing a
//! dedicated `FsWatcherAdapter`) so a future revision can fold this into the
//! adapter framework alongside the other Wave-B source units.

pub mod parser;
pub mod unified_node;

pub use parser::FilesystemParser;
pub use unified_node::{FilesystemConfig, FilesystemNode, FilesystemState};

use crate::{register_node_factory, register_parser};
use sinex_primitives::proof::{
    CheckpointFamily, Horizon, OccurrenceIdentity, PrivacyTier, RetentionPolicy, RuntimeShape,
    SourceUnitBinding, SourceUnitDescriptor, SubjectRef,
};
use sinex_primitives::{register_source_unit, register_source_unit_binding};

register_node_factory!("fs", FilesystemNode);
register_parser!("fs", FilesystemParser);

// Source-unit descriptor (issue #690 / #734). The fs ingestor observes inotify
// on watched roots and emits typed file events. Continuous path is an
// append-stream against the inotify cursor.
register_source_unit! {
    SourceUnitDescriptor {
        id: "fs",
        namespace: "filesystem",
        event_types: &[
            ("fs-watcher", "file.created"),
            ("fs-watcher", "file.modified"),
            ("fs-watcher", "file.deleted"),
            ("fs-watcher", "file.moved"),
        ],
        // Paths can leak home-directory layout and filenames may carry secrets.
        // Path redaction is applied in unified_node.rs via redact_metadata().
        // Treat as Secret during ingestion.
        privacy_tier: PrivacyTier::Secret,
        horizons: &[Horizon::Continuous, Horizon::Historical],
        retention: RetentionPolicy::Forever,
        proof_obligations: &[],
        occurrence_identity: OccurrenceIdentity::Anchor,
        access_policy: "configured_watch_roots",
    }
}

register_source_unit_binding! {
    SourceUnitBinding::builder(
        SubjectRef::from_static("source_unit:fs"),
        "fs",
        "filesystem",
    )
    .implementation("sinex-source-worker")
    .adapter("IngestorNodeAdapter")
    .output_event_type("file.created")
    .privacy_context("fs_path")
    .material_policy("inotify_anchor")
    .checkpoint_policy("append_stream")
    .resource_shape("continuous_inotify")
    .source_unit_id("fs")
    .runner_pack("source-worker")
    .checkpoint_family(CheckpointFamily::AppendStream)
    .runtime_shape(RuntimeShape::Continuous)
    .package_impact("no_new_output")
    .implementation_mode("rust_in_pack:source-worker")
    .build_impact(sinex_primitives::proof::SourceUnitBuildImpact::ZERO)
    .build()
}
