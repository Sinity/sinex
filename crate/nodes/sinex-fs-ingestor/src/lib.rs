#![doc = include_str!("../docs/README.md")]
#![doc = include_str!("../../../lib/sinex-node-sdk/docs/overview.md")]

//! Filesystem ingestor facade.

pub mod unified_node;

// Re-export the unified node as the primary interface
pub use unified_node::{FilesystemConfig, FilesystemNode, FilesystemState};

use sinex_primitives::proof::{
    CheckpointFamily, Horizon, OccurrenceIdentity, PrivacyTier, RetentionPolicy, RuntimeShape,
    SourceUnitBinding, SourceUnitDescriptor, SubjectRef,
};
use sinex_primitives::{register_source_unit, register_source_unit_binding};

// Source-unit descriptor (issue #690 / #734). The fs ingestor observes inotify
// on watched roots and emits typed file events. Continuous path is an
// append-stream against the inotify cursor.
register_source_unit! {
    SourceUnitDescriptor {
        id: "fs",
        namespace: "filesystem",
        runner_pack: "fs",
        checkpoint_family: CheckpointFamily::AppendStream,
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
        runtime_shape: RuntimeShape::Continuous,
        horizons: &[Horizon::Continuous, Horizon::Historical],
        retention: RetentionPolicy::Forever,
        proof_obligations: &[],
        occurrence_identity: OccurrenceIdentity::Anchor,
        access_policy: "configured_watch_roots",
        package_impact: "no_new_output",
        implementation_mode: "rust_in_pack:fs",
        build_impact: sinex_primitives::proof::SourceUnitBuildImpact::ZERO,
    }
}

register_source_unit_binding! {
    SourceUnitBinding::builder(
        SubjectRef::from_static("source_unit:fs"),
        "fs",
        "filesystem",
    )
    .implementation("sinex-fs-ingestor")
    .adapter("IngestorNodeAdapter")
    .output_event_type("file.created")
    .privacy_context("fs_path")
    .material_policy("inotify_anchor")
    .checkpoint_policy("append_stream")
    .resource_shape("continuous_inotify")
    .source_unit_id("fs")
    .build()
}
