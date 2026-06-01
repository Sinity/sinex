//! Filesystem source unit (`fs`).
//!
//! Moved from the legacy `sinex-fs-ingestor` crate during Wave B. The runtime
//! now uses the SDK's content-materializing file-drop adapter plus the
//! filesystem parser, so watcher policy, source-material staging, and parser
//! dispatch share the same adapter-backed source-unit surface as the rest of
//! the source worker.

pub mod parser;

pub use parser::FilesystemParser;

use crate::node_sdk::parser::FileContentDropAdapter;
use crate::register_adapter_ingestor;
use sinex_primitives::proof::{
    CheckpointFamily, Horizon, OccurrenceIdentity, PrivacyTier, RetentionPolicy, RuntimeShape,
    SourceUnitBinding, SourceUnitDescriptor, SubjectRef,
};
use sinex_primitives::{register_source_unit, register_source_unit_binding};

register_adapter_ingestor!(
    source_unit_id: "fs",
    adapter: FileContentDropAdapter,
    parser: FilesystemParser,
);

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
        // FilesystemParser emits path hints consumed by admission policy.
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
    .adapter("FileContentDropAdapter")
    .output_event_type("file.created")
    .sensitivity_profile("fs_path")
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
