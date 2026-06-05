//! Filesystem source (`fs`).
//!
//! Uses the runtime's content-materializing file-drop adapter plus the filesystem
//! parser, so watcher policy, source-material staging, and parser dispatch
//! share the same adapter-backed source surface as the rest of the source
//! unit host.

pub mod parser;

pub use parser::FilesystemParser;

use crate::register_source;
use crate::runtime::parser::FileContentDropAdapter;
use sinex_primitives::source_contracts::{
    CheckpointFamily, Horizon, OccurrenceIdentity, PrivacyTier, RetentionPolicy, RuntimeShape,
    SourceContract, SourceRuntimeBinding, SubjectRef,
};
use sinex_primitives::{register_source_contract, register_source_runtime_binding};

register_source!(
    source_id: "fs",
    adapter: FileContentDropAdapter,
    parser: FilesystemParser,
);

// Source contract (issue #690 / #734). The fs source observes inotify
// on watched roots and emits typed file events. Continuous path is an
// append-stream against the inotify cursor.
register_source_contract! {
    SourceContract {
        id: "fs",
        namespace: "filesystem",
        event_types: &[
            ("fs-watcher", "file.created"),
            ("fs-watcher", "file.modified"),
            ("fs-watcher", "file.deleted"),
            ("fs-watcher", "file.moved"),
        ],
        // Paths can leak home-directory layout and filenames may carry secrets.
        // FilesystemParser applies metadata-context path redaction. Treat as
        // Secret during ingestion.
        privacy_tier: PrivacyTier::Secret,
        horizons: &[Horizon::Continuous, Horizon::Historical],
        retention: RetentionPolicy::Forever,
        occurrence_identity: OccurrenceIdentity::Anchor,
        access_policy: "configured_watch_roots",
    }
}

register_source_runtime_binding! {
    SourceRuntimeBinding::builder(
        SubjectRef::from_static("source:fs"),
        "fs",
        "filesystem",
    )
    .implementation("sinexd")
    .adapter("FileContentDropAdapter")
    .output_event_type("file.created")
    .privacy_context("fs_path")
    .material_policy("inotify_anchor")
    .checkpoint_policy("append_stream")
    .resource_shape("continuous_inotify")
    .source_id("fs")
    .runner_pack("sinexd-source")
    .checkpoint_family(CheckpointFamily::AppendStream)
    .runtime_shape(RuntimeShape::Continuous)
    .package_impact("no_new_output")
    .implementation_mode("sinexd:source")
    .build_impact(sinex_primitives::source_contracts::SourceBuildImpact::ZERO)
    .build()
}
