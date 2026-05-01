#![doc = include_str!("../docs/README.md")]
#![doc = include_str!("../../../lib/sinex-node-sdk/docs/overview.md")]

//! Filesystem ingestor facade.

pub mod unified_node;

// Re-export the unified node as the primary interface
pub use unified_node::{FilesystemConfig, FilesystemNode, FilesystemState};

use sinex_primitives::register_source_unit;
use sinex_primitives::source_unit::{
    CheckpointFamily, Horizon, OccurrenceIdentity, PrivacyTier, RetentionPolicy, RuntimeShape,
    SourceUnitDescriptor,
};

// Source-unit descriptor (issue #690 / #734). The fs ingestor observes inotify
// on watched roots and emits typed file/dir events. Continuous path is an
// append-stream against the inotify cursor; historical scans walk the tree
// once and emit a `*.discovered` event per existing entry.
register_source_unit! {
    SourceUnitDescriptor {
        id: "fs",
        namespace: "filesystem",
        checkpoint_family: CheckpointFamily::AppendStream,
        event_types: &[
            ("fs-watcher", "file.created"),
            ("fs-watcher", "file.modified"),
            ("fs-watcher", "file.deleted"),
            ("fs-watcher", "file.moved"),
            ("fs-watcher", "file.discovered"),
            ("fs-watcher", "dir.created"),
            ("fs-watcher", "dir.deleted"),
            ("fs-watcher", "dir.discovered"),
        ],
        // Paths can leak home-directory layout and filenames may carry secrets;
        // path-bearing events are unredacted today (#555 tracks the engine-pass
        // gap). Treat as Secret until that lands.
        privacy_tier: PrivacyTier::Secret,
        runtime_shape: RuntimeShape::Continuous,
        horizons: &[Horizon::Continuous, Horizon::Historical],
        retention: RetentionPolicy::Forever,
        proof_obligations: &[],
        occurrence_identity: OccurrenceIdentity::Anchor,
    }
}
