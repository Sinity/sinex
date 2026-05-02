#![doc = include_str!("../docs/README.md")]
#![doc = include_str!("../../../lib/sinex-node-sdk/docs/overview.md")]

//! Terminal command canonicalizer.

pub mod unified_node;

pub use unified_node::{TerminalCommandCanonicalizer, TerminalCommandCanonicalizerNode};

use sinex_primitives::register_source_unit;
use sinex_primitives::source_unit::{
    CheckpointFamily as SuCheckpointFamily, Horizon as SuHorizon,
    OccurrenceIdentity as SuOccurrenceIdentity, PrivacyTier as SuPrivacyTier,
    RetentionPolicy as SuRetentionPolicy, RuntimeShape as SuRuntimeShape, SourceUnitDescriptor,
};

// Source-unit descriptor (issue #690 / #734). The terminal canonicalizer
// transduces shell-history events into normalized `command.canonical`
// outputs.
register_source_unit! {
    SourceUnitDescriptor {
        id: "terminal-canonicalizer",
        namespace: "derived",
        runner_pack: "terminal-canonicalizer",
        checkpoint_family: SuCheckpointFamily::AppendStream,
        event_types: &[
            ("canonical.terminal", "command.canonical"),
        ],
        privacy_tier: SuPrivacyTier::Sensitive,
        runtime_shape: SuRuntimeShape::Continuous,
        horizons: &[SuHorizon::Continuous],
        retention: SuRetentionPolicy::Forever,
        proof_obligations: &[],
        occurrence_identity: SuOccurrenceIdentity::Uuid5From(
            "(source_unit, parent_event_id)",
        ),
        access_policy: "event_stream_read",
        package_impact: "no_new_output",
        implementation_mode: "rust_in_pack:terminal-canonicalizer",
        build_impact: sinex_primitives::source_unit::SourceUnitBuildImpact::ZERO,
    }
}
