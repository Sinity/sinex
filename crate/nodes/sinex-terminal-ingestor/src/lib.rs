#![doc = include_str!("../docs/README.md")]
#![doc = include_str!("../docs/overview.md")]
#![doc = include_str!("../../../lib/sinex-node-sdk/docs/overview.md")]

//! Terminal ingestor that streams command history via the shared node pattern.

pub mod shell_detection;

// Atuin shell history SQLite parser
pub mod atuin_history;

// Fish shell history SQLite parser
pub mod fish_history;

pub mod unified_node;

pub use unified_node::{HistorySourceConfig, TerminalConfig, TerminalNode, TerminalState};

use sinex_primitives::register_source_unit;
use sinex_primitives::source_unit::{
    CheckpointFamily, Horizon, OccurrenceIdentity, PrivacyTier, RetentionPolicy, RuntimeShape,
    SourceUnitDescriptor,
};

// Source-unit declaration & promotion contract (issue #690).
//
// Terminal source-unit declarations are the operator-visible logical leaves
// hosted by the shared `terminal` runner pack.
register_source_unit! {
    SourceUnitDescriptor {
        id: "terminal.monitor",
        namespace: "terminal",
        runner_pack: "terminal",
        checkpoint_family: CheckpointFamily::LiveObservation,
        event_types: &[("terminal", "shell.terminal_monitoring_started")],
        privacy_tier: PrivacyTier::Sensitive,
        runtime_shape: RuntimeShape::Continuous,
        horizons: &[Horizon::Continuous],
        retention: RetentionPolicy::Forever,
        proof_obligations: &[],
        occurrence_identity: OccurrenceIdentity::Uuid5From("(source_unit, run_id)"),
        access_policy: "runtime_self_observation",
        package_impact: "no_new_output",
        implementation_mode: "rust_in_pack:terminal",
        build_impact: sinex_primitives::source_unit::SourceUnitBuildImpact::ZERO,
    }
}

register_source_unit! {
    SourceUnitDescriptor {
        id: "terminal.text-history",
        namespace: "terminal",
        runner_pack: "terminal",
        checkpoint_family: CheckpointFamily::AppendStream,
        event_types: &[("shell.history", "command.imported")],
        privacy_tier: PrivacyTier::Sensitive,
        runtime_shape: RuntimeShape::Continuous,
        horizons: &[Horizon::Continuous, Horizon::Historical],
        retention: RetentionPolicy::Forever,
        proof_obligations: &[
            "obligation:source_unit.material_provenance",
            "obligation:source_unit.package_impact_rationale",
        ],
        occurrence_identity: OccurrenceIdentity::Anchor,
        access_policy: "target_home_read:shell_history_text",
        package_impact: "no_new_output",
        implementation_mode: "rust_in_pack:terminal",
        build_impact: sinex_primitives::source_unit::SourceUnitBuildImpact::ZERO,
    }
}

register_source_unit! {
    SourceUnitDescriptor {
        id: "terminal.atuin-history",
        namespace: "terminal",
        runner_pack: "terminal",
        checkpoint_family: CheckpointFamily::MutableSnapshot {
            backing_store_kind: "sqlite",
            occurrence_anchor: "atuin_history_id",
        },
        event_types: &[("shell.atuin", "command.executed")],
        privacy_tier: PrivacyTier::Sensitive,
        runtime_shape: RuntimeShape::Continuous,
        horizons: &[Horizon::Continuous, Horizon::Historical],
        retention: RetentionPolicy::Forever,
        proof_obligations: &[
            "obligation:source_unit.material_provenance",
            "obligation:source_unit.package_impact_rationale",
        ],
        occurrence_identity: OccurrenceIdentity::Natural,
        access_policy: "target_home_read:.local/share/atuin/history.db",
        package_impact: "no_new_output",
        implementation_mode: "rust_in_pack:terminal",
        build_impact: sinex_primitives::source_unit::SourceUnitBuildImpact::ZERO,
    }
}

register_source_unit! {
    SourceUnitDescriptor {
        id: "terminal.zsh-history",
        namespace: "terminal",
        runner_pack: "terminal",
        checkpoint_family: CheckpointFamily::AppendStream,
        event_types: &[("shell.history", "command.imported")],
        privacy_tier: PrivacyTier::Sensitive,
        runtime_shape: RuntimeShape::Continuous,
        horizons: &[Horizon::Continuous, Horizon::Historical],
        retention: RetentionPolicy::Forever,
        proof_obligations: &[
            "obligation:source_unit.material_provenance",
            "obligation:source_unit.package_impact_rationale",
        ],
        occurrence_identity: OccurrenceIdentity::Anchor,
        access_policy: "target_home_read:.zsh_history",
        package_impact: "no_new_output",
        implementation_mode: "rust_in_pack:terminal",
        build_impact: sinex_primitives::source_unit::SourceUnitBuildImpact::ZERO,
    }
}

register_source_unit! {
    SourceUnitDescriptor {
        id: "terminal.fish-history",
        namespace: "terminal",
        runner_pack: "terminal",
        checkpoint_family: CheckpointFamily::MutableSnapshot {
            backing_store_kind: "sqlite",
            occurrence_anchor: "fish_history_row_id",
        },
        event_types: &[("shell.history.fish", "command.executed")],
        privacy_tier: PrivacyTier::Sensitive,
        runtime_shape: RuntimeShape::Continuous,
        horizons: &[Horizon::Continuous, Horizon::Historical],
        retention: RetentionPolicy::Forever,
        proof_obligations: &[
            "obligation:source_unit.material_provenance",
            "obligation:source_unit.package_impact_rationale",
        ],
        occurrence_identity: OccurrenceIdentity::Natural,
        access_policy: "target_home_read:.local/share/fish/fish_history",
        package_impact: "no_new_output",
        implementation_mode: "rust_in_pack:terminal",
        build_impact: sinex_primitives::source_unit::SourceUnitBuildImpact::ZERO,
    }
}

register_source_unit! {
    SourceUnitDescriptor {
        id: "terminal.bash-history",
        namespace: "terminal",
        runner_pack: "terminal",
        checkpoint_family: CheckpointFamily::AppendStream,
        event_types: &[("shell.history", "command.imported")],
        privacy_tier: PrivacyTier::Sensitive,
        runtime_shape: RuntimeShape::Continuous,
        horizons: &[Horizon::Continuous, Horizon::Historical],
        retention: RetentionPolicy::Forever,
        proof_obligations: &[
            "obligation:source_unit.material_provenance",
            "obligation:source_unit.package_impact_rationale",
        ],
        occurrence_identity: OccurrenceIdentity::Anchor,
        access_policy: "target_home_read:.bash_history",
        package_impact: "no_new_output",
        implementation_mode: "rust_in_pack:terminal",
        build_impact: sinex_primitives::source_unit::SourceUnitBuildImpact::ZERO,
    }
}
