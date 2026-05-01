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
// Terminal is the runner-level source unit for the binary. The shell-history
// adapters below are the operator-visible logical source units hosted by this
// same runner pack.
register_source_unit! {
    SourceUnitDescriptor {
        id: "terminal",
        namespace: "shell",
        runner_pack: "terminal",
        // Atuin SQLite is the primary backing store; Fish history is also
        // SQLite. Generic shell history files (zsh/bash) are append-stream
        // but funnel through the same checkpoint family for consistency.
        checkpoint_family: CheckpointFamily::MutableSnapshot {
            backing_store_kind: "sqlite",
            occurrence_anchor: "atuin_history_id",
        },
        event_types: &[
            ("shell.atuin", "command.executed"),
            ("shell.history", "command.imported"),
            ("shell.history.fish", "command.executed"),
            ("terminal", "shell.terminal_monitoring_started"),
        ],
        privacy_tier: PrivacyTier::Sensitive,
        runtime_shape: RuntimeShape::Continuous,
        horizons: &[Horizon::Continuous, Horizon::Historical],
        retention: RetentionPolicy::Forever,
        proof_obligations: &[
            // Names match scenarios registered in the proof catalog. The
            // xtask issue-drift detector (#694) verifies these resolve.
            "terminal_smoke",
            "terminal_history_replay",
        ],
        occurrence_identity: OccurrenceIdentity::Uuid5From(
            "(source_unit, atuin_history_id)",
        ),
        access_policy: "target_home_read:shell_history",
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
