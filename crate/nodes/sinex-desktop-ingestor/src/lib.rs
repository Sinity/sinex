#![doc = include_str!("../docs/README.md")]
#![doc = include_str!("../docs/overview.md")]
#![doc = include_str!("../../../lib/sinex-node-sdk/docs/overview.md")]

//! Desktop ingestor integrating clipboard and window-sensing feeds.

mod activitywatch_history;
mod clipboard;
mod window_manager;

pub mod unified_node;

pub use clipboard::ClipboardWatcher;
pub use window_manager::{WindowManagerType, WindowManagerWatcher};

// Re-export the new unified node as the primary interface
pub use unified_node::{
    ClipboardStatus, DesktopMonitorHealth, DesktopNode, DesktopState, WindowManagerStatus,
};

use sinex_primitives::register_source_unit;
use sinex_primitives::source_unit::{
    CheckpointFamily as SuCheckpointFamily, Horizon as SuHorizon,
    OccurrenceIdentity as SuOccurrenceIdentity, PrivacyTier as SuPrivacyTier,
    RetentionPolicy as SuRetentionPolicy, RuntimeShape as SuRuntimeShape, SourceUnitDescriptor,
};

// Source-unit descriptors (issue #690 / #734). Desktop is one runner pack, but
// operators should see the logical capture leaves it hosts.
register_source_unit! {
    SourceUnitDescriptor {
        id: "desktop.clipboard",
        namespace: "desktop",
        runner_pack: "desktop",
        checkpoint_family: SuCheckpointFamily::LiveObservation,
        event_types: &[
            ("clipboard", "clipboard.copied"),
            ("clipboard", "clipboard.selected"),
        ],
        // Clipboard payloads routinely contain secrets.
        privacy_tier: SuPrivacyTier::Secret,
        runtime_shape: SuRuntimeShape::Continuous,
        horizons: &[SuHorizon::Continuous, SuHorizon::Historical],
        retention: SuRetentionPolicy::Forever,
        proof_obligations: &[],
        occurrence_identity: SuOccurrenceIdentity::Anchor,
        access_policy: "target_runtime_bridge:clipboard",
        package_impact: "no_new_output",
        implementation_mode: "rust_in_pack:desktop",
        build_impact: sinex_primitives::source_unit::SourceUnitBuildImpact::ZERO,
    }
}

register_source_unit! {
    SourceUnitDescriptor {
        id: "desktop.window-manager",
        namespace: "desktop",
        runner_pack: "desktop",
        checkpoint_family: SuCheckpointFamily::LiveObservation,
        event_types: &[
            ("wm.hyprland", "window.opened"),
            ("wm.hyprland", "window.closed"),
            ("wm.hyprland", "window.focused"),
            ("wm.hyprland", "window.moved"),
            ("wm.hyprland", "window.title_changed"),
            ("wm.hyprland", "workspace.switched"),
            ("wm.hyprland", "monitor.focused"),
            ("wm.hyprland", "state.captured"),
            ("wm.hyprland", "wm.unhandled"),
        ],
        privacy_tier: SuPrivacyTier::Sensitive,
        runtime_shape: SuRuntimeShape::Continuous,
        horizons: &[SuHorizon::Continuous, SuHorizon::Historical],
        retention: SuRetentionPolicy::Forever,
        proof_obligations: &[],
        occurrence_identity: SuOccurrenceIdentity::Anchor,
        access_policy: "target_runtime_bridge:window_manager",
        package_impact: "no_new_output",
        implementation_mode: "rust_in_pack:desktop",
        build_impact: sinex_primitives::source_unit::SourceUnitBuildImpact::ZERO,
    }
}

register_source_unit! {
    SourceUnitDescriptor {
        id: "desktop.activitywatch",
        namespace: "desktop",
        runner_pack: "desktop",
        checkpoint_family: SuCheckpointFamily::MutableSnapshot {
            backing_store_kind: "sqlite",
            occurrence_anchor: "bucket_event_timestamp",
        },
        event_types: &[
            ("activitywatch", "window.active"),
            ("activitywatch", "afk.changed"),
            ("activitywatch", "browser.tab.active"),
        ],
        privacy_tier: SuPrivacyTier::Secret,
        runtime_shape: SuRuntimeShape::OnDemand,
        horizons: &[SuHorizon::Historical],
        retention: SuRetentionPolicy::Forever,
        proof_obligations: &[],
        occurrence_identity: SuOccurrenceIdentity::Uuid5From(
            "(source_unit, bucket_id, event_timestamp)",
        ),
        access_policy: "target_home_read:activitywatch_sqlite",
        package_impact: "no_new_output",
        implementation_mode: "rust_in_pack:desktop",
        build_impact: sinex_primitives::source_unit::SourceUnitBuildImpact::ZERO,
    }
}
