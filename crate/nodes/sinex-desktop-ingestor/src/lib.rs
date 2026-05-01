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

// Source-unit descriptor (issue #690 / #734). The desktop ingestor observes
// the active window manager (Hyprland today) plus clipboard and ActivityWatch
// state. State is observed live (no journal cursor); occurrences are anchored
// by event timestamp + content fingerprint.
register_source_unit! {
    SourceUnitDescriptor {
        id: "desktop",
        namespace: "desktop",
        runner_pack: "desktop",
        checkpoint_family: SuCheckpointFamily::LiveObservation,
        event_types: &[
            ("desktop", "desktop.monitoring_started"),
            ("desktop", "desktop.snapshot"),
            ("desktop", "clipboard.historical"),
            ("desktop", "window.wm_historical"),
            ("clipboard", "clipboard.copied"),
            ("clipboard", "clipboard.selected"),
            ("wm.hyprland", "window.opened"),
            ("wm.hyprland", "window.closed"),
            ("wm.hyprland", "window.focused"),
            ("wm.hyprland", "window.moved"),
            ("wm.hyprland", "window.title_changed"),
            ("wm.hyprland", "workspace.switched"),
            ("wm.hyprland", "monitor.focused"),
            ("wm.hyprland", "state.captured"),
            ("activitywatch", "window.active"),
            ("activitywatch", "afk.changed"),
            ("activitywatch", "browser.tab.active"),
        ],
        // Clipboard contents and window titles routinely contain secrets.
        privacy_tier: SuPrivacyTier::Secret,
        runtime_shape: SuRuntimeShape::Continuous,
        horizons: &[SuHorizon::Continuous, SuHorizon::Historical],
        retention: SuRetentionPolicy::Forever,
        proof_obligations: &[],
        occurrence_identity: SuOccurrenceIdentity::Anchor,
        access_policy: "target_runtime_bridge:desktop",
        package_impact: "no_new_output",
        implementation_mode: "rust_in_pack:desktop",
        build_impact: sinex_primitives::source_unit::SourceUnitBuildImpact::ZERO,
    }
}
