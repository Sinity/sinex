//! System source units — Wave B fold of sinex-system-ingestor.
//!
//! Five source units:
//! - `system.monitor`  — fire-once startup annotation
//! - `system.journald` — all journal entries via `JournalctlStreamAdapter`
//! - `system.systemd`  — systemd unit events filtered from journald
//! - `system.dbus`     — D-Bus signals via `DbusStreamAdapter`
//! - `system.udev`     — udev device events via `FileDropAdapter`

pub mod dbus;
pub mod journald;
pub mod monitor;
pub mod systemd;
pub mod udev;
