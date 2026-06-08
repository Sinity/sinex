//! System source contracts — Wave B fold of sinex-system-source.
//!
//! Eight source contracts:
//! - `system.monitor`  — fire-once startup annotation
//! - `system.journald` — all journal entries via `JournalctlStreamAdapter`
//! - `system.systemd`  — systemd unit events filtered from journald
//! - `system.dbus`     — D-Bus signals via `DbusStreamAdapter`
//! - `system.udev`     — udev device events via `FileDropAdapter`
//! - `desktop.notification` — notification.sent via `DbusStreamAdapter`
//! - `desktop.notification.closed`  — NotificationClosed D-Bus signal
//! - `desktop.notification.action`  — ActionInvoked D-Bus signal

pub mod dbus;
pub mod journald;
pub mod monitor;
pub mod notification_action;
pub mod notification_closed;
pub mod notifications;
pub mod systemd;
pub mod udev;
