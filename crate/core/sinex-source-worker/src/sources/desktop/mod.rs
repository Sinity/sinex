//! Desktop source units (Wave B).
//!
//! - `desktop.window-manager` — Hyprland IPC socket (`UnixSocketStreamAdapter`)
//! - `desktop.clipboard`      — clipboard polling (`ClipboardPollingAdapter`)
//! - `desktop.activitywatch`  — `ActivityWatch` `SQLite` DB (`SqliteRowAdapter`)

pub mod activitywatch;
pub mod clipboard;
pub mod window_manager;
