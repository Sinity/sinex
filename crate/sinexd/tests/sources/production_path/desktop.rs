//! Wave B production-path obligation tests for desktop source contracts.
//!
//! Source contracts covered:
//! - `desktop.activitywatch`   (`SqliteRowAdapter` + `ActivityWatchParser`)
//! - `desktop.clipboard`       (`ClipboardPollingAdapter` + `ClipboardParser`)
//! - `desktop.window-manager`  (`UnixSocketStreamAdapter` + `HyprlandParser`)
//!
//! `desktop.activitywatch` uses pre-serialised JSON rows (as `SqliteRowAdapter` produces).
//! `desktop.clipboard` passes raw UTF-8 text bytes.
//! `desktop.window-manager` is covered with both parser fixtures and an in-process
//! line-delimited Unix socket fixture.

#[cfg(test)]
#[path = "desktop_test.rs"]
mod tests;
