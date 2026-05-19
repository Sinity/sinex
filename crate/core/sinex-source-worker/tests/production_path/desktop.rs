//! Wave B production-path obligation tests for desktop source units.
//!
//! Source units covered:
//! - `desktop.activitywatch`   (`SqliteRowAdapter` + `ActivityWatchParser`)
//! - `desktop.clipboard`       (`ClipboardPollingAdapter` + `ClipboardParser`)
//! - `desktop.window-manager`  (`UnixSocketStreamAdapter` + `HyprlandParser`)
//!
//! `desktop.activitywatch` uses pre-serialised JSON rows (as `SqliteRowAdapter` produces).
//! `desktop.clipboard` passes raw UTF-8 text bytes.
//! `desktop.window-manager` requires a live Unix socket (Hyprland IPC); those tests
//! are marked `#[ignore]` and tracked in #1234.

#[cfg(test)]
mod tests {
    use xtask::sandbox::prelude::*;

    // -------------------------------------------------------------------------
    // Fixtures
    // -------------------------------------------------------------------------

    /// `ActivityWatch` `SQLite` row for a window-watcher event, serialised as JSON.
    /// Fields: `bucket_id` (bucket name, determines event type), `started_at` (ISO8601),
    /// duration (fractional seconds), data (JSON object with app/title).
    const AW_WINDOW_FIXTURE: &[u8] = br#"{"bucket_id":"aw-watcher-window_sinnix-prime","started_at":"2024-01-15T14:23:45.000000+00:00","duration":12.5,"data":{"app":"kitty","title":"~/project/sinex"}}"#;

    /// `ActivityWatch` `SQLite` row for an AFK-watcher event.
    const AW_AFW_FIXTURE: &[u8] = br#"{"bucket_id":"aw-watcher-afk_sinnix-prime","started_at":"2024-01-15T14:23:50.000000+00:00","duration":5.0,"data":{"status":"not-afk"}}"#;

    /// `ActivityWatch` `SQLite` row for a web-watcher event.
    const AW_WEB_FIXTURE: &[u8] = br#"{"bucket_id":"aw-watcher-web-firefox","started_at":"2024-01-15T14:24:00.000000+00:00","duration":30.0,"data":{"url":"https://example.com","title":"Example Domain"}}"#;

    /// Clipboard text payload — plain UTF-8 content.
    const CLIPBOARD_FIXTURE: &[u8] = b"hello from clipboard";

    /// Hyprland IPC line for `activewindow` — `TYPE>>class,title` format.
    /// This fixture is defined for completeness but only used by the ignored test.
    const HYPRLAND_FOCUSED_FIXTURE: &[u8] = b"activewindow>>kitty,~/project/sinex";

    // -------------------------------------------------------------------------
    // desktop.activitywatch — window.active
    // -------------------------------------------------------------------------

    #[sinex_test]
    async fn desktop_activitywatch_window_obligations(_ctx: TestContext) -> TestResult<()> {
        let failures = crate::_run_case(
            "desktop.activitywatch",
            crate::AdapterKind::SqliteRow,
            AW_WINDOW_FIXTURE,
            &["window.active"],
            crate::ALL_OBLIGATIONS,
        )
        .await;
        assert!(
            failures.is_empty(),
            "desktop.activitywatch (window.active) obligations failed: {failures:#?}"
        );
        Ok(())
    }

    // -------------------------------------------------------------------------
    // desktop.activitywatch — afk.changed
    // -------------------------------------------------------------------------

    #[sinex_test]
    async fn desktop_activitywatch_afk_obligations(_ctx: TestContext) -> TestResult<()> {
        let failures = crate::_run_case(
            "desktop.activitywatch",
            crate::AdapterKind::SqliteRow,
            AW_AFW_FIXTURE,
            &["afk.changed"],
            crate::ALL_OBLIGATIONS,
        )
        .await;
        assert!(
            failures.is_empty(),
            "desktop.activitywatch (afk.changed) obligations failed: {failures:#?}"
        );
        Ok(())
    }

    // -------------------------------------------------------------------------
    // desktop.activitywatch — browser.tab.active
    // -------------------------------------------------------------------------

    #[sinex_test]
    async fn desktop_activitywatch_web_obligations(_ctx: TestContext) -> TestResult<()> {
        let failures = crate::_run_case(
            "desktop.activitywatch",
            crate::AdapterKind::SqliteRow,
            AW_WEB_FIXTURE,
            &["browser.tab.active"],
            crate::ALL_OBLIGATIONS,
        )
        .await;
        assert!(
            failures.is_empty(),
            "desktop.activitywatch (browser.tab.active) obligations failed: {failures:#?}"
        );
        Ok(())
    }

    // -------------------------------------------------------------------------
    // desktop.clipboard
    // -------------------------------------------------------------------------

    #[sinex_test]
    async fn desktop_clipboard_obligations(_ctx: TestContext) -> TestResult<()> {
        let failures = crate::_run_case(
            "desktop.clipboard",
            crate::AdapterKind::Clipboard,
            CLIPBOARD_FIXTURE,
            &["clipboard.copied"],
            crate::ALL_OBLIGATIONS,
        )
        .await;
        assert!(
            failures.is_empty(),
            "desktop.clipboard obligations failed: {failures:#?}"
        );
        Ok(())
    }

    // -------------------------------------------------------------------------
    // desktop.window-manager — requires live Hyprland Unix socket
    // -------------------------------------------------------------------------

    #[sinex_test]
    #[ignore = "requires live Hyprland IPC socket - tracked in #1234"]
    async fn desktop_window_manager_obligations(_ctx: TestContext) -> TestResult<()> {
        let failures = crate::_run_case(
            "desktop.window-manager",
            crate::AdapterKind::UnixSocket,
            HYPRLAND_FOCUSED_FIXTURE,
            &["window.focused"],
            crate::ALL_OBLIGATIONS,
        )
        .await;
        assert!(
            failures.is_empty(),
            "desktop.window-manager obligations failed: {failures:#?}"
        );
        Ok(())
    }
}
