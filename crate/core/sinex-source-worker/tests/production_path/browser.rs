//! Production-path obligation tests for the `browser` domain (Wave B).
//!
//! Exercises `browser.history` registered in
//! `sinex_source_worker::sources::browser::history`.

#[cfg(test)]
mod tests {
    use xtask::sandbox::prelude::*;

    /// Qutebrowser `SQLite` row serialised as JSON.
    ///
    /// Fields match what `SqliteRowAdapter` produces from the `History` table:
    /// `rowid`, `url`, `title`, `atime` (Unix seconds), `redirect` (0/1).
    const QUTEBROWSER_FIXTURE: &[u8] =
        br#"{"rowid":1,"url":"https://example.com","title":"Example","atime":1700000000,"redirect":0}"#;

    /// Chromium `SQLite` row serialised as JSON.
    ///
    /// Fields match what `SqliteRowAdapter` produces from the `visits` table:
    /// `rowid`, `url`, `title`, `visit_time` (Windows FILETIME µs), `transition`, `visit_duration` (µs).
    const CHROMIUM_FIXTURE: &[u8] =
        br#"{"rowid":1,"url":"https://chromium.org","title":"Chromium","visit_time":13305000000000000,"transition":0,"visit_duration":5000000}"#;

    /// Minimal JSONL dump fixture (secondary leg).
    const JSONL_DUMP_FIXTURE: &[u8] =
        b"{\"url\":\"https://dump.example.com\",\"title\":\"Dump\",\"time\":1700002000}\n";

    #[sinex_test]
    async fn browser_history_qutebrowser_initial_ingestion(_ctx: TestContext) -> TestResult<()> {
        let failures = crate::_run_case(
            "browser.history",
            crate::AdapterKind::SqliteRow,
            QUTEBROWSER_FIXTURE,
            &["page.visited"],
            crate::ALL_OBLIGATIONS,
        )
        .await;
        assert!(
            failures.is_empty(),
            "browser.history qutebrowser obligations failed:\n{failures:#?}"
        );
        Ok(())
    }

    #[sinex_test]
    async fn browser_history_chromium_initial_ingestion(_ctx: TestContext) -> TestResult<()> {
        let failures = crate::_run_case(
            "browser.history",
            crate::AdapterKind::SqliteRow,
            CHROMIUM_FIXTURE,
            &["page.visited"],
            crate::ALL_OBLIGATIONS,
        )
        .await;
        assert!(
            failures.is_empty(),
            "browser.history chromium obligations failed:\n{failures:#?}"
        );
        Ok(())
    }

    #[sinex_test]
    async fn browser_history_jsonl_dump_initial_ingestion(_ctx: TestContext) -> TestResult<()> {
        let failures = crate::_run_case(
            "browser.history",
            crate::AdapterKind::AppendOnlyFile,
            JSONL_DUMP_FIXTURE,
            &["page.visited"],
            crate::ALL_OBLIGATIONS,
        )
        .await;
        assert!(
            failures.is_empty(),
            "browser.history JSONL dump obligations failed:\n{failures:#?}"
        );
        Ok(())
    }
}
