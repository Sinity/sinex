//! Production-path obligation tests for the `browser` domain (Wave B).
//!
//! Exercises `browser.history` registered in
//! `sinex_source_worker::sources::browser::history`.

#[cfg(test)]
mod tests {
    use xtask::sandbox::prelude::*;

    /// Minimal qutebrowser-style History table fixture.
    ///
    /// Columns: `url TEXT, title TEXT, atime INTEGER, redirect INTEGER`.
    /// `atime` is Unix seconds — qutebrowser schema.
    const QUTEBROWSER_FIXTURE: &[u8] = b"\
CREATE TABLE History (url TEXT, title TEXT, atime INTEGER, redirect INTEGER);
INSERT INTO History VALUES ('https://example.com', 'Example', 1700000000, 0);
INSERT INTO History VALUES ('https://rust-lang.org', 'Rust', 1700001000, 0);
";

    /// Minimal Chromium-style visits fixture.
    ///
    /// Columns: `url TEXT, title TEXT, visit_time INTEGER, transition INTEGER,
    /// visit_duration INTEGER`. `visit_time` is Windows FILETIME microseconds.
    const CHROMIUM_FIXTURE: &[u8] = b"\
CREATE TABLE visits (url TEXT, title TEXT, visit_time INTEGER, transition INTEGER, visit_duration INTEGER);
INSERT INTO visits VALUES ('https://chromium.org', 'Chromium', 13305000000000000, 0, 5000000);
";

    /// Minimal JSONL dump fixture (secondary leg).
    const JSONL_DUMP_FIXTURE: &[u8] =
        b"{\"url\":\"https://dump.example.com\",\"title\":\"Dump\",\"time\":1700002000}\n";

    #[sinex_test]
    async fn browser_history_qutebrowser_initial_ingestion(
        _ctx: TestContext,
    ) -> TestResult<()> {
        let failures = crate::production_path::_run_case(
            "browser.history",
            crate::production_path::AdapterKind::SqliteRow,
            QUTEBROWSER_FIXTURE,
            &["page.visited"],
            crate::production_path::ALL_OBLIGATIONS,
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
        let failures = crate::production_path::_run_case(
            "browser.history",
            crate::production_path::AdapterKind::SqliteRow,
            CHROMIUM_FIXTURE,
            &["page.visited"],
            crate::production_path::ALL_OBLIGATIONS,
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
        let failures = crate::production_path::_run_case(
            "browser.history",
            crate::production_path::AdapterKind::AppendOnlyFile,
            JSONL_DUMP_FIXTURE,
            &["page.visited"],
            crate::production_path::ALL_OBLIGATIONS,
        )
        .await;
        assert!(
            failures.is_empty(),
            "browser.history JSONL dump obligations failed:\n{failures:#?}"
        );
        Ok(())
    }
}
