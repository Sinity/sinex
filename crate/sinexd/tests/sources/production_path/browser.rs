//! Production-path obligation tests for the `browser` domain (Wave B).
//!
//! Exercises `browser.history` registered in
//! `sinexd::sources::source_contracts::browser::history`.

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

    const QUTEBROWSER_CASE: crate::ProductionPathCase = crate::ProductionPathCase::new(
        "browser.history qutebrowser",
        "browser.history",
        crate::AdapterKind::SqliteRow,
        QUTEBROWSER_FIXTURE,
        &["page.visited"],
    );

    const CHROMIUM_CASE: crate::ProductionPathCase = crate::ProductionPathCase::new(
        "browser.history chromium",
        "browser.history",
        crate::AdapterKind::SqliteRow,
        CHROMIUM_FIXTURE,
        &["page.visited"],
    );

    const JSONL_DUMP_CASE: crate::ProductionPathCase = crate::ProductionPathCase::new(
        "browser.history JSONL dump",
        "browser.history",
        crate::AdapterKind::AppendOnlyFile,
        JSONL_DUMP_FIXTURE,
        &["page.visited"],
    );

    #[sinex_test]
    async fn browser_history_qutebrowser_initial_ingestion() -> TestResult<()> {
        crate::run_production_path_case(QUTEBROWSER_CASE)
            .await
            .map_err(|e| color_eyre::eyre::eyre!("{e}"))?;
        Ok(())
    }

    #[sinex_test]
    async fn browser_history_chromium_initial_ingestion() -> TestResult<()> {
        crate::run_production_path_case(CHROMIUM_CASE)
            .await
            .map_err(|e| color_eyre::eyre::eyre!("{e}"))?;
        Ok(())
    }

    #[sinex_test]
    async fn browser_history_jsonl_dump_initial_ingestion() -> TestResult<()> {
        crate::run_production_path_case(JSONL_DUMP_CASE)
            .await
            .map_err(|e| color_eyre::eyre::eyre!("{e}"))?;
        Ok(())
    }
}
