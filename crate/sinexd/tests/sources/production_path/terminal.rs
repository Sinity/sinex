//! Wave B production-path obligation tests for terminal source contracts.
//!
//! Source contracts covered:
//! - `terminal.atuin-history`  (`SqliteRowAdapter` + `AtuinHistoryParser`)
//! - `terminal.bash-history`   (`AppendOnlyFileAdapter` + `BashHistoryParser`)
//! - `terminal.zsh-history`    (`AppendOnlyFileAdapter` + `ZshHistoryParser`)
//! - `terminal.text-history`   (`AppendOnlyFileAdapter` + `TextHistoryParser`)
//! - `terminal.fish-history`   (`SqliteRowAdapter` + `FishHistoryParser`)
//!
//! SQLite-backed fixtures (atuin, fish) pass pre-serialised JSON rows
//! as fixture bytes, matching what `SqliteRowAdapter` would produce.
//! `AppendOnlyFile` fixtures (bash, zsh, text) pass plain-text lines.

#[cfg(test)]
mod tests {
    use xtask::sandbox::prelude::*;

    // -------------------------------------------------------------------------
    // Fixtures
    // -------------------------------------------------------------------------

    /// Atuin `SQLite` row serialised as JSON.
    /// Fields match what `SqliteRowAdapter` produces from the `history` table:
    /// id, command, cwd, session, hostname, timestamp (ns), duration (ns), exit.
    const ATUIN_FIXTURE: &[u8] = br#"{"id":"01HW0000000000000000000001","command":"echo hello","cwd":"/home/sinity","session":"01HW0000000000000000000002","hostname":"sinnix-prime","timestamp":1700000000000000000,"duration":12345678,"exit":0}"#;

    /// Fish `SQLite` row serialised as JSON.
    /// Fields: command (required), when (optional Unix seconds).
    const FISH_FIXTURE: &[u8] = br#"{"ROWID":1,"command":"git status","when":1700000000}"#;

    /// A plain bash history line (no timestamp prefix).
    const BASH_FIXTURE: &[u8] = b"ls -la /home/sinity\n";

    /// A plain zsh history line (no extended prefix).
    const ZSH_PLAIN_FIXTURE: &[u8] = b"cd /realm/project/sinex\n";

    /// A zsh history line with extended-history prefix `: ts:elapsed;cmd`.
    const ZSH_EXTENDED_FIXTURE: &[u8] = b": 1700000000:0;cargo check\n";

    /// A plain text history line (catch-all parser).
    const TEXT_FIXTURE: &[u8] = b"make build\n";

    // -------------------------------------------------------------------------
    // terminal.atuin-history
    // -------------------------------------------------------------------------

    #[sinex_test]
    async fn terminal_atuin_history_obligations() -> TestResult<()> {
        let failures = crate::_run_case(
            "terminal.atuin-history",
            crate::AdapterKind::SqliteRow,
            ATUIN_FIXTURE,
            &["command.executed"],
            crate::ALL_OBLIGATIONS,
        )
        .await;
        assert!(
            failures.is_empty(),
            "terminal.atuin-history obligations failed: {failures:#?}"
        );
        Ok(())
    }

    // -------------------------------------------------------------------------
    // terminal.bash-history
    // -------------------------------------------------------------------------

    #[sinex_test]
    async fn terminal_bash_history_obligations() -> TestResult<()> {
        let failures = crate::_run_case(
            "terminal.bash-history",
            crate::AdapterKind::AppendOnlyFile,
            BASH_FIXTURE,
            &["command.imported"],
            crate::ALL_OBLIGATIONS,
        )
        .await;
        assert!(
            failures.is_empty(),
            "terminal.bash-history obligations failed: {failures:#?}"
        );
        Ok(())
    }

    // -------------------------------------------------------------------------
    // terminal.zsh-history (plain)
    // -------------------------------------------------------------------------

    #[sinex_test]
    async fn terminal_zsh_history_plain_obligations() -> TestResult<()> {
        let failures = crate::_run_case(
            "terminal.zsh-history",
            crate::AdapterKind::AppendOnlyFile,
            ZSH_PLAIN_FIXTURE,
            &["command.imported"],
            crate::ALL_OBLIGATIONS,
        )
        .await;
        assert!(
            failures.is_empty(),
            "terminal.zsh-history (plain) obligations failed: {failures:#?}"
        );
        Ok(())
    }

    // -------------------------------------------------------------------------
    // terminal.zsh-history (extended prefix)
    // -------------------------------------------------------------------------

    #[sinex_test]
    async fn terminal_zsh_history_extended_obligations() -> TestResult<()> {
        let failures = crate::_run_case(
            "terminal.zsh-history",
            crate::AdapterKind::AppendOnlyFile,
            ZSH_EXTENDED_FIXTURE,
            &["command.imported"],
            crate::ALL_OBLIGATIONS,
        )
        .await;
        assert!(
            failures.is_empty(),
            "terminal.zsh-history (extended) obligations failed: {failures:#?}"
        );
        Ok(())
    }

    // -------------------------------------------------------------------------
    // terminal.text-history
    // -------------------------------------------------------------------------

    #[sinex_test]
    async fn terminal_text_history_obligations() -> TestResult<()> {
        let failures = crate::_run_case(
            "terminal.text-history",
            crate::AdapterKind::AppendOnlyFile,
            TEXT_FIXTURE,
            &["command.imported"],
            crate::ALL_OBLIGATIONS,
        )
        .await;
        assert!(
            failures.is_empty(),
            "terminal.text-history obligations failed: {failures:#?}"
        );
        Ok(())
    }

    // -------------------------------------------------------------------------
    // terminal.fish-history
    // -------------------------------------------------------------------------

    #[sinex_test]
    async fn terminal_fish_history_obligations() -> TestResult<()> {
        let failures = crate::_run_case(
            "terminal.fish-history",
            crate::AdapterKind::SqliteRow,
            FISH_FIXTURE,
            &["command.imported"],
            crate::ALL_OBLIGATIONS,
        )
        .await;
        assert!(
            failures.is_empty(),
            "terminal.fish-history obligations failed: {failures:#?}"
        );
        Ok(())
    }

    // -------------------------------------------------------------------------
    // terminal.asciinema
    //
    // AsciinemaParser dispatches on the record's logical filename
    // (`session.json` → `session.recorded`, `events.jsonl` → `session.prompt`),
    // so the byte-level `_run_case` harness (which leaves `logical_path = None`)
    // cannot exercise it. Drive the parser directly with a session.json record —
    // this is the obligation evidence cited by the smoke matrix.
    // -------------------------------------------------------------------------

    #[sinex_test]
    async fn terminal_asciinema_session_json_ingestion() -> TestResult<()> {
        use sinex_primitives::Uuid;
        use sinex_primitives::ids::Id;
        use sinex_primitives::parser::{MaterialAnchor, ParserContext, SourceId, SourceRecord};
        use sinex_primitives::temporal::Timestamp;
        use sinexd::runtime::parser::MaterialParser;
        use sinexd::sources::source_contracts::terminal::asciinema::AsciinemaParser;

        const SESSION_JSON: &[u8] =
            br#"{"session_id":"sess-abc","ts_ms":1700000000000,"cwd":"/home/sinity","schema":"v1"}"#;

        let mut parser = AsciinemaParser;
        let record = SourceRecord {
            material_id: Id::new(),
            anchor: MaterialAnchor::ByteRange {
                start: 0,
                len: SESSION_JSON.len() as u64,
            },
            bytes: SESSION_JSON.to_vec(),
            logical_path: Some("2024/06/01/sess-abc/session.json".into()),
            source_ts_hint: None,
            metadata: serde_json::Value::Null,
        };
        let ctx = ParserContext {
            source_id: SourceId::from_static("terminal.asciinema"),
            source_material_id: Id::new(),
            record_anchor: MaterialAnchor::ByteRange {
                start: 0,
                len: SESSION_JSON.len() as u64,
            },
            operation_id: Uuid::new_v4(),
            job_id: Uuid::new_v4(),
            host: "test-host".into(),
            acquisition_time: Timestamp::now(),
        };

        let intents = parser
            .parse_record(record, &ctx)
            .await
            .map_err(|e| color_eyre::eyre::eyre!("asciinema parse_record failed: {e}"))?;

        assert_eq!(
            intents.len(),
            1,
            "session.json must yield exactly one session.recorded event"
        );
        assert_eq!(intents[0].event_type.as_str(), "session.recorded");
        Ok(())
    }
}
