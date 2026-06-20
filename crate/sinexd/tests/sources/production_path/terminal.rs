//! Wave B production-path obligation tests for terminal source contracts.
//!
//! Source contracts covered:
//! - `terminal.atuin-history`  (`SqliteRowAdapter` + `AtuinHistoryParser`)
//! - `terminal.bash-history`   (`AppendOnlyFileAdapter` + `BashHistoryParser`)
//! - `terminal.zsh-history`    (`AppendOnlyFileAdapter` + `ZshHistoryParser`)
//! - `terminal.text-history`   (`AppendOnlyFileAdapter` + `TextHistoryParser`)
//! - `terminal.fish-history`   (`SqliteRowAdapter` + `FishHistoryParser`)
//! - `terminal.kitty-osc-live` (`UnixSocketStreamAdapter` + `KittyOscParser`)
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

    /// A line-framed Kitty OSC JSON command observation.
    const KITTY_OSC_FIXTURE: &[u8] = br#"{"sequence":42,"command":"git status","cwd":"/realm/project/sinex","exit_status":0,"execution_time_ms":12,"shell_type":"zsh","kitty_window_id":"window-1","kitty_tab_id":"tab-1","timestamp_ns":1700000000000000000}"#;

    const ATUIN_CASE: crate::ProductionPathCase = crate::ProductionPathCase::new(
        "terminal.atuin-history",
        "terminal.atuin-history",
        crate::AdapterKind::SqliteRow,
        ATUIN_FIXTURE,
        &["command.executed"],
    );

    const BASH_CASE: crate::ProductionPathCase = crate::ProductionPathCase::new(
        "terminal.bash-history",
        "terminal.bash-history",
        crate::AdapterKind::AppendOnlyFile,
        BASH_FIXTURE,
        &["command.imported"],
    );

    const ZSH_PLAIN_CASE: crate::ProductionPathCase = crate::ProductionPathCase::new(
        "terminal.zsh-history plain",
        "terminal.zsh-history",
        crate::AdapterKind::AppendOnlyFile,
        ZSH_PLAIN_FIXTURE,
        &["command.imported"],
    );

    const ZSH_EXTENDED_CASE: crate::ProductionPathCase = crate::ProductionPathCase::new(
        "terminal.zsh-history extended",
        "terminal.zsh-history",
        crate::AdapterKind::AppendOnlyFile,
        ZSH_EXTENDED_FIXTURE,
        &["command.imported"],
    );

    const TEXT_CASE: crate::ProductionPathCase = crate::ProductionPathCase::new(
        "terminal.text-history",
        "terminal.text-history",
        crate::AdapterKind::AppendOnlyFile,
        TEXT_FIXTURE,
        &["command.imported"],
    );

    const FISH_CASE: crate::ProductionPathCase = crate::ProductionPathCase::new(
        "terminal.fish-history",
        "terminal.fish-history",
        crate::AdapterKind::SqliteRow,
        FISH_FIXTURE,
        &["command.imported"],
    );

    // -------------------------------------------------------------------------
    // terminal.atuin-history
    // -------------------------------------------------------------------------

    #[sinex_test]
    async fn terminal_atuin_history_obligations() -> TestResult<()> {
        crate::run_production_path_case(ATUIN_CASE)
            .await
            .map_err(|e| color_eyre::eyre::eyre!("{e}"))?;
        Ok(())
    }

    // -------------------------------------------------------------------------
    // terminal.bash-history
    // -------------------------------------------------------------------------

    #[sinex_test]
    async fn terminal_bash_history_obligations() -> TestResult<()> {
        crate::run_production_path_case(BASH_CASE)
            .await
            .map_err(|e| color_eyre::eyre::eyre!("{e}"))?;
        Ok(())
    }

    // -------------------------------------------------------------------------
    // terminal.zsh-history (plain)
    // -------------------------------------------------------------------------

    #[sinex_test]
    async fn terminal_zsh_history_plain_obligations() -> TestResult<()> {
        crate::run_production_path_case(ZSH_PLAIN_CASE)
            .await
            .map_err(|e| color_eyre::eyre::eyre!("{e}"))?;
        Ok(())
    }

    // -------------------------------------------------------------------------
    // terminal.zsh-history (extended prefix)
    // -------------------------------------------------------------------------

    #[sinex_test]
    async fn terminal_zsh_history_extended_obligations() -> TestResult<()> {
        crate::run_production_path_case(ZSH_EXTENDED_CASE)
            .await
            .map_err(|e| color_eyre::eyre::eyre!("{e}"))?;
        Ok(())
    }

    // -------------------------------------------------------------------------
    // terminal.text-history
    // -------------------------------------------------------------------------

    #[sinex_test]
    async fn terminal_text_history_obligations() -> TestResult<()> {
        crate::run_production_path_case(TEXT_CASE)
            .await
            .map_err(|e| color_eyre::eyre::eyre!("{e}"))?;
        Ok(())
    }

    // -------------------------------------------------------------------------
    // terminal.fish-history
    // -------------------------------------------------------------------------

    #[sinex_test]
    async fn terminal_fish_history_obligations() -> TestResult<()> {
        crate::run_production_path_case(FISH_CASE)
            .await
            .map_err(|e| color_eyre::eyre::eyre!("{e}"))?;
        Ok(())
    }

    // -------------------------------------------------------------------------
    // terminal.kitty-osc-live
    //
    // This mode is a live Unix-socket source. Drive the socket adapter and
    // parser together so the evidence exercises the runtime input shape rather
    // than only the byte-dispatch parser helper.
    // -------------------------------------------------------------------------

    async fn parse_kitty_osc_socket_fixture(
        fixture_data: &[u8],
    ) -> TestResult<Vec<sinex_primitives::parser::ParsedEventIntent>> {
        use futures::StreamExt;
        use sinex_primitives::Uuid;
        use sinex_primitives::events::SourceMaterial;
        use sinex_primitives::ids::Id;
        use sinex_primitives::parser::{ParserContext, SourceId};
        use sinex_primitives::temporal::Timestamp;
        use sinexd::runtime::parser::{
            InputShapeAdapter, MaterialParser, UnixSocketStreamAdapter, UnixSocketStreamConfig,
            UnixSocketStreamMode,
        };
        use sinexd::sources::source_contracts::terminal::kitty_osc::KittyOscParser;
        use tokio::io::AsyncWriteExt;
        use tokio::time::{Duration, timeout};

        let temp = tempfile::TempDir::new()?;
        let socket_path = temp.path().join("kitty-osc.sock");

        let material_id = Id::<SourceMaterial>::from_uuid(Uuid::now_v7());
        let adapter = UnixSocketStreamAdapter;
        let config = UnixSocketStreamConfig {
            socket_path: camino::Utf8PathBuf::from_path_buf(socket_path.clone())
                .map_err(|path| color_eyre::eyre::eyre!("non-UTF8 socket path: {path:?}"))?,
            mode: UnixSocketStreamMode::Listen,
            reconnect_on_eof: false,
        };
        let mut stream = adapter.open(material_id, &config, None).await?;

        let mut producer = tokio::net::UnixStream::connect(&socket_path).await?;
        producer.write_all(fixture_data).await?;
        drop(producer);

        let source_id = SourceId::from_static("terminal.kitty-osc-live");
        let make_ctx = |record: &sinex_primitives::parser::SourceRecord| -> ParserContext {
            ParserContext {
                source_id: source_id.clone(),
                source_material_id: material_id,
                record_anchor: record.anchor.clone(),
                operation_id: Uuid::now_v7(),
                job_id: Uuid::now_v7(),
                host: "fixture-host".to_string(),
                acquisition_time: Timestamp::now(),
            }
        };

        let mut parser = KittyOscParser;
        let mut events = Vec::new();
        let expected_records = fixture_data
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
            .count();

        for _ in 0..expected_records {
            let record = timeout(Duration::from_secs(1), stream.next())
                .await?
                .ok_or_else(|| color_eyre::eyre::eyre!("Kitty OSC receiver stream ended"))??;
            events.extend(
                parser
                    .parse_record(record.clone(), &make_ctx(&record))
                    .await?,
            );
        }

        Ok(events)
    }

    #[sinex_test]
    async fn terminal_kitty_osc_live_socket_adapter_parses_command_frame() -> TestResult<()> {
        let events = parse_kitty_osc_socket_fixture(KITTY_OSC_FIXTURE).await?;

        assert_eq!(events.len(), 1);
        let event = &events[0];
        assert_eq!(event.source_id.as_str(), "terminal.kitty-osc-live");
        assert_eq!(event.event_source.as_str(), "shell.kitty");
        assert_eq!(event.event_type.as_str(), "command.executed");
        assert_eq!(event.payload["command"], "git status");
        assert_eq!(event.payload["working_directory"], "/realm/project/sinex");
        assert_eq!(event.payload["kitty_window_id"], "window-1");
        assert_eq!(event.payload["kitty_tab_id"], "tab-1");

        let occurrence = event
            .occurrence_key
            .as_ref()
            .expect("Kitty OSC live events carry occurrence identity");
        assert!(
            occurrence
                .fields
                .iter()
                .any(|(field, value)| field == "sequence_or_frame" && value == "sequence:42"),
            "OSC sequence should participate in occurrence identity"
        );

        Ok(())
    }

    #[sinex_test]
    async fn terminal_kitty_osc_live_socket_adapter_surfaces_malformed_frame() -> TestResult<()> {
        let error = match parse_kitty_osc_socket_fixture(b"{not-json}\n").await {
            Ok(_) => {
                return Err(color_eyre::eyre::eyre!(
                    "malformed Kitty OSC frame was accepted"
                ));
            }
            Err(error) => error,
        };

        assert!(
            error.to_string().contains("kitty OSC JSON frame"),
            "malformed frame should remain attributable to Kitty OSC parsing: {error}"
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
