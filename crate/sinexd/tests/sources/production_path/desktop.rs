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
    // Hyprland fires a v1+v2 pair for every focus change; the parser merges
    // them into one window.focused event. The fixture must include both lines.
    const HYPRLAND_FOCUSED_FIXTURE: &[u8] =
        b"activewindow>>kitty,~/project/sinex\nactivewindowv2>>0x1234abcd\n";

    // -------------------------------------------------------------------------
    // desktop.activitywatch — window.active
    // -------------------------------------------------------------------------

    #[sinex_test]
    async fn desktop_activitywatch_window_obligations() -> TestResult<()> {
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
    async fn desktop_activitywatch_afk_obligations() -> TestResult<()> {
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
    async fn desktop_activitywatch_web_obligations() -> TestResult<()> {
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

    #[sinex_test]
    async fn desktop_activitywatch_titles_are_not_parser_redacted() -> TestResult<()> {
        use sinex_primitives::events::SourceMaterial;
        use sinex_primitives::ids::Id;
        use sinex_primitives::parser::{MaterialAnchor, ParserContext, SourceId, SourceRecord};
        use sinex_primitives::temporal::Timestamp;
        use sinexd::runtime::parser::MaterialParser;
        use sinexd::sources::source_contracts::desktop::activitywatch::ActivityWatchParser;

        let material_id = Id::<SourceMaterial>::from_uuid(sinex_primitives::Uuid::now_v7());
        let source_id = SourceId::from_static("desktop.activitywatch");
        let record = SourceRecord {
            material_id,
            anchor: MaterialAnchor::SqliteRow {
                table: "events".to_string(),
                rowid: 1,
            },
            bytes: br#"{"bucket_id":"aw-watcher-web-firefox","started_at":"2024-01-15T14:24:00.000000+00:00","duration":30.0,"data":{"url":"https://example.com","title":"KeePass - Database.kdbx"}}"#.to_vec(),
            logical_path: None,
            source_ts_hint: None,
            metadata: serde_json::Value::Null,
        };
        let ctx = ParserContext {
            source_id,
            source_material_id: material_id,
            record_anchor: record.anchor.clone(),
            operation_id: sinex_primitives::Uuid::now_v7(),
            job_id: sinex_primitives::Uuid::now_v7(),
            host: "fixture-host".to_string(),
            acquisition_time: Timestamp::now(),
        };

        let mut parser = ActivityWatchParser;
        let events = parser.parse_record(record, &ctx).await?;

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type.as_str(), "browser.tab.active");
        assert_eq!(
            events[0].payload["title"], "KeePass - Database.kdbx",
            "ActivityWatch title policy belongs to DB admission rules, not parser-local redaction"
        );

        Ok(())
    }

    // -------------------------------------------------------------------------
    // desktop.clipboard
    // -------------------------------------------------------------------------

    #[sinex_test]
    async fn desktop_clipboard_obligations() -> TestResult<()> {
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
    // desktop.window-manager
    // -------------------------------------------------------------------------

    #[sinex_test]
    async fn desktop_window_manager_obligations() -> TestResult<()> {
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

    #[sinex_test]
    async fn desktop_window_manager_unix_socket_adapter_parses_hyprland_frame() -> TestResult<()> {
        use futures::StreamExt;
        use sinex_primitives::events::SourceMaterial;
        use sinex_primitives::ids::Id;
        use sinex_primitives::parser::{ParserContext, SourceId};
        use sinex_primitives::temporal::Timestamp;
        use sinexd::runtime::parser::{
            InputShapeAdapter, MaterialParser, UnixSocketStreamAdapter, UnixSocketStreamConfig,
        };
        use sinexd::sources::source_contracts::desktop::window_manager::HyprlandParser;

        // Hyprland fires v1 (class+title) immediately followed by v2 (address).
        // The parser buffers v1 and emits one merged window.focused on v2.
        let fixture = crate::fixtures::unix_socket::build(
            b"activewindow>>kitty,~/project/sinex\nactivewindowv2>>0x1234abcd\n",
        )
        .await
        .map_err(|error| color_eyre::eyre::eyre!("{error}"))?;
        let socket_path = match &fixture.binding {
            crate::fixtures::FixtureBinding::UnixSocketPath(path) => path.clone(),
            other => {
                return Err(color_eyre::eyre::eyre!(
                    "unix socket fixture returned unexpected binding: {other:?}"
                ));
            }
        };

        let material_id = Id::<SourceMaterial>::from_uuid(sinex_primitives::Uuid::now_v7());
        let adapter = UnixSocketStreamAdapter;
        let config = UnixSocketStreamConfig {
            socket_path: camino::Utf8PathBuf::from_path_buf(socket_path)
                .map_err(|path| color_eyre::eyre::eyre!("non-UTF8 socket path: {path:?}"))?,
            reconnect_on_eof: false,
        };
        let mut stream = adapter.open(material_id, &config, None).await?;
        let source_id = SourceId::from_static("desktop.window-manager");
        let make_ctx = |record: &sinex_primitives::parser::SourceRecord| -> ParserContext {
            ParserContext {
                source_id: source_id.clone(),
                source_material_id: material_id,
                record_anchor: record.anchor.clone(),
                operation_id: sinex_primitives::Uuid::now_v7(),
                job_id: sinex_primitives::Uuid::now_v7(),
                host: "fixture-host".to_string(),
                acquisition_time: Timestamp::now(),
            }
        };

        let mut parser = HyprlandParser::default();

        // v1 (activewindow): buffered — no events yet.
        let record_v1 = stream
            .next()
            .await
            .ok_or_else(|| color_eyre::eyre::eyre!("unix socket fixture emitted no frames"))??;
        let events_v1 = parser
            .parse_record(record_v1.clone(), &make_ctx(&record_v1))
            .await?;
        assert_eq!(events_v1.len(), 0, "v1 alone should buffer and not emit");

        // v2 (activewindowv2): merges with buffered v1 → one complete window.focused.
        let record_v2 = stream.next().await.ok_or_else(|| {
            color_eyre::eyre::eyre!("unix socket fixture did not emit v2 frame")
        })??;
        let events = parser
            .parse_record(record_v2.clone(), &make_ctx(&record_v2))
            .await?;

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type.as_str(), "window.focused");
        assert_eq!(events[0].event_source.as_str(), "wm.hyprland");
        assert_eq!(events[0].payload["window_class"], "kitty");
        assert_eq!(events[0].payload["window_title"], "~/project/sinex");
        assert_eq!(events[0].payload["window_id"], "0x1234abcd");

        Ok(())
    }
}
