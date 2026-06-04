//! Initial ingestion obligation.
//!
//! Verifies that for a registered source unit, running the dispatch function
//! against fixture material produces events of the expected types.
//!
//! ## What this obligation proves
//!
//! - The source unit's parser is registered in the dispatch registry.
//! - The parser accepts the fixture bytes without error.
//! - The parser emits at least one event per expected type.
//!
//! ## Binary path coverage
//!
//! The default obligation still drives the parser dispatch function directly
//! so Wave-B cases can cover many source units cheaply. The `binary_path`
//! canary below separately launches the real `sinexd` binary,
//! publishes through NATS, runs event_engine, and verifies the resulting DB row.
//!
//! ## Per-domain fenced regions
//!
//! Per-source-unit modules call `_run_case(...)` directly. Do not move or
//! rename the fence comments — the orchestrator uses them for conflict
//! detection.

use crate::AdapterKind;
use sinex_primitives::Uuid;
use sinexd::sources::dispatch::default_parser_dispatch;

/// Run the initial ingestion obligation for a source unit.
///
/// # Parameters
///
/// - `source_unit_id` — the registered source unit id
/// - `adapter_kind` — which adapter this unit uses (informational; dispatch is byte-level)
/// - `fixture_data` — raw bytes to dispatch through the parser
/// - `expected_event_types` — event type strings that must appear in parse output
///
/// # Errors
///
/// Returns an error string if the parser is missing, returns an error, or
/// the expected event types are not all present in the output.
pub async fn run(
    source_unit_id: &str,
    _adapter_kind: AdapterKind,
    fixture_data: &[u8],
    expected_event_types: &[&str],
) -> Result<(), String> {
    // Verify registry entry exists before dispatching.
    let validated_id = sinex_primitives::parser::SourceUnitId::new(source_unit_id)
        .map_err(|e| format!("invalid source unit id '{source_unit_id}': {e}"))?;
    let factory =
        sinexd::sources::dispatch::find_parser_factory(&validated_id).ok_or_else(|| {
            format!(
                "source unit '{source_unit_id}' has no parser registered. \
                 Register it with register_parser!(\"{source_unit_id}\", YourParser) \
                 in the source unit's module."
            )
        })?;
    let _ = factory; // existence check only

    let dispatch = default_parser_dispatch();
    let material_id = Uuid::now_v7();

    let outcome = dispatch(source_unit_id, fixture_data, Some(material_id))
        .map_err(|e| format!("dispatch error for '{source_unit_id}': {e}"))?;

    if outcome.events.is_empty() {
        return Err(format!(
            "initial ingestion for '{source_unit_id}': parser returned no events for fixture data ({} bytes)",
            fixture_data.len()
        ));
    }

    // Verify expected event types.
    let produced_types: Vec<String> = outcome
        .events
        .iter()
        .map(|e| e.event_type.as_str().to_string())
        .collect();

    for &expected in expected_event_types {
        if !produced_types.iter().any(|t| t == expected) {
            return Err(format!(
                "initial ingestion for '{source_unit_id}': expected event type '{expected}' \
                 not found in output. Produced: {produced_types:?}"
            ));
        }
    }

    Ok(())
}

// =============================================================================
// Per-domain fenced regions — production-path cases live here.
// =============================================================================

// === terminal ===
// (terminal cases live here)

// === browser ===
// (browser cases live here)

// === document ===
// (document cases live here)

// === fs ===
// (fs cases live here)

// === system ===
// (system cases live here)

// === desktop ===
// (desktop cases live here)

// =============================================================================
// Canary case — weechat.message declarative parser
// =============================================================================
//
// This proves the harness end-to-end and serves as a copy-paste template for
// Wave B subagents.
//
// To adapt for your source unit:
// 1. Change `source_unit_id` to your unit's registered id.
// 2. Change `adapter_kind` to the appropriate `AdapterKind` variant.
// 3. Replace `WEECHAT_FIXTURE_LINE` with fixture bytes for your source.
// 4. Update `expected_event_types` to match your parser's output.
// 5. Move the `_run_case(...)` call inside your domain's fenced region above.

/// Canary: proves `weechat.message` declarative parser round-trips through
/// the harness end-to-end. Used as a copy-paste template for Wave B subagents.
#[cfg(test)]
mod canary {
    use xtask::sandbox::prelude::*;

    /// `WeeChat` log line that the declarative `WeeChatMessageRecord` parser
    /// accepts. Must match the tab-separated format:
    /// `YYYY-MM-DD HH:MM:SS\tnick\tmessage`
    const WEECHAT_FIXTURE_LINE: &[u8] = b"2024-01-15 14:23:45\tsinity\thello from harness canary";

    /// Prove that the `weechat.message` declarative parser is reachable through
    /// the production-path harness and produces `irc.message` events.
    ///
    /// This is the Wave A end-to-end proof. Wave B subagents add analogous
    /// tests inside the fenced regions of this file or by calling `run()`
    /// directly from their own `#[sinex_test]`.
    #[sinex_test]
    async fn weechat_message_canary() -> TestResult<()> {
        let result = super::run(
            "weechat.message",
            crate::AdapterKind::AppendOnlyFile,
            WEECHAT_FIXTURE_LINE,
            &["irc.message"],
        )
        .await;

        result.map_err(|e| color_eyre::eyre::eyre!("{e}"))
    }
}

// =============================================================================
// Binary path canary
// =============================================================================

#[cfg(test)]
mod binary_path {
    use sinex_primitives::privacy::{RuntimePrivateModeState, save_private_mode_state};
    use sinex_primitives::temporal::Timestamp;
    use xtask::sandbox::prelude::*;

    const WEECHAT_MESSAGE: &str = "hello from source-unit host binary";
    const WEECHAT_SUPPRESSED_MESSAGE: &str = "private mode should suppress this";
    const WEECHAT_MALFORMED_STATE_MESSAGE: &str =
        "malformed private-mode state should suppress this";
    const WEECHAT_OUT_OF_SCOPE_MESSAGE: &str = "desktop scope should not suppress terminal";
    const BASH_SUPPRESSED_COMMAND: &str = "echo private mode should suppress bash history";

    fn weechat_node_config(log_path: &std::path::Path) -> serde_json::Value {
        serde_json::json!({
            "path": log_path,
            "skip_empty": true,
        })
    }

    fn append_only_node_config(path: &std::path::Path) -> serde_json::Value {
        serde_json::json!({
            "path": path,
            "skip_empty": true,
        })
    }

    struct SourceUnitHostIngestStack {
        event_engine: TestEventEngineHandle,
        _work_dir: tempfile::TempDir,
    }

    impl SourceUnitHostIngestStack {
        async fn start(ctx: &Sandbox) -> TestResult<Self> {
            ctx.reset_database_slot().await?;

            let nats = ctx.nats_handle()?;
            let work_dir = tempfile::tempdir()?;
            let event_engine = start_test_event_engine_with_config(
                TestEventEngineConfig {
                    nats: nats.connection_config(),
                    database_url: ctx.database_url().to_string(),
                    work_dir: Some(work_dir.path().to_path_buf()),
                    namespace: Some(ctx.pipeline_namespace().prefix().to_string()),
                    consumer_fetch_max_messages: 32,
                    consumer_fetch_timeout_ms: 50,
                    database_pool_size: 4,
                    reject_initial_replay: false,
                },
                Some(ctx),
            )
            .await?;

            Ok(Self {
                event_engine,
                _work_dir: work_dir,
            })
        }

        async fn shutdown(mut self) -> TestResult<()> {
            self.event_engine.stop().await?;
            Ok(())
        }
    }

    async fn write_weechat_fixture(log_path: &std::path::Path, message: &str) -> TestResult<()> {
        tokio::fs::write(
            log_path,
            format!("2024-01-15 14:23:45\tsinity\t{message}\n"),
        )
        .await?;
        Ok(())
    }

    async fn write_bash_fixture(history_path: &std::path::Path, command: &str) -> TestResult<()> {
        tokio::fs::write(history_path, format!("{command}\n")).await?;
        Ok(())
    }

    async fn count_irc_messages(ctx: &Sandbox, message: &str) -> TestResult<i64> {
        let count = sqlx::query_scalar(
            r"
            SELECT COUNT(*)::bigint
            FROM core.events
            WHERE source = 'irc'
              AND event_type = 'irc.message'
              AND payload->>'message' = $1
            ",
        )
        .bind(message)
        .fetch_one(ctx.pool())
        .await?;
        Ok(count)
    }

    async fn count_bash_commands(ctx: &Sandbox, command: &str) -> TestResult<i64> {
        let count = sqlx::query_scalar(
            r"
            SELECT COUNT(*)::bigint
            FROM core.events
            WHERE source = 'shell.history'
              AND event_type = 'command.imported'
              AND payload->>'command' = $1
            ",
        )
        .bind(command)
        .fetch_one(ctx.pool())
        .await?;
        Ok(count)
    }

    async fn run_weechat_scan(
        ctx: &Sandbox,
        tempdir: &tempfile::TempDir,
        case: &str,
        node_config: serde_json::Value,
    ) -> TestResult<String> {
        let worker_dir = tempdir.path().join(format!("worker-{case}"));
        tokio::fs::create_dir_all(&worker_dir).await?;

        let mut config = TestSourceUnitConfig::new("weechat");
        config.nats = ctx.nats_handle()?.connection_config();
        config.database_url = ctx.database_url().to_string();
        config.namespace = Some(ctx.pipeline_namespace().prefix().to_string());
        config.work_dir = Some(worker_dir);
        config.node_config = Some(node_config.to_string());

        let output = run_test_source_unit_scan(config, &[], Some(ctx)).await?;
        Ok(output.stdout)
    }

    async fn run_bash_scan(
        ctx: &Sandbox,
        tempdir: &tempfile::TempDir,
        case: &str,
        node_config: serde_json::Value,
    ) -> TestResult<String> {
        let worker_dir = tempdir.path().join(format!("worker-{case}"));
        tokio::fs::create_dir_all(&worker_dir).await?;

        let mut config = TestSourceUnitConfig::new("terminal.bash-history");
        config.nats = ctx.nats_handle()?.connection_config();
        config.database_url = ctx.database_url().to_string();
        config.namespace = Some(ctx.pipeline_namespace().prefix().to_string());
        config.work_dir = Some(worker_dir);
        config.node_config = Some(node_config.to_string());

        let output = run_test_source_unit_scan(config, &[], Some(ctx)).await?;
        Ok(output.stdout)
    }

    /// Proves the real `sinexd scan-source-unit` path for adapter-backed
    /// source units: binary launch, adapter config, parser, NATS publish,
    /// event_engine persistence, DB payload visibility, and private-mode policy.
    #[sinex_test(timeout = 120)]
    async fn source_unit_host_scan_private_mode_matrix(ctx: TestContext) -> TestResult<()> {
        let ctx = ctx.with_nats().shared().await?;
        let stack = SourceUnitHostIngestStack::start(&ctx).await?;
        let tempdir = tempfile::tempdir()?;

        let baseline_log_path = tempdir.path().join("weechat.log");
        write_weechat_fixture(&baseline_log_path, WEECHAT_MESSAGE).await?;

        let private_log_path = tempdir.path().join("weechat-private.log");
        write_weechat_fixture(&private_log_path, WEECHAT_SUPPRESSED_MESSAGE).await?;
        let private_state_dir = tempdir.path().join("weechat-state");
        save_private_mode_state(
            &private_state_dir,
            &RuntimePrivateModeState::enabled_by(
                "test-operator",
                vec!["weechat".to_string()],
                Timestamp::UNIX_EPOCH,
            ),
        )?;
        let mut private_config = weechat_node_config(&private_log_path);
        private_config["private_mode_state_dir"] =
            serde_json::Value::String(private_state_dir.display().to_string());

        let malformed_log_path = tempdir.path().join("weechat-malformed-state.log");
        write_weechat_fixture(&malformed_log_path, WEECHAT_MALFORMED_STATE_MESSAGE).await?;
        let malformed_state_dir = tempdir.path().join("malformed-state");
        let private_mode_path =
            sinex_primitives::privacy::private_mode_state_path(&malformed_state_dir);
        let private_mode_parent = private_mode_path
            .parent()
            .ok_or_else(|| color_eyre::eyre::eyre!("private-mode state path must have parent"))?;
        tokio::fs::create_dir_all(private_mode_parent).await?;
        tokio::fs::write(&private_mode_path, b"{not-json").await?;
        let mut malformed_config = weechat_node_config(&malformed_log_path);
        malformed_config["private_mode_state_dir"] =
            serde_json::Value::String(malformed_state_dir.display().to_string());

        let history_path = tempdir.path().join(".bash_history");
        write_bash_fixture(&history_path, BASH_SUPPRESSED_COMMAND).await?;
        let bash_state_dir = tempdir.path().join("terminal-state");
        save_private_mode_state(
            &bash_state_dir,
            &RuntimePrivateModeState::enabled_by(
                "test-operator",
                vec!["terminal".to_string()],
                Timestamp::UNIX_EPOCH,
            ),
        )?;
        let mut bash_config = append_only_node_config(&history_path);
        bash_config["private_mode_state_dir"] =
            serde_json::Value::String(bash_state_dir.display().to_string());

        let out_of_scope_log_path = tempdir.path().join("weechat-out-of-scope.log");
        write_weechat_fixture(&out_of_scope_log_path, WEECHAT_OUT_OF_SCOPE_MESSAGE).await?;
        let out_of_scope_state_dir = tempdir.path().join("desktop-state");
        save_private_mode_state(
            &out_of_scope_state_dir,
            &RuntimePrivateModeState::enabled_by(
                "test-operator",
                vec!["desktop".to_string()],
                Timestamp::UNIX_EPOCH,
            ),
        )?;
        let mut out_of_scope_config = weechat_node_config(&out_of_scope_log_path);
        out_of_scope_config["private_mode_state_dir"] =
            serde_json::Value::String(out_of_scope_state_dir.display().to_string());

        let (baseline_output, private_output, malformed_output, bash_output, out_of_scope_output) =
            tokio::try_join!(
                run_weechat_scan(
                    &ctx,
                    &tempdir,
                    "baseline",
                    weechat_node_config(&baseline_log_path),
                ),
                run_weechat_scan(&ctx, &tempdir, "weechat-private", private_config),
                run_weechat_scan(&ctx, &tempdir, "weechat-malformed", malformed_config),
                run_bash_scan(&ctx, &tempdir, "bash-private", bash_config),
                run_weechat_scan(&ctx, &tempdir, "weechat-out-of-scope", out_of_scope_config),
            )?;

        ctx.assert("source-unit host scan processed one event").that(
            baseline_output.contains("Events processed: 1"),
            "scan output should report one processed event",
        )?;
        WaitHelpers::wait_for_condition(
            || async {
                let count = count_irc_messages(&ctx, WEECHAT_MESSAGE)
                    .await
                    .map_err(|error| sqlx::Error::Protocol(error.to_string()))?;
                Ok::<bool, sqlx::Error>(count >= 1)
            },
            10,
        )
        .await?;

        ctx.assert("private-mode scan suppressed all events").that(
            private_output.contains("Events processed: 0"),
            "scan output should report no processed events when source-unit private mode is active",
        )?;
        let count = count_irc_messages(&ctx, WEECHAT_SUPPRESSED_MESSAGE).await?;
        ctx.assert("suppressed private-mode scan persisted no irc.message events")
            .eq(&count, &0)?;

        ctx.assert("malformed private-mode state suppressed all events")
            .that(
                malformed_output.contains("Events processed: 0"),
                "scan output should report no processed events when private-mode state is unreadable",
            )?;
        let count = count_irc_messages(&ctx, WEECHAT_MALFORMED_STATE_MESSAGE).await?;
        ctx.assert("fail-closed malformed state persisted no irc.message events")
            .eq(&count, &0)?;

        ctx.assert("private-mode bash scan suppressed all events").that(
            bash_output.contains("Events processed: 0"),
            "scan output should report no processed events when terminal private mode is active",
        )?;
        let count = count_bash_commands(&ctx, BASH_SUPPRESSED_COMMAND).await?;
        ctx.assert("suppressed private-mode scan persisted no shell.history events")
            .eq(&count, &0)?;

        ctx.assert("out-of-scope private mode preserves acquisition").that(
            out_of_scope_output.contains("Events processed: 1"),
            "scan output should report one processed event when private mode is scoped elsewhere",
        )?;
        WaitHelpers::wait_for_condition(
            || async {
                let count = count_irc_messages(&ctx, WEECHAT_OUT_OF_SCOPE_MESSAGE)
                    .await
                    .map_err(|error| sqlx::Error::Protocol(error.to_string()))?;
                Ok::<bool, sqlx::Error>(count >= 1)
            },
            10,
        )
        .await?;

        stack.shutdown().await?;
        Ok(())
    }
}
