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
//! canary below separately launches the real `sinex-source-worker` binary,
//! publishes through NATS, runs ingestd, and verifies the resulting DB row.
//!
//! ## Per-domain fenced regions
//!
//! Wave B subagents add `case!(...)` calls inside the fence for their domain.
//! Do not move or rename the fence comments — the orchestrator uses them for
//! conflict detection.

use crate::AdapterKind;
use sinex_primitives::Uuid;
use sinex_source_worker::dispatch::default_parser_dispatch;

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
        sinex_source_worker::dispatch::find_parser_factory(&validated_id).ok_or_else(|| {
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
// Per-domain fenced regions — Wave B adds case!() invocations here
// =============================================================================

// === terminal ===
// (Wave B terminal subagent adds case! invocations here)

// === browser ===
// (Wave B browser subagent adds case! invocations here)

// === document ===
// (Wave B document subagent adds case! invocations here)

// === fs ===
// (Wave B fs subagent adds case! invocations here)

// === system ===
// (Wave B system subagent adds case! invocations here)

// === desktop ===
// (Wave B desktop subagent adds case! invocations here)

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
// 5. Move the `case!()` call inside your domain's fenced region above.

/// Canary: proves `weechat.message` declarative parser round-trips through
/// the harness end-to-end. Used as a copy-paste template for Wave B subagents.
#[cfg(test)]
mod canary {
    use xtask::sandbox::prelude::*;

    /// WeeChat log line that the declarative `WeeChatMessageRecord` parser
    /// accepts. Must match the tab-separated format:
    /// `YYYY-MM-DD HH:MM:SS\tnick\tmessage`
    const WEECHAT_FIXTURE_LINE: &[u8] = b"2024-01-15 14:23:45\tsinity\thello from harness canary";

    /// Prove that the `weechat.message` declarative parser is reachable through
    /// the production-path harness and produces `irc.message` events.
    ///
    /// This is the Wave A end-to-end proof. Wave B subagents add analogous
    /// tests inside the fenced regions of this file (using `case!()` or by
    /// calling `run()` directly from their own `#[sinex_test]`).
    #[sinex_test]
    async fn weechat_message_canary(_ctx: TestContext) -> TestResult<()> {
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
    use xtask::sandbox::prelude::*;

    const WEECHAT_MESSAGE: &str = "hello from source-worker binary";

    /// Proves the real `sinex-source-worker scan` path for an adapter-backed
    /// source unit: binary launch, adapter config, parser, NATS publish,
    /// ingestd persistence, and DB payload visibility.
    #[sinex_test(timeout = 120)]
    async fn weechat_source_worker_binary_scan_persists_message(
        ctx: TestContext,
    ) -> TestResult<()> {
        let ctx = ctx.with_nats().shared().await?;
        let stack = TestCoreStack::new(&ctx).await?;

        let tempdir = tempfile::tempdir()?;
        let log_path = tempdir.path().join("weechat.log");
        tokio::fs::write(
            &log_path,
            format!("2024-01-15 14:23:45\tsinity\t{WEECHAT_MESSAGE}\n"),
        )
        .await?;
        let worker_dir = tempdir.path().join("worker");
        tokio::fs::create_dir_all(&worker_dir).await?;

        let mut config = TestSourceWorkerConfig::new("weechat");
        config.nats = ctx.nats_handle()?.connection_config();
        config.database_url = ctx.database_url().to_string();
        config.namespace = Some(ctx.pipeline_namespace().prefix().to_string());
        config.work_dir = Some(worker_dir);
        config.node_config = Some(
            serde_json::json!({
                "path": log_path,
                "skip_empty": true,
            })
            .to_string(),
        );

        let output = run_test_source_worker_scan(config, &[], Some(&ctx)).await?;
        ctx.assert("source-worker scan processed one event").that(
            output.stdout.contains("Events processed: 1"),
            "scan output should report one processed event",
        )?;

        WaitHelpers::wait_for_condition(
            || async {
                let count: i64 = sqlx::query_scalar(
                    r"
                    SELECT COUNT(*)::bigint
                    FROM core.events
                    WHERE source = 'irc' AND event_type = 'irc.message'
                    ",
                )
                .fetch_one(ctx.pool())
                .await?;
                Ok::<bool, sqlx::Error>(count >= 1)
            },
            10,
        )
        .await?;

        let payload: serde_json::Value = sqlx::query_scalar(
            r"
            SELECT payload
            FROM core.events
            WHERE source = 'irc' AND event_type = 'irc.message'
            ORDER BY ts_orig
            LIMIT 1
            ",
        )
        .fetch_one(ctx.pool())
        .await?;

        ctx.assert("persisted irc.message payload")
            .eq(&payload["message"].as_str(), &Some(WEECHAT_MESSAGE))?;

        stack.shutdown().await?;
        Ok(())
    }
}
