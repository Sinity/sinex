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
//! ## Binary-launcher gap (substrate note)
//!
//! This obligation drives the dispatch function directly. A full end-to-end
//! path (binary launch → NATS → ingestd → `core.events` DB row) requires
//! `TestSourceWorkerHandle` (the binary launcher, analogous to
//! `start_test_ingestd_with_config`). That helper is marked as a substrate
//! gap: the orchestrator must expose `pub fn source_worker_binary_path(workspace_root)`
//! and `pub async fn start_test_source_worker(config)` before the binary path
//! can be activated. See `substrate_gaps()` below.
//!
//! ## Per-domain fenced regions
//!
//! Wave B subagents add `case!(...)` calls inside the fence for their domain.
//! Do not move or rename the fence comments — the orchestrator uses them for
//! conflict detection.

use crate::AdapterKind;
use sinex_source_worker::dispatch::default_parser_dispatch;
use sinex_primitives::Uuid;

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
    let factory = sinex_source_worker::dispatch::find_parser_factory(source_unit_id)
        .ok_or_else(|| {
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
// Substrate gap: binary launcher
// =============================================================================

/// Returns the substrate gaps that must be filled before the full binary-path
/// obligation (source-worker → NATS → ingestd → DB) can be activated.
///
/// Currently:
///
/// 1. `xtask::sandbox::orchestrator::source_worker_binary_path(workspace_root)` —
///    analogous to `runtime_binary_path` but for `sinex-source-worker`.
///
/// 2. `xtask::sandbox::orchestrator::start_test_source_worker(config)` —
///    spawns the binary with `--source-unit <id>` + env vars pointing at the
///    test NATS URL + DB URL, returns a `TestSourceWorkerHandle` with `stop()`.
///    Modelled on `start_test_ingestd_with_config`.
///
/// Until these are added, obligations drive `default_parser_dispatch()` directly
/// (unit-test level) rather than the full binary path (integration level).
#[allow(dead_code)]
pub fn substrate_gaps() -> &'static [&'static str] {
    &[
        "xtask::sandbox::orchestrator::source_worker_binary_path",
        "xtask::sandbox::orchestrator::start_test_source_worker",
    ]
}
