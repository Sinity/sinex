//! Initial ingestion obligation.
//!
//! Verifies that for a registered source, running the dispatch function
//! against fixture material produces events of the expected types.
//!
//! ## What this obligation proves
//!
//! - The source's parser is registered in the dispatch registry.
//! - The parser accepts the fixture bytes without error.
//! - The parser emits at least one event per expected type.
//!
//! ## Binary path coverage
//!
//! The default obligation still drives the parser dispatch function directly
//! so Wave-B cases can cover many source contracts cheaply. The `binary_path`
//! canary below separately launches the real `sinexd` binary,
//! publishes through NATS, runs event_engine, and verifies the resulting DB row.
//!
//! ## Per-domain fenced regions
//!
//! Per-source modules call `_run_case(...)` directly. Do not move or
//! rename the fence comments — the orchestrator uses them for conflict
//! detection.

use crate::AdapterKind;
use sinex_primitives::Uuid;
use sinexd::sources::dispatch::default_parser_dispatch;

/// Run the initial ingestion obligation for a source.
///
/// # Parameters
///
/// - `source_id` — the registered source id
/// - `adapter_kind` — which adapter this unit uses (informational; dispatch is byte-level)
/// - `fixture_data` — raw bytes to dispatch through the parser
/// - `expected_event_types` — event type strings that must appear in parse output
///
/// # Errors
///
/// Returns an error string if the parser is missing, returns an error, or
/// the expected event types are not all present in the output.
pub async fn run(
    source_id: &str,
    _adapter_kind: AdapterKind,
    fixture_data: &[u8],
    expected_event_types: &[&str],
) -> Result<(), String> {
    // Verify registry entry exists before dispatching.
    let validated_id = sinex_primitives::parser::SourceId::new(source_id)
        .map_err(|e| format!("invalid source id '{source_id}': {e}"))?;
    let factory =
        sinexd::sources::dispatch::find_parser_factory(&validated_id).ok_or_else(|| {
            format!(
                "source '{source_id}' has no parser registered. \
                 Register it with register_source!(source_id: \"{source_id}\", parser: YourParser) \
                 in the source's module."
            )
        })?;
    let _ = factory; // existence check only

    let dispatch = default_parser_dispatch();
    let material_id = Uuid::now_v7();

    let outcome = dispatch(source_id, fixture_data, Some(material_id))
        .map_err(|e| format!("dispatch error for '{source_id}': {e}"))?;

    if outcome.events.is_empty() {
        return Err(format!(
            "initial ingestion for '{source_id}': parser returned no events for fixture data ({} bytes)",
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
                "initial ingestion for '{source_id}': expected event type '{expected}' \
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
// To adapt for your source:
// 1. Change `source_id` to your unit's registered id.
// 2. Change `adapter_kind` to the appropriate `AdapterKind` variant.
// 3. Replace `WEECHAT_FIXTURE_LINE` with fixture bytes for your source.
// 4. Update `expected_event_types` to match your parser's output.
// 5. Move the `_run_case(...)` call inside your domain's fenced region above.

/// Canary: proves `weechat.message` declarative parser round-trips through
/// the harness end-to-end. Used as a copy-paste template for Wave B subagents.
#[cfg(test)]
#[path = "initial_ingestion_canary.rs"]
mod canary;

// =============================================================================
// Binary path canary
// =============================================================================

#[cfg(test)]
#[path = "initial_ingestion_binary_path.rs"]
mod binary_path;
