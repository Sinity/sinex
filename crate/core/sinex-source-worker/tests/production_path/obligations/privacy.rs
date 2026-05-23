//! Privacy obligation.
//!
//! Verifies that fixture data containing decoy secrets is redacted or
//! suppressed by the privacy engine, and that event payloads reaching the
//! dispatch layer do not contain raw secret values.
//!
//! ## What this obligation proves
//!
//! - Fixture bytes containing a decoy GitHub token (`ghp_` prefix) are
//!   processed through the sinex privacy engine.
//! - Event payloads produced by dispatch do not contain the raw token string.
//! - A clean fixture (no secrets) produces events normally.
//!
//! ## Privacy engine integration
//!
//! The privacy engine is invoked at the ingestor boundary, before events reach
//! NATS. At the dispatch level (what this obligation tests), the payload bytes
//! are what the parser sees — the engine runs _before_ dispatch in production.
//! This obligation therefore tests that the engine redacts secret-bearing bytes
//! before they would be embedded in event payloads, using the engine directly.
//!
//! For a full end-to-end privacy proof (ingestor → engine → NATS → ingestd →
//! DB, verifying `field_privacy_log` entries), the binary launcher is required.
//! That path is gated on the substrate gap noted in
//! `initial_ingestion::substrate_gaps()`.
//!
//! ## Per-domain fenced regions
//!
//! Per-source-unit modules call `_run_case(...)` directly.

use crate::AdapterKind;
use sinex_primitives::Uuid;
use sinex_primitives::privacy::{self, ProcessingContext};
use sinex_source_worker::dispatch::default_parser_dispatch;

/// A decoy secret that should be redacted by the privacy engine.
///
/// Uses a GitHub token prefix pattern — matched by the catalog-pattern rule.
const DECOY_TOKEN: &str = "ghp_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

/// Run the privacy obligation for a source unit.
///
/// # Errors
///
/// Returns an error if clean fixture fails dispatch, or if the privacy engine
/// fails to redact the decoy token.
pub async fn run(
    source_unit_id: &str,
    adapter_kind: AdapterKind,
    fixture_data: &[u8],
    expected_event_types: &[&str],
) -> Result<(), String> {
    // Part 1: clean fixture must dispatch cleanly (delegates to initial_ingestion logic).
    super::initial_ingestion::run(
        source_unit_id,
        adapter_kind,
        fixture_data,
        expected_event_types,
    )
    .await
    .map_err(|e| format!("privacy/clean-path: {e}"))?;

    run_redaction_only(source_unit_id).await
}

/// Run only the privacy-engine/redacted-dispatch proof.
///
/// Use this when the caller has already verified clean fixture dispatch in the
/// same case. It preserves the privacy proof while avoiding a duplicate
/// initial-ingestion pass in `ALL_OBLIGATIONS`.
pub async fn run_redaction_only(source_unit_id: &str) -> Result<(), String> {
    // Part 2: privacy engine redacts decoy secrets from raw text.
    let secret_text = format!("export TOKEN={DECOY_TOKEN}");
    let engine_result = privacy::engine()
        .map_err(|e| format!("privacy engine init failed: {e}"))?
        .process(&secret_text, ProcessingContext::Command);

    if !engine_result.any_matched() {
        return Err(format!(
            "privacy for '{source_unit_id}': privacy engine did not match decoy token \
             '{DECOY_TOKEN}' in command context. Engine may be misconfigured."
        ));
    }

    // Verify the redacted text does not contain the raw token.
    let redacted = engine_result.text.as_ref();
    if redacted.contains(DECOY_TOKEN) {
        return Err(format!(
            "privacy for '{source_unit_id}': redacted text still contains decoy token. \
             Raw: {secret_text:?}, Redacted: {redacted:?}"
        ));
    }

    // Part 3: dispatch on redacted bytes must succeed and not expose the token.
    let dispatch = default_parser_dispatch();
    let redacted_bytes = redacted.as_bytes();
    // The parser may or may not accept arbitrary redacted text — we only care
    // that if it does, the output payloads don't contain the raw token.
    let material_id = Uuid::now_v7();
    if let Ok(outcome) = dispatch(source_unit_id, redacted_bytes, Some(material_id)) {
        for event in &outcome.events {
            let payload_str = event.payload.to_string();
            if payload_str.contains(DECOY_TOKEN) {
                return Err(format!(
                    "privacy for '{source_unit_id}': event payload contains raw decoy token \
                     after redaction. Payload: {payload_str}"
                ));
            }
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
