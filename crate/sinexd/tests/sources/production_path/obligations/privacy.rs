//! Privacy metadata obligation.
//!
//! Verifies that sensitive source parsers declare privacy context metadata
//! for the DB-backed admission policy.
//!
//! ## What this obligation proves
//!
//! - A clean fixture (no secrets) produces events normally.
//! - Non-public source contracts expose parser privacy contexts.
//!
//! ## Privacy engine integration
//!
//! The parser boundary is not a redaction boundary. Parsers preserve
//! interpreted payload values and attach `ProcessingContext` metadata; event
//! admission owns redaction, hashing, encryption, and suppression.
//!
//! ## Per-domain fenced regions
//!
//! Per-source modules call `_run_case(...)` directly.

use crate::AdapterKind;
use sinex_primitives::parser::SourceId;
use sinex_primitives::source_contracts::{self, PrivacyTier};
use sinexd::sources::dispatch::find_parser_factory;

/// Run the privacy obligation for a source.
///
/// # Errors
///
/// Returns an error if clean fixture fails dispatch, or if a non-public source
/// unit lacks parser privacy context metadata.
pub async fn run(
    source_id: &str,
    adapter_kind: AdapterKind,
    fixture_data: &[u8],
    expected_event_types: &[&str],
) -> Result<(), String> {
    // Part 1: clean fixture must dispatch cleanly (delegates to initial_ingestion logic).
    super::initial_ingestion::run(source_id, adapter_kind, fixture_data, expected_event_types)
        .await
        .map_err(|e| format!("privacy/clean-path: {e}"))?;

    run_metadata_only(source_id).await
}

/// Run only the privacy metadata check.
///
/// Use this when the caller has already verified clean fixture dispatch in the
/// same case. It preserves the privacy check while avoiding a duplicate
/// initial-ingestion pass in `ALL_OBLIGATIONS`.
pub async fn run_metadata_only(source_id: &str) -> Result<(), String> {
    let source_id = SourceId::new(source_id.to_owned())
        .map_err(|error| format!("privacy metadata: invalid source id: {error}"))?;
    let descriptor = source_contracts::find_source_contract(&source_id)
        .ok_or_else(|| format!("privacy metadata: unknown source '{}'", source_id.as_str()))?;
    if descriptor.privacy_tier == PrivacyTier::Public {
        return Ok(());
    }

    let factory = find_parser_factory(&source_id).ok_or_else(|| {
        format!(
            "privacy metadata: source '{}' has no registered parser factory",
            source_id.as_str()
        )
    })?;
    let manifest = factory().manifest();
    if manifest.privacy_contexts.is_empty() {
        return Err(format!(
            "privacy metadata: non-public source '{}' parser '{}' declares no privacy contexts",
            source_id.as_str(),
            manifest.parser_id
        ));
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
