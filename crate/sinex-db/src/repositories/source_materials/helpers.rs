use super::types::material_types;
use serde_json::Value as JsonValue;
use sinex_primitives::domain::{SourceMaterialFormat, SourceMaterialTimingInfoType};
use sinex_primitives::rpc::sources::{
    SOURCE_MATERIAL_CONTRACT_METADATA_KEY, SourceMaterialMetadataContract,
    SourceMaterialStatistics, SourceOrigin,
};

/// Top-level metadata keys reserved for system use.
///
/// These keys are set exclusively by the DB layer or the runtime and must not
/// be overwritten by caller-supplied payloads passed to `update_metadata`.
/// The `update_metadata` path re-applies existing values for these keys on top
/// of any caller merge, so the system always wins on conflicts.
pub(super) const RESERVED_METADATA_KEYS: &[&str] = &[
    "encoding",
    "content_preview",
    "retention_policy",
    "blake3",
    "total_bytes",
    SOURCE_MATERIAL_CONTRACT_METADATA_KEY,
    "staged_by",
    "staged_on_host",
    "_meta",
];

pub(super) fn contract_for_source(
    format: SourceMaterialFormat,
    timing: SourceMaterialTimingInfoType,
    source_uri: Option<&str>,
    total_bytes: Option<i64>,
) -> SourceMaterialMetadataContract {
    let mut contract = SourceMaterialMetadataContract::new(format, timing);
    contract.origin = source_uri.map(|uri| SourceOrigin {
        source_uri: Some(uri.to_string()),
        ..SourceOrigin::default()
    });
    contract.statistics = Some(SourceMaterialStatistics {
        total_bytes,
        ..SourceMaterialStatistics::default()
    });
    contract
}

pub(super) fn format_for_material_type(
    material_type: &str,
    source_uri: Option<&str>,
) -> SourceMaterialFormat {
    match material_type {
        material_types::FILE => source_uri.map_or(
            SourceMaterialFormat::Unknown,
            SourceMaterialFormat::infer_from_path,
        ),
        material_types::STREAM => SourceMaterialFormat::Jsonl,
        material_types::BLOB_TEXT => SourceMaterialFormat::Text,
        material_types::BLOB | material_types::BLOB_BINARY => SourceMaterialFormat::Binary,
        material_types::CHUNK => SourceMaterialFormat::Binary,
        _ => SourceMaterialFormat::Unknown,
    }
}

/// Strip reserved system keys from a caller-supplied metadata object so they
/// cannot be overwritten via `update_metadata`. Non-object values are
/// returned unchanged (they carry no top-level keys to strip).
pub(super) fn strip_reserved_metadata_keys(mut metadata: JsonValue) -> JsonValue {
    if let JsonValue::Object(ref mut map) = metadata {
        for key in RESERVED_METADATA_KEYS {
            map.remove(*key);
        }
    }
    metadata
}

pub(super) fn derive_source_family(source_identifier: &str, _material_kind: &str) -> &'static str {
    let lower = source_identifier.to_ascii_lowercase();
    if lower.starts_with("integration.") || lower.starts_with("analysis.") {
        // External producer envelopes use dotted source identifiers.
        return "integration";
    }
    if lower.contains("atuin") || lower.contains("zsh_history") {
        return "terminal";
    }
    if lower.contains("firefox") || lower.contains("chromium") || lower.contains("places.sqlite") {
        return "browser";
    }
    if lower.contains("activitywatch") {
        return "desktop";
    }
    if lower.contains("polylogue") || lower.contains("conversations") {
        return "chat";
    }
    if lower.contains("/var/log") || lower.contains("journal") {
        return "system";
    }
    "generic"
}

/// Apply privacy redaction to a source identifier for display in readiness.
///
/// Filesystem paths are routed through [`sinex_primitives::privacy::classify_material_path`].
/// Dotted identifiers (e.g. `integration.polylogue`) pass through unchanged.
pub(super) fn redact_identifier_for_display(source_identifier: &str) -> String {
    if source_identifier.starts_with('/') || source_identifier.starts_with('~') {
        let (_class, display) =
            sinex_primitives::privacy::classify_material_path(source_identifier);
        if display.is_empty() {
            "<redacted>".to_string()
        } else {
            display
        }
    } else {
        source_identifier.to_string()
    }
}

pub(super) fn is_valid_relation_type(value: &str) -> bool {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return false;
    };

    first.is_ascii_lowercase()
        && chars.all(|ch| {
            ch.is_ascii_lowercase() || ch.is_ascii_digit() || matches!(ch, '_' | '.' | '-')
        })
}
