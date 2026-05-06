//! Entity extractor automaton — Stage 1 of the entity intelligence pipeline.
//!
//! A [`TransducerNode`] that scans incoming events for deterministic entity
//! patterns (URLs, file paths, commands, emails) and emits `entity.extracted`
//! synthesis events. Downstream stages (resolver, relation extractor, enricher)
//! canonicalize and enrich these raw extractions.
//!
//! ## v1 pattern catalog
//!
//! | Pattern | Example | Entity type |
//! |---------|---------|-------------|
//! | URL | `https://example.com/path` | `url` |
//! | File path | `/home/user/file.txt` | `file` |
//! | Command | `git commit -m "msg"` | `tool` |
//! | Email | `user@example.com` | `person` |
//!
//! ## Input
//!
//! Any event whose payload contains text fields (currently `document.chunked`,
//! `command.canonical`, and `command.executed`).
//!
//! ## Output
//!
//! One `entity.extracted` event per matched pattern, with synthesis provenance
//! from the source event. The entity resolver (Stage 2) assigns deterministic
//! UUIDv5 identities.
//!
//! Ref: `.agent/scratch/071-issue-331-entity-extractor-spec.md`.

use regex::Regex;
use sinex_node_sdk::derived_node::{DerivedOutput, DerivedTriggerContext, TransducerNodeAdapter};
use sinex_node_sdk::{InputProvenanceFilter, NodeLogicError, TransducerNode};
use sinex_primitives::domain::EntityTypeName;
use sinex_primitives::events::payloads::EntityExtractedPayload;
use sinex_primitives::privacy::ProcessingContext;
use std::sync::LazyLock;

// ── Pattern catalog ────────────────────────────────────────────────────

static URL_PATTERN: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new("https?://[^\\s<>\"{}|\\\\^`\\[\\]]+").expect("compile URL regex")
});

static FILE_PATH_PATTERN: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new("(?:/~?|[./]?)(?:[\\w.-]+/)+[\\w.-]+(?:\\.\\w+)?").expect("compile file path regex")
});

static COMMAND_PATTERN: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        "\\b(git|nix|cargo|docker|kubectl|ssh|curl|wget|systemctl|journalctl)\\b",
    )
    .expect("compile command regex")
});

static EMAIL_PATTERN: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new("[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}").expect("compile email regex")
});

// ── Node ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Default)]
pub struct EntityExtractor;

impl TransducerNode for EntityExtractor {
    type State = ();
    type Input = serde_json::Value;
    type Output = EntityExtractedPayload;

    fn name(&self) -> &'static str {
        "entity-extractor"
    }

    fn input_event_type(&self) -> &'static str {
        "*"
    }

    fn output_event_type(&self) -> &'static str {
        "entity.extracted"
    }

    fn output_privacy_context(&self) -> ProcessingContext {
        ProcessingContext::Document
    }

    fn input_provenance_filter(&self) -> InputProvenanceFilter {
        InputProvenanceFilter::Any
    }

    async fn process(
        &mut self,
        _state: &mut Self::State,
        input: serde_json::Value,
        context: &DerivedTriggerContext,
    ) -> Result<Option<DerivedOutput<EntityExtractedPayload>>, NodeLogicError> {
        // Extract text fields from the input event.
        let text = extract_text_fields(&input);

        if text.is_empty() {
            return Ok(None);
        }

        // For v1, emit only the first matched entity per event to keep
        // the event stream bounded. The resolver and downstream stages
        // handle deduplication and enrichment.
        if let Some(entity) = find_first_entity(&text) {
            let ts_orig = context.ts_orig.unwrap_or_else(sinex_primitives::Timestamp::now);
            let output =
                DerivedOutput::transduced(entity, ts_orig, context.trigger_uuid());
            Ok(Some(output))
        } else {
            Ok(None)
        }
    }
}

// ── Entity extraction ──────────────────────────────────────────────────

struct ExtractedEntity {
    entity_type: EntityTypeName,
    raw_name: String,
    confidence: f64,
}

fn extract_text_fields(value: &serde_json::Value) -> String {
    let mut text = String::new();

    // Recurse into objects and collect all string values.
    fn collect_strings(value: &serde_json::Value, buf: &mut String) {
        match value {
            serde_json::Value::String(s) => {
                if !buf.is_empty() {
                    buf.push(' ');
                }
                buf.push_str(s);
            }
            serde_json::Value::Object(map) => {
                for (key, val) in map {
                    // Skip known non-text fields.
                    if matches!(
                        key.as_str(),
                        "id"
                            | "document_id"
                            | "event_id"
                            | "source_material_id"
                            | "ts_orig"
                            | "timestamp"
                            | "byte_offset"
                            | "source_anchor"
                            | "chunk_index"
                    ) || key.ends_with("_offset")
                        || key.ends_with("_id")
                        || key.ends_with("_at")
                    {
                        continue;
                    }
                    collect_strings(val, buf);
                }
            }
            serde_json::Value::Array(arr) => {
                for val in arr {
                    collect_strings(val, buf);
                }
            }
            _ => {}
        }
    }

    collect_strings(value, &mut text);
    text
}

fn find_first_entity(text: &str) -> Option<EntityExtractedPayload> {
    // Try patterns in priority order: URL > file path > email > command.

    if let Some(m) = URL_PATTERN.find(text) {
        let raw = m.as_str().to_string();
        return Some(EntityExtractedPayload {
            entity_type: EntityTypeName::new("url"),
            raw_name: raw,
            confidence: 0.95,
        });
    }

    if let Some(m) = EMAIL_PATTERN.find(text) {
        let raw = m.as_str().to_string();
        return Some(EntityExtractedPayload {
            entity_type: EntityTypeName::new("person"),
            raw_name: raw,
            confidence: 0.9,
        });
    }

    if let Some(m) = FILE_PATH_PATTERN.find(text) {
        let raw = m.as_str().to_string();
        // Only match paths that look like real file paths (contain a directory
        // separator and are at least 4 characters).
        if raw.len() >= 4 && raw.contains('/') {
            return Some(EntityExtractedPayload {
                entity_type: EntityTypeName::new("file"),
                raw_name: raw,
                confidence: 0.7,
            });
        }
    }

    if let Some(m) = COMMAND_PATTERN.find(text) {
        let raw = m.as_str().to_string();
        return Some(EntityExtractedPayload {
            entity_type: EntityTypeName::new("tool"),
            raw_name: raw,
            confidence: 0.85,
        });
    }

    None
}

// ── Type alias ──────────────────────────────────────────────────────────

pub type EntityExtractorNode = TransducerNodeAdapter<EntityExtractor>;

// ── Source-unit descriptor ─────────────────────────────────────────────

use sinex_primitives::proof::{
    CheckpointFamily as SuCheckpointFamily, Horizon as SuHorizon,
    OccurrenceIdentity as SuOccurrenceIdentity, PrivacyTier as SuPrivacyTier,
    RetentionPolicy as SuRetentionPolicy, RuntimeShape as SuRuntimeShape, SourceUnitDescriptor,
};
use sinex_primitives::register_source_unit;

register_source_unit! {
    SourceUnitDescriptor {
        id: "entity-extractor",
        namespace: "derived",
        runner_pack: "process",
        checkpoint_family: SuCheckpointFamily::AppendStream,
        event_types: &[
            ("entity-extractor", "entity.extracted"),
        ],
        privacy_tier: SuPrivacyTier::Sensitive,
        runtime_shape: SuRuntimeShape::Continuous,
        horizons: &[SuHorizon::Continuous],
        retention: SuRetentionPolicy::Forever,
        proof_obligations: &[],
        occurrence_identity: SuOccurrenceIdentity::Uuid5From(
            "(source_unit, parent_event_id, entity_type, raw_name)",
        ),
        access_policy: "event_stream_read",
        package_impact: "no_new_output",
        implementation_mode: "rust_in_pack:process",
        build_impact: sinex_primitives::proof::SourceUnitBuildImpact::ZERO,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use xtask::sandbox::{sinex_test, TestResult};
    use serde_json::json;

    #[sinex_test]
    async fn test_url_extraction() -> TestResult<()> {
        let text = "Check out https://github.com/Sinity/sinex for more info.";
        let result = find_first_entity(text);
        assert!(result.is_some());
        let entity = result.unwrap();
        assert_eq!(entity.entity_type, EntityTypeName::new("url"));
        assert!(entity.raw_name.contains("github.com"));
        Ok(())
    }

    #[sinex_test]
    async fn test_email_extraction() -> TestResult<()> {
        let text = "Contact user@example.com for support.";
        let result = find_first_entity(text);
        assert!(result.is_some());
        let entity = result.unwrap();
        assert_eq!(entity.entity_type, EntityTypeName::new("person"));
        assert_eq!(entity.raw_name, "user@example.com");
        Ok(())
    }

    #[sinex_test]
    async fn test_file_path_extraction() -> TestResult<()> {
        let text = "Reading from /home/user/.config/nix/nix.conf.";
        let result = find_first_entity(text);
        assert!(result.is_some());
        let entity = result.unwrap();
        assert_eq!(entity.entity_type, EntityTypeName::new("file"));
        Ok(())
    }

    #[sinex_test]
    async fn test_command_extraction() -> TestResult<()> {
        let text = "Run nix build to compile the project.";
        let result = find_first_entity(text);
        assert!(result.is_some());
        let entity = result.unwrap();
        assert_eq!(entity.entity_type, EntityTypeName::new("tool"));
        assert_eq!(entity.raw_name, "nix");
        Ok(())
    }

    #[sinex_test]
    async fn test_url_priority_over_file_path() -> TestResult<()> {
        let text = "See https://example.com/foo/bar for details.";
        let result = find_first_entity(text);
        assert!(result.is_some());
        let entity = result.unwrap();
        // URL should match first, not file path
        assert_eq!(entity.entity_type, EntityTypeName::new("url"));
        Ok(())
    }

    #[sinex_test]
    async fn test_empty_text() -> TestResult<()> {
        let result = find_first_entity("");
        assert!(result.is_none());
        Ok(())
    }

    #[sinex_test]
    async fn test_no_entity() -> TestResult<()> {
        let result = find_first_entity("This is a simple sentence with nothing extractable.");
        assert!(result.is_none());
        Ok(())
    }

    #[sinex_test]
    async fn test_extract_text_fields() -> TestResult<()> {
        let input = json!({
            "text": "Hello https://example.com world",
            "id": "should-be-skipped",
            "byte_offset": 42,
            "nested": {"body": "another text"}
        });
        let text = extract_text_fields(&input);
        assert!(text.contains("Hello"));
        assert!(text.contains("https://example.com"));
        assert!(text.contains("another text"));
        assert!(!text.contains("should-be-skipped"));
        Ok(())
    }
}
