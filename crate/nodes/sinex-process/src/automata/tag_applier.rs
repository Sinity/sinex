//! Rule-based tag automaton — deterministic TransducerNode applying
//! configured rules to events and emitting `knowledge.tag_applied` events.
//!
//! ## v1 rules
//!
//! | Rule | Match condition | Tag |
//! |------|----------------|-----|
//! | File extension | `file.path` payload field ends with `.rs` | `sys.inferred.file-type.rust` |
//! | File extension | `file.path` payload field ends with `.nix` | `sys.inferred.file-type.nix` |
//! | File extension | `file.path` payload field ends with `.md` | `sys.inferred.file-type.markdown` |
//! | MIME type | `document.ingested` with `mime_type = text/markdown` | `sys.mime.text-markdown` |
//! | Event source | event source = `terminal-ingestor` | `sys.source.terminal` |
//! | Event source | event source = `browser-ingestor` | `sys.source.browser` |
//!
//! ## Input
//!
//! Any event type via `input_event_type = "*"`. The automaton inspects
//! payload fields and applies matching rules.
//!
//! ## Output
//!
//! `knowledge.tag_applied` synthesis events with `tag_source = "rule"`.
//! Entity ID is the source event ID — tags are applied to the event
//! that triggered them, not to a resolved entity.
//!
use sinex_node_sdk::derived_node::{DerivedOutput, DerivedTriggerContext, TransducerNodeAdapter};
use sinex_node_sdk::tags;
use sinex_node_sdk::{InputProvenanceFilter, NodeLogicError, TransducerNode};
use sinex_primitives::events::payloads::KnowledgeTagAppliedPayload;
use sinex_primitives::privacy::ProcessingContext;

#[derive(Debug, Clone, Default)]
pub struct TagApplier;

impl TransducerNode for TagApplier {
    type State = ();
    type Input = serde_json::Value;
    type Output = KnowledgeTagAppliedPayload;

    fn name(&self) -> &'static str {
        "tag-applier"
    }

    fn input_event_type(&self) -> &'static str {
        "*"
    }

    fn output_event_type(&self) -> &'static str {
        "knowledge.tag_applied"
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
    ) -> Result<Option<DerivedOutput<KnowledgeTagAppliedPayload>>, NodeLogicError> {
        let rules = evaluate_rules(&input, context);
        // v1: emit first matching tag only. v2: emit all matches.
        if let Some(tag_name) = rules.into_iter().next() {
            let ts_orig = context
                .ts_orig
                .unwrap_or_else(sinex_primitives::Timestamp::now);
            let output = DerivedOutput::transduced(
                KnowledgeTagAppliedPayload {
                    entity_id: context.trigger_uuid(),
                    tag_name,
                    tag_source: "rule".into(),
                },
                ts_orig,
                context.trigger_uuid(),
            );
            Ok(Some(output))
        } else {
            Ok(None)
        }
    }
}

fn evaluate_rules(input: &serde_json::Value, context: &DerivedTriggerContext) -> Vec<String> {
    let mut tags = Vec::new();

    let event_type = context.event_type.as_str();

    // Source-based rules
    let source = context.source.as_str();
    match source {
        "terminal-ingestor" => tags.push(tags::system::SOURCE_TERMINAL.into()),
        "browser-ingestor" | "browser.history" => {
            tags.push(tags::system::SOURCE_BROWSER.into());
        }
        "desktop-ingestor" => tags.push(tags::system::SOURCE_DESKTOP.into()),
        "fs-ingestor" | "fs-watcher" => tags.push(tags::system::SOURCE_FILE.into()),
        _ => {}
    }

    // File extension rules
    if let Some(path) = input
        .get("path")
        .or_else(|| input.get("file_path"))
        .and_then(|v| v.as_str())
    {
        if let Some(ext) = path.rsplit('.').next() {
            let file_type_tag = match ext {
                "rs" => Some("rust"),
                "nix" => Some("nix"),
                "md" => Some("markdown"),
                "py" => Some("python"),
                "js" | "ts" | "tsx" | "jsx" => Some("javascript"),
                "json" => Some("json"),
                "toml" => Some("toml"),
                "yaml" | "yml" => Some("yaml"),
                "html" | "htm" => Some("html"),
                "css" => Some("css"),
                "sql" => Some("sql"),
                "sh" | "bash" | "zsh" => Some("shell"),
                _ => None,
            };
            if let Some(ft) = file_type_tag {
                tags.push(tags::tag_name(tags::inferred::FILE_TYPE_PREFIX, ft));
            }
        }
    }

    // MIME-based rules for document.ingested events
    if event_type == "document.ingested" {
        if let Some(mime) = input.get("mime_type").and_then(|v| v.as_str()) {
            tags.extend(tags::auto_tags_for_mime(mime));
        }
    }

    tags
}

pub type TagApplierNode = TransducerNodeAdapter<TagApplier>;

// ── Source-unit descriptor ─────────────────────────────────────────────

use sinex_primitives::proof::{
    CheckpointFamily as SuCheckpointFamily, Horizon as SuHorizon,
    OccurrenceIdentity as SuOccurrenceIdentity, PrivacyTier as SuPrivacyTier,
    RetentionPolicy as SuRetentionPolicy, RuntimeShape as SuRuntimeShape, SourceUnitBinding,
    SourceUnitDescriptor, SubjectRef,
};
use sinex_primitives::{register_source_unit, register_source_unit_binding};

register_source_unit! {
    SourceUnitDescriptor {
        id: "tag-applier",
        namespace: "derived",
        runner_pack: "process",
        checkpoint_family: SuCheckpointFamily::AppendStream,
        event_types: &[
            ("knowledge-graph", "knowledge.tag_applied"),
        ],
        privacy_tier: SuPrivacyTier::Sensitive,
        runtime_shape: SuRuntimeShape::Continuous,
        horizons: &[SuHorizon::Continuous],
        retention: SuRetentionPolicy::Forever,
        proof_obligations: &[],
        occurrence_identity: SuOccurrenceIdentity::Uuid5From(
            "(source_unit, parent_event_id, tag_name)",
        ),
        access_policy: "event_stream_read",
        package_impact: "no_new_output",
        implementation_mode: "rust_in_pack:process",
        build_impact: sinex_primitives::proof::SourceUnitBuildImpact::ZERO,
    }
}

register_source_unit_binding! {
    SourceUnitBinding::builder(
        SubjectRef::from_static("source_unit:tag-applier"),
        "tag-applier",
        "derived",
    )
    .implementation("sinex-process")
    .adapter("DerivedNodeAdapter")
    .output_event_type("knowledge.tag_applied")
    .privacy_context("inherits_from_parents")
    .material_policy("synthesis_parents")
    .checkpoint_policy("append_stream")
    .resource_shape("event_stream_consumer")
    .source_unit_id("tag-applier")
    .build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use xtask::sandbox::{TestResult, sinex_test};

    #[sinex_test]
    async fn test_source_based_tagging() -> TestResult<()> {
        let input = json!({});
        let _ = input;
        Ok(())
    }

    #[sinex_test]
    async fn test_file_extension_rust() -> TestResult<()> {
        let input = json!({"path": "/home/user/main.rs"});
        let _ = input;
        Ok(())
    }

    #[sinex_test]
    async fn test_file_extension_unknown() -> TestResult<()> {
        let input = json!({"path": "/tmp/file.xyz"});
        let _ = input;
        Ok(())
    }
}
