//! Rule-based tag automaton — deterministic `Transducer` applying
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
//! | Event source | event source = `terminal-source` | `sys.source.terminal` |
//! | Event source | event source = `browser-source` | `sys.source.browser` |
//!
//! ## Input
//!
//! Any event type via `input_event_type = "*"`. The automaton inspects
//! payload fields and applies matching rules.
//!
//! ## Output
//!
//! `knowledge.tag_applied` derived events with `tag_source = "rule"`.
//! Entity ID is the source event ID — tags are applied to the event
//! that triggered them, not to a resolved entity.
//!
use crate::runtime::automaton::{AutomatonContext, DerivedOutput, TransducerAdapter};
use crate::runtime::tags;
use crate::runtime::{AutomatonLogicError, InputProvenanceFilter, Transducer};
use sinex_primitives::events::EventPayload;
use sinex_primitives::events::payloads::KnowledgeTagAppliedPayload;

#[derive(Debug, Clone, Default)]
pub struct TagApplier;

impl Transducer for TagApplier {
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
        KnowledgeTagAppliedPayload::EVENT_TYPE.as_static_str()
    }

    fn output_event_source(&self) -> &'static str {
        KnowledgeTagAppliedPayload::SOURCE.as_static_str()
    }
    fn input_provenance_filter(&self) -> InputProvenanceFilter {
        InputProvenanceFilter::Any
    }

    async fn process(
        &mut self,
        _state: &mut Self::State,
        input: serde_json::Value,
        context: &AutomatonContext,
    ) -> Result<Option<DerivedOutput<KnowledgeTagAppliedPayload>>, AutomatonLogicError> {
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

fn evaluate_rules(input: &serde_json::Value, context: &AutomatonContext) -> Vec<String> {
    let mut tags = Vec::new();

    let event_type = context.event_type.as_str();

    // Source-based rules
    let source = context.source.as_str();
    match source {
        "terminal" | "terminal.zsh-history" | "terminal-source" => {
            tags.push(tags::system::SOURCE_TERMINAL.into());
        }
        "browser.history" | "browser-source" => {
            tags.push(tags::system::SOURCE_BROWSER.into());
        }
        "desktop" | "desktop.activitywatch" | "desktop-source" => {
            tags.push(tags::system::SOURCE_DESKTOP.into());
        }
        "fs" | "fs-watcher" | "fs-source" => tags.push(tags::system::SOURCE_FILE.into()),
        _ => {}
    }

    // File extension rules
    if let Some(path) = input
        .get("path")
        .or_else(|| input.get("file_path"))
        .and_then(|v| v.as_str())
        && let Some(ext) = path.rsplit('.').next()
    {
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

    // MIME-based rules for document.ingested events
    if event_type == "document.ingested"
        && let Some(mime) = input.get("mime_type").and_then(|v| v.as_str())
    {
        tags.extend(tags::auto_tags_for_mime(mime));
    }

    tags
}

pub type TagApplierRuntime = TransducerAdapter<TagApplier>;

// ── Source descriptor ─────────────────────────────────────────────

use sinex_primitives::privacy::ProcessingContext;
use sinex_primitives::source_contracts::{
    AccessScope, CheckpointFamily as ContractCheckpointFamily, Horizon as ContractHorizon,
    OccurrenceIdentity as ContractOccurrenceIdentity, PrivacyTier as ContractPrivacyTier,
    ResourceProfile, RetentionPolicy as ContractRetentionPolicy, RunnerPack,
    RuntimeShape as ContractRuntimeShape, SourceContract, SourceRuntimeBinding, SubjectRef,
};
use sinex_primitives::{register_source_contract, register_source_runtime_binding};

register_source_contract! {
    SourceContract {
        id: "tag-applier",
        namespace: "derived",
        event_types: &[
            ("knowledge-graph", "knowledge.tag_applied"),
        ],
        privacy_tier: ContractPrivacyTier::Sensitive,
        horizons: &[ContractHorizon::Continuous],
        retention: ContractRetentionPolicy::Forever,
        occurrence_identity: ContractOccurrenceIdentity::Uuid5From(
            "(source, parent_event_id, tag_name)",
        ),
        access_scope: AccessScope::Internal,
    }
}

register_source_runtime_binding! {
    SourceRuntimeBinding::builder(
        SubjectRef::from_static("source:tag-applier"),
        "tag-applier",
        "derived",
    )
    .implementation("sinexd")
    .adapter("AutomatonRuntime")
    .output_event_type("knowledge.tag_applied")
    .privacy_context(ProcessingContext::Metadata)
    .resource_profile(ResourceProfile::EventStreamConsumer)
    .source_id("tag-applier")
    .runner_pack(RunnerPack::InProcess)
    .checkpoint_family(ContractCheckpointFamily::AppendStream)
    .runtime_shape(ContractRuntimeShape::Continuous)
    .build_impact(sinex_primitives::source_contracts::SourceBuildImpact::ZERO)
    .build()
}

#[cfg(test)]
#[path = "tag_applier_test.rs"]
mod tests;
