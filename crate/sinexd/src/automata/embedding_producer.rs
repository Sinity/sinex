//! Embedding producer — Transducer for document chunk embeddings (#1076).

use crate::node_sdk::derived_node::{AutomatonContext, DerivedOutput, TransducerNodeAdapter};
use crate::node_sdk::{InputProvenanceFilter, NodeLogicError, Transducer};
use serde_json::Value as JsonValue;
use sinex_primitives::llm::{ModelEffectRequest, hash_model_input};
use sinex_primitives::privacy::ProcessingContext;

#[derive(Default)]
pub struct EmbeddingProducer;

impl Transducer for EmbeddingProducer {
    type State = ();
    type Input = JsonValue;
    type Output = JsonValue;

    fn name(&self) -> &'static str { "embedding-producer" }
    fn input_event_type(&self) -> &'static str { "document.chunked" }
    fn output_event_type(&self) -> &'static str { "document.embedded" }
    fn output_event_source(&self) -> &'static str { "embedding-producer" }
    fn input_provenance_filter(&self) -> InputProvenanceFilter {
        InputProvenanceFilter::Any
    }
    fn output_privacy_context(&self) -> ProcessingContext {
        ProcessingContext::Document
    }

    async fn process(
        &mut self,
        _state: &mut Self::State,
        input: Self::Input,
        ctx: &AutomatonContext,
    ) -> Result<Option<DerivedOutput<Self::Output>>, NodeLogicError> {
        let chunk_id = input.get("chunk_id").and_then(|v| v.as_str()).unwrap_or("unknown");
        let chunk_hash = input.get("chunk_hash").and_then(|v| v.as_str()).unwrap_or("");
        let document_id = input.get("document_id").and_then(|v| v.as_str()).unwrap_or("");

        let input_hash = hash_model_input(&input.to_string());
        let request = ModelEffectRequest {
            provider: "local".into(),
            model: "embedding".into(),
            prompt_hash: chunk_hash.into(),
            schema_hash: None,
            input_hash,
        };

        let ts_orig = ctx.require_ts_orig()?;
        let source_id = ctx.trigger_uuid();
        Ok(Some(DerivedOutput::transduced(
            serde_json::json!({
                "chunk_id": chunk_id,
                "document_id": document_id,
                "chunk_hash": chunk_hash,
                "effect_key": request.composite_key(),
                "replay_policy": "reuse_recorded",
            }),
            ts_orig,
            source_id,
        )))
    }
}

pub type EmbeddingProducerNode = TransducerNodeAdapter<EmbeddingProducer>;
