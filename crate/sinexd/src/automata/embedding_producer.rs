//! Embedding producer — Transducer for document chunk embeddings (#1076).

use crate::runtime::automaton::{AutomatonContext, DerivedOutput, TransducerAdapter};
use crate::runtime::{AutomatonLogicError, InputProvenanceFilter, Transducer};
use serde_json::Value as JsonValue;
use sinex_primitives::derivation::{
    ClaimSupportTemplate, DerivationOutputDeclaration, DerivationWriteSurface,
    DerivedProductClass, InputEligibility,
};
use sinex_primitives::llm::{ModelEffectRequest, hash_model_input};

/// Derivation control-plane declaration for `embedding-producer` (sinex-0vx.1/0vx.3).
///
/// `report_artifact`: the current output is an effect-key/replay-policy
/// receipt for a chunk embedding, not the embedding vector itself — never a
/// default-eligible input to further canonical derivation. Support defaults
/// to the doctrine-mandated unknown/low baseline (`ClaimSupportTemplate::UNKNOWN`)
/// rather than a fabricated `model_inferred` grade until the real vector is
/// recorded (see blueprint `04-interpretation-plane-blueprint.report.md`
/// per-automaton classification table).
pub const EMBEDDING_PRODUCER_OUTPUT_DECLARATIONS: &[DerivationOutputDeclaration] =
    &[DerivationOutputDeclaration {
        declaration_id: "embedding-producer.document.embedded",
        owner: "embedding-producer",
        product_class: DerivedProductClass::ReportArtifact,
        write_surface: DerivationWriteSurface::DerivedOutput,
        output_source: Some("embedding-producer"),
        output_event_type: Some("document.embedded"),
        projection_kind: None,
        artifact_kind: None,
        proposal_kind: None,
        semantics_version: "1.0.0",
        input_eligibility: InputEligibility::NeverInput,
        default_support: ClaimSupportTemplate::UNKNOWN,
        verification_command: "xtask test -p sinexd -E 'test(embedding_producer)'",
    }];

#[derive(Default)]
pub struct EmbeddingProducer;

impl Transducer for EmbeddingProducer {
    type State = ();
    type Input = JsonValue;
    type Output = JsonValue;

    fn name(&self) -> &'static str {
        "embedding-producer"
    }
    fn input_event_type(&self) -> &'static str {
        "document.chunked"
    }
    fn output_event_type(&self) -> &'static str {
        "document.embedded"
    }
    fn output_event_source(&self) -> &'static str {
        "embedding-producer"
    }
    fn input_provenance_filter(&self) -> InputProvenanceFilter {
        InputProvenanceFilter::Any
    }

    const OUTPUT_DECLARATIONS: &'static [DerivationOutputDeclaration] =
        EMBEDDING_PRODUCER_OUTPUT_DECLARATIONS;

    async fn process(
        &mut self,
        _state: &mut Self::State,
        input: Self::Input,
        ctx: &AutomatonContext,
    ) -> Result<Option<DerivedOutput<Self::Output>>, AutomatonLogicError> {
        let chunk_id = input
            .get("chunk_id")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");
        let chunk_hash = input
            .get("chunk_hash")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let document_id = input
            .get("document_id")
            .and_then(|v| v.as_str())
            .unwrap_or("");

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
        let declaration = &EMBEDDING_PRODUCER_OUTPUT_DECLARATIONS[0];
        Ok(Some(
            DerivedOutput::transduced(
                serde_json::json!({
                    "chunk_id": chunk_id,
                    "document_id": document_id,
                    "chunk_hash": chunk_hash,
                    "effect_key": request.composite_key(),
                    "replay_policy": "reuse_recorded",
                }),
                ts_orig,
                source_id,
            )
            .with_declaration_id(declaration.declaration_id)
            .with_product_class(declaration.product_class)
            // Zero evidence counts: this is a receipt, not the embedding vector
            // itself (see the declaration's doc — ClaimSupportTemplate::UNKNOWN
            // until sinex-5v6 lands real model effects).
            .with_claim_support(declaration.default_support.instantiate(0, 0, 0, 0)),
        ))
    }
}

pub type EmbeddingProducerRuntime = TransducerAdapter<EmbeddingProducer>;
