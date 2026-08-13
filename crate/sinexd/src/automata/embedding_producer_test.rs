use super::*;
use crate::runtime::Transducer;
use crate::runtime::automaton::AutomatonContext;
use async_trait::async_trait;
use sinex_db::DynamicPayload;
use sinex_primitives::domain::{ProcessingMode, TriggerKind};
use sinex_primitives::events::{Event, SourceMaterial};
use sinex_primitives::{EventSource, EventType, Id, JsonValue, Timestamp, Uuid};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use xtask::sandbox::{prelude::*, sinex_test};

#[derive(Clone)]
struct FakeEmbeddingProvider {
    calls: Arc<AtomicUsize>,
    provider: &'static str,
    model: &'static str,
    dimensions: i32,
}

#[async_trait]
impl EmbeddingProvider for FakeEmbeddingProvider {
    fn provider(&self) -> &str {
        self.provider
    }

    fn model(&self) -> &str {
        self.model
    }

    fn dimensions(&self) -> i32 {
        self.dimensions
    }

    async fn embed_batch(&self, texts: &[String]) -> sinex_primitives::Result<Vec<Vec<f32>>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(texts.iter().map(|_| vec![1.0, 0.0, 0.0]).collect())
    }
}

fn fake_config() -> EmbeddingWorkerConfig {
    EmbeddingWorkerConfig {
        max_events_per_run: 100,
        max_materials_per_run: 10,
        allowed_models: vec!["fake/test-model".into()],
        allowed_event_types: vec!["document.chunked".into()],
        allowed_source_families: vec!["document-parser".into()],
        allowed_material_source_prefixes: vec!["document".into()],
        max_estimated_tokens: 10_000,
        max_estimated_cost_microusd: 10_000,
        estimated_cost_per_1k_tokens_microusd: 1_000,
        model_provider: "fake".into(),
        model: "test-model".into(),
        model_dimensions: 3,
        batch_size: 32,
        ollama_base_url: "http://127.0.0.1:9".into(),
    }
}

fn chunked_context() -> AutomatonContext {
    let trigger_event_id: Id<Event<JsonValue>> = Id::new();
    AutomatonContext {
        trigger_event_id,
        source: EventSource::from_static("document-parser"),
        event_type: EventType::from_static("document.chunked"),
        ts_orig: Some(Timestamp::now()),
        ts_coided: trigger_event_id
            .timestamp()
            .expect("test ID must be UUIDv7"),
        processing_mode: ProcessingMode::Live,
        trigger_kind: TriggerKind::NewEvent,
        created_by_operation_id: None,
        trigger_material_id: None,
        trigger_anchor_byte: None,
    }
}

async fn seed_document_parent(
    ctx: &TestContext,
    material_id: Id<SourceMaterial>,
    document_id: Uuid,
) -> TestResult<()> {
    ctx.pool
        .events()
        .insert(
            DynamicPayload::new(
                "document-parser",
                "document.parsed",
                serde_json::json!({
                    "document_id": document_id,
                    "kind": "dendron_markdown",
                    "natural_key": format!("test/{document_id}"),
                    "extraction_version": 1,
                    "chunk_count": 1,
                    "text_byte_len": 32,
                    "side_data": {},
                }),
            )
            .from_material(material_id)
            .build()?,
        )
        .await?;
    Ok(())
}

// Regression test for sinex-im80: embedding_producer previously constructed
// its DerivedOutput without equivalence_key/semantics_version. See
// entity_extractor_test.rs's identical regression test for the full
// rationale. embedding_producer had no test file at all before this commit.
#[sinex_test]
async fn embedding_producer_stamps_equivalence_key_and_semantics_version() -> TestResult<()> {
    let context = chunked_context();
    let input = serde_json::json!({
        "chunk_id": "chunk-abc123",
        "chunk_hash": "blake3:deadbeef",
        "document_id": "doc-1",
    });

    let output = EmbeddingProducer
        .process(&mut (), input, &context)
        .await?
        .expect("valid chunk input should produce a document.embedded receipt");

    assert_eq!(
        output.semantics_version.as_deref(),
        Some("1.0.0"),
        "semantics_version must match the declared DerivationOutputDeclaration value"
    );
    assert_eq!(
        output.equivalence_key.as_deref(),
        Some("embedding-producer:chunk-abc123"),
        "equivalence_key must be keyed by chunk_id so re-processing the same chunk \
         (e.g. after a restart-during-catchup) dedupes to one receipt, not a duplicate"
    );
    Ok(())
}

// A chunk with no chunk_id falls back to the literal "unknown" per
// embedding_producer.rs's `.unwrap_or("unknown")` — document this as
// intentional current behavior (a real bug on its own, tracked separately,
// not this regression test's concern) rather than let it silently produce
// an untested equivalence_key shape.
#[sinex_test]
async fn embedding_producer_missing_chunk_id_uses_unknown_placeholder() -> TestResult<()> {
    let context = chunked_context();
    let input = serde_json::json!({
        "chunk_hash": "blake3:deadbeef",
        "document_id": "doc-1",
    });

    let output = EmbeddingProducer
        .process(&mut (), input, &context)
        .await?
        .expect("missing chunk_id should still produce a receipt with a placeholder key");

    assert_eq!(
        output.equivalence_key.as_deref(),
        Some("embedding-producer:unknown"),
        "documents current (arguably still-buggy) fallback behavior: chunks missing \
         chunk_id collapse onto a shared equivalence_key and will dedupe against each \
         other incorrectly -- see the wave128 finding on embedding_producer.rs's \
         chunk_id/chunk_hash defaulting for the tracked follow-up"
    );
    Ok(())
}

/// Production-route fuse proof: the worker loads one event beyond the hard
/// cap, rejects the run during preflight, and therefore never invokes the
/// provider. A test of `plan()` alone would not protect this call boundary.
#[sinex_test]
async fn embedding_worker_aborts_before_provider_call_on_event_ceiling(
    ctx: TestContext,
) -> TestResult<()> {
    let material_id = ctx
        .create_source_material(Some("document-first-run"))
        .await?;
    for index in 0..2 {
        let document_id = Uuid::new_v4();
        seed_document_parent(&ctx, material_id, document_id).await?;
        ctx.pool
            .events()
            .insert(
                DynamicPayload::new(
                    "document-parser",
                    "document.chunked",
                    serde_json::json!({
                        "content": format!("eligible {index}"),
                        "document_id": document_id,
                    }),
                )
                .from_material(material_id)
                .build()?,
            )
            .await?;
    }

    let calls = Arc::new(AtomicUsize::new(0));
    let provider = FakeEmbeddingProvider {
        calls: calls.clone(),
        provider: "fake",
        model: "test-model",
        dimensions: 3,
    };
    let mut config = fake_config();
    config.max_events_per_run = 1;
    let worker = EmbeddingWorker::new(provider, config)?;

    let error = worker
        .run_once(&ctx.pool)
        .await
        .expect_err("the run must stop at the static event ceiling");
    assert!(error.to_string().contains("max events ceiling"));
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "provider must not be called"
    );
    Ok(())
}

/// Production-route quarantine proof: a normal document chunk is the only
/// provider input; agent-session and declared-private material remain in the
/// report/quarantine path even though their event type is otherwise requested.
#[sinex_test]
async fn embedding_worker_quarantines_private_and_agent_session_material(
    ctx: TestContext,
) -> TestResult<()> {
    let allowed_material = ctx
        .create_source_material(Some("document-first-run"))
        .await?;
    let allowed_document_id = Uuid::new_v4();
    seed_document_parent(&ctx, allowed_material, allowed_document_id).await?;
    ctx.pool
        .events()
        .insert(
            DynamicPayload::new(
                "document-parser",
                "document.chunked",
                serde_json::json!({
                    "content": "eligible document",
                    "document_id": allowed_document_id,
                }),
            )
            .from_material(allowed_material)
            .build()?,
        )
        .await?;

    let private_material = ctx.create_source_material(Some("document-private")).await?;
    let private_document_id = Uuid::new_v4();
    seed_document_parent(&ctx, private_material, private_document_id).await?;
    sqlx::query("UPDATE raw.source_material_registry SET privacy_class = 'personal' WHERE id = $1")
        .bind(private_material)
        .execute(&ctx.pool)
        .await?;
    ctx.pool
        .events()
        .insert(
            DynamicPayload::new(
                "document-parser",
                "document.chunked",
                serde_json::json!({
                    "content": "private document",
                    "document_id": private_document_id,
                }),
            )
            .from_material(private_material)
            .build()?,
        )
        .await?;

    let session_material = ctx
        .create_source_material(Some("ai-session-claude"))
        .await?;
    let session_document_id = Uuid::new_v4();
    seed_document_parent(&ctx, session_material, session_document_id).await?;
    ctx.pool
        .events()
        .insert(
            DynamicPayload::new(
                "claude",
                "document.chunked",
                serde_json::json!({
                    "content": "agent session",
                    "document_id": session_document_id,
                }),
            )
            .from_material(session_material)
            .build()?,
        )
        .await?;

    let calls = Arc::new(AtomicUsize::new(0));
    let worker = EmbeddingWorker::new(
        FakeEmbeddingProvider {
            calls: calls.clone(),
            provider: "fake",
            model: "test-model",
            dimensions: 3,
        },
        fake_config(),
    )?;

    let report = worker.run_once(&ctx.pool).await?;
    assert_eq!(report.embedded_events, 1);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(report.estimate.quarantined_events, 2);
    assert_eq!(report.estimate.quarantined_by_reason["private_material"], 1);
    assert_eq!(report.estimate.quarantined_by_reason["agent_session"], 1);
    Ok(())
}
