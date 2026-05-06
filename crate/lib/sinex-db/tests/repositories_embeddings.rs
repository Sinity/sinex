use serde_json::json;
use sinex_db::DynamicPayload;
use sinex_db::repositories::{CacheEntry, DbPoolExt, EventEmbeddingRow};
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn embedding_repository_batches_cache_and_backfill(ctx: TestContext) -> TestResult<()> {
    let repo = ctx.pool.embeddings();
    let model_id = repo.ensure_model("test-provider", "test-model", 3).await?;
    let material_id = ctx.create_source_material(Some("embedding-repo")).await?;

    let first = ctx
        .pool
        .events()
        .insert(
            DynamicPayload::new(
                "embedding-test",
                "embedding.target",
                json!({"content": "rust async runtime debugging"}),
            )
            .from_material(material_id)
            .build()?,
        )
        .await?
        .id
        .expect("inserted event has id")
        .into();
    let second = ctx
        .pool
        .events()
        .insert(
            DynamicPayload::new(
                "embedding-test",
                "embedding.target",
                json!({"content": "postgres vector search"}),
            )
            .from_material(material_id)
            .build()?,
        )
        .await?
        .id
        .expect("inserted event has id")
        .into();

    let targets = repo
        .events_without_embeddings(model_id, &["embedding.target"], 10)
        .await?;
    assert_eq!(targets.len(), 2);
    assert_eq!(targets[0].event_type, "embedding.target");

    let inserted = repo
        .insert_event_embeddings(&[
            EventEmbeddingRow {
                event_id: first,
                model_id,
                embedded_text: "rust async runtime debugging".to_string(),
                embedding: vec![1.0, 0.0, 0.0],
            },
            EventEmbeddingRow {
                event_id: second,
                model_id,
                embedded_text: "postgres vector search".to_string(),
                embedding: vec![0.0, 1.0, 0.0],
            },
        ])
        .await?;
    assert_eq!(inserted, 2);

    let duplicate_inserted = repo
        .insert_event_embeddings(&[EventEmbeddingRow {
            event_id: first,
            model_id,
            embedded_text: "rust async runtime debugging".to_string(),
            embedding: vec![1.0, 0.0, 0.0],
        }])
        .await?;
    assert_eq!(duplicate_inserted, 0);

    let targets = repo
        .events_without_embeddings(model_id, &["embedding.target"], 10)
        .await?;
    assert!(targets.is_empty());

    repo.cache_upsert(
        &[CacheEntry {
            text_hash: "hash-rust".to_string(),
            text_sample: "rust async runtime debugging".to_string(),
            embedding: vec![1.0, 0.0, 0.0],
        }],
        model_id,
    )
    .await?;
    let hits = repo
        .cache_lookup(&["hash-rust".to_string(), "missing".to_string()], model_id)
        .await?;
    assert_eq!(hits.get("hash-rust"), Some(&vec![1.0, 0.0, 0.0]));

    let nearest = repo.knn_search(&[1.0, 0.0, 0.0], model_id, 2, 20).await?;
    assert_eq!(nearest[0].event_id, first);
    assert!(
        nearest[0].cosine_distance < nearest[1].cosine_distance,
        "nearest vector should have lower cosine distance"
    );

    Ok(())
}

#[sinex_test]
async fn embedding_repository_rejects_wrong_dimension(ctx: TestContext) -> TestResult<()> {
    let repo = ctx.pool.embeddings();
    let model_id = repo
        .ensure_model("test-provider", "dimension-validation", 3)
        .await?;
    let material_id = ctx
        .create_source_material(Some("embedding-dimension-validation"))
        .await?;
    let event_id = ctx
        .pool
        .events()
        .insert(
            DynamicPayload::new(
                "embedding-test",
                "embedding.dimension",
                json!({"content": "dimension mismatch"}),
            )
            .from_material(material_id)
            .build()?,
        )
        .await?
        .id
        .expect("inserted event has id")
        .into();

    let error = repo
        .store_event_embedding(event_id, model_id, "dimension mismatch", &[1.0, 0.0])
        .await
        .expect_err("wrong vector dimension should be rejected before insert");
    assert!(
        error.message().contains("dimension mismatch"),
        "unexpected error: {error}"
    );

    Ok(())
}
