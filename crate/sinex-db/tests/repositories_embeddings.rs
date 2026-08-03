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
async fn embedding_repository_rejects_dimension_change_on_reregister(
    ctx: TestContext,
) -> TestResult<()> {
    // sinex-audit-embedding-dim-change: dimensions is immutable once a
    // (provider, model_name) pair is first registered. The partial HNSW
    // index for a model is keyed only on model_id and bakes the dimension
    // into a `vector(N)` cast; rebuilding it in place is not actually
    // achievable even after purging every old-dimension embedding row in
    // the same transaction, because plain `CREATE INDEX` scans with
    // `SnapshotAny` and still sees same-transaction-deleted-but-not-yet-
    // vacuumed tuples (confirmed empirically: a same-transaction
    // DELETE-then-CREATE-INDEX sequence on an otherwise-empty table still
    // fails with a pgvector dimension-mismatch error, and VACUUM/CONCURRENTLY
    // cannot run inside the ambient transaction a trigger executes in).
    // register_model therefore rejects a dimension change outright instead
    // of attempting a rebuild that cannot succeed.
    let repo = ctx.pool.embeddings();
    let provider = "test-provider";
    let model_name = "dimension-change-model";

    let model_id = repo.ensure_model(provider, model_name, 3).await?;

    let material_id = ctx
        .create_source_material(Some("embedding-dimension-change"))
        .await?;
    let first_event = ctx
        .pool
        .events()
        .insert(
            DynamicPayload::new(
                "embedding-test",
                "embedding.dimension_change",
                json!({"content": "original dimension embedding"}),
            )
            .from_material(material_id)
            .build()?,
        )
        .await?
        .id
        .expect("inserted event has id")
        .into();

    repo.store_event_embedding(
        first_event,
        model_id,
        "original dimension embedding",
        &[1.0, 0.0, 0.0],
    )
    .await?;

    // Re-registering the same (provider, model_name) with a DIFFERENT
    // dimension count must be rejected, not silently accepted.
    let err = repo
        .ensure_model(provider, model_name, 5)
        .await
        .expect_err("a dimension change on an existing model must be rejected, not accepted");
    assert!(
        err.to_string().contains("immutable"),
        "rejection error should explain the immutability rule, got: {err}"
    );

    // Re-registering with the SAME dimension count must still succeed
    // (idempotent re-registration, e.g. metadata refresh) and keep the
    // same model_id.
    let reregistered_model_id = repo.ensure_model(provider, model_name, 3).await?;
    assert_eq!(
        reregistered_model_id, model_id,
        "re-registering with an unchanged dimension count must keep the same model_id"
    );

    // The original embedding must survive untouched -- nothing was ever
    // purged, since the rejected write never reached the database.
    assert_eq!(repo.count_embeddings().await?, 1);
    let nearest = repo.knn_search(&[1.0, 0.0, 0.0], model_id, 1, 20).await?;
    assert_eq!(nearest[0].event_id, first_event);

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
