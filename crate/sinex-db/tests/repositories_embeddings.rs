use serde_json::json;
use sinex_db::DynamicPayload;
use sinex_db::repositories::{CacheEntry, DbPoolExt, EventEmbeddingRow};
use sqlx::Row;
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

/// Closes a gap in `embedding_repository_rejects_dimension_change_on_reregister`:
/// that test's own comment claims same-dimension re-registration is an
/// "idempotent re-registration, e.g. metadata refresh", but nothing ever
/// verified metadata is actually refreshed by the `ON CONFLICT DO UPDATE SET
/// metadata = EXCLUDED.metadata` clause -- only that `is_active`/`model_id`
/// survive. This is a real, distinct assertion (a bug here would silently
/// pin operator-supplied model metadata to whatever was first registered).
#[sinex_test]
async fn embedding_repository_reregister_refreshes_metadata(ctx: TestContext) -> TestResult<()> {
    let repo = ctx.pool.embeddings();
    let provider = "test-provider";
    let model_name = "metadata-refresh-model";

    repo.register_model(provider, model_name, 4, &json!({"version": "1.0"}))
        .await?;
    repo.register_model(
        provider,
        model_name,
        4,
        &json!({"version": "2.0", "revised": true}),
    )
    .await?;

    let record = repo
        .get_active_model(provider, model_name)
        .await?
        .expect("model should exist after re-registration");
    assert_eq!(
        record.metadata,
        json!({"version": "2.0", "revised": true}),
        "re-registering with an unchanged dimension count must refresh metadata, not keep the \
         value from the first registration"
    );

    Ok(())
}

/// sinex-dkkz (part 1): `register_model`'s dimension-immutability check is a
/// classic read-then-write TOCTOU, not an atomic compare-and-set. The
/// `dimensions` column itself can never actually drift (it's excluded from
/// the `ON CONFLICT ... DO UPDATE SET` clause), so the DB-visible invariant
/// holds regardless of interleaving -- but a caller whose differing-dimension
/// registration attempt races another caller's first-ever registration can
/// still get back `Ok(id)` while its requested dimensions were silently
/// ignored, which is the actual contract violation: every `Ok` return must
/// mean "the model is now registered with THESE dimensions", never "a model
/// with this name exists, dimensions undisclosed."
#[sinex_test]
async fn embedding_repository_concurrent_registration_never_lies_about_dimensions(
    ctx: TestContext,
) -> TestResult<()> {
    let repo_a = ctx.pool.embeddings();
    let repo_b = ctx.pool.embeddings();
    let provider = "test-provider";
    let model_name = "concurrent-dimension-race-model";

    // Two callers race to be the FIRST registration of a brand-new
    // (provider, model_name) pair with two different dimension counts.
    // Postgres MVCC + two live connections make this a genuine race, not a
    // simulated one -- the assertion below holds under either interleaving
    // (A-wins, B-wins, or a serialization retry), which is the point: no
    // interleaving may produce a caller that got `Ok` for dimensions that
    // were not actually the ones stored.
    let meta_a = json!({"caller": "a"});
    let meta_b = json!({"caller": "b"});
    let (result_a, result_b) = tokio::join!(
        repo_a.register_model(provider, model_name, 3, &meta_a),
        repo_b.register_model(provider, model_name, 5, &meta_b),
    );

    let stored = repo_a
        .get_active_model(provider, model_name)
        .await?
        .expect("exactly one registration must have won and left a stored model");

    for (label, dimensions, result) in [("a", 3, &result_a), ("b", 5, &result_b)] {
        if let Ok(_id) = result {
            assert_eq!(
                dimensions, stored.dimensions,
                "caller {label} received Ok(..) claiming dimensions={dimensions} were \
                 registered, but the stored model actually has dimensions={} -- register_model \
                 must never return Ok for a dimension count it did not actually persist \
                 (sinex-dkkz)",
                stored.dimensions
            );
        }
    }
    assert!(
        result_a.is_ok() || result_b.is_ok(),
        "at least one of the two racing first-time registrations must succeed"
    );

    Ok(())
}

/// sinex-bksb item 1: every HNSW index on `core.event_embeddings` is built on
/// the expression `(embedding::vector(N))` (see
/// `core.create_embedding_model_index` in `crate/sinex-schema/src/apply.rs`),
/// but `search_similar`/`knn_search`/`hybrid_search` all order by the raw
/// `embedding` column with no matching cast -- Postgres cannot match a bare
/// column reference to an expression index, so the index this trigger just
/// built is unusable by the very queries it exists for; every similarity
/// search is an O(n) sequential scan once any data exists. `enable_seqscan =
/// off` forces the planner to prefer any usable index over a scan regardless
/// of table size (a tiny test table would otherwise seq-scan even with a
/// correctly matching index, making a plain EXPLAIN non-discriminating).
#[sinex_test]
#[ignore = "sinex-bksb open (item 1): HNSW index built on embedding::vector(N) cast but ORDER BY uses raw column -- fails until fixed"]
async fn embedding_repository_search_similar_uses_hnsw_index(ctx: TestContext) -> TestResult<()> {
    let repo = ctx.pool.embeddings();
    let model_id = repo
        .ensure_model("test-provider", "hnsw-index-usage-model", 3)
        .await?;
    let material_id = ctx.create_source_material(Some("embedding-hnsw-index")).await?;
    let event_id = ctx
        .pool
        .events()
        .insert(
            DynamicPayload::new(
                "embedding-test",
                "embedding.hnsw",
                json!({"content": "hnsw index usage probe"}),
            )
            .from_material(material_id)
            .build()?,
        )
        .await?
        .id
        .expect("inserted event has id")
        .into();
    repo.store_event_embedding(event_id, model_id, "hnsw index usage probe", &[1.0, 0.0, 0.0])
        .await?;

    let mut tx = ctx.pool.begin().await?;
    sqlx::query("SET LOCAL enable_seqscan = off")
        .execute(&mut *tx)
        .await?;
    let plan_rows = sqlx::query(
        r#"
        EXPLAIN (FORMAT TEXT)
        SELECT ee.event_id, ee.embedded_text,
               (1.0::float8 - (ee.embedding <=> '[1,0,0]'::text::vector)) as similarity
        FROM core.event_embeddings ee
        WHERE ee.embedding_model_id = $1
        ORDER BY ee.embedding <=> '[1,0,0]'::text::vector
        LIMIT 5
        "#,
    )
    .bind(model_id)
    .fetch_all(&mut *tx)
    .await?;
    tx.rollback().await?;

    let plan_text: String = plan_rows
        .iter()
        .map(|row| row.get::<String, _>(0))
        .collect::<Vec<_>>()
        .join("\n");

    // A genuine HNSW-backed KNN scan returns rows pre-ordered by distance and
    // needs no separate Sort node. Checking for a bare "Index Scan" substring
    // is not sufficient: the WHERE-clause equality on embedding_model_id is
    // itself satisfied by the unrelated `uk_event_embeddings_event_model`
    // unique btree index (Bitmap Index Scan), which produces a plan
    // containing "Index Scan" even when the ORDER BY falls through to an
    // explicit Sort -- exactly the bug this test exists to catch. The real
    // signal is the presence/absence of that Sort node.
    assert!(
        !plan_text.contains("Sort"),
        "search_similar's ORDER BY does not match the HNSW index's \
         `(embedding::vector(N))` expression, so the vector index cannot \
         provide pre-sorted rows and the planner must add an explicit Sort \
         node (sinex-bksb item 1). Plan:\n{plan_text}"
    );

    Ok(())
}
