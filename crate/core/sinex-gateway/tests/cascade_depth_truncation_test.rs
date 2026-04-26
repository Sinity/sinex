//! Regression tests for issue #553 — cascade depth honesty.
//!
//! Before the fix, `core.expand_cascade` and the Rust-side
//! `expand_cascade_from` would both silently truncate the cascade graph
//! when `current_depth` reached `max_depth`. The preview path then
//! reported "all reachable" while the execution path archived only the
//! first N levels, leaving the graph with dangling provenance edges.
//!
//! These tests pin the new contract: hitting `max_depth` while there
//! are still pending children must surface as a typed error, not a
//! silent return.

use serde_json::json;
use sinex_db::repositories::EventRepositoryTx;
use sinex_primitives::temporal;
use sqlx::PgPool;
use uuid::Uuid;
use xtask::sandbox::sinex_test;

async fn cascade_prereqs_available(pool: &PgPool) -> color_eyre::Result<bool> {
    let exists: bool = sqlx::query_scalar!(
        r#"
        SELECT EXISTS(
            SELECT 1
            FROM pg_proc p
            JOIN pg_namespace n ON n.oid = p.pronamespace
            WHERE n.nspname = 'core'
              AND p.proname = 'prepare_cascade_session'
        ) AS "exists!"
        "#
    )
    .fetch_one(pool)
    .await?;
    Ok(exists)
}

/// Insert a strict parent-child chain of synthesis events, length `depth`.
/// Returns the chain in root-to-leaf order. Each event's `source_event_ids`
/// points at the previous event, so cascade expansion from the root must
/// walk the full chain.
///
/// The root event uses a synthetic upstream parent UUID. `source_event_ids`
/// is not a foreign key, so the dangling reference is fine for the purposes
/// of this test — what matters is that every event satisfies the XOR
/// provenance CHECK and that the cascade expansion walks downward from
/// the root.
async fn build_chain(pool: &PgPool, depth: usize) -> color_eyre::Result<Vec<Uuid>> {
    assert!(depth >= 1, "build_chain requires at least one event");
    let mut ids = Vec::with_capacity(depth);
    let synthetic_upstream = vec![Uuid::now_v7()];

    let root = Uuid::now_v7();
    sqlx::query!(
        r#"
        INSERT INTO core.events (
            id, source, event_type, host, payload, ts_orig,
            source_event_ids
        ) VALUES (
            $1::uuid, $2, $3, $4, $5, $6, $7::uuid[]
        )
        "#,
        root,
        "cascade.depth.test",
        "cascade.root",
        "localhost",
        json!({"depth": 0_u32}),
        *temporal::now(),
        &synthetic_upstream
    )
    .execute(pool)
    .await?;
    ids.push(root);

    for level in 1..depth {
        let id = Uuid::now_v7();
        let parents = vec![ids[level - 1]];
        sqlx::query!(
            r#"
            INSERT INTO core.events (
                id, source, event_type, host, payload, ts_orig,
                source_event_ids
            ) VALUES (
                $1::uuid, $2, $3, $4, $5, $6, $7::uuid[]
            )
            "#,
            id,
            "cascade.depth.test",
            "cascade.link",
            "localhost",
            json!({"depth": level as u32}),
            *temporal::now(),
            &parents
        )
        .execute(pool)
        .await?;
        ids.push(id);
    }

    Ok(ids)
}

#[sinex_test]
async fn expand_cascade_raises_when_chain_exceeds_max_depth(
    ctx: TestContext,
) -> color_eyre::Result<()> {
    let pool = ctx.pool.clone();
    color_eyre::eyre::ensure!(
        cascade_prereqs_available(&pool).await?,
        "core.prepare_cascade_session missing; run migrations before tests"
    );

    let chain = build_chain(&pool, 8).await?;
    // Cascade work runs on TEMP TABLEs which are connection-local, so the
    // prepare / populate / expand sequence must share a single tx the same
    // way `replay_control::derive_cascade_ids` does in production.
    let mut tx = pool.begin().await?;
    let mut repo = EventRepositoryTx::new(&mut tx);
    let session_id = format!("trunc_{}", &Uuid::now_v7().simple().to_string()[..12]);
    let table_name = repo.prepare_cascade_session(&session_id, false).await?;
    repo.populate_cascade_roots(&table_name, &chain[..1]).await?;

    // Chain is length 8 (depths 0..7). max_depth=4 means the cascade
    // would truncate at depth 4 with descendants still pending; this MUST
    // raise rather than silently return.
    let outcome = repo.expand_cascade(&table_name, 4).await;
    let err = outcome.expect_err("expand_cascade must refuse to truncate");
    let msg = format!("{err}");
    assert!(
        msg.contains("max depth") || msg.contains("cascade"),
        "expected truncation error to mention cascade/max depth, got: {msg}"
    );
    // Tx will roll back on drop; nothing to commit.
    drop(repo);
    tx.rollback().await?;
    Ok(())
}

#[sinex_test]
async fn expand_cascade_succeeds_when_chain_fits_within_limit(
    ctx: TestContext,
) -> color_eyre::Result<()> {
    let pool = ctx.pool.clone();
    color_eyre::eyre::ensure!(
        cascade_prereqs_available(&pool).await?,
        "core.prepare_cascade_session missing; run migrations before tests"
    );

    let chain = build_chain(&pool, 6).await?;
    let mut tx = pool.begin().await?;
    let mut repo = EventRepositoryTx::new(&mut tx);
    let session_id = format!("complete_{}", &Uuid::now_v7().simple().to_string()[..12]);
    let table_name = repo.prepare_cascade_session(&session_id, false).await?;
    repo.populate_cascade_roots(&table_name, &chain[..1]).await?;

    // Chain is length 6 (depths 0..5). max_depth=10 leaves headroom; the
    // cascade should expand fully and return without error.
    let depth = repo
        .expand_cascade(&table_name, 10)
        .await
        .expect("expand_cascade should succeed when chain fits the limit");

    assert!(
        depth >= 5,
        "expected to walk at least to depth 5 (chain length 6), got {depth}"
    );
    drop(repo);
    tx.rollback().await?;
    Ok(())
}
