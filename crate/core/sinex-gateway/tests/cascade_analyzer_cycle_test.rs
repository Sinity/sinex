use serde_json::json;
use sinex_gateway::cascade_analyzer::{CascadeAnalyzerConfig, StreamingCascadeAnalyzer};
use sinex_primitives::temporal;
use sinex_primitives::Ulid as CoreUlid;
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

#[sinex_test]
async fn detects_cycles_beyond_default_depth(ctx: TestContext) -> color_eyre::Result<()> {
    ctx.ensure_clean().await?;
    let pool = ctx.pool.clone();
    color_eyre::eyre::ensure!(
        cascade_prereqs_available(&pool).await?,
        "core.prepare_cascade_session missing; run migrations before tests"
    );
    let analyzer = StreamingCascadeAnalyzer::with_config(
        pool.clone(),
        CascadeAnalyzerConfig {
            batch_size: 10,
            max_depth: 128,
            include_weak_dependencies: false,
            memory_limit_bytes: Some(32 * 1024 * 1024),
            timeout: std::time::Duration::from_secs(30),
        },
    );

    let cycle_len = 16;
    let event_ids: Vec<_> = (0..cycle_len).map(|_| CoreUlid::new()).collect();

    for (idx, ulid) in event_ids.iter().enumerate() {
        let parent = event_ids[(idx + cycle_len - 1) % cycle_len];
        let parent_uuid = parent.to_uuid();
        let parent_array = vec![parent_uuid];

        sqlx::query!(
            r#"
            INSERT INTO core.events (
                id,
                source,
                event_type,
                host,
                payload,
                ts_orig,
                source_event_ids
            ) VALUES (
                $1::uuid::ulid,
                $2,
                $3,
                $4,
                $5,
                $6,
                $7::uuid[]::ulid[]
            )
            "#,
            ulid.to_uuid(),
            "cycle.source",
            "cycle.event",
            "localhost",
            json!({"idx": idx }),
            *temporal::now(),
            &parent_array
        )
        .execute(&pool)
        .await?;
    }

    let start_ids: Vec<CoreUlid> = event_ids.clone();

    let analysis = analyzer.analyze_cascades(&start_ids).await?;
    assert!(
        !analysis.circular_dependencies.is_empty(),
        "expected to find at least one cycle"
    );
    assert!(
        analysis
            .circular_dependencies
            .iter()
            .any(|cycle| cycle.cycle.len() >= cycle_len),
        "expected to detect the long cycle"
    );
    Ok(())
}

#[sinex_test]
async fn handles_mixed_uuid_arrays(ctx: TestContext) -> color_eyre::Result<()> {
    ctx.ensure_clean().await?;
    let pool = ctx.pool.clone();
    color_eyre::eyre::ensure!(
        cascade_prereqs_available(&pool).await?,
        "core.prepare_cascade_session missing; run migrations before tests"
    );
    let analyzer = StreamingCascadeAnalyzer::new(pool.clone());

    let parent = CoreUlid::new();
    let child = CoreUlid::new();
    let stray_uuid = Uuid::new_v4();

    sqlx::query!(
        r#"
        INSERT INTO core.events (
            id,
            source,
            event_type,
            host,
            payload,
            ts_orig,
            source_event_ids
        ) VALUES (
            $1::uuid::ulid,
            $2,
            $3,
            $4,
            $5,
            $6,
            ARRAY[$1::uuid]::uuid[]::ulid[]
        )
        "#,
        parent.to_uuid(),
        "mixed.source",
        "mixed.anchor",
        "localhost",
        json!({"kind": "anchor"}),
        *temporal::now()
    )
    .execute(&pool)
    .await?;

    sqlx::query!(
        r#"
        INSERT INTO core.events (
            id,
            source,
            event_type,
            host,
            payload,
            ts_orig,
            source_event_ids
        ) VALUES (
            $1::uuid::ulid,
            $2,
            $3,
            $4,
            $5,
            $6,
            ARRAY[$7::uuid, $8::uuid]::uuid[]::ulid[]
        )
        "#,
        child.to_uuid(),
        "mixed.source",
        "mixed.child",
        "localhost",
        json!({"kind": "dependent"}),
        *temporal::now(),
        parent.to_uuid(),
        stray_uuid
    )
    .execute(&pool)
    .await?;

    let analysis = analyzer.analyze_cascades(&[parent]).await?;
    assert_eq!(analysis.total_affected, 2);
    assert!(
        analysis.integrity_violations.is_empty(),
        "unexpected integrity violations: {:?}",
        analysis.integrity_violations
    );
    Ok(())
}

#[sinex_test]
async fn timeout_prevents_indefinite_transaction_hold(ctx: TestContext) -> color_eyre::Result<()> {
    ctx.ensure_clean().await?;
    let pool = ctx.pool.clone();
    color_eyre::eyre::ensure!(
        cascade_prereqs_available(&pool).await?,
        "core.prepare_cascade_session missing; run migrations before tests"
    );

    // Create a very short timeout to test the timeout mechanism
    let analyzer = StreamingCascadeAnalyzer::with_config(
        pool.clone(),
        CascadeAnalyzerConfig {
            batch_size: 1,
            max_depth: 1000, // Large depth
            include_weak_dependencies: false,
            memory_limit_bytes: Some(1024 * 1024),
            timeout: std::time::Duration::from_millis(1), // Very short timeout
        },
    );

    // Create a simple event
    let event_id = CoreUlid::new();
    let empty_parents: Vec<Uuid> = vec![];
    sqlx::query!(
        r#"
        INSERT INTO core.events (
            id,
            source,
            event_type,
            host,
            payload,
            ts_orig,
            source_event_ids
        ) VALUES (
            $1::uuid::ulid,
            $2,
            $3,
            $4,
            $5,
            $6,
            $7::uuid[]::ulid[]
        )
        "#,
        event_id.to_uuid(),
        "timeout.source",
        "timeout.test",
        "localhost",
        json!({"test": "timeout"}),
        *temporal::now(),
        &empty_parents
    )
    .execute(&pool)
    .await?;

    // Analysis should timeout
    let result = analyzer.analyze_cascades(&[event_id]).await;
    assert!(
        result.is_err(),
        "Expected timeout error, but analysis succeeded"
    );
    let err_str = result.unwrap_err().to_string();
    assert!(
        err_str.contains("timeout") || err_str.contains("Timeout"),
        "Expected timeout error message, got: {err_str}"
    );

    Ok(())
}
