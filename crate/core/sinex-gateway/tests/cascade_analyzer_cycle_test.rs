use chrono::Utc;
use serde_json::json;
use sinex_core::types::ulid::Ulid as CoreUlid;
use sinex_gateway::cascade_analyzer::{CascadeAnalyzerConfig, StreamingCascadeAnalyzer};
use sinex_test_utils::{sinex_test, TestContext};
use uuid::Uuid;

#[sinex_test]
async fn detects_cycles_beyond_default_depth(ctx: TestContext) -> color_eyre::Result<()> {
    let pool = ctx.pool.clone();
    let analyzer = StreamingCascadeAnalyzer::with_config(
        pool.clone(),
        CascadeAnalyzerConfig {
            batch_size: 10,
            max_depth: 128,
            include_weak_dependencies: false,
            memory_limit_bytes: Some(32 * 1024 * 1024),
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
            Utc::now(),
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
    let pool = ctx.pool.clone();
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
        Utc::now()
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
        Utc::now(),
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
