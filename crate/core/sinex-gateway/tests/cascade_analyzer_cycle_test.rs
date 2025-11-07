use chrono::Utc;
use serde_json::json;
use sinex_core::types::ulid::Ulid as CoreUlid;
use sinex_gateway::cascade_analyzer::{CascadeAnalyzerConfig, StreamingCascadeAnalyzer};
use sinex_test_utils::{sinex_test, TestContext};

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
    let event_ids: Vec<_> = (0..cycle_len)
        .map(|_| sinex_schema::ulid::Ulid::new())
        .collect();

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

    let start_ids: Vec<CoreUlid> = event_ids
        .iter()
        .map(|id| CoreUlid::from_bytes(id.to_bytes()))
        .collect();

    let analysis = analyzer.analyze_cascades(&start_ids[..1]).await?;
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
