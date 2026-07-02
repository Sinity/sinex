use super::*;
use serde_json::json;
use sinex_primitives::temporal::now;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn session_id_validation_enforces_length() -> TestResult<()> {
    assert!(StreamingCascadeAnalyzer::validate_session_id(&"a".repeat(64)).is_ok());
    assert!(StreamingCascadeAnalyzer::validate_session_id(&"a".repeat(65)).is_err());
    Ok(())
}

#[sinex_test]
async fn session_id_validation_rejects_invalid_chars() -> TestResult<()> {
    assert!(StreamingCascadeAnalyzer::validate_session_id("valid_session_1").is_ok());
    assert!(StreamingCascadeAnalyzer::validate_session_id("invalid-session").is_err());
    Ok(())
}

#[sinex_test]
async fn generated_session_ids_use_validator_safe_format() -> TestResult<()> {
    let session_id = Uuid::now_v7().simple().to_string();
    assert!(StreamingCascadeAnalyzer::validate_session_id(&session_id).is_ok());
    Ok(())
}

#[sinex_test]
async fn record_dependency_inserts_missing_keys() -> TestResult<()> {
    let mut dependencies = HashMap::new();
    let mut in_degree = HashMap::new();
    let source_id = Uuid::now_v7();
    let event_id = Uuid::now_v7();

    record_dependency(&mut dependencies, &mut in_degree, source_id, event_id);

    assert_eq!(dependencies.get(&source_id), Some(&vec![event_id]));
    assert_eq!(in_degree.get(&event_id), Some(&1));
    Ok(())
}

#[sinex_test]
async fn cascade_config_from_env_applies_valid_overrides() -> TestResult<()> {
    let mut env = EnvGuard::new();
    env.set("SINEX_CASCADE_BATCH_SIZE", "128");
    env.set("SINEX_CASCADE_MAX_DEPTH", "64");
    env.set("SINEX_CASCADE_INCLUDE_WEAK", "yes");
    env.set("SINEX_CASCADE_MEMORY_LIMIT_BYTES", "4096");
    env.set("SINEX_CASCADE_TIMEOUT_SECS", "15");

    let config = CascadeAnalyzerConfig::from_env();

    assert_eq!(config.batch_size, 128);
    assert_eq!(config.max_depth, 64);
    assert!(config.include_weak_dependencies);
    assert_eq!(config.memory_limit_bytes, Some(4096));
    assert_eq!(config.timeout, Duration::from_secs(15));
    Ok(())
}

#[sinex_test]
async fn cascade_config_from_env_rejects_invalid_overrides() -> TestResult<()> {
    let mut env = EnvGuard::new();
    env.set("SINEX_CASCADE_BATCH_SIZE", "0");
    env.set("SINEX_CASCADE_MAX_DEPTH", "many");
    env.set("SINEX_CASCADE_INCLUDE_WEAK", "sometimes");
    env.set("SINEX_CASCADE_MEMORY_LIMIT_BYTES", "-1");
    env.set("SINEX_CASCADE_TIMEOUT_SECS", "0");

    let config = CascadeAnalyzerConfig::from_env();

    assert_eq!(config.batch_size, DEFAULT_CASCADE_BATCH_SIZE);
    assert_eq!(config.max_depth, DEFAULT_CASCADE_MAX_DEPTH);
    assert!(!config.include_weak_dependencies);
    assert_eq!(
        config.memory_limit_bytes,
        Some(DEFAULT_CASCADE_MEMORY_LIMIT)
    );
    assert_eq!(
        config.timeout,
        Duration::from_secs(DEFAULT_CASCADE_TIMEOUT_SECS)
    );
    Ok(())
}

#[sinex_test]
async fn cascade_order_detects_cycles(ctx: TestContext) -> TestResult<()> {
    let analyzer = StreamingCascadeAnalyzer::new(ctx.pool.clone());
    let current_time = now();
    let payload = json!({});

    let a = Uuid::now_v7();
    let b = Uuid::now_v7();
    let c = Uuid::now_v7();
    let cycle_links = vec![(a, vec![b]), (b, vec![c]), (c, vec![a])];

    for (event_id, parents) in &cycle_links {
        let parents_uuid: Vec<Uuid> = parents.clone();
        sqlx::query(
            "INSERT INTO core.events (id, source, event_type, host, payload, ts_orig, source_event_ids) \
             VALUES ($1::uuid, $2, $3, $4, $5, $6, $7::uuid[]::uuid[])",
        )
        .bind(*event_id)
        .bind("cascade-test")
        .bind("cascade.test")
        .bind("test-host")
        .bind(payload.clone())
        .bind(current_time)
        .bind(parents_uuid)
        .execute(&ctx.pool)
        .await?;
    }

    let err = analyzer
        .plan_cascade_order(&[a, b, c])
        .await
        .expect_err("cycle should be detected in cascade ordering");
    assert_eq!(
        err.context_map().get("error_class"),
        Some(&"cascade_cycle_detected".to_string())
    );

    Ok(())
}
