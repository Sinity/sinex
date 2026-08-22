use super::*;
use serde_json::json;
use sinex_primitives::derivation::DerivedProductClass;
use sinex_primitives::temporal::now;
use xtask::sandbox::prelude::*;

/// Register a `derivation.product_declarations` row so
/// `derivation.enforce_event_product_declaration()` accepts a test-built
/// derived event that declares `product_class` (sinex-0vx.4). This is a
/// src-level unit-test module (not `crate/sinexd/tests/**`), so it can't
/// reach the `tests/api/common` helper of the same name -- mirrors
/// `persistence_test.rs::seed_product_declaration` /
/// `automata_handlers_test.rs::seed_product_declaration` (sinex-egyf).
async fn seed_product_declaration(
    pool: &sqlx::PgPool,
    declaration_id: &str,
    product_class: DerivedProductClass,
    output_source: &str,
    output_event_type: &str,
) -> sinex_primitives::Result<()> {
    sqlx::query!(
        r#"
        INSERT INTO derivation.product_declarations (
            declaration_id, owner, product_class, write_surface,
            output_source, output_event_type, semantics_version,
            input_eligibility, default_claim_support, verification_command
        ) VALUES (
            $1, 'sinex-egyf-test', $2, 'derived_output',
            $3, $4, 'v1', 'default_canonical_input', '{}'::jsonb, 'true'
        )
        ON CONFLICT (declaration_id) DO NOTHING
        "#,
        declaration_id,
        product_class.as_str(),
        output_source,
        output_event_type,
    )
    .execute(pool)
    .await
    .map_err(|e| {
        sinex_primitives::SinexError::database("seed product declaration").with_source(e)
    })?;
    Ok(())
}

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
    let product_class = DerivedProductClass::CanonicalDerivedEvent;
    let declaration_id = "sinex.test.cascade_order_detects_cycles";
    seed_product_declaration(
        &ctx.pool,
        declaration_id,
        product_class,
        "cascade-test",
        "cascade.test",
    )
    .await?;

    let a = Uuid::now_v7();
    let b = Uuid::now_v7();
    let c = Uuid::now_v7();
    let cycle_links = vec![(a, vec![b]), (b, vec![c]), (c, vec![a])];

    for (event_id, parents) in &cycle_links {
        let parents_uuid: Vec<Uuid> = parents.clone();
        sqlx::query(
            "INSERT INTO core.events (id, source, event_type, host, payload, ts_orig, source_event_ids, \
             product_class, claim_support, derivation_declaration_id) \
             VALUES ($1::uuid, $2, $3, $4, $5, $6, $7::uuid[]::uuid[], $8, $9, $10)",
        )
        .bind(*event_id)
        .bind("cascade-test")
        .bind("cascade.test")
        .bind("test-host")
        .bind(payload.clone())
        .bind(current_time)
        .bind(parents_uuid)
        .bind(product_class.as_str())
        .bind(json!({}))
        .bind(declaration_id)
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
