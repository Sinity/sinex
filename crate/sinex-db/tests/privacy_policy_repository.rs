use serde_json::json;
use sinex_db::repositories::DbPoolExt;
use sinex_primitives::privacy::PrivacyPolicySeedRule;
use xtask::sandbox::prelude::*;

/// Built-in catalog seeding is an explicit DB mutation and repeated runs
/// converge by rule name.
#[sinex_test]
async fn privacy_policy_seed_rules_are_idempotent_db_rows(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool();
    let repo = pool.privacy_policy();
    let seed = vec![PrivacyPolicySeedRule {
        name: "seed-api-token".to_string(),
        description: "initial description".to_string(),
        matcher_type: "regex".to_string(),
        matcher_value: "TOKEN=[^ ]+".to_string(),
        matcher_config: json!({
            "seed_source": "test_catalog",
            "catalog_contexts": ["command"]
        }),
        recognizer_kind: "local_pattern".to_string(),
        case_sensitive: false,
        action: "redact".to_string(),
        action_label: Some("<TOKEN>".to_string()),
        key_namespace: "default".to_string(),
        enabled: false,
    }];

    let first = repo.seed_rules(&seed).await?;
    assert_eq!(first.inserted, 1);
    assert_eq!(first.updated, 0);
    assert_eq!(first.unchanged, 0);
    assert!(
        repo.load_enabled_rules().await?.is_empty(),
        "disabled seed rows must not become runtime policy"
    );

    let second = repo.seed_rules(&seed).await?;
    assert_eq!(second.inserted, 0);
    assert_eq!(second.updated, 0);
    assert_eq!(second.unchanged, 1);

    let mut enabled_seed = seed;
    enabled_seed[0].description = "updated description".to_string();
    enabled_seed[0].enabled = true;
    let third = repo.seed_rules(&enabled_seed).await?;
    assert_eq!(third.inserted, 0);
    assert_eq!(third.updated, 1);
    assert_eq!(third.unchanged, 0);

    let loaded = repo.load_enabled_rules().await?;
    assert_eq!(loaded.len(), 1);
    assert_eq!(loaded[0].rule.name, "seed-api-token");
    assert_eq!(loaded[0].rule.description, "updated description");
    assert_eq!(loaded[0].rule.matcher_config["seed_source"], "test_catalog");
    Ok(())
}

/// A rule that references a *disabled* recognizer backend must be skipped
/// during load, not abort the whole policy. Regression for the case where one
/// disabled backend would otherwise take down all DB-backed privacy policy.
#[sinex_test]
async fn disabled_backend_skips_rule_without_aborting_load(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool();
    let repo = pool.privacy_policy();

    // A disabled recognizer backend.
    let backend_id = repo
        .add_recognizer_backend(
            "disabled-presidio",
            "presidio",
            Some("http://127.0.0.1:9/analyze"),
            json!({}),
            false,
        )
        .await?;

    // An enabled rule that depends on the disabled backend (defaults to enabled).
    repo.add_recognizer_rule(
        "rule-needs-disabled-backend",
        "depends on a disabled backend",
        "presidio_entity",
        "PERSON",
        json!({ "entities": ["PERSON"] }),
        Some(backend_id),
        "presidio_entity",
        false,
        "redact",
        Some("<X>"),
        "default",
    )
    .await?;

    // An independent enabled global regex rule that must survive the load.
    repo.add_rule(
        "rule-survivor",
        "independent rule",
        "regex",
        r"SURVIVE_\w+",
        false,
        "redact",
        Some("<S>"),
        "default",
    )
    .await?;
    repo.bind_field_rule("rule-survivor", None, None, None, 0)
        .await?;

    // Before the fix this returned a `not_found` error on the disabled backend
    // and aborted the entire load. It must now succeed.
    let loaded = repo.load_enabled_rules().await?;
    assert!(
        loaded.iter().any(|r| r.rule.name == "rule-survivor"),
        "independent rule must still load when an unrelated rule's backend is disabled"
    );
    assert!(
        loaded
            .iter()
            .all(|r| r.rule.name != "rule-needs-disabled-backend"),
        "rule referencing a disabled backend must be skipped, not abort the load"
    );
    Ok(())
}
