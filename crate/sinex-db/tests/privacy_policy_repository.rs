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
