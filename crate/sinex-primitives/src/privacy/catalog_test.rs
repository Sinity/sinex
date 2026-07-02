use super::*;
use crate::privacy::CategorySet;
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn catalog_has_expected_count() -> ::xtask::sandbox::TestResult<()> {
    let rules = builtin_rules();
    // Built-in seed catalog after #1042: the four WindowTitle-policy rules
    // (password_entry_title, login_window_title, password_manager_title,
    // sensitive_file_title) are gone — WindowTitle is no longer a policy
    // concept — and former infrastructure rules fold into PII.
    // 20 secret + 11 PII + 3 privacy = 34.
    let count = |cat: RuleCategory| rules.iter().filter(|r| r.category == cat).count();
    assert_eq!(count(RuleCategory::Secret), 20, "secret rule count");
    assert_eq!(count(RuleCategory::Pii), 11, "PII rule count");
    assert_eq!(count(RuleCategory::Privacy), 3, "privacy rule count");
    assert_eq!(rules.len(), 34, "total built-in rule count");
    Ok(())
}

#[sinex_test]
async fn all_rules_have_unique_names() -> ::xtask::sandbox::TestResult<()> {
    let rules = builtin_rules();
    let mut names: Vec<&str> = rules.iter().map(|r| r.name.as_str()).collect();
    names.sort_unstable();
    names.dedup();
    assert_eq!(names.len(), rules.len(), "duplicate rule names found");
    Ok(())
}

/// Names of rules that ship disabled-by-default. These are opt-in
/// aggressive variants the operator must explicitly enable via overrides.
const OPT_IN_RULES: &[&str] = &["user_home_path_aggressive"];

#[sinex_test]
async fn default_enablement_matches_opt_in_list() -> ::xtask::sandbox::TestResult<()> {
    let rules = builtin_rules();
    for rule in &rules {
        let expected = !OPT_IN_RULES.contains(&rule.name.as_str());
        assert_eq!(
            rule.enabled, expected,
            "rule '{}' enablement disagrees with OPT_IN_RULES list",
            rule.name
        );
    }
    Ok(())
}

#[sinex_test]
async fn opt_in_rules_actually_exist() -> ::xtask::sandbox::TestResult<()> {
    let rules = builtin_rules();
    for name in OPT_IN_RULES {
        assert!(
            rules.iter().any(|r| r.name == *name),
            "OPT_IN_RULES references '{name}' but no such rule is in the catalog"
        );
    }
    Ok(())
}

#[sinex_test]
async fn builtin_seed_projection_is_explicit_db_policy_data() -> ::xtask::sandbox::TestResult<()>
{
    let rules = builtin_policy_seed_rules(false);
    let aws = rules
        .iter()
        .find(|rule| rule.name == "aws_access_key")
        .expect("aws access key seed rule exists");
    assert_eq!(aws.matcher_type, "regex");
    assert_eq!(aws.recognizer_kind, "local_pattern");
    assert_eq!(aws.action, "redact");
    assert_eq!(aws.matcher_config["seed_source"], "builtin_catalog");
    assert_eq!(aws.matcher_config["category"], "secret");
    assert!(!aws.enabled);

    let ssn = rules
        .iter()
        .find(|rule| rule.name == "ssn")
        .expect("ssn seed rule exists");
    assert_eq!(ssn.matcher_type, "structural");
    assert_eq!(ssn.matcher_value, "ssn");
    assert!(
        ssn.matcher_config["catalog_contexts"]
            .as_array()
            .is_some_and(|contexts| !contexts.is_empty()),
        "old catalog contexts should survive only as seed metadata"
    );
    Ok(())
}

#[sinex_test]
async fn aggressive_path_rule_replaces_collapse_when_opted_in()
-> ::xtask::sandbox::TestResult<()> {
    // Document the operator-facing pattern: turning on the aggressive
    // variant and turning off the soft variant produces hashed output
    // for $HOME paths instead of `<HOME>/...` collapse.
    use crate::privacy::{PrivacyConfig, PrivacyEngine, ProcessingContext, RuleOverride};
    use std::collections::HashMap;

    unsafe {
        std::env::set_var("HOME", "/home/sinity-test-aggressive-redact");
    }

    let mut overrides = HashMap::new();
    overrides.insert(
        "user_home_path_aggressive".to_string(),
        RuleOverride {
            enabled: Some(true),
            ..Default::default()
        },
    );
    overrides.insert(
        "user_home_path".to_string(),
        RuleOverride {
            enabled: Some(false),
            ..Default::default()
        },
    );

    let config = PrivacyConfig {
        builtin_categories: CategorySet::All,
        overrides,
        ..PrivacyConfig::default()
    };
    let engine = PrivacyEngine::new(config).expect("engine builds");
    let result = engine.process(
        "/home/sinity-test-aggressive-redact/projects/sinex/Cargo.toml",
        ProcessingContext::Metadata,
    );

    // Without a key, Hash degrades to a generic redact label rather than
    // a real hash. Either way the literal home prefix must be gone, AND
    // the soft `<HOME>/...` collapse must NOT be the output (proving the
    // override flipped which rule fired).
    assert!(
        !result.text.contains("/home/sinity-test-aggressive-redact/"),
        "aggressive variant must redact the home prefix, got {:?}",
        result.text
    );
    assert!(
        !result.text.contains("<HOME>"),
        "aggressive variant must not emit the soft <HOME>/... label, got {:?}",
        result.text
    );
    Ok(())
}

// ── New API keys ──────────────────────────────────────────

fn rule_exists(name: &str) -> bool {
    builtin_rules().iter().any(|r| r.name == name)
}

#[sinex_test]
async fn anthropic_api_key_rule_exists() -> ::xtask::sandbox::TestResult<()> {
    assert!(rule_exists("anthropic_api_key"));
    Ok(())
}

#[sinex_test]
async fn openai_api_key_rule_exists() -> ::xtask::sandbox::TestResult<()> {
    assert!(rule_exists("openai_api_key"));
    Ok(())
}

#[sinex_test]
async fn huggingface_token_rule_exists() -> ::xtask::sandbox::TestResult<()> {
    assert!(rule_exists("huggingface_token"));
    Ok(())
}

// ── New Polish PII rules ──────────────────────────────────

#[sinex_test]
async fn pesel_rule_exists() -> ::xtask::sandbox::TestResult<()> {
    assert!(rule_exists("pesel"));
    Ok(())
}

#[sinex_test]
async fn nip_rule_exists() -> ::xtask::sandbox::TestResult<()> {
    assert!(rule_exists("nip"));
    Ok(())
}

#[sinex_test]
async fn regon_rule_exists() -> ::xtask::sandbox::TestResult<()> {
    assert!(rule_exists("regon"));
    Ok(())
}

#[sinex_test]
async fn pesel_rule_uses_hash_strategy() -> ::xtask::sandbox::TestResult<()> {
    let rules = builtin_rules();
    let rule = rules
        .iter()
        .find(|r| r.name == "pesel")
        .expect("pesel rule");
    assert!(
        matches!(rule.strategy, Strategy::Hash),
        "PESEL should use Hash strategy, got {:?}",
        rule.strategy
    );
    Ok(())
}

#[sinex_test]
async fn nip_rule_uses_hash_strategy() -> ::xtask::sandbox::TestResult<()> {
    let rules = builtin_rules();
    let rule = rules.iter().find(|r| r.name == "nip").expect("nip rule");
    assert!(
        matches!(rule.strategy, Strategy::Hash),
        "NIP should use Hash strategy, got {:?}",
        rule.strategy
    );
    Ok(())
}

#[sinex_test]
async fn regon_rule_uses_hash_strategy() -> ::xtask::sandbox::TestResult<()> {
    let rules = builtin_rules();
    let rule = rules
        .iter()
        .find(|r| r.name == "regon")
        .expect("regon rule");
    assert!(
        matches!(rule.strategy, Strategy::Hash),
        "REGON should use Hash strategy, got {:?}",
        rule.strategy
    );
    Ok(())
}

#[sinex_test]
async fn anthropic_key_uses_suppress_strategy() -> ::xtask::sandbox::TestResult<()> {
    let rules = builtin_rules();
    let rule = rules
        .iter()
        .find(|r| r.name == "anthropic_api_key")
        .expect("anthropic_api_key rule");
    assert!(
        matches!(rule.strategy, Strategy::Suppress),
        "Anthropic API key should be suppressed, got {:?}",
        rule.strategy
    );
    Ok(())
}

#[sinex_test]
async fn openai_key_uses_suppress_strategy() -> ::xtask::sandbox::TestResult<()> {
    let rules = builtin_rules();
    let rule = rules
        .iter()
        .find(|r| r.name == "openai_api_key")
        .expect("openai_api_key rule");
    assert!(
        matches!(rule.strategy, Strategy::Suppress),
        "OpenAI API key should be suppressed, got {:?}",
        rule.strategy
    );
    Ok(())
}
