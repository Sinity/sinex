// Exception to per-crate tests/: this exercises private privacy-engine
// initialization helpers without widening the public API.
use super::*;
use std::ffi::OsString;
use std::sync::LazyLock;
use xtask::sandbox::sinex_test;

static ENV_LOCK: LazyLock<tokio::sync::Mutex<()>> =
    LazyLock::new(|| tokio::sync::Mutex::new(()));

fn restore_var(key: &str, value: Option<OsString>) {
    match value {
        Some(value) => unsafe { std::env::set_var(key, value) },
        None => unsafe { std::env::remove_var(key) },
    }
}

#[sinex_test]
async fn explicit_env_config_load_propagates_config_errors() -> ::xtask::sandbox::TestResult<()>
{
    let _guard = ENV_LOCK.lock().await;
    let old_extra_rules = std::env::var_os("SINEX_PRIVACY_EXTRA_RULES");
    unsafe { std::env::set_var("SINEX_PRIVACY_EXTRA_RULES", "{not-json") };

    let result = PrivacyConfig::from_env()
        .map_err(PrivacyError::Config)
        .and_then(PrivacyEngine::new);

    restore_var("SINEX_PRIVACY_EXTRA_RULES", old_extra_rules);

    let Err(err) = result else {
        panic!("invalid privacy env override should fail honestly")
    };
    assert!(matches!(err, PrivacyError::Config(_)));
    assert!(
        err.to_string()
            .contains("invalid privacy environment override SINEX_PRIVACY_EXTRA_RULES")
    );
    Ok(())
}

#[sinex_test]
async fn explicit_env_config_load_accepts_default_configuration()
-> ::xtask::sandbox::TestResult<()> {
    let _guard = ENV_LOCK.lock().await;
    let old_extra_rules = std::env::var_os("SINEX_PRIVACY_EXTRA_RULES");
    let old_builtin = std::env::var_os("SINEX_PRIVACY_BUILTIN_CATEGORIES");
    unsafe { std::env::remove_var("SINEX_PRIVACY_EXTRA_RULES") };
    unsafe { std::env::remove_var("SINEX_PRIVACY_BUILTIN_CATEGORIES") };

    let engine = PrivacyEngine::new(PrivacyConfig::from_env().map_err(PrivacyError::Config)?)?;
    let processed = engine.process("token=abc", ProcessingContext::Command);

    restore_var("SINEX_PRIVACY_EXTRA_RULES", old_extra_rules);
    restore_var("SINEX_PRIVACY_BUILTIN_CATEGORIES", old_builtin);

    // Default config loads cleanly, but built-in catalog rules are opt-in
    // (#1042): with no extra rules and the default `CategorySet::None`, the
    // engine performs no automatic redaction. Policy now comes from DB/user
    // rules, not an always-on catalog.
    assert!(
        !processed.any_matched(),
        "default-from-env engine must not auto-redact; built-in rules are opt-in"
    );
    Ok(())
}

#[sinex_test]
async fn sensitivity_hint_does_not_auto_act() -> ::xtask::sandbox::TestResult<()> {
    // AC4 (#1611): a `SensitivityHint` is an exported annotation for policy
    // tooling. It is *not* wired to the privacy engine and must never cause
    // redaction on its own. The default engine (no DB/user rules) sees a
    // field value classified as `FreeText` / `PotentiallySensitive` and
    // leaves it untouched, because there is no seeded/user rule for it.
    let _guard = ENV_LOCK.lock().await;
    let old_extra_rules = std::env::var_os("SINEX_PRIVACY_EXTRA_RULES");
    let old_builtin = std::env::var_os("SINEX_PRIVACY_BUILTIN_CATEGORIES");
    unsafe { std::env::remove_var("SINEX_PRIVACY_EXTRA_RULES") };
    unsafe { std::env::remove_var("SINEX_PRIVACY_BUILTIN_CATEGORIES") };

    let engine = PrivacyEngine::new(PrivacyConfig::from_env().map_err(PrivacyError::Config)?)?;

    // A window title is annotated `FreeText` + `PotentiallySensitive` at the
    // parser layer. The hint is documentation, not a trigger.
    let hinted_value = "vim ~/Documents/quarterly-review.md - Neovim";
    let processed = engine.process(hinted_value, ProcessingContext::Document);

    restore_var("SINEX_PRIVACY_EXTRA_RULES", old_extra_rules);
    restore_var("SINEX_PRIVACY_BUILTIN_CATEGORIES", old_builtin);

    assert!(
        !processed.any_matched(),
        "sensitivity hints must not auto-redact without a seeded/user rule"
    );
    assert_eq!(
        processed.text.as_ref(),
        hinted_value,
        "hinted field value must pass through unchanged when no rule binds"
    );
    Ok(())
}

#[sinex_test]
async fn privacy_engine_extra_rules_merge_correctly() -> ::xtask::sandbox::TestResult<()> {
    // DB/user policy compiles rows into extra rules. Verify that a caller
    // can explicitly combine seed catalog rules with caller-supplied rules.
    let scoped_rule = PatternRule {
        name: "test_scoped_sentinel".into(),
        description: "fires only in scoped engine".into(),
        category: RuleCategory::Custom,
        matcher: Matcher::Regex {
            pattern: r"SCOPED_SENTINEL_XYZ".into(),
        },
        strategy: Strategy::Redact {
            label: Some("<SCOPED_RULE>".into()),
        },
        contexts: vec![ProcessingContext::Command],
        enabled: true,
    };

    let mut config = PrivacyConfig::default();
    config.builtin_categories = CategorySet::All;
    config.extra_rules.push(scoped_rule);
    let engine = PrivacyEngine::new(config)?;

    // Global rule fires.
    let token = ["ghp_", "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghij"].concat();
    let token_input = format!("export TOKEN={token}");
    let result = engine.process(&token_input, ProcessingContext::Command);
    assert!(
        result.any_matched(),
        "global rule should fire in merged engine"
    );

    // Scoped rule fires.
    let result2 = engine.process("SCOPED_SENTINEL_XYZ", ProcessingContext::Command);
    assert!(
        result2.any_matched(),
        "scoped rule should fire in merged engine"
    );
    assert!(
        result2.text.contains("<SCOPED_RULE>"),
        "got: {}",
        result2.text
    );
    Ok(())
}
