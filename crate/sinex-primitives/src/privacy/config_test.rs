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
async fn default_config_round_trips_through_toml() -> ::xtask::sandbox::TestResult<()> {
    let config = PrivacyConfig::default();
    let toml_str = toml::to_string_pretty(&config).expect("serialize");
    let parsed: PrivacyConfig = toml::from_str(&toml_str).expect("deserialize");

    assert!(parsed.enabled);
    assert!(matches!(parsed.builtin_categories, CategorySet::None));
    assert!(parsed.extra_rules.is_empty());
    assert!(parsed.overrides.is_empty());
    assert!(!parsed.track_stats);
    Ok(())
}

#[sinex_test]
async fn category_set_deserializes_all_forms() -> ::xtask::sandbox::TestResult<()> {
    // String "all"
    let val: CategorySet = toml::from_str::<TomlWrap>("c = \"all\"").unwrap().c;
    assert!(matches!(val, CategorySet::All));

    // String "none"
    let val: CategorySet = toml::from_str::<TomlWrap>("c = \"none\"").unwrap().c;
    assert!(matches!(val, CategorySet::None));

    // Array of categories
    let val: CategorySet = toml::from_str::<TomlWrap>("c = [\"secret\", \"pii\"]")
        .unwrap()
        .c;
    match val {
        CategorySet::Only(cats) => {
            assert_eq!(cats.len(), 2);
            assert_eq!(cats[0], RuleCategory::Secret);
            assert_eq!(cats[1], RuleCategory::Pii);
        }
        other => panic!("expected Only, got {other:?}"),
    }

    // Empty array → None
    let val: CategorySet = toml::from_str::<TomlWrap>("c = []").unwrap().c;
    assert!(matches!(val, CategorySet::None));
    Ok(())
}

/// Helper for testing `CategorySet` deserialization in isolation.
#[derive(Deserialize)]
struct TomlWrap {
    c: CategorySet,
}

#[sinex_test]
async fn from_file_parses_realistic_config() -> ::xtask::sandbox::TestResult<()> {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("privacy.toml");
    std::fs::write(
        &path,
        r#"
enabled = true
builtin_categories = ["secret", "pii"]
default_strategy = { action = "encrypt" }
track_stats = true

[key]
file = "/tmp/test.key"

[overrides.email_address]
enabled = false

[overrides.ipv4_address]
strategy = { action = "hash" }

[[extra_rules]]
name = "my_rule"
description = "Custom pattern"
category = "custom"
matcher = { type = "regex", pattern = "CUSTOM-\\d+" }
strategy = { action = "redact", label = "<CUSTOM>" }
contexts = ["command"]
"#,
    )
    .unwrap();

    let config = PrivacyConfig::from_file(&path).unwrap();
    assert!(config.enabled);
    assert!(config.track_stats);
    assert!(matches!(config.default_strategy, Strategy::Encrypt));
    assert_eq!(config.key.key_file.as_deref(), Some("/tmp/test.key"));

    // Categories
    match &config.builtin_categories {
        CategorySet::Only(cats) => {
            assert_eq!(cats.len(), 2);
        }
        other => panic!("expected Only, got {other:?}"),
    }

    // Overrides
    assert_eq!(config.overrides.len(), 2);
    assert_eq!(config.overrides["email_address"].enabled, Some(false));
    assert!(matches!(
        config.overrides["ipv4_address"].strategy,
        Some(Strategy::Hash)
    ));

    // Extra rules
    assert_eq!(config.extra_rules.len(), 1);
    assert_eq!(config.extra_rules[0].name, "my_rule");
    Ok(())
}

#[sinex_test]
async fn from_file_missing_fields_use_defaults() -> ::xtask::sandbox::TestResult<()> {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("minimal.toml");
    std::fs::write(&path, "track_stats = true\n").unwrap();

    let config = PrivacyConfig::from_file(&path).unwrap();
    assert!(config.enabled); // default
    assert!(matches!(config.builtin_categories, CategorySet::None)); // default
    assert!(config.track_stats); // overridden
    Ok(())
}

#[sinex_test]
async fn from_file_nonexistent_returns_error() -> ::xtask::sandbox::TestResult<()> {
    let result = PrivacyConfig::from_file(Path::new("/nonexistent/privacy.toml"));
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("failed to read"));
    Ok(())
}

#[sinex_test]
async fn from_file_invalid_toml_returns_error() -> ::xtask::sandbox::TestResult<()> {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bad.toml");
    std::fs::write(&path, "enabled = [[[invalid").unwrap();

    let result = PrivacyConfig::from_file(&path);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("failed to parse"));
    Ok(())
}

#[sinex_test]
async fn key_config_toml_field_names() -> ::xtask::sandbox::TestResult<()> {
    // Verify the TOML-friendly field names (file/hex instead of key_file/key_hex)
    let toml_str = r#"
[key]
file = "/path/to/key"
hex = "abcd1234"
"#;
    let config: PrivacyConfig = toml::from_str(toml_str).unwrap();
    assert_eq!(config.key.key_file.as_deref(), Some("/path/to/key"));
    assert_eq!(config.key.key_hex.as_deref(), Some("abcd1234"));
    Ok(())
}

#[sinex_test]
async fn from_env_loads_explicit_file() -> ::xtask::sandbox::TestResult<()> {
    let _guard = ENV_LOCK.lock().await;
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("privacy.toml");
    std::fs::write(&path, "track_stats = true\n")?;

    let old_privacy_config = std::env::var_os("SINEX_PRIVACY_CONFIG");
    let old_state_dir = std::env::var_os("SINEX_STATE_DIR");
    unsafe {
        std::env::set_var("SINEX_PRIVACY_CONFIG", &path);
        std::env::remove_var("SINEX_STATE_DIR");
    }

    let result = PrivacyConfig::from_env();

    restore_var("SINEX_PRIVACY_CONFIG", old_privacy_config);
    restore_var("SINEX_STATE_DIR", old_state_dir);

    let config = result?;
    assert!(config.track_stats);
    Ok(())
}

#[sinex_test]
async fn from_env_surfaces_missing_explicit_file() -> ::xtask::sandbox::TestResult<()> {
    let _guard = ENV_LOCK.lock().await;
    let missing = PathBuf::from("/tmp/sinex-privacy-config-does-not-exist.toml");
    let old_privacy_config = std::env::var_os("SINEX_PRIVACY_CONFIG");
    let old_state_dir = std::env::var_os("SINEX_STATE_DIR");
    unsafe {
        std::env::set_var("SINEX_PRIVACY_CONFIG", &missing);
        std::env::remove_var("SINEX_STATE_DIR");
    }

    let result = PrivacyConfig::from_env();

    restore_var("SINEX_PRIVACY_CONFIG", old_privacy_config);
    restore_var("SINEX_STATE_DIR", old_state_dir);

    let error = result.expect_err("missing explicit privacy config must surface");
    assert!(error.to_string().contains("failed to read privacy config"));
    assert!(
        error
            .to_string()
            .contains(missing.to_string_lossy().as_ref())
    );
    Ok(())
}

#[sinex_test]
async fn from_env_surfaces_invalid_state_dir_config() -> ::xtask::sandbox::TestResult<()> {
    let _guard = ENV_LOCK.lock().await;
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("privacy.toml");
    std::fs::write(&path, "enabled = [[[invalid")?;

    let old_privacy_config = std::env::var_os("SINEX_PRIVACY_CONFIG");
    let old_state_dir = std::env::var_os("SINEX_STATE_DIR");
    unsafe {
        std::env::remove_var("SINEX_PRIVACY_CONFIG");
        std::env::set_var("SINEX_STATE_DIR", dir.path());
    }

    let result = PrivacyConfig::from_env();

    restore_var("SINEX_PRIVACY_CONFIG", old_privacy_config);
    restore_var("SINEX_STATE_DIR", old_state_dir);

    let error = result.expect_err("invalid state-dir privacy config must surface");
    assert!(error.to_string().contains("failed to parse privacy config"));
    assert!(error.to_string().contains(path.to_string_lossy().as_ref()));
    Ok(())
}

#[sinex_test]
async fn from_env_rejects_invalid_extra_rules_json() -> ::xtask::sandbox::TestResult<()> {
    let _guard = ENV_LOCK.lock().await;
    let old_extra_rules = std::env::var_os("SINEX_PRIVACY_EXTRA_RULES");
    unsafe { std::env::set_var("SINEX_PRIVACY_EXTRA_RULES", "{not-json") };

    let result = PrivacyConfig::from_env();

    restore_var("SINEX_PRIVACY_EXTRA_RULES", old_extra_rules);

    let error = result.expect_err("invalid JSON override must surface");
    assert!(
        error
            .to_string()
            .contains("invalid privacy environment override SINEX_PRIVACY_EXTRA_RULES")
    );
    assert!(error.to_string().contains("failed to parse JSON override"));
    Ok(())
}

#[sinex_test]
async fn from_env_rejects_invalid_builtin_categories() -> ::xtask::sandbox::TestResult<()> {
    let _guard = ENV_LOCK.lock().await;
    let old_builtin = std::env::var_os("SINEX_PRIVACY_BUILTIN");
    unsafe { std::env::set_var("SINEX_PRIVACY_BUILTIN", "secret,wat") };

    let result = PrivacyConfig::from_env();

    restore_var("SINEX_PRIVACY_BUILTIN", old_builtin);

    let error = result.expect_err("invalid builtin categories must surface");
    assert!(
        error
            .to_string()
            .contains("invalid privacy environment override SINEX_PRIVACY_BUILTIN")
    );
    assert!(error.to_string().contains("unknown categories: wat"));
    Ok(())
}

#[sinex_test]
async fn from_env_rejects_invalid_default_strategy() -> ::xtask::sandbox::TestResult<()> {
    let _guard = ENV_LOCK.lock().await;
    let old_strategy = std::env::var_os("SINEX_PRIVACY_DEFAULT_STRATEGY");
    unsafe { std::env::set_var("SINEX_PRIVACY_DEFAULT_STRATEGY", "explode") };

    let result = PrivacyConfig::from_env();

    restore_var("SINEX_PRIVACY_DEFAULT_STRATEGY", old_strategy);

    let error = result.expect_err("invalid strategy override must surface");
    assert!(
        error
            .to_string()
            .contains("invalid privacy environment override SINEX_PRIVACY_DEFAULT_STRATEGY")
    );
    assert!(
        error
            .to_string()
            .contains("expected redact, encrypt, hash, or suppress")
    );
    Ok(())
}
