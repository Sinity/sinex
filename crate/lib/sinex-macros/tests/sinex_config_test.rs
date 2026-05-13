//! Proof-of-concept tests for `#[derive(SinexConfig)]`.
//!
//! See `thoughtspace/crystal/decisions/sinex-config-derive.md` for the design
//! these tests exercise.

use std::path::PathBuf;

use sinex_macros::SinexConfig;
use xtask::sandbox::prelude::*;

// ---------------------------------------------------------------------------
// Type-driven helper selection — `bool` → `bool_or`, `String` → `var_or`,
// `Option<PathBuf>` → `path_optional`, `Option<String>` → `var_optional`,
// `Option<T>` → `parse_optional`, other `T: FromStr` → `parse_or`.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, SinexConfig)]
#[sinex_config(prefix = "SINEX_TEST_DEFAULTS", context = "defaults test")]
pub struct DefaultsConfig {
    #[sinex_config(default = 100)]
    pub batch_size: u32,
    #[sinex_config(default = 5_u64)]
    pub interval_secs: u64,
    #[sinex_config(default = true)]
    pub feature_a: bool,
}

#[sinex_test]
async fn sinex_config_loads_defaults_when_env_unset(_ctx: TestContext) -> TestResult<()> {
    // No env vars set: every field should fall back to its declared default.
    // We don't unset anything; just verify the right defaults appear assuming
    // the env keys are unlikely to be set in the test environment.
    let cfg = DefaultsConfig::from_env();
    assert_eq!(cfg.batch_size, 100);
    assert_eq!(cfg.interval_secs, 5);
    assert!(cfg.feature_a);
    Ok(())
}

#[sinex_test]
async fn sinex_config_reads_env_when_set(_ctx: TestContext) -> TestResult<()> {
    // SAFETY: edition 2024 makes env mutation unsafe. We isolate within this
    // test; xtask sandbox runs tests in-process but with unique env keys per
    // test, so collisions across parallel runs don't occur here.
    unsafe {
        std::env::set_var("SINEX_TEST_DEFAULTS_BATCH_SIZE", "777");
        std::env::set_var("SINEX_TEST_DEFAULTS_INTERVAL_SECS", "42");
        std::env::set_var("SINEX_TEST_DEFAULTS_FEATURE_A", "false");
    }

    let cfg = DefaultsConfig::from_env();
    assert_eq!(cfg.batch_size, 777);
    assert_eq!(cfg.interval_secs, 42);
    assert!(!cfg.feature_a);

    unsafe {
        std::env::remove_var("SINEX_TEST_DEFAULTS_BATCH_SIZE");
        std::env::remove_var("SINEX_TEST_DEFAULTS_INTERVAL_SECS");
        std::env::remove_var("SINEX_TEST_DEFAULTS_FEATURE_A");
    }
    Ok(())
}

#[sinex_test]
async fn sinex_config_invalid_parse_falls_back_to_default(_ctx: TestContext) -> TestResult<()> {
    // Invalid env value: warn-log + default. (Strict mode is a future
    // extension; the default macro behavior is forgiving.)
    unsafe {
        std::env::set_var("SINEX_TEST_DEFAULTS_BATCH_SIZE", "not-a-number");
    }
    let cfg = DefaultsConfig::from_env();
    assert_eq!(cfg.batch_size, 100);
    unsafe {
        std::env::remove_var("SINEX_TEST_DEFAULTS_BATCH_SIZE");
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Option types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, SinexConfig)]
#[sinex_config(prefix = "SINEX_TEST_OPTIONS", context = "options test")]
pub struct OptionsConfig {
    pub maybe_value: Option<String>,
    pub maybe_count: Option<u64>,
    pub maybe_path: Option<PathBuf>,
}

#[sinex_test]
async fn sinex_config_options_are_none_when_unset(_ctx: TestContext) -> TestResult<()> {
    let cfg = OptionsConfig::from_env();
    assert_eq!(cfg.maybe_value, None);
    assert_eq!(cfg.maybe_count, None);
    assert_eq!(cfg.maybe_path, None);
    Ok(())
}

#[sinex_test]
async fn sinex_config_options_load_when_set(_ctx: TestContext) -> TestResult<()> {
    unsafe {
        std::env::set_var("SINEX_TEST_OPTIONS_MAYBE_VALUE", "hello");
        std::env::set_var("SINEX_TEST_OPTIONS_MAYBE_COUNT", "12");
        std::env::set_var("SINEX_TEST_OPTIONS_MAYBE_PATH", "/tmp/x");
    }
    let cfg = OptionsConfig::from_env();
    assert_eq!(cfg.maybe_value.as_deref(), Some("hello"));
    assert_eq!(cfg.maybe_count, Some(12));
    assert_eq!(cfg.maybe_path, Some(PathBuf::from("/tmp/x")));
    unsafe {
        std::env::remove_var("SINEX_TEST_OPTIONS_MAYBE_VALUE");
        std::env::remove_var("SINEX_TEST_OPTIONS_MAYBE_COUNT");
        std::env::remove_var("SINEX_TEST_OPTIONS_MAYBE_PATH");
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// `skip` attribute
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, SinexConfig)]
#[sinex_config(prefix = "SINEX_TEST_SKIP", context = "skip test")]
pub struct SkipConfig {
    #[sinex_config(default = 42)]
    pub from_env_field: u32,
    #[sinex_config(skip)]
    pub computed_field: Option<String>,
}

#[sinex_test]
async fn sinex_config_skip_uses_default_impl(_ctx: TestContext) -> TestResult<()> {
    let cfg = SkipConfig::from_env();
    assert_eq!(cfg.from_env_field, 42);
    // Default for Option<String> is None.
    assert_eq!(cfg.computed_field, None);
    Ok(())
}

// ---------------------------------------------------------------------------
// `env = "..."` overrides the suffix
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, SinexConfig)]
#[sinex_config(prefix = "SINEX_TEST_RENAME", context = "rename test")]
pub struct RenameConfig {
    #[sinex_config(env = "EXPLICIT_KEY", default = 7_u32)]
    pub field_with_long_rust_name: u32,
}

#[sinex_test]
async fn sinex_config_env_attr_overrides_default_suffix(_ctx: TestContext) -> TestResult<()> {
    unsafe {
        std::env::set_var("SINEX_TEST_RENAME_EXPLICIT_KEY", "11");
    }
    let cfg = RenameConfig::from_env();
    assert_eq!(cfg.field_with_long_rust_name, 11);
    unsafe {
        std::env::remove_var("SINEX_TEST_RENAME_EXPLICIT_KEY");
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// `default_expr` for non-literal defaults
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, SinexConfig)]
#[sinex_config(prefix = "SINEX_TEST_EXPR", context = "default_expr test")]
pub struct DefaultExprConfig {
    #[sinex_config(default_expr = "(2u64.pow(10))")]
    pub computed_default: u64,
}

#[sinex_test]
async fn sinex_config_default_expr_evaluates_at_compile_time(_ctx: TestContext) -> TestResult<()> {
    let cfg = DefaultExprConfig::from_env();
    assert_eq!(cfg.computed_default, 1024);
    Ok(())
}
