//! Proof-of-concept tests for `#[derive(SinexConfig)]`.
//!
//! See `thoughtspace/crystal/decisions/sinex-config-derive.md` for the design
//! these tests exercise.

use std::{ffi::OsString, path::PathBuf};

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

// ---------------------------------------------------------------------------
// `env = "..."` overrides the suffix
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, SinexConfig)]
#[sinex_config(prefix = "SINEX_TEST_RENAME", context = "rename test")]
pub struct RenameConfig {
    #[sinex_config(env = "EXPLICIT_KEY", default = 7_u32)]
    pub field_with_long_rust_name: u32,
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

const ENV_KEYS: &[&str] = &[
    "SINEX_TEST_DEFAULTS_BATCH_SIZE",
    "SINEX_TEST_DEFAULTS_INTERVAL_SECS",
    "SINEX_TEST_DEFAULTS_FEATURE_A",
    "SINEX_TEST_OPTIONS_MAYBE_VALUE",
    "SINEX_TEST_OPTIONS_MAYBE_COUNT",
    "SINEX_TEST_OPTIONS_MAYBE_PATH",
    "SINEX_TEST_RENAME_EXPLICIT_KEY",
];

struct EnvSnapshot {
    values: Vec<(&'static str, Option<OsString>)>,
}

impl EnvSnapshot {
    fn capture(keys: &'static [&'static str]) -> Self {
        Self {
            values: keys
                .iter()
                .map(|key| (*key, std::env::var_os(key)))
                .collect(),
        }
    }
}

impl Drop for EnvSnapshot {
    fn drop(&mut self) {
        for (key, value) in &self.values {
            unsafe {
                match value {
                    Some(value) => std::env::set_var(key, value),
                    None => std::env::remove_var(key),
                }
            }
        }
    }
}

fn clear_env(keys: &[&str]) {
    for key in keys {
        unsafe { std::env::remove_var(key) };
    }
}

#[sinex_test]
async fn sinex_config_derive_loads_env_contracts() -> TestResult<()> {
    let _env_snapshot = EnvSnapshot::capture(ENV_KEYS);
    clear_env(ENV_KEYS);

    let cfg = DefaultsConfig::from_env();
    assert_eq!(cfg.batch_size, 100);
    assert_eq!(cfg.interval_secs, 5);
    assert!(cfg.feature_a);

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
        std::env::set_var("SINEX_TEST_DEFAULTS_BATCH_SIZE", "not-a-number");
    }
    let cfg = DefaultsConfig::from_env();
    assert_eq!(cfg.batch_size, 100);

    clear_env(ENV_KEYS);
    let cfg = OptionsConfig::from_env();
    assert_eq!(cfg.maybe_value, None);
    assert_eq!(cfg.maybe_count, None);
    assert_eq!(cfg.maybe_path, None);

    unsafe {
        std::env::set_var("SINEX_TEST_OPTIONS_MAYBE_VALUE", "hello");
        std::env::set_var("SINEX_TEST_OPTIONS_MAYBE_COUNT", "12");
        std::env::set_var("SINEX_TEST_OPTIONS_MAYBE_PATH", "/tmp/x");
    }
    let cfg = OptionsConfig::from_env();
    assert_eq!(cfg.maybe_value.as_deref(), Some("hello"));
    assert_eq!(cfg.maybe_count, Some(12));
    assert_eq!(cfg.maybe_path, Some(PathBuf::from("/tmp/x")));

    clear_env(ENV_KEYS);
    let cfg = SkipConfig::from_env();
    assert_eq!(cfg.from_env_field, 42);
    assert_eq!(cfg.computed_field, None);

    unsafe {
        std::env::set_var("SINEX_TEST_RENAME_EXPLICIT_KEY", "11");
    }
    let cfg = RenameConfig::from_env();
    assert_eq!(cfg.field_with_long_rust_name, 11);

    clear_env(ENV_KEYS);
    let cfg = DefaultExprConfig::from_env();
    assert_eq!(cfg.computed_default, 1024);

    Ok(())
}
