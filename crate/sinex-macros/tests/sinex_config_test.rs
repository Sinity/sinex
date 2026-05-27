//! Proof-of-concept tests for `#[derive(SinexConfig)]`.
//!
//! See `thoughtspace/crystal/decisions/sinex-config-derive.md` for the design
//! these tests exercise.

use std::{ffi::OsString, path::PathBuf};

use camino::Utf8PathBuf;
use sinex_macros::SinexConfig;
use sinex_primitives::error::SinexError;
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

// ---------------------------------------------------------------------------
// Fallible mode — `from_env()` returns `Result<Self, SinexError>`.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, SinexConfig)]
#[sinex_config(prefix = "SINEX_TEST_FALLIBLE", context = "fallible test", fallible)]
pub struct FallibleConfig {
    #[sinex_config(default = 8080_u16)]
    pub port: u16,
    #[sinex_config(default = false)]
    pub debug: bool,
    pub maybe_tag: Option<String>,
    #[sinex_config(default_expr = "\"localhost\".to_string()")]
    pub host: String,
}

const FALLIBLE_KEYS: &[&str] = &[
    "SINEX_TEST_FALLIBLE_PORT",
    "SINEX_TEST_FALLIBLE_DEBUG",
    "SINEX_TEST_FALLIBLE_MAYBE_TAG",
    "SINEX_TEST_FALLIBLE_HOST",
];

#[sinex_test]
async fn sinex_config_fallible_defaults() -> TestResult<()> {
    let _snap = EnvSnapshot::capture(FALLIBLE_KEYS);
    clear_env(FALLIBLE_KEYS);

    let cfg = FallibleConfig::from_env().expect("fallible from_env with defaults should succeed");
    assert_eq!(cfg.port, 8080);
    assert!(!cfg.debug);
    assert_eq!(cfg.maybe_tag, None);
    assert_eq!(cfg.host, "localhost");
    Ok(())
}

#[sinex_test]
async fn sinex_config_fallible_reads_env() -> TestResult<()> {
    let _snap = EnvSnapshot::capture(FALLIBLE_KEYS);
    clear_env(FALLIBLE_KEYS);

    unsafe {
        std::env::set_var("SINEX_TEST_FALLIBLE_PORT", "9090");
        std::env::set_var("SINEX_TEST_FALLIBLE_DEBUG", "true");
        std::env::set_var("SINEX_TEST_FALLIBLE_MAYBE_TAG", "prod");
        std::env::set_var("SINEX_TEST_FALLIBLE_HOST", "example.com");
    }
    let cfg = FallibleConfig::from_env().expect("from_env with valid env should succeed");
    assert_eq!(cfg.port, 9090);
    assert!(cfg.debug);
    assert_eq!(cfg.maybe_tag.as_deref(), Some("prod"));
    assert_eq!(cfg.host, "example.com");
    Ok(())
}

#[sinex_test]
async fn sinex_config_fallible_propagates_parse_error() -> TestResult<()> {
    let _snap = EnvSnapshot::capture(FALLIBLE_KEYS);
    clear_env(FALLIBLE_KEYS);

    unsafe {
        std::env::set_var("SINEX_TEST_FALLIBLE_PORT", "not-a-port");
    }
    let result = FallibleConfig::from_env();
    assert!(result.is_err(), "invalid port value should yield Err");
    let err_str = result.unwrap_err().to_string();
    assert!(
        err_str.contains("SINEX_TEST_FALLIBLE_PORT"),
        "error should mention the env key; got: {err_str}"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// `nested` attribute — delegate to field type's `from_env()`.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, SinexConfig)]
#[sinex_config(prefix = "SINEX_TEST_INNER", context = "inner config")]
pub struct InnerConfig {
    #[sinex_config(default = 42_u32)]
    pub value: u32,
}

#[derive(Debug, Clone, SinexConfig)]
#[sinex_config(prefix = "SINEX_TEST_OUTER", context = "outer config")]
pub struct OuterConfig {
    #[sinex_config(default = 7_u32)]
    pub outer_value: u32,
    #[sinex_config(nested)]
    pub inner: InnerConfig,
}

const NESTED_KEYS: &[&str] = &["SINEX_TEST_OUTER_OUTER_VALUE", "SINEX_TEST_INNER_VALUE"];

#[sinex_test]
async fn sinex_config_nested_infallible() -> TestResult<()> {
    let _snap = EnvSnapshot::capture(NESTED_KEYS);
    clear_env(NESTED_KEYS);

    let cfg = OuterConfig::from_env();
    assert_eq!(cfg.outer_value, 7);
    assert_eq!(cfg.inner.value, 42);

    unsafe {
        std::env::set_var("SINEX_TEST_OUTER_OUTER_VALUE", "99");
        std::env::set_var("SINEX_TEST_INNER_VALUE", "100");
    }
    let cfg = OuterConfig::from_env();
    assert_eq!(cfg.outer_value, 99);
    assert_eq!(cfg.inner.value, 100);
    Ok(())
}

// nested (infallible inner) inside a fallible parent — no `?`
#[derive(Debug, Clone, SinexConfig)]
#[sinex_config(prefix = "SINEX_TEST_FALLIBLE_OUTER", context = "fallible outer", fallible)]
pub struct FallibleOuterConfig {
    #[sinex_config(default = 1_u32)]
    pub count: u32,
    // InnerConfig::from_env() is infallible; use plain `nested`.
    #[sinex_config(nested)]
    pub inner: InnerConfig,
}

const FALLIBLE_OUTER_KEYS: &[&str] =
    &["SINEX_TEST_FALLIBLE_OUTER_COUNT", "SINEX_TEST_INNER_VALUE"];

#[sinex_test]
async fn sinex_config_nested_inside_fallible() -> TestResult<()> {
    let _snap = EnvSnapshot::capture(FALLIBLE_OUTER_KEYS);
    clear_env(FALLIBLE_OUTER_KEYS);

    let cfg = FallibleOuterConfig::from_env().expect("nested inside fallible should succeed");
    assert_eq!(cfg.count, 1);
    assert_eq!(cfg.inner.value, 42);
    Ok(())
}

// nested_fallible — inner type is also fallible; propagate its Result with `?`.
#[derive(Debug, Clone, SinexConfig)]
#[sinex_config(prefix = "SINEX_TEST_FALLIBLE_INNER", context = "fallible inner", fallible)]
pub struct FallibleInnerConfig {
    #[sinex_config(default = 0_u32)]
    pub inner_val: u32,
}

#[derive(Debug, Clone, SinexConfig)]
#[sinex_config(
    prefix = "SINEX_TEST_NESTED_FALLIBLE",
    context = "nested fallible parent",
    fallible
)]
pub struct NestedFallibleParentConfig {
    #[sinex_config(default = 5_u32)]
    pub parent_val: u32,
    // FallibleInnerConfig::from_env() returns Result; use `nested_fallible`.
    #[sinex_config(nested_fallible)]
    pub inner: FallibleInnerConfig,
}

const NESTED_FALLIBLE_KEYS: &[&str] = &[
    "SINEX_TEST_NESTED_FALLIBLE_PARENT_VAL",
    "SINEX_TEST_FALLIBLE_INNER_INNER_VAL",
];

#[sinex_test]
async fn sinex_config_nested_fallible_propagates_result() -> TestResult<()> {
    let _snap = EnvSnapshot::capture(NESTED_FALLIBLE_KEYS);
    clear_env(NESTED_FALLIBLE_KEYS);

    let cfg =
        NestedFallibleParentConfig::from_env().expect("nested_fallible defaults should succeed");
    assert_eq!(cfg.parent_val, 5);
    assert_eq!(cfg.inner.inner_val, 0);

    unsafe {
        std::env::set_var("SINEX_TEST_FALLIBLE_INNER_INNER_VAL", "not-a-number");
    }
    let result = NestedFallibleParentConfig::from_env();
    assert!(result.is_err(), "bad inner value should propagate as Err");
    Ok(())
}

// ---------------------------------------------------------------------------
// `normalize_fn` attribute
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, SinexConfig)]
#[sinex_config(prefix = "SINEX_TEST_NORM", context = "normalize test", normalize_fn = "normalize")]
pub struct NormalizeConfig {
    #[sinex_config(default_expr = "\"raw\".to_string()")]
    pub name: String,
    /// Set by normalize().
    #[sinex_config(skip)]
    pub normalized_name: String,
}

impl NormalizeConfig {
    fn normalize(mut self) -> Self {
        self.normalized_name = self.name.to_uppercase();
        self
    }
}

const NORM_KEYS: &[&str] = &["SINEX_TEST_NORM_NAME"];

#[sinex_test]
async fn sinex_config_normalize_fn_called() -> TestResult<()> {
    let _snap = EnvSnapshot::capture(NORM_KEYS);
    clear_env(NORM_KEYS);

    let cfg = NormalizeConfig::from_env();
    assert_eq!(cfg.name, "raw");
    assert_eq!(cfg.normalized_name, "RAW");

    unsafe {
        std::env::set_var("SINEX_TEST_NORM_NAME", "hello");
    }
    let cfg = NormalizeConfig::from_env();
    assert_eq!(cfg.normalized_name, "HELLO");
    Ok(())
}

// normalize_fn on fallible struct returns Result<Self, SinexError>
#[derive(Debug, Clone, SinexConfig)]
#[sinex_config(
    prefix = "SINEX_TEST_FALLIBLE_NORM",
    context = "fallible normalize",
    fallible,
    normalize_fn = "normalize"
)]
pub struct FallibleNormalizeConfig {
    #[sinex_config(default = 0_u32)]
    pub count: u32,
}

impl FallibleNormalizeConfig {
    fn normalize(self) -> Result<Self, SinexError> {
        if self.count > 100 {
            return Err(SinexError::configuration(
                "count must not exceed 100 in FallibleNormalizeConfig",
            ));
        }
        Ok(self)
    }
}

const FALLIBLE_NORM_KEYS: &[&str] = &["SINEX_TEST_FALLIBLE_NORM_COUNT"];

#[sinex_test]
async fn sinex_config_fallible_normalize_fn_ok() -> TestResult<()> {
    let _snap = EnvSnapshot::capture(FALLIBLE_NORM_KEYS);
    clear_env(FALLIBLE_NORM_KEYS);

    let cfg = FallibleNormalizeConfig::from_env().expect("count=0 should pass normalize");
    assert_eq!(cfg.count, 0);
    Ok(())
}

#[sinex_test]
async fn sinex_config_fallible_normalize_fn_err() -> TestResult<()> {
    let _snap = EnvSnapshot::capture(FALLIBLE_NORM_KEYS);
    clear_env(FALLIBLE_NORM_KEYS);

    unsafe {
        std::env::set_var("SINEX_TEST_FALLIBLE_NORM_COUNT", "200");
    }
    let result = FallibleNormalizeConfig::from_env();
    assert!(result.is_err(), "count > 100 should fail normalize");
    Ok(())
}

// ---------------------------------------------------------------------------
// `Utf8PathBuf` support (both infallible and fallible).
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, SinexConfig)]
#[sinex_config(prefix = "SINEX_TEST_UTF8PATH", context = "utf8path test")]
pub struct Utf8PathConfig {
    #[sinex_config(default_expr = "Utf8PathBuf::from(\"/tmp/default\")")]
    pub work_dir: Utf8PathBuf,
    pub maybe_dir: Option<Utf8PathBuf>,
}

const UTF8_KEYS: &[&str] = &["SINEX_TEST_UTF8PATH_WORK_DIR", "SINEX_TEST_UTF8PATH_MAYBE_DIR"];

#[sinex_test]
async fn sinex_config_utf8pathbuf_infallible() -> TestResult<()> {
    let _snap = EnvSnapshot::capture(UTF8_KEYS);
    clear_env(UTF8_KEYS);

    let cfg = Utf8PathConfig::from_env();
    assert_eq!(cfg.work_dir, Utf8PathBuf::from("/tmp/default"));
    assert_eq!(cfg.maybe_dir, None);

    unsafe {
        std::env::set_var("SINEX_TEST_UTF8PATH_WORK_DIR", "/tmp/override");
        std::env::set_var("SINEX_TEST_UTF8PATH_MAYBE_DIR", "/tmp/opt");
    }
    let cfg = Utf8PathConfig::from_env();
    assert_eq!(cfg.work_dir, Utf8PathBuf::from("/tmp/override"));
    assert_eq!(cfg.maybe_dir, Some(Utf8PathBuf::from("/tmp/opt")));
    Ok(())
}

#[derive(Debug, Clone, SinexConfig)]
#[sinex_config(prefix = "SINEX_TEST_UTF8PATH_F", context = "utf8path fallible", fallible)]
pub struct Utf8PathFallibleConfig {
    #[sinex_config(default_expr = "Utf8PathBuf::from(\"/tmp/default\")")]
    pub work_dir: Utf8PathBuf,
    pub maybe_dir: Option<Utf8PathBuf>,
}

const UTF8_FALLIBLE_KEYS: &[&str] = &[
    "SINEX_TEST_UTF8PATH_F_WORK_DIR",
    "SINEX_TEST_UTF8PATH_F_MAYBE_DIR",
];

#[sinex_test]
async fn sinex_config_utf8pathbuf_fallible() -> TestResult<()> {
    let _snap = EnvSnapshot::capture(UTF8_FALLIBLE_KEYS);
    clear_env(UTF8_FALLIBLE_KEYS);

    let cfg =
        Utf8PathFallibleConfig::from_env().expect("utf8pathbuf fallible defaults should work");
    assert_eq!(cfg.work_dir, Utf8PathBuf::from("/tmp/default"));
    assert_eq!(cfg.maybe_dir, None);

    unsafe {
        std::env::set_var("SINEX_TEST_UTF8PATH_F_WORK_DIR", "/tmp/fallible-override");
    }
    let cfg = Utf8PathFallibleConfig::from_env()
        .expect("utf8pathbuf fallible with valid path should work");
    assert_eq!(cfg.work_dir, Utf8PathBuf::from("/tmp/fallible-override"));
    Ok(())
}

// ---------------------------------------------------------------------------
// `prefix` with trailing `_` is stripped (no double-underscore).
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, SinexConfig)]
#[sinex_config(prefix = "SINEX_TEST_TRIM_", context = "prefix trim test")]
pub struct PrefixTrimConfig {
    #[sinex_config(default = 5_u32)]
    pub value: u32,
}

const PREFIX_TRIM_KEYS: &[&str] = &["SINEX_TEST_TRIM_VALUE"];

#[sinex_test]
async fn sinex_config_prefix_trailing_underscore_trimmed() -> TestResult<()> {
    let _snap = EnvSnapshot::capture(PREFIX_TRIM_KEYS);
    clear_env(PREFIX_TRIM_KEYS);

    // Default
    let cfg = PrefixTrimConfig::from_env();
    assert_eq!(cfg.value, 5);

    unsafe { std::env::set_var("SINEX_TEST_TRIM_VALUE", "77") };
    let cfg = PrefixTrimConfig::from_env();
    assert_eq!(cfg.value, 77);
    Ok(())
}

// ---------------------------------------------------------------------------
// `default_fn = "..."` sugar (calls the named function with no args).
// ---------------------------------------------------------------------------

fn my_default_count() -> u32 {
    99
}

#[derive(Debug, Clone, SinexConfig)]
#[sinex_config(prefix = "SINEX_TEST_DEFAULT_FN", context = "default_fn test")]
pub struct DefaultFnConfig {
    #[sinex_config(default_fn = "my_default_count")]
    pub count: u32,
}

const DEFAULT_FN_KEYS: &[&str] = &["SINEX_TEST_DEFAULT_FN_COUNT"];

#[sinex_test]
async fn sinex_config_default_fn_attr() -> TestResult<()> {
    let _snap = EnvSnapshot::capture(DEFAULT_FN_KEYS);
    clear_env(DEFAULT_FN_KEYS);

    let cfg = DefaultFnConfig::from_env();
    assert_eq!(cfg.count, 99);

    unsafe { std::env::set_var("SINEX_TEST_DEFAULT_FN_COUNT", "3") };
    let cfg = DefaultFnConfig::from_env();
    assert_eq!(cfg.count, 3);
    Ok(())
}
