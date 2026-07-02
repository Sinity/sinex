use super::*;
use crate::sandbox::sinex_test;

use crate::sandbox::EnvGuard;

fn env_set(key: &str, value: Option<std::ffi::OsString>) -> EnvGuard {
    let mut guard = EnvGuard::new();
    match value {
        Some(v) => guard.set(key, v),
        None => guard.clear(key),
    }
    guard
}

#[sinex_test]
async fn parse_positive_u64_env_or_default_rejects_invalid_values() -> TestResult<()> {
    let _guard = env_set("SINEX_TEST_TIMEOUT", Some("not-a-number".into()));

    assert_eq!(
        parse_positive_u64_env_or_default("SINEX_TEST_TIMEOUT", 42, "test timeout"),
        42
    );
    Ok(())
}

#[sinex_test]
async fn open_history_db_uses_declared_access_mode() -> TestResult<()> {
    let temp = tempfile::tempdir()?;
    let history_db = temp.path().join("xtask-history-test.db");
    let mut env = EnvGuard::with_keys(&["XTASK_HISTORY_DB"]);
    env.set("XTASK_HISTORY_DB", &history_db);

    let _query = open_history_db(HistoryAccessMode::Query)?;
    let _write = open_history_db(HistoryAccessMode::ReadWrite)?;
    let Err(error) = open_history_db(HistoryAccessMode::None) else {
        bail!("commands with no declared history access must not open the DB");
    };
    assert!(format!("{error:#}").contains("declared no history access"));
    Ok(())
}

#[sinex_test]
async fn observational_metadata_uses_query_history_without_tracking() -> TestResult<()> {
    let status = commands::StatusCommand {
        watch: false,
        summary: true,
        schemas: false,
        next: false,
    }
    .metadata();
    assert!(!status.track_in_history);
    assert_eq!(status.history_access, HistoryAccessMode::Query);

    let history = commands::history::HistoryCommand {
        subcommand: commands::history::HistorySubcommand::List {
            limit: 10,
            command: None,
            first: false,
            no_limit: false,
            offset: 0,
            after_invocation: None,
            before_invocation: None,
            sort_by: "newest".to_string(),
            since: None,
            with_diagnostics: false,
            with_stages: false,
            with_tests: false,
            include_zombies: false,
        },
    }
    .metadata();
    assert!(!history.track_in_history);
    assert_eq!(history.history_access, HistoryAccessMode::Query);

    let analytics = commands::AnalyticsCommand {
        subcommand: commands::analytics::AnalyticsSubcommand::Velocity,
    }
    .metadata();
    assert!(!analytics.track_in_history);
    assert_eq!(analytics.history_access, HistoryAccessMode::Query);
    Ok(())
}

#[sinex_test]
async fn parse_positive_u64_env_or_default_rejects_zero() -> TestResult<()> {
    let _guard = env_set("SINEX_TEST_TIMEOUT", Some("0".into()));

    assert_eq!(
        parse_positive_u64_env_or_default("SINEX_TEST_TIMEOUT", 42, "test timeout"),
        42
    );
    Ok(())
}

#[sinex_test]
async fn parse_one_shot_i64_env_returns_value_and_clears_env() -> TestResult<()> {
    let _guard = env_set("SINEX_TEST_CLAIM", Some("123".into()));

    assert_eq!(
        parse_one_shot_i64_env("SINEX_TEST_CLAIM", "test claim"),
        Some(123)
    );
    assert!(
        std::env::var_os("SINEX_TEST_CLAIM").is_none(),
        "one-shot env var must be removed after claim"
    );
    Ok(())
}

#[sinex_test]
async fn parse_one_shot_i64_env_rejects_invalid_values_and_clears_env() -> TestResult<()> {
    let _guard = env_set("SINEX_TEST_CLAIM", Some("abc".into()));

    assert_eq!(
        parse_one_shot_i64_env("SINEX_TEST_CLAIM", "test claim"),
        None
    );
    assert!(
        std::env::var_os("SINEX_TEST_CLAIM").is_none(),
        "invalid one-shot env var must still be removed"
    );
    Ok(())
}

#[cfg(unix)]
#[sinex_test]
async fn parse_one_shot_i64_env_rejects_non_unicode_and_clears_env() -> TestResult<()> {
    use std::ffi::OsString;
    use std::os::unix::ffi::OsStringExt;

    let _guard = env_set("SINEX_TEST_CLAIM", Some(OsString::from_vec(vec![0xff])));

    assert_eq!(
        parse_one_shot_i64_env("SINEX_TEST_CLAIM", "test claim"),
        None
    );
    assert!(
        std::env::var_os("SINEX_TEST_CLAIM").is_none(),
        "non-unicode one-shot env var must still be removed"
    );
    Ok(())
}
