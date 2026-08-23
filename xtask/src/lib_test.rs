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
async fn command_deadline_returns_typed_timeout() -> TestResult<()> {
    let mut timed_out = false;
    let error = execute_with_optional_timeout(
        std::future::pending::<Result<crate::command::CommandResult>>(),
        Some(std::time::Duration::ZERO),
        "test",
        &mut timed_out,
    )
    .await
    .expect_err("expired command deadline must fail");
    assert!(timed_out);
    assert!(process::report_is_process_timeout(&error));
    Ok(())
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
async fn source_bindings_own_structured_cancellation() -> TestResult<()> {
    let all_sources = Commands::Run(commands::RunCommand {
        subcommand: commands::run::RunSubcommand::AllSources {
            instance_id: None,
            reconcile: false,
            service_name: None,
            include_default_excluded: false,
        },
        watch: false,
        release: false,
        dry_run: false,
        logs: false,
        metrics: false,
        dev_journal: false,
    });
    let core = Commands::Run(commands::RunCommand {
        subcommand: commands::run::RunSubcommand::Core { instance_id: None },
        watch: false,
        release: false,
        dry_run: false,
        logs: false,
        metrics: false,
        dev_journal: false,
    });

    assert!(command_owns_structured_source_cancellation(Some(
        &all_sources
    )));
    assert!(!command_owns_structured_source_cancellation(Some(&core)));
    Ok(())
}

#[sinex_test]
async fn observational_metadata_uses_query_history_without_tracking() -> TestResult<()> {
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
