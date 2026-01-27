#[cfg(feature = "testing")]
use sinex_core::environment::override_environment_for_tests;
use sinex_core::environment::{environment, SinexEnvironment};
use xtask::sandbox::sinex_test;
use sinex_test_utils::TestResult;
use std::env;
use std::path::PathBuf;
use url::Url;

fn assert_equivalent_db_url(actual: &str, expected: &str) -> TestResult<()> {
    let actual_url = Url::parse(actual)?;
    let expected_url = Url::parse(expected)?;
    assert_eq!(actual_url.scheme(), expected_url.scheme());
    assert_eq!(actual_url.path(), expected_url.path());
    assert_eq!(
        actual_url.query_pairs().collect::<Vec<_>>(),
        expected_url.query_pairs().collect::<Vec<_>>()
    );
    Ok(())
}

#[sinex_test]
async fn environment_creation() -> TestResult<()> {
    let env = SinexEnvironment::new("dev").unwrap();
    assert_eq!(env.name(), "dev");
    assert!(env.is_dev());
    assert!(!env.is_prod());

    let env = SinexEnvironment::new("prod").unwrap();
    assert_eq!(env.name(), "prod");
    assert!(env.is_prod());
    assert!(!env.is_dev());
    Ok(())
}

#[sinex_test]
async fn invalid_environment_is_rejected() -> TestResult<()> {
    let result = SinexEnvironment::new("invalid!");
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Invalid environment"));
    Ok(())
}

#[sinex_test]
async fn custom_environment_names_are_allowed() -> TestResult<()> {
    let env = SinexEnvironment::new("qa-42").unwrap();
    assert_eq!(env.name(), "qa-42");
    Ok(())
}

#[sinex_test]
async fn database_names_are_namespaced() -> TestResult<()> {
    let env = SinexEnvironment::new("dev").unwrap();
    assert_eq!(env.database_name("sinex"), "sinex_dev");

    let env = SinexEnvironment::new("prod").unwrap();
    assert_eq!(env.database_name("sinex"), "sinex_prod");
    Ok(())
}

#[sinex_test]
async fn database_urls_are_namespaced_once() -> TestResult<()> {
    let env = SinexEnvironment::new("dev").unwrap();

    let base_url = "postgresql:///sinex?host=/run/postgresql";
    let namespaced = env.database_url(base_url).unwrap();
    assert_equivalent_db_url(&namespaced, "postgresql:///sinex_dev?host=/run/postgresql")?;

    let unchanged = env.database_url(&namespaced).unwrap();
    assert_eq!(unchanged, namespaced);
    Ok(())
}

#[sinex_test]
async fn database_urls_dbname_query_support() -> TestResult<()> {
    let env = SinexEnvironment::new("dev").unwrap();

    let base_url = "postgresql:///?host=/run/postgresql&dbname=sinex";
    let namespaced = env.database_url(base_url)?;
    assert_equivalent_db_url(
        &namespaced,
        "postgresql:///?host=/run/postgresql&dbname=sinex_dev",
    )?;

    // Should not double-namespace
    let unchanged = env.database_url(&namespaced)?;
    assert_eq!(unchanged, namespaced);
    Ok(())
}

#[sinex_test]
async fn database_urls_dbname_case_insensitive() -> TestResult<()> {
    let env = SinexEnvironment::new("staging").unwrap();
    let base_url = "postgresql:///?DBNAME=sinex&host=/run/postgresql";
    let namespaced = env.database_url(base_url)?;
    assert_equivalent_db_url(
        &namespaced,
        "postgresql:///?DBNAME=sinex_staging&host=/run/postgresql",
    )?;
    Ok(())
}

#[sinex_test]
async fn database_urls_database_param_supported() -> TestResult<()> {
    let env = SinexEnvironment::new("prod").unwrap();
    let base_url = "postgresql:///?host=/run/postgresql&database=sinex";
    let namespaced = env.database_url(base_url)?;
    assert_equivalent_db_url(
        &namespaced,
        "postgresql:///?host=/run/postgresql&database=sinex_prod",
    )?;
    Ok(())
}

#[sinex_test]
async fn database_urls_reject_empty_dbname() -> TestResult<()> {
    let env = SinexEnvironment::new("dev").unwrap();
    let result = env.database_url("postgresql:///?dbname=&host=/run/postgresql");
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("dbname query parameter is empty"));
    Ok(())
}

#[sinex_test]
async fn nats_subjects_are_namespaced_once() -> TestResult<()> {
    let env = SinexEnvironment::new("dev").unwrap();

    let subject = env.nats_subject("sinex.events.raw.>");
    assert_eq!(subject, "dev.sinex.events.raw.>");

    let unchanged = env.nats_subject(&subject);
    assert_eq!(unchanged, subject);
    Ok(())
}

#[sinex_test]
async fn nats_streams_are_namespaced_once() -> TestResult<()> {
    let env = SinexEnvironment::new("dev").unwrap();

    let stream = env.nats_stream_name("SINEX_RAW_EVENTS");
    assert_eq!(stream, "DEV_SINEX_RAW_EVENTS");

    let unchanged = env.nats_stream_name(&stream);
    assert_eq!(unchanged, stream);
    Ok(())
}

#[sinex_test]
async fn socket_paths_are_namespaced_once() -> TestResult<()> {
    let env = SinexEnvironment::new("dev").unwrap();

    let path = env.socket_path("/tmp/sinex-host.sock");
    assert_eq!(path, PathBuf::from("/tmp-dev/sinex-host.sock"));

    let unchanged = env.socket_path(&path);
    assert_eq!(unchanged, path);
    Ok(())
}

#[sinex_test]
async fn work_directories_are_namespaced_once() -> TestResult<()> {
    let env = SinexEnvironment::new("staging").unwrap();

    let dir = env.work_directory("/tmp/sinex");
    assert_eq!(dir, PathBuf::from("/tmp/sinex-staging"));

    let unchanged = env.work_directory(&dir);
    assert_eq!(unchanged, dir);
    Ok(())
}

#[sinex_test]
async fn config_prefix_matches_environment() -> TestResult<()> {
    let env = SinexEnvironment::new("dev").unwrap();
    assert_eq!(env.config_prefix(), "SINEX_DEV_");

    let env = SinexEnvironment::new("prod").unwrap();
    assert_eq!(env.config_prefix(), "SINEX_PROD_");
    Ok(())
}

#[sinex_test]
async fn environment_variable_overrides_current() -> TestResult<()> {
    env::set_var("SINEX_ENVIRONMENT", "staging");
    let env = SinexEnvironment::current().unwrap();
    assert_eq!(env.name(), "staging");
    env::remove_var("SINEX_ENVIRONMENT");
    Ok(())
}

#[sinex_test]
async fn global_environment_exposes_a_supported_name() -> TestResult<()> {
    let env = environment();
    assert!(!env.name().is_empty());
    Ok(())
}

#[cfg(feature = "testing")]
#[sinex_test]
async fn environment_override_guard_restores_previous_value() -> TestResult<()> {
    let original = environment();
    {
        let _guard = override_environment_for_tests("qa")?;
        let env = environment();
        assert_eq!(env.name(), "qa");
    }
    let restored = environment();
    assert_eq!(restored.name(), original.name());
    Ok(())
}
