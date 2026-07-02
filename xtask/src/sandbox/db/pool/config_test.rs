use super::*;
use crate::sandbox::sinex_test;

#[sinex_test]
async fn test_parse_num_cpus_expression_supports_offsets() -> Result<()> {
    assert_eq!(parse_num_cpus_expression("num-cpus", 24)?, Some(24));
    assert_eq!(parse_num_cpus_expression("num-cpus-2", 24)?, Some(22));
    assert_eq!(parse_num_cpus_expression("num-cpus+3", 24)?, Some(27));
    Ok(())
}

#[sinex_test]
async fn test_parse_configured_pool_size_accepts_explicit_size() -> Result<()> {
    assert_eq!(parse_configured_pool_size("24")?, Some(24));
    assert_eq!(parse_configured_pool_size(" 48 ")?, Some(48));
    Ok(())
}

#[sinex_test]
async fn test_parse_configured_pool_size_accepts_auto() -> Result<()> {
    assert_eq!(parse_configured_pool_size("auto")?, None);
    assert_eq!(parse_configured_pool_size("AUTO")?, None);
    assert_eq!(parse_configured_pool_size("  ")?, None);
    Ok(())
}

#[sinex_test]
async fn test_parse_configured_pool_size_rejects_zero() -> Result<()> {
    let err = parse_configured_pool_size("0").expect_err("zero size should fail");
    assert!(
        err.to_string().contains("greater than zero"),
        "unexpected error: {err:#}"
    );
    Ok(())
}

#[sinex_test]
async fn test_parse_num_cpus_expression_rejects_invalid_offsets() -> Result<()> {
    let err =
        parse_num_cpus_expression("num-cpus-bad", 24).expect_err("invalid offset should fail");
    assert!(
        err.to_string()
            .contains("invalid nextest test-threads expression"),
        "unexpected error: {err:#}"
    );
    Ok(())
}

#[sinex_test]
async fn test_nextest_test_threads_from_config_parses_profile() -> Result<()> {
    let config: Value = toml::from_str(
        r#"
        [profile.default]
        test-threads = "num-cpus-1"
        "#,
    )?;
    assert_eq!(
        nextest_test_threads_from_config(&config, "default", 24)?,
        Some(23)
    );
    Ok(())
}

#[sinex_test]
async fn test_nextest_test_threads_from_config_ignores_missing_profile() -> Result<()> {
    let config: Value = toml::from_str(
        r"
        [profile.ci]
        test-threads = 8
        ",
    )?;
    assert_eq!(
        nextest_test_threads_from_config(&config, "default", 24)?,
        None
    );
    Ok(())
}

#[sinex_test]
async fn test_replace_db_name_preserves_query_parameters() -> Result<()> {
    let replaced = replace_db_name(
        "postgresql://postgres@localhost/sinex_dev?host=/run/postgresql&sslmode=disable",
        "sinex_test_pool_1",
    );
    assert_eq!(
        replaced,
        "postgresql://postgres@localhost/sinex_test_pool_1?host=/run/postgresql&sslmode=disable"
    );
    Ok(())
}
