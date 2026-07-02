use super::render_invocation_scoped_nextest_config;
use crate::sandbox::sinex_test;

#[sinex_test]
async fn test_render_invocation_scoped_nextest_config_overrides_only_junit_path()
-> ::xtask::sandbox::TestResult<()> {
    let rendered = render_invocation_scoped_nextest_config(
        r#"
[profile.default]
retries = 2

[profile.default.junit]
path = "junit.xml"
store-success-output = true
"#,
        "default",
        std::path::Path::new("/tmp/custom-junit.xml"),
    )?;
    let parsed: toml::Value = toml::from_str(&rendered)?;

    assert_eq!(
        parsed["profile"]["default"]["junit"]["path"].as_str(),
        Some("/tmp/custom-junit.xml")
    );
    assert_eq!(
        parsed["profile"]["default"]["retries"].as_integer(),
        Some(2)
    );
    assert_eq!(
        parsed["profile"]["default"]["junit"]["store-success-output"].as_bool(),
        Some(true)
    );
    Ok(())
}
