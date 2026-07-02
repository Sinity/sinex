// These tests assert that rendered TOML remains parseable; failures are
// test fixture failures rather than operator-path behavior.
#![allow(clippy::expect_used)]
use super::*;
use xtask::sandbox::prelude::sinex_test;

#[sinex_test]
async fn render_user_preferences_toml_escapes_structured_values()
-> xtask::sandbox::TestResult<()> {
    let rendered = Config::render_user_preferences_toml(
        OutputFormat::Json,
        r#"nvim "\path\with\quotes""#.to_string(),
        "minimal".to_string(),
    )
    .expect("render config");

    let body = rendered
        .lines()
        .filter(|line| !line.starts_with('#'))
        .collect::<Vec<_>>()
        .join("\n");
    let parsed: UserConfigFile = toml::from_str(&body).expect("rendered TOML parses");

    assert_eq!(parsed.default_format, Some(OutputFormat::Json));
    assert_eq!(
        parsed.editor,
        Some(r#"nvim "\path\with\quotes""#.to_string())
    );
    assert_eq!(
        parsed.theme.map(|theme| theme.table_style),
        Some("minimal".to_string())
    );
    Ok(())
}
