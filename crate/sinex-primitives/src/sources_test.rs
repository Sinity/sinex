use super::*;
use xtask::sandbox::prelude::sinex_test;

#[sinex_test]
async fn source_family_validates_charset() -> xtask::sandbox::TestResult<()> {
    SourceFamily::new("filesystem").unwrap();
    SourceFamily::new("browser.history").unwrap();
    SourceFamily::new("integration_polylogue").unwrap();
    assert!(SourceFamily::new("").is_err());
    assert!(SourceFamily::new("Has Caps").is_err());
    assert!(SourceFamily::new("with/slash").is_err());
    Ok(())
}

#[sinex_test]
async fn source_family_const_constructor() -> xtask::sandbox::TestResult<()> {
    const FILESYSTEM: SourceFamily = SourceFamily::from_static("filesystem");
    assert_eq!(FILESYSTEM.as_str(), "filesystem");
    Ok(())
}

#[sinex_test]
async fn source_family_round_trips_serde() -> xtask::sandbox::TestResult<()> {
    let family = SourceFamily::new("terminal").unwrap();
    let json = serde_json::to_string(&family).unwrap();
    assert_eq!(json, "\"terminal\"");
    let back: SourceFamily = serde_json::from_str(&json).unwrap();
    assert_eq!(back, family);
    Ok(())
}

#[sinex_test]
async fn source_identity_family_aliases_match_operator_families() -> xtask::sandbox::TestResult<()>
{
    assert_eq!(source_family("terminal.atuin-history"), "terminal");
    assert_eq!(source_family("git"), "git");
    assert_eq!(
        source_family_aliases("browser"),
        &["web", "webhistory", "raindrop"]
    );
    assert!(source_identity_matches_family(
        "raindrop-bookmarks",
        "web",
        "browser"
    ));
    assert!(source_identity_matches_family(
        "webhistory",
        "generic",
        "browser"
    ));
    assert!(!source_identity_matches_family(
        "terminal.atuin-history",
        "terminal",
        "browser"
    ));
    Ok(())
}

#[sinex_test]
async fn source_role_classifier_matches_event_and_material_lanes() -> xtask::sandbox::TestResult<()>
{
    assert_eq!(source_role("sinex"), SourceRole::Reflection);
    assert_eq!(source_role("sinex.metric"), SourceRole::Reflection);
    assert_eq!(source_role("sinexd.api"), SourceRole::Reflection);
    assert_eq!(source_role("derived.sinex.health"), SourceRole::Activity);
    assert_eq!(source_role("shell.atuin"), SourceRole::Activity);
    assert_eq!(source_role("browser.history"), SourceRole::Activity);

    assert!(is_self_observation_source("sinex"));
    assert!(is_self_observation_source("sinex.metric"));
    assert!(is_self_observation_source("sinexd.api"));
    assert!(!is_self_observation_source("derived.sinex.health"));
    assert!(!is_self_observation_source("shell.atuin"));
    assert!(!is_self_observation_source("browser.history"));

    assert!(is_self_observation_material_source(
        "sinex.self-observation.browser.history#material=019f231e-1fb7-7a38-bf78-98854bc450bc"
    ));
    assert!(!is_self_observation_material_source(
        "browser.history#material=019f231e-1fb7-7a38-bf78-98854bc450bc"
    ));
    Ok(())
}

#[sinex_test]
async fn source_role_sql_fragments_use_reflection_vocabulary() -> xtask::sandbox::TestResult<()> {
    assert_eq!(
        source_role_sql_case("source"),
        "CASE WHEN source = 'sinex' OR source LIKE 'sinex.%' OR source LIKE 'sinexd.%' THEN 'reflection' ELSE 'activity' END"
    );
    assert_eq!(
        throughput_component_sql_case("source"),
        "CASE WHEN source LIKE 'sinexd.api%' THEN 'gateway' WHEN source LIKE 'derived.%' THEN 'derived' WHEN (CASE WHEN source = 'sinex' OR source LIKE 'sinex.%' OR source LIKE 'sinexd.%' THEN 'reflection' ELSE 'activity' END) = 'reflection' THEN 'reflection' ELSE 'ingestion' END"
    );
    Ok(())
}
