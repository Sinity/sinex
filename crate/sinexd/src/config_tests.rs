use super::*;

const BASE: &str = r#"listen="127.0.0.1:41037"
state_dir="/synthetic/state"
store_dir="/synthetic/store"
writer_connection_file="/synthetic/writer"
reader_connection_file="/synthetic/reader"
token_file="/synthetic/token"
"#;

#[test]
fn old_replay_token_field_is_rejected() {
    let old = format!("{BASE}replay_token_file=\"/synthetic/operator-token\"\n");
    assert!(toml::from_str::<Config>(&old).is_err());
    let new = format!("{BASE}operator_token_file=\"/synthetic/operator-token\"\n");
    assert!(toml::from_str::<Config>(&new).unwrap().validate().is_ok());
}
#[test]
fn unknown_field_is_rejected() {
    assert!(toml::from_str::<Config>("unknown = true").is_err());
}

#[test]
fn chromium_profile_needs_source_enabled_nonnil_generation_and_unique_absolute_path() {
    let binding = r#"[[chromium]]
profile="synthetic"
database_generation="95d3d942-c8a4-444e-8016-29d2e312664c"
path="/synthetic/History"
"#;
    let good = format!("{BASE}sources=[\"browser.history.chromium\"]\n{binding}");
    assert!(toml::from_str::<Config>(&good).unwrap().validate().is_ok());
    for bad in [
        format!("{BASE}{binding}"),
        good.replace(
            "95d3d942-c8a4-444e-8016-29d2e312664c",
            "00000000-0000-0000-0000-000000000000",
        ),
        format!("{good}{binding}"),
        good.replace("/synthetic/History", "relative"),
    ] {
        assert!(toml::from_str::<Config>(&bad).unwrap().validate().is_err());
    }
}
