use super::*;
#[test]
fn browser_declaration_refuses_missing_enablement_nil_generation_and_duplicate_scope() {
    let base = r#"listen="127.0.0.1:41037"
state_dir="/synthetic/state"
store_dir="/synthetic/store"
writer_connection_file="/synthetic/writer"
reader_connection_file="/synthetic/reader"
token_file="/synthetic/token"
"#;
    let binding = r#"[[chromium]]
profile="synthetic"
database_generation="95d3d942-c8a4-444e-8016-29d2e312664c"
path="/synthetic/History"
"#;
    let good = format!("{base}sources=[\"browser.history.chromium\"]\n{binding}");
    assert!(toml::from_str::<Config>(&good).unwrap().validate().is_ok());
    for bad in [
        format!("{base}{binding}"),
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
