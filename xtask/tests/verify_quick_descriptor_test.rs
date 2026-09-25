#[test]
fn verify_quick_uses_database_free_static_check_without_caller_inputs() {
    let descriptor: toml::Value =
        toml::from_str(include_str!("../../.agentctl/project.toml")).expect("parse descriptor");
    let verify_quick = &descriptor["operations"]["verify_quick"];
    assert_eq!(
        descriptor["workspace"]["verify"]["focused"].as_str(),
        Some("verify_quick")
    );
    assert_eq!(
        verify_quick["exec"]
            .as_array()
            .expect("quick exec")
            .iter()
            .map(|value| value.as_str().expect("quick exec argument"))
            .collect::<Vec<_>>(),
        vec!["bash", "xtask/scripts/verify-quick.sh"]
    );
    assert_eq!(verify_quick["pool"].as_str(), Some("normal"));
    assert!(verify_quick.get("dependencies").is_none());
    assert!(verify_quick.get("inputs").is_none());
}
