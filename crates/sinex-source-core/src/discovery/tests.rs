use super::*;
use serde_json::json;

fn process() -> DiscoveryContext {
    DiscoveryContext {
        host: "cli-host".into(),
        spool: "/cli/state/spool".into(),
        home: Some("/process/home".into()),
        data_home: Some("/process/data".into()),
        config_home: Some("/process/config".into()),
        runtime_dir: Some("/process/run".into()),
        environment: BTreeMap::from([
            ("PATH".into(), "/process/bin".into()),
            ("PROCESS_ONLY".into(), "present".into()),
        ]),
        watch: vec!["/cli/watch".into()],
        ..Default::default()
    }
}

#[test]
fn capture_configuration_passes_daemon_sections_without_activating_them() {
    let value = json!({"signals":{"enabled":true,"rules":[{"fixture":"inert"}]},
        "model_accounting":{"actual_receipts":true},
        "recorded_media":{"screen":{"instruction_version":"fixture-1"},"speech":null}});
    let config: Configuration = serde_json::from_value(value.clone()).unwrap();
    config.recorded_media.validate().unwrap();
    let encoded = serde_json::to_value(config).unwrap();
    for field in ["signals", "model_accounting"] {
        assert_eq!(encoded[field], value[field]);
    }
    assert_eq!(
        encoded["recorded_media"]["screen"]["instruction_version"],
        "fixture-1"
    );
    assert!(serde_json::from_value::<Configuration>(json!({"unknown_host_option":true})).is_err());
    let directory=tempfile::tempdir().unwrap();
    let path=directory.path().join("sources.json");
    std::fs::write(&path,br#"{"recorded_media":{"screen":{"instruction_version":" "}}}"#).unwrap();
    assert!(Configuration::load(&path).is_err(),"the binary configuration boundary validates explicit selector conditions");
}

#[test]
fn configured_discovery_overrides_process_facts_and_cli_host_spool_watch_remain_last() {
    let config: Configuration = serde_json::from_value(json!({"discovery":{
        "host":"file-host","spool":"/file/spool","home":"/file/home","dataHome":"/file/data",
        "configHome":"/file/config","runtimeDir":"/file/run","layoutFile":"/file/layout.json",
        "inputRoots":{"projects":["/file/projects"]},"inputs":{"media":["/file/media"]},
        "environment":{"PATH":"/file/bin","FILE_ONLY":"present"},"watch":["/file/watch"]}}))
    .unwrap();
    let context = config.resolve_context(process());
    assert_eq!(context.host, "cli-host");
    assert_eq!(context.spool, PathBuf::from("/cli/state/spool"));
    for (actual, expected) in [
        (&context.home, "/file/home"),
        (&context.data_home, "/file/data"),
        (&context.config_home, "/file/config"),
        (&context.runtime_dir, "/file/run"),
        (&context.layout_file, "/file/layout.json"),
    ] {
        assert_eq!(actual.as_deref(), Some(std::path::Path::new(expected)));
    }
    assert_eq!(
        context.input_roots["projects"],
        vec![PathBuf::from("/file/projects")]
    );
    assert_eq!(context.inputs["media"], vec![PathBuf::from("/file/media")]);
    assert_eq!(context.environment["PATH"], "/file/bin");
    assert_eq!(context.environment["PROCESS_ONLY"], "present");
    assert_eq!(context.environment["FILE_ONLY"], "present");
    assert_eq!(
        context.watch,
        vec![PathBuf::from("/file/watch"), PathBuf::from("/cli/watch")]
    );
}

#[test]
fn exclude_only_config_and_empty_override_map_preserve_process_capabilities() {
    let config: Configuration =
        serde_json::from_value(json!({"exclude":{"desktop.clipboard":{"reason":"fixture"}},
        "discovery":{"environment":{}}}))
        .unwrap();
    let context = config.resolve_context(process());
    assert_eq!(context.environment, process().environment);
    assert_eq!(context.runtime_dir, process().runtime_dir);
    assert_eq!(context.home, process().home);
    assert_eq!(
        context.exclude["desktop.clipboard"]
            .as_ref()
            .unwrap()
            .reason,
        "fixture"
    );
}
