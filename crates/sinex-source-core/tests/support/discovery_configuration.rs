use sinex_source_core::discovery::DiscoveryContext;
use std::path::{Path, PathBuf};

pub fn write(root: &Path) -> PathBuf {
    let path = root.join("sources.json");
    let value = serde_json::json!({
        "signals":{"fixture":"capture keeps this inert"},"model_accounting":{"actual_receipts":true},
        "recorded_media":{"screen":{"instruction_version":"fixture-1"}},
        "discovery":{
            "host":"configured-host","spool":root.join("configured-spool"),
            "home":root.join("home"),"dataHome":root.join("data"),"configHome":root.join("config"),
            "runtimeDir":root.join("run"),"layoutFile":root.join("layout.json"),
            "inputRoots":{"projects":[root.join("projects")]},"inputs":{"media":[root.join("media")]},
            "environment":{"PATH":"/synthetic/configured/bin","CONFIGURED_ONLY":"fixture"},"watch":[root.join("configured-watch")]
        }
    });
    std::fs::write(&path, serde_json::to_vec(&value).unwrap()).unwrap();
    path
}

pub fn assert_context(context: &DiscoveryContext, root: &Path) {
    assert_eq!(context.host, "cli-host");
    assert_eq!(context.spool, root.join("state/spool"));
    for (actual, path) in [
        (&context.home, "home"),
        (&context.data_home, "data"),
        (&context.config_home, "config"),
        (&context.runtime_dir, "run"),
        (&context.layout_file, "layout.json"),
    ] {
        assert_eq!(actual.as_ref(), Some(&root.join(path)));
    }
    assert_eq!(context.input_roots["projects"], vec![root.join("projects")]);
    assert_eq!(context.inputs["media"], vec![root.join("media")]);
    assert_eq!(context.environment["PATH"], "/synthetic/configured/bin");
    assert_eq!(context.environment["CONFIGURED_ONLY"], "fixture");
    assert_eq!(
        context.watch,
        vec![root.join("configured-watch"), root.join("cli-watch")]
    );
}
