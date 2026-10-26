use super::*;

fn config(json: Value) -> Config {
    serde_json::from_value(json).unwrap()
}

#[devkit::test]
fn plans_the_enabled_sources_bindings_from_their_paths() {
    let dir = std::env::temp_dir().join(format!("sinex-runner-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&dir).unwrap();
    for name in ["b.zip", "a.zip"] {
        std::fs::write(dir.join(name), b"x").unwrap();
    }
    let planned = plan(
        &config(serde_json::json!({
            "sources": {
                "media.spotify-history": {"enable": true, "paths": [dir.display().to_string()]},
                "reading.raindrop": {"enable": false, "paths": ["/nowhere"]},
            }
        })),
        "h",
    )
    .unwrap();
    let names: Vec<&str> = planned.iter().map(|(_, b)| b.name.as_str()).collect();
    assert_eq!(names.len(), 2, "{names:?}");
    assert!(planned.iter().all(|(k, b)| k.name == "spotify"
        && b.source == "media.spotify-history"
        && b.settings.host == "h"));
    std::fs::remove_dir_all(&dir).unwrap();
}

#[devkit::test]
fn keeps_only_the_named_source_of_a_format_reading_several() {
    // Chromium's format plans visits and URLs; enabling one source keeps one.
    let dir = std::env::temp_dir().join(format!("sinex-runner-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(dir.join("Default")).unwrap();
    std::fs::write(dir.join("Default/History"), b"").unwrap();
    let planned = plan(
        &config(serde_json::json!({
            "sources": {"browser.chromium-visits": {"enable": true, "paths": [dir.display().to_string()]}}
        })),
        "h",
    )
    .unwrap();
    assert_eq!(planned.len(), 1);
    assert_eq!(planned[0].1.source, "browser.chromium-visits");
    std::fs::remove_dir_all(&dir).unwrap();
}

#[devkit::test]
fn an_unknown_source_is_refused_by_name() {
    let Err(e) = plan(
        &config(serde_json::json!({"sources": {"no.such": {"enable": true, "paths": ["/x"]}}})),
        "h",
    ) else {
        panic!("an unknown source was planned");
    };
    assert!(e.contains("no.such"), "{e}");
}

#[devkit::test]
fn a_configured_format_chooses_among_formats_sharing_a_name() {
    let dir = std::env::temp_dir().join(format!("sinex-runner-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&dir).unwrap();
    let planned = plan(
        &config(serde_json::json!({
            "sources": {"desktop.clipboard": {
                "enable": true, "format": "sinnix-lake", "paths": [dir.display().to_string()]
            }}
        })),
        "h",
    )
    .unwrap();
    assert!(
        planned
            .iter()
            .all(|(k, b)| k.name == "sinnix-lake" && b.parser.starts_with("sinnix-lake:"))
    );
    let Err(e) = plan(
        &config(serde_json::json!({
            "sources": {"desktop.clipboard": {"enable": true, "format": "spotify", "paths": ["/x"]}}
        })),
        "h",
    ) else {
        panic!("a format not reading the source was accepted");
    };
    assert!(e.contains("spotify"), "{e}");
    std::fs::remove_dir_all(&dir).unwrap();
}

#[devkit::test]
fn planning_again_starts_files_that_appeared_since() {
    let dir = std::env::temp_dir().join(format!("sinex-runner-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("a.zip"), b"x").unwrap();
    let config = config(serde_json::json!({
        "sources": {"media.spotify-history": {"enable": true, "paths": [dir.display().to_string()]}}
    }));
    let mut bindings = Bindings::default();
    bindings.extend(&config, "h");
    assert_eq!(bindings.bound.len(), 1);
    std::fs::write(dir.join("b.zip"), b"x").unwrap();
    bindings.extend(&config, "h");
    bindings.extend(&config, "h");
    let mut names: Vec<&str> = bindings
        .bound
        .iter()
        .map(|b| b.binding.name.as_str())
        .collect();
    names.sort_unstable();
    names.dedup();
    assert_eq!(names.len(), 2, "each file is bound once: {names:?}");
    std::fs::remove_dir_all(&dir).unwrap();
}

#[devkit::test]
fn a_path_with_nothing_to_read_is_reported_until_it_has() {
    let dir = std::env::temp_dir().join(format!("sinex-runner-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&dir).unwrap();
    let config = config(serde_json::json!({
        "sources": {"media.spotify-history": {"enable": true, "paths": [dir.display().to_string()]}}
    }));
    let mut bindings = Bindings::default();
    bindings.extend(&config, "h");
    let key = format!("path media.spotify-history {}", dir.display());
    assert!(
        bindings.said[&key].contains("nothing to read"),
        "{:?}",
        bindings.said
    );
    std::fs::write(dir.join("a.zip"), b"x").unwrap();
    bindings.extend(&config, "h");
    assert!(!bindings.said.contains_key(&key));
    std::fs::remove_dir_all(&dir).unwrap();
}

#[devkit::test]
fn an_enabled_source_emits_its_types_before_any_file_exists() {
    let types = emitted(&config(serde_json::json!({
        "sources": {
            "desktop.clipboard": {"enable": true, "format": "sinnix-lake", "paths": ["/nowhere"]},
            "terminal.atuin-history": {"enable": true, "paths": ["/nowhere"]},
            "reading.raindrop": {"enable": false},
        }
    })))
    .unwrap();
    assert_eq!(types, vec!["command.executed", "lake.observed"]);
}

#[devkit::test]
fn a_derivation_runs_unless_the_configuration_turns_it_off() {
    let config = config(serde_json::json!({
        "derivations": {"metric.hourly": false, "command.fusion": true}
    }));
    assert!(!config.runs("metric.hourly"));
    assert!(config.runs("command.fusion"));
    assert!(config.runs("shell.sessions"), "absent means it runs");
}
