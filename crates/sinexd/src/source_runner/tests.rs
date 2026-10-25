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
fn keeps_only_the_named_source_of_a_kind_reading_several() {
    // Chromium's kind plans visits and URLs; enabling one source keeps one.
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
fn a_configured_kind_chooses_among_kinds_sharing_a_name() {
    let dir = std::env::temp_dir().join(format!("sinex-runner-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&dir).unwrap();
    let planned = plan(
        &config(serde_json::json!({
            "sources": {"desktop.clipboard": {
                "enable": true, "kind": "sinnix-lake", "paths": [dir.display().to_string()]
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
            "sources": {"desktop.clipboard": {"enable": true, "kind": "spotify", "paths": ["/x"]}}
        })),
        "h",
    ) else {
        panic!("a kind not reading the source was accepted");
    };
    assert!(e.contains("spotify"), "{e}");
    std::fs::remove_dir_all(&dir).unwrap();
}
