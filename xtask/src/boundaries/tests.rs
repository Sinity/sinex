use super::*;
use serde_json::{Value, json};

fn dep(name: &str, kind: Option<&str>) -> Value {
    json!({"name": name, "kind": kind, "target": null, "features": [], "uses_default_features": true})
}

/// Metadata for the given members, each with declared dependencies and
/// resolved normal edges (by package name).
fn metadata(members: &[(&str, Vec<Value>, Vec<&str>)]) -> Metadata {
    let mut packages = Vec::new();
    let mut nodes = Vec::new();
    let mut ids = Vec::new();
    for (name, deps, edges) in members {
        packages.push(json!({"id": name, "name": name, "dependencies": deps}));
        let edges: Vec<Value> = edges
            .iter()
            .map(|e| json!({"pkg": e, "dep_kinds": [{"kind": null}]}))
            .collect();
        nodes.push(json!({"id": name, "deps": edges}));
        ids.push(*name);
    }
    for name in ["tokio", "sqlx", "wrapper"] {
        packages.push(json!({"id": name, "name": name, "dependencies": []}));
        let deps = if name == "wrapper" {
            vec![json!({"pkg": "tokio", "dep_kinds": [{"kind": null}]})]
        } else {
            vec![]
        };
        nodes.push(json!({"id": name, "deps": deps}));
    }
    serde_json::from_value(json!({
        "packages": packages,
        "workspace_members": ids,
        "resolve": {"nodes": nodes},
    }))
    .unwrap()
}

fn workspace(protocol_deps: Vec<Value>) -> Metadata {
    metadata(&[
        ("sinex-model", vec![], vec![]),
        ("sinex-store", vec![dep("sinex-model", None)], vec![]),
        ("sinex-protocol", protocol_deps, vec![]),
    ])
}

#[devkit::test]
fn allowed_graph_passes() {
    assert!(validate(workspace(vec![dep("sinex-model", None)])).is_ok());
}

#[devkit::test]
fn internal_edge_is_refused_for_every_dependency_kind() {
    for kind in [None, Some("dev"), Some("build")] {
        let error = validate(workspace(vec![dep("sinex-store", kind)])).unwrap_err();
        assert!(error.starts_with("forbidden dependency: sinex-protocol -> sinex-store"));
    }
}

#[devkit::test]
fn pure_crates_refuse_io_dependencies_directly() {
    assert!(validate(workspace(vec![dep("tokio", None)])).is_err());
}

#[devkit::test]
fn pure_dependency_checks_follow_the_selected_runtime_features() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    std::fs::write(
        root.join("Cargo.toml"),
        "[workspace]\nresolver='2'\nmembers=['model','wrapper','consumer','tokio']\n",
    )
    .unwrap();
    for name in ["model", "wrapper", "consumer", "tokio"] {
        std::fs::create_dir_all(root.join(name).join("src")).unwrap();
        std::fs::write(root.join(name).join("src/lib.rs"), "").unwrap();
        std::fs::write(
            root.join(name).join("Cargo.toml"),
            format!("[package]\nname='{name}'\nversion='0.1.0'\nedition='2024'\n"),
        )
        .unwrap();
    }
    let append = |name: &str, text: &str| {
        use std::io::Write;
        std::fs::OpenOptions::new()
            .append(true)
            .open(root.join(name).join("Cargo.toml"))
            .unwrap()
            .write_all(text.as_bytes())
            .unwrap();
    };
    append(
        "wrapper",
        "[dependencies]\ntokio={path='../tokio',optional=true}\n[features]\nstd=['tokio?/std']\nio=['dep:tokio']\n",
    );
    append("tokio", "[features]\nstd=[]\n");
    append(
        "consumer",
        "[dev-dependencies]\nwrapper={path='../wrapper',features=['io']}\n",
    );
    let model = "[package]\nname='model'\nversion='0.1.0'\nedition='2024'\n[dependencies]\nwrapper={path='../wrapper',features=['std']}\n";
    std::fs::write(root.join("model/Cargo.toml"), model).unwrap();
    let lock = devkit::cargo_command()
        .args(["generate-lockfile", "--offline"])
        .current_dir(root)
        .output()
        .unwrap();
    assert!(
        lock.status.success(),
        "{}",
        String::from_utf8_lossy(&lock.stderr)
    );
    let host = host_triple().unwrap();
    // Another workspace member's development feature and a weak feature edge
    // cannot add a runtime dependency to this model.
    runtime_dependencies(root, "model", RuntimeRule::NoIo, &host).unwrap();
    std::fs::write(
        root.join("model/Cargo.toml"),
        model.replace("['std']", "['std','io']"),
    )
    .unwrap();
    assert_eq!(
        runtime_dependencies(root, "model", RuntimeRule::NoIo, &host).unwrap_err(),
        "forbidden transitive dependency: model -> tokio"
    );
}

#[devkit::test]
fn only_store_schema_and_daemon_use_sqlx() {
    for client in ["sinexctl", "sinex-sources", "sinex-library"] {
        assert!(
            validate(metadata(&[(client, vec![dep("sqlx", None)], vec![])])).is_err(),
            "{client}"
        );
    }
    for allowed in ["sinex-store", "sinex-schema", "sinexd"] {
        assert!(
            validate(metadata(&[(allowed, vec![dep("sqlx", None)], vec![])])).is_ok(),
            "{allowed}"
        );
    }
}

#[devkit::test]
fn unknown_workspace_member_is_refused() {
    assert_eq!(
        validate(metadata(&[("sinex-surprise", vec![], vec![])])).unwrap_err(),
        "unassigned workspace package: sinex-surprise"
    );
}

#[devkit::test]
fn malformed_metadata_is_an_error() {
    for input in ["{}", r#"{"packages":[],"workspace_members":[]}"#] {
        assert!(serde_json::from_str::<Metadata>(input).is_err());
    }
}

#[devkit::test]
fn this_workspace_passes() {
    check(Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap()).unwrap();
}

#[devkit::test]
fn transport_owns_io_but_cannot_return_through_a_pure_protocol_edge() {
    let graph = metadata(&[
        ("sinex-model", vec![], vec![]),
        ("sinex-protocol", vec![dep("sinex-model", None)], vec![]),
        (
            "sinex-transport",
            vec![dep("sinex-protocol", None), dep("tempfile", None)],
            vec![],
        ),
        ("sinex-sources", vec![dep("sinex-transport", None)], vec![]),
    ]);
    assert!(validate(graph).is_ok());
    for kind in [None, Some("dev"), Some("build")] {
        let graph = metadata(&[
            ("sinex-protocol", vec![dep("sinex-transport", kind)], vec![]),
            ("sinex-transport", vec![], vec![]),
        ]);
        assert!(validate(graph).is_err());
        assert!(validate(workspace(vec![dep("tempfile", kind)])).is_err());
    }
}

#[devkit::test]
fn a_pure_wrapper_cannot_hide_transport_io() {
    let mut graph = metadata(&[
        (
            "sinex-protocol",
            vec![dep("wrapper", None)],
            vec!["wrapper"],
        ),
        ("sinex-transport", vec![], vec![]),
    ]);
    graph
        .resolve
        .nodes
        .iter_mut()
        .find(|n| n.id == "wrapper")
        .unwrap()
        .deps[0]
        .pkg = "sinex-transport".into();
    assert_eq!(
        validate(graph).unwrap_err(),
        "forbidden transitive dependency: sinex-protocol -> sinex-transport"
    );
}

#[devkit::test]
fn quoted_catalog_names_are_found_in_store_text() {
    let names = catalog_names(
        r#"{"sources":[{"name":"terminal.zsh-history","eventTypes":["command.imported"]}],
            "derivations":[{"name":"command.fusion","inputs":["command.executed"],"outputs":["command.fused"]}]}"#,
    )
    .unwrap();
    assert_eq!(
        named_sources(
            "WHERE event_type = 'command.fused' AND x = \"terminal.zsh-history\"",
            &names
        ),
        vec!["command.fused", "terminal.zsh-history"]
    );
    assert!(named_sources("command.imported without quotes", &names).is_empty());
}

#[devkit::test]
fn cli_cannot_acquire_database_work_through_a_wrapper() {
    let mut graph = metadata(&[("sinexctl", vec![dep("wrapper", None)], vec!["wrapper"])]);
    let wrapper = graph
        .resolve
        .nodes
        .iter_mut()
        .find(|node| node.id == "wrapper")
        .unwrap();
    wrapper.deps[0].pkg = "sqlx".into();
    assert_eq!(
        validate(graph).unwrap_err(),
        "forbidden transitive dependency: sinexctl -> sqlx"
    );
}

#[devkit::test]
fn compilation_follows_selected_packages_to_database_dependencies() {
    let graph = metadata(&[
        ("sinexctl", vec![], vec!["sinex-protocol"]),
        ("sinex-protocol", vec![], vec![]),
        ("sinexd", vec![], vec!["sinex-store"]),
        ("sinex-store", vec![], vec!["sqlx"]),
        ("sinex-schema", vec![], vec!["sqlx"]),
        ("new-package", vec![], vec!["sinexd"]),
    ]);
    assert!(selection_database_free(&graph, &["sinexctl"], false, false));
    // Bootstrap uses dynamic SQL and compiles before the schema database exists.
    assert!(selection_database_free(
        &graph,
        &["sinex-schema"],
        false,
        false
    ));
    for selected in [
        vec!["sinexd"],
        vec!["new-package"],
        vec!["sinexctl", "sinexd"],
        vec!["unknown"],
    ] {
        assert!(!selection_database_free(&graph, &selected, false, false));
    }
    assert!(!selection_database_free(&graph, &[], true, false));
}

#[devkit::test]
fn compilation_counts_development_dependencies_only_for_test_targets() {
    let mut graph = metadata(&[("application", vec![dep("sqlx", Some("dev"))], vec!["sqlx"])]);
    graph.resolve.nodes[0].deps[0].dep_kinds[0].kind = Some("dev".into());
    assert!(selection_database_free(
        &graph,
        &["application"],
        false,
        false
    ));
    assert!(!selection_database_free(
        &graph,
        &["application"],
        false,
        true
    ));
}
