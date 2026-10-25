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
fn pure_crates_refuse_io_dependencies_directly_and_transitively() {
    assert!(validate(workspace(vec![dep("tokio", None)])).is_err());
    let wrapped = metadata(&[("sinex-model", vec![dep("wrapper", None)], vec!["wrapper"])]);
    assert_eq!(
        validate(wrapped).unwrap_err(),
        "forbidden transitive dependency: sinex-model -> tokio"
    );
}

#[devkit::test]
fn an_optional_dependency_counts_only_when_a_feature_enables_it() {
    let graph = |features: Vec<&str>| -> Metadata {
        serde_json::from_value(json!({
            "packages": [
                {"id": "sinex-model", "name": "sinex-model", "dependencies": [dep("timelib", None)]},
                {"id": "timelib", "name": "timelib",
                 "dependencies": [{"name": "tokio", "kind": null, "target": null, "optional": true}],
                 "features": {"std": ["tokio?/std"], "io": ["dep:tokio"]}},
                {"id": "tokio", "name": "tokio", "dependencies": []},
            ],
            "workspace_members": ["sinex-model"],
            "resolve": {"nodes": [
                {"id": "sinex-model", "deps": [{"pkg": "timelib", "dep_kinds": [{"kind": null}]}]},
                {"id": "timelib", "deps": [{"pkg": "tokio", "dep_kinds": [{"kind": null}]}], "features": features},
                {"id": "tokio", "deps": []},
            ]},
        }))
        .unwrap()
    };
    // `tokio?/std` names tokio weakly; it is not built.
    assert!(validate(graph(vec!["std"])).is_ok());
    assert_eq!(
        validate(graph(vec!["std", "io"])).unwrap_err(),
        "forbidden transitive dependency: sinex-model -> tokio"
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
        named_sources("WHERE event_type = 'command.fused' AND x = \"terminal.zsh-history\"", &names),
        vec!["command.fused", "terminal.zsh-history"]
    );
    assert!(named_sources("command.imported without quotes", &names).is_empty());
}
