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

#[test]
fn allowed_graph_passes() {
    assert!(validate(workspace(vec![dep("sinex-model", None)])).is_ok());
}

#[test]
fn internal_edge_is_refused_for_every_dependency_kind() {
    for kind in [None, Some("dev"), Some("build")] {
        let error = validate(workspace(vec![dep("sinex-store", kind)])).unwrap_err();
        assert!(error.starts_with("forbidden dependency: sinex-protocol -> sinex-store"));
    }
}

#[test]
fn pure_crates_refuse_io_dependencies_directly_and_transitively() {
    assert!(validate(workspace(vec![dep("tokio", None)])).is_err());
    let wrapped = metadata(&[("sinex-model", vec![dep("wrapper", None)], vec!["wrapper"])]);
    assert_eq!(
        validate(wrapped).unwrap_err(),
        "forbidden transitive dependency: sinex-model -> tokio"
    );
}

#[test]
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

#[test]
fn unknown_workspace_member_is_refused() {
    assert_eq!(
        validate(metadata(&[("sinex-surprise", vec![], vec![])])).unwrap_err(),
        "unassigned workspace package: sinex-surprise"
    );
}

#[test]
fn malformed_metadata_is_an_error() {
    for input in ["{}", r#"{"packages":[],"workspace_members":[]}"#] {
        assert!(serde_json::from_str::<Metadata>(input).is_err());
    }
}

#[test]
fn this_workspace_passes() {
    check(Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap()).unwrap();
}
