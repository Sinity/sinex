use super::*;

#[devkit::test]
fn tools_come_from_the_command_table() {
    let tools = tools();
    let names: Vec<&str> = tools.iter().map(|t| t.name.as_ref()).collect();
    assert!(names.contains(&"sinex_query"));
    assert!(names.contains(&"sinex_recall"));
    for name in ["sinex_stage", "sinex_same", "sinex_distinct", "sinex_mark"] {
        assert!(!names.contains(&name), "{name} is not implemented");
    }
    assert!(!names.contains(&"sinex_follow"), "follow streams");
    assert!(
        !names.contains(&"sinex_excise"),
        "excision is the operator's"
    );
    let query = tools.iter().find(|t| t.name == "sinex_query").unwrap();
    assert_eq!(query.input_schema["required"], json!(["query"]));
    let recall = tools.iter().find(|t| t.name == "sinex_recall").unwrap();
    assert_eq!(recall.input_schema["properties"]["cwd"]["type"], "string");
    let request = Request::from_args(
        "recall",
        serde_json::from_value(json!({"around":"last","cwd":"/src/tide (notes)"})).unwrap(),
    )
    .unwrap();
    assert!(
        matches!(request, Request::Recall {cwd: Some(ref path), ..} if path == "/src/tide (notes)")
    );
}

#[devkit::test]
fn declaration_discovery_exposes_the_same_paginated_requests() {
    for name in ["kinds", "fields"] {
        let tools = tools();
        let tool = tools
            .iter()
            .find(|t| t.name == format!("sinex_{name}"))
            .unwrap();
        assert_eq!(tool.input_schema["properties"]["page"]["type"], "integer");
        assert_eq!(tool.input_schema["properties"]["cursor"]["type"], "string");
        let request = Request::from_args(
            name,
            serde_json::from_value(json!({"kind":"example.reading","page":3,"cursor":"saved"}))
                .unwrap(),
        )
        .unwrap();
        let encoded = serde_json::to_value(request).unwrap();
        assert_eq!(encoded["method"], name);
        assert_eq!(encoded["kind"], "example.reading");
        assert_eq!(encoded["page"], 3);
        assert_eq!(encoded["cursor"], "saved");
    }
}
