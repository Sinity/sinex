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
fn tool_effects_match_backend_command_access() {
    let tools = tools();
    for (name, read_only) in [
        ("sinex_status", true),
        ("sinex_query", true),
        ("sinex_sql", true),
        ("sinex_hide", false),
        ("sinex_forecast-issue", false),
        ("sinex_library-tag", false),
    ] {
        let tool = tools.iter().find(|tool| tool.name == name).unwrap();
        assert_eq!(
            tool.annotations.as_ref().and_then(|a| a.read_only_hint),
            Some(read_only),
            "{name}",
        );
    }
    for spec in COMMANDS.iter().filter(|spec| spec.mcp) {
        let tool = tools
            .iter()
            .find(|tool| tool.name == format!("sinex_{}", spec.name))
            .unwrap();
        assert_eq!(
            tool.annotations.as_ref().unwrap().read_only_hint,
            Some(spec.access == sinex_protocol::Access::Read)
        );
    }
}

#[devkit::test]
fn status_data_is_structured_without_losing_the_data_notice() {
    let data = json!({"commit_seq": 42, "search_index": {"position": 40, "behind": 2}});
    let reply = success_result(data.clone());
    assert_eq!(reply.structured_content, Some(data));
    let encoded = serde_json::to_value(reply).unwrap();
    assert!(
        encoded["content"][0]["text"]
            .as_str()
            .unwrap()
            .starts_with(DATA_NOTICE)
    );
    assert_eq!(encoded["isError"], false);
}

#[devkit::test]
fn array_results_keep_their_text_without_invalid_structured_content() {
    let data = json!([{ "n": 2 }]);
    let reply = success_result(data.clone());
    assert!(reply.structured_content.is_none());
    let encoded = serde_json::to_value(reply).unwrap();
    let text = encoded["content"][0]["text"].as_str().unwrap();
    assert_eq!(
        text.strip_prefix(DATA_NOTICE).unwrap().trim(),
        serde_json::to_string_pretty(&data).unwrap()
    );
}
