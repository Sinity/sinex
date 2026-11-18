use super::*;
use sinex_protocol::Request;

#[devkit::test]
fn tool_examples_follow_the_table() {
    let tools = tools();
    eprintln!(
        "MCP tool-schema bytes: {}",
        serde_json::to_vec(&tools).unwrap().len()
    );
    for command in COMMANDS.iter().filter(|command| command.mcp) {
        let tool = tools
            .iter()
            .find(|tool| tool.name == format!("sinex_{}", command.name))
            .unwrap();
        let description = tool.description.as_deref().unwrap();
        for example in command.examples {
            assert!(description.contains(example), "{example}: {description}");
        }
    }
    let names: Vec<&str> = tools.iter().map(|t| t.name.as_ref()).collect();
    assert!(names.contains(&"sinex_read"));
    assert!(names.contains(&"sinex_write"));
    assert!(names.contains(&"sinex_recall"));
    for name in ["sinex_same", "sinex_distinct"] {
        assert!(names.contains(&name), "agents decide duplicates: {name}");
    }
    for name in ["sinex_stage", "sinex_stage-list", "sinex_stage-show"] {
        assert!(
            names.contains(&name),
            "agents stage and read stages: {name}"
        );
    }
    let stage = tools.iter().find(|t| t.name == "sinex_stage").unwrap();
    assert_eq!(stage.input_schema["required"], json!(["path"]));
    assert_eq!(stage.input_schema["properties"]["path"]["type"], "array");
    assert_eq!(stage.input_schema["properties"]["parser"]["type"], "array");
    assert!(names.contains(&"sinex_act"));
    let note: sinex_protocol::rows::Step =
        serde_json::from_value(json!({"verb":"note","text":"synthetic"})).unwrap();
    assert_eq!(
        sinex_commands::required_access(&[note]),
        command_surface::Access::Permanent
    );
    for name in [
        "sinex_context",
        "sinex_timeline",
        "sinex_query-list",
        "sinex_query-complete",
    ] {
        assert!(names.contains(&name), "{name}");
    }

    assert_eq!(
        sinex_protocol::command("tombstone").unwrap().access,
        sinex_protocol::Access::Operator
    );
    assert!(names.contains(&"sinex_resume"));
    let write = tools.iter().find(|t| t.name == "sinex_write").unwrap();
    assert!(
        write.input_schema["properties"]["arguments"]["properties"]["steps"]["items"]["properties"]
            ["verb"]["enum"]
            .as_array()
            .unwrap()
            .contains(&json!("undo"))
    );
    let undo: sinex_protocol::rows::Step = serde_json::from_value(json!({"verb":"undo"})).unwrap();
    assert_eq!(
        sinex_commands::required_access(&[undo]),
        command_surface::Access::Write
    );
    let read = tools.iter().find(|t| t.name == "sinex_read").unwrap();
    assert_eq!(
        read.input_schema["properties"]["operation"]["enum"],
        json!(["rows", "status", "sql"])
    );
    let request = sinex_commands::declarations().parse_mcp_call("sinex_read", json!({
        "arguments":{"selection":"events", "page_size":3}, "cursor":"saved", "deadline":null,
    })).unwrap();
    assert_eq!(request.operation, "rows");
    assert_eq!(
        request.arguments,
        json!({"selection":"events", "page_size":3})
    );
    assert_eq!(request.cursor.as_deref(), Some("saved"));
    assert_eq!(request.deadline, command_surface::Deadline::None);
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
        assert_eq!(
            tool.input_schema["properties"]["page-size"]["type"],
            "integer"
        );
        assert_eq!(tool.input_schema["properties"]["cursor"]["type"], "string");
        let request = Request::from_args(
            name,
            serde_json::from_value(
                json!({"kind":"example.reading","page-size":3,"cursor":"saved"}),
            )
            .unwrap(),
        )
        .unwrap();
        let encoded = serde_json::to_value(request).unwrap();
        assert_eq!(encoded["method"], name);
        assert_eq!(encoded["kind"], "example.reading");
        assert_eq!(encoded["page-size"], 3);
        assert_eq!(encoded["cursor"], "saved");
    }
}

#[devkit::test]
fn tool_effects_match_backend_command_access() {
    let tools = tools();
    for (name, read_only) in [
        ("sinex_read", true),
        ("sinex_write", false),
        ("sinex_forecast-issue", false),
        ("sinex_library-curate", false),
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
    let data = json!({"commit_seq": 42, "verdict":{"state":"healthy"}, "consumers": [{"name":"search index","position":40,"behind":2}]});
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

#[test]
fn read_write_and_act_cards_expose_their_actual_step_access() {
    let tools = tools();
    let read = tools.iter().find(|tool| tool.name == "sinex_read").unwrap();
    let write = tools
        .iter()
        .find(|tool| tool.name == "sinex_write")
        .unwrap();
    assert_eq!(
        read.input_schema["properties"]["arguments"]["properties"]["steps"]["items"]["properties"]
            ["verb"]["enum"],
        json!(["read"])
    );
    assert_eq!(
        write.input_schema["properties"]["arguments"]["properties"]["steps"]["items"]["properties"]
            ["verb"]["enum"],
        json!(["read", "save", "archive", "restore"])
    );
}

#[test]
fn act_card_and_note_declaration_expose_the_supported_arguments() {
    let tools = tools();
    let act = tools.iter().find(|tool| tool.name == "sinex_act").unwrap();
    let step = &act.input_schema["properties"]["arguments"]["properties"]["steps"]["items"];
    assert!(
        step["properties"]["verb"]["enum"]
            .as_array()
            .unwrap()
            .contains(&json!("note"))
    );
    let note = sinex_commands::steps()
        .verbs()
        .iter()
        .find(|step| step.verb() == "note")
        .unwrap()
        .input_schema();
    for field in [
        "text",
        "at",
        "cwd",
        "command",
        "app",
        "title",
        "command_record",
        "focus_record",
    ] {
        assert!(note["properties"].get(field).is_some(), "{field}: {note}");
    }
    assert_eq!(note["required"], json!(["text"]));
}
