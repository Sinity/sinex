use assert_cmd::cargo;
use serde_json::{Value, json};
use sinex_primitives::rpc::{RpcMutability, RpcRole, method_catalog, methods};
use sinex_primitives::temporal::{Duration, Timestamp};
use sinexctl::client::{ClientConfig, GatewayClient, RetryConfig};
use sinexctl::mcp::{
    MCP_PROTOCOL_VERSION, MCP_SUPPORTED_PROTOCOL_VERSIONS, McpSurfaceKind,
    assert_read_only_tool_names, call_tool, tool_catalog, tools,
};
use sinexctl::validation::{parse_time_input, parse_time_input_with_now, validate_time_range};
use std::collections::BTreeSet;
use std::io::{BufRead, BufReader, Read, Write};
use std::process::{Command, Stdio};
use std::time::Duration as StdDuration;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn parse_time_input_with_now_handles_relative_and_absolute_inputs() -> TestResult<()> {
    let now = Timestamp::parse_rfc3339("2026-03-24T10:00:00Z")?;

    assert_eq!(
        parse_time_input_with_now("1h", now)?,
        now - Duration::hours(1)
    );
    assert_eq!(
        parse_time_input_with_now("30m", now)?,
        now - Duration::minutes(30)
    );
    assert_eq!(
        parse_time_input_with_now("2026-03-24T09:15:00Z", now)?,
        Timestamp::parse_rfc3339("2026-03-24T09:15:00Z")?
    );
    assert_eq!(
        parse_time_input_with_now("2026-03-24", now)?,
        Timestamp::parse_rfc3339("2026-03-24T00:00:00Z")?
    );

    Ok(())
}

#[sinex_test]
async fn parse_time_input_rejects_invalid_formats() -> TestResult<()> {
    assert!(parse_time_input("not-a-date").is_err());
    assert!(parse_time_input("2026/03/24").is_err());
    Ok(())
}

#[sinex_test]
async fn validate_time_range_rejects_inverted_and_equal_bounds() -> TestResult<()> {
    let now = Timestamp::now();
    let past = Timestamp::new(now.inner() - Duration::hours(1));
    let future = Timestamp::new(now.inner() + Duration::hours(1));

    assert!(validate_time_range(Some(past), Some(now)).is_ok());
    assert!(validate_time_range(Some(now), Some(future)).is_ok());
    assert!(validate_time_range(None, Some(future)).is_ok());
    assert!(validate_time_range(Some(past), None).is_ok());
    assert!(validate_time_range(None, None).is_ok());

    assert!(validate_time_range(Some(future), Some(past)).is_err());
    assert!(validate_time_range(Some(now), Some(now)).is_err());
    Ok(())
}

#[sinex_test]
async fn mcp_tool_order_matches_catalog_order() -> TestResult<()> {
    let live_tools = tools();
    let catalog = tool_catalog();
    let tool_names = live_tools.iter().map(|tool| tool.name).collect::<Vec<_>>();
    let catalog_names = catalog.iter().map(|entry| entry.name).collect::<Vec<_>>();

    assert_eq!(tool_names, catalog_names);
    for (tool, entry) in live_tools.iter().zip(catalog.iter()) {
        assert_eq!(
            tool.description, entry.description,
            "MCP tool `{}` must use catalog-owned description",
            tool.name
        );
    }
    assert_read_only_tool_names()?;
    Ok(())
}

#[sinex_test]
async fn mcp_tool_schemas_are_closed_objects() -> TestResult<()> {
    for tool in tools() {
        assert_eq!(tool.input_schema["type"], "object");
        assert_eq!(tool.input_schema["additionalProperties"], false);
        assert!(tool.input_schema["properties"].is_object());
    }
    Ok(())
}

#[sinex_test]
async fn mcp_docs_tool_table_matches_live_tools() -> TestResult<()> {
    let docs_path =
        workspace_root_from_manifest_dir()?.join("crate/sinexctl/docs/mcp_readonly_server.md");
    let docs = std::fs::read_to_string(&docs_path)?;
    let documented = docs
        .lines()
        .filter_map(|line| {
            line.strip_prefix("| `sinex_")
                .and_then(|rest| rest.split_once('`'))
                .map(|(suffix, _)| format!("sinex_{suffix}"))
        })
        .collect::<BTreeSet<_>>();
    let live = tools()
        .into_iter()
        .map(|tool| tool.name.to_string())
        .collect::<BTreeSet<_>>();

    assert_eq!(
        documented,
        live,
        "MCP docs tool table must match live tools in {}",
        docs_path.display()
    );
    Ok(())
}

#[sinex_test]
async fn mcp_omits_raw_content_read_methods_until_redacted_variants_exist() -> TestResult<()> {
    let backing_methods = tool_catalog()
        .into_iter()
        .flat_map(|entry| entry.backing_rpc_methods.iter().copied())
        .collect::<BTreeSet<_>>();

    assert!(
        !backing_methods.contains(methods::CONTENT_RETRIEVE_BLOB),
        "MCP must not expose raw blob retrieval without a redacted read contract"
    );
    assert!(
        !backing_methods.contains(methods::DOCUMENTS_GET_CHUNKS),
        "MCP must not expose raw document chunk text without a redacted read contract"
    );
    Ok(())
}

#[sinex_test]
async fn mcp_covers_or_explicitly_omits_safe_read_only_rpc_methods() -> TestResult<()> {
    let exposed_methods = tool_catalog()
        .into_iter()
        .flat_map(|entry| entry.backing_rpc_methods.iter().copied())
        .collect::<BTreeSet<_>>();
    let explicitly_omitted = BTreeSet::from([
        methods::CONTENT_RETRIEVE_BLOB,
        methods::CURATION_DUPLICATE_CANDIDATES_LIST,
        methods::DOCUMENTS_GET_CHUNKS,
        methods::PRIVACY_POLICY_LIST,
    ]);

    let missing = method_catalog()
        .into_iter()
        .filter(|method| {
            method.role == RpcRole::ReadOnly
                && method.mutability == RpcMutability::ReadOnly
                && !exposed_methods.contains(method.name)
                && !explicitly_omitted.contains(method.name)
        })
        .map(|method| method.name)
        .collect::<Vec<_>>();

    assert!(
        missing.is_empty(),
        "MCP must expose safe read-only RPC methods or list a deliberate omission: {missing:?}"
    );
    Ok(())
}

#[sinex_test]
async fn mcp_catalog_exactly_covers_live_tools() -> TestResult<()> {
    let tool_names = tools()
        .iter()
        .map(|tool| tool.name)
        .collect::<BTreeSet<_>>();
    let catalog_names = tool_catalog()
        .iter()
        .map(|entry| entry.name)
        .collect::<BTreeSet<_>>();

    assert_eq!(catalog_names, tool_names);
    for entry in tool_catalog() {
        assert_eq!(entry.kind, McpSurfaceKind::Tool);
        assert!(entry.read_only, "MCP v1 catalog entry must be read-only");
        assert!(
            entry
                .name
                .chars()
                .all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '-'),
            "MCP tool `{}` must use identifier-safe characters for Codex/tool registry compatibility",
            entry.name
        );
        assert!(
            !entry.backing_rpc_methods.is_empty() || entry.name == "sinex_orient",
            "MCP entry `{}` must declare backing RPC descriptors unless it is the local orientation surface",
            entry.name
        );
    }
    Ok(())
}

fn workspace_root_from_manifest_dir() -> TestResult<std::path::PathBuf> {
    let manifest_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    manifest_dir
        .parent()
        .and_then(std::path::Path::parent)
        .map(std::path::Path::to_path_buf)
        .ok_or_else(|| color_eyre::eyre::eyre!("cannot resolve workspace root from manifest dir"))
}

#[sinex_test]
async fn mcp_catalog_backing_methods_are_typed_read_only_rpc() -> TestResult<()> {
    let rpc_catalog = method_catalog()
        .into_iter()
        .map(|method| (method.name, method))
        .collect::<std::collections::BTreeMap<_, _>>();

    for entry in tool_catalog() {
        for method_name in entry.backing_rpc_methods {
            let method = rpc_catalog.get(method_name).ok_or_else(|| {
                color_eyre::eyre::eyre!(
                    "MCP entry `{}` references unknown RPC method `{method_name}`",
                    entry.name
                )
            })?;
            assert_eq!(
                method.mutability,
                RpcMutability::ReadOnly,
                "MCP entry `{}` must not expose mutating RPC method `{method_name}`",
                entry.name
            );
            assert_eq!(
                method.role,
                RpcRole::ReadOnly,
                "MCP entry `{}` must not require elevated RPC role `{method_name}`",
                entry.name
            );
        }
    }
    Ok(())
}

#[sinex_test]
async fn mcp_catalog_serializes_as_machine_readable_matrix() -> TestResult<()> {
    let value = serde_json::to_value(tool_catalog())?;
    let entries = value
        .as_array()
        .ok_or_else(|| color_eyre::eyre::eyre!("MCP catalog must serialize as an array"))?;

    assert_eq!(entries.len(), tools().len());
    for entry in entries {
        assert!(entry["name"].as_str().is_some());
        assert_eq!(entry["kind"], "tool");
        assert_eq!(entry["read_only"], true);
        assert!(entry["backing_rpc_methods"].as_array().is_some());
    }
    Ok(())
}

#[sinex_test]
async fn mcp_protocol_version_is_pinned() -> TestResult<()> {
    assert_eq!(MCP_PROTOCOL_VERSION, "2025-06-18");
    assert!(
        MCP_SUPPORTED_PROTOCOL_VERSIONS.contains(&"2024-11-05"),
        "MCP server must continue to negotiate with the original stdio protocol version"
    );
    Ok(())
}

#[sinex_test]
async fn mcp_stdio_accepts_codex_json_line_transport() -> TestResult<()> {
    let mut child = Command::new(cargo::cargo_bin!("sinex-mcp-server"))
        .env("SINEX_API_TOKEN", "mcp-stdio-test-token")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;

    let mut stdin = child
        .stdin
        .take()
        .ok_or_else(|| color_eyre::eyre::eyre!("missing child stdin"))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| color_eyre::eyre::eyre!("missing child stdout"))?;
    let mut stderr = child
        .stderr
        .take()
        .ok_or_else(|| color_eyre::eyre::eyre!("missing child stderr"))?;
    let mut reader = BufReader::new(stdout);

    writeln!(
        stdin,
        "{}",
        json!({
            "jsonrpc": "2.0",
            "id": 0,
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-06-18",
                "capabilities": { "elicitation": {} },
                "clientInfo": {
                    "name": "codex-mcp-client",
                    "title": "Codex",
                    "version": "test"
                }
            }
        })
    )?;

    let mut line = String::new();
    reader.read_line(&mut line)?;
    if line.trim().is_empty() {
        let mut stderr_text = String::new();
        let _ = stderr.read_to_string(&mut stderr_text);
        return Err(color_eyre::eyre::eyre!(
            "sinex-mcp-server exited before initialize response: {stderr_text}"
        ));
    }
    let initialize_response: Value = serde_json::from_str(line.trim_end())?;
    assert_eq!(
        initialize_response["result"]["protocolVersion"],
        MCP_PROTOCOL_VERSION
    );

    writeln!(
        stdin,
        "{}",
        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/list",
            "params": {}
        })
    )?;

    line.clear();
    reader.read_line(&mut line)?;
    let tools_response: Value = serde_json::from_str(line.trim_end())?;
    assert_eq!(
        tools_response["result"]["tools"][0]["name"],
        "sinex_orient"
    );
    assert_eq!(
        tools_response["result"]["tools"]
            .as_array()
            .map(Vec::len)
            .unwrap_or_default(),
        tools().len()
    );

    drop(stdin);
    child.kill()?;
    let _ = child.wait()?;
    Ok(())
}

#[sinex_test]
async fn mcp_surface_uses_typed_gateway_client_methods() -> TestResult<()> {
    let mcp_source = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src")
            .join("mcp.rs"),
    )?;
    assert!(
        !mcp_source.contains("call_raw_rpc"),
        "MCP tools must use typed GatewayClient methods"
    );
    Ok(())
}

#[sinex_test]
async fn mcp_orient_call_uses_shared_orientation_document() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(&client, "sinex_orient", json!({ "focus": "provenance" })).await?;

    assert_eq!(response["source_surface"], "sinex_orient");
    assert_eq!(response["query_echo"]["focus"], "provenance");
    assert_eq!(
        response["payload"]["source_document"],
        "crate/sinexctl/docs/agent_orientation.md"
    );
    let orientation = response["payload"]["orientation_markdown"]
        .as_str()
        .ok_or_else(|| color_eyre::eyre::eyre!("orientation markdown missing"))?;
    assert!(orientation.contains("Material provenance"));
    assert!(orientation.contains("Derived provenance"));
    assert!(orientation.contains("ts_orig"));
    assert!(orientation.contains("sinex_trace_lineage"));
    Ok(())
}

#[sinex_test]
async fn mcp_search_events_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(
        &client,
        "sinex_search_events",
        json!({ "sources": ["fixture"], "limit": 1 }),
    )
    .await?;

    assert_eq!(response["source_surface"], "sinex_search_events");
    assert_eq!(response["query_echo"]["sources"][0], "fixture");
    assert_eq!(
        response["payload"]["result"]["schema_version"],
        "event_card_list.v1"
    );
    assert_eq!(
        response["payload"]["result"]["cards"][0]["payload_preview"]["reason"],
        "server_disclosed"
    );
    assert_eq!(
        response["payload"]["result"]["cards"][0]["summary"],
        "disclosed fixture event"
    );
    assert_eq!(response["privacy_state"]["state"], "redacted");
    assert!(
        !response.to_string().contains("secret_fixture_value"),
        "MCP event search leaked raw query payload or snippet text"
    );
    Ok(())
}

#[sinex_test]
async fn mcp_trace_lineage_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;
    let event_id = fixture_event_id();

    let response = call_tool(
        &client,
        "sinex_trace_lineage",
        json!({ "event_id": event_id, "direction": "ancestors" }),
    )
    .await?;

    assert_eq!(response["source_surface"], "sinex_trace_lineage");
    assert_eq!(response["query_echo"]["event_id"], event_id);
    assert_eq!(response["payload"]["result"]["root"]["id"], event_id);
    assert_eq!(
        response["payload"]["result"]["root"]["payload"]["reason"],
        "mcp_raw_samples_disabled"
    );
    assert_eq!(response["payload"]["result"]["ancestors"], json!([]));
    assert_eq!(
        response["payload"]["result"]["material_links"][0]["metadata"]["reason"],
        "mcp_raw_samples_disabled"
    );
    assert_eq!(response["privacy_state"]["state"], "redacted");
    assert_eq!(response["caveats"][0]["id"], "mcp.raw_samples_redacted");
    assert!(
        !response.to_string().contains("secret_fixture_value"),
        "MCP lineage leaked raw payload or material metadata text"
    );
    Ok(())
}

#[sinex_test]
async fn mcp_relation_evidence_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(
        &client,
        "sinex_relation_evidence",
        json!({
            "seed_query": {
                "sources": ["fixture"],
                "limit": 1
            },
            "relation": {
                "relation": "within",
                "within_secs": 300
            }
        }),
    )
    .await?;

    assert_eq!(response["source_surface"], "sinex_relation_evidence");
    assert_eq!(
        response["query_echo"]["seed_query"]["sources"][0],
        "fixture"
    );
    assert_eq!(response["query_echo"]["relation"]["relation"], "within");
    assert_eq!(
        response["payload"]["result"]["payload"]["support_refs"][0]["object"]["id"],
        fixture_event_id()
    );
    assert_eq!(
        response["payload"]["result"]["payload"]["query"]["relation"],
        "within"
    );
    assert_eq!(response["privacy_state"]["state"], "redacted");
    Ok(())
}

#[sinex_test]
async fn mcp_source_readiness_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(
        &client,
        "sinex_source_readiness",
        json!({
            "source_family": "terminal",
            "source_id": "terminal.atuin-history",
            "include_caveats": false
        }),
    )
    .await?;

    assert_eq!(response["source_surface"], "sinex_source_readiness");
    assert_eq!(response["query_echo"]["source_family"], "terminal");
    let Some(sources) = response["payload"]["result"]["sources"].as_array() else {
        return Err(color_eyre::eyre::eyre!(
            "source readiness response did not contain a sources array"
        ));
    };
    assert_eq!(sources.len(), 1);
    assert_eq!(sources[0]["source_id"], "terminal.atuin-history");
    assert_eq!(sources[0]["evidence"]["sample"], "[REDACTED]");
    assert_eq!(response["payload"]["caveats"], "suppressed_by_request");
    assert_eq!(response["privacy_state"]["state"], "redacted");
    assert!(
        !response.to_string().contains("secret_fixture_value"),
        "MCP fixture response leaked raw sensitive sample text"
    );
    Ok(())
}

#[sinex_test]
async fn mcp_source_continuity_list_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(
        &client,
        "sinex_source_continuity",
        json!({ "since": "2026-05-19T00:00:00Z" }),
    )
    .await?;

    assert_eq!(response["source_surface"], "sinex_source_continuity");
    assert_eq!(response["query_echo"]["since"], "2026-05-19T00:00:00Z");
    assert_eq!(
        response["payload"]["result"]["reports"][0]["source_family"],
        "terminal"
    );
    assert_eq!(
        response["payload"]["result"]["reports"][0]["gaps"][0]["kind"],
        "private_mode"
    );
    assert_eq!(response["privacy_state"]["state"], "redacted");
    Ok(())
}

#[sinex_test]
async fn mcp_source_drift_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(
        &client,
        "sinex_source_drift",
        json!({
            "source_id": "browser.history",
            "limit": 5
        }),
    )
    .await?;

    assert_eq!(response["source_surface"], "sinex_source_drift");
    assert_eq!(
        response["payload"]["result"]["drifts"][0]["source_id"],
        "browser.history"
    );
    assert_eq!(
        response["payload"]["result"]["drifts"][0]["type_changes"][0]["key"],
        "visit_time"
    );
    Ok(())
}

#[sinex_test]
async fn mcp_source_continuity_get_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(
        &client,
        "sinex_source_continuity",
        json!({ "source_family": "terminal" }),
    )
    .await?;

    assert_eq!(response["source_surface"], "sinex_source_continuity");
    assert_eq!(response["query_echo"]["source_family"], "terminal");
    assert_eq!(
        response["payload"]["result"]["report"]["source_family"],
        "terminal"
    );
    assert_eq!(
        response["payload"]["result"]["report"]["replayability"]["raw_bytes_preserved"],
        true
    );
    assert_eq!(response["privacy_state"]["state"], "redacted");
    Ok(())
}

#[sinex_test]
async fn mcp_source_gap_explain_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(
        &client,
        "sinex_source_gap_explain",
        json!({
            "source_family": "terminal",
            "at": "2026-05-19T12:05:00Z"
        }),
    )
    .await?;

    assert_eq!(response["source_surface"], "sinex_source_gap_explain");
    assert_eq!(response["query_echo"]["source_family"], "terminal");
    assert_eq!(response["query_echo"]["at"], "2026-05-19T12:05:00Z");
    assert_eq!(response["payload"]["result"]["gap"]["kind"], "private_mode");
    let explanation = response["payload"]["result"]["explanation"]
        .as_str()
        .expect("source gap explanation must be a string");
    assert!(
        explanation.contains("coverage gap"),
        "source gap explanation should describe the coverage gap: {explanation}"
    );
    assert_eq!(response["privacy_state"]["state"], "redacted");
    Ok(())
}

#[sinex_test]
async fn mcp_source_identifier_continuity_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(
        &client,
        "sinex_source_identifier_continuity",
        json!({
            "source_identifier": "/realm/data/captures/fixture.jsonl",
            "material_kind": "local_cas"
        }),
    )
    .await?;

    assert_eq!(
        response["source_surface"],
        "sinex_source_identifier_continuity"
    );
    assert_eq!(
        response["query_echo"]["source_identifier"],
        "/realm/data/captures/fixture.jsonl"
    );
    assert_eq!(response["query_echo"]["material_kind"], "local_cas");
    assert_eq!(
        response["payload"]["result"]["source_identifier"],
        "/realm/data/captures/fixture.jsonl"
    );
    assert_eq!(
        response["payload"]["result"]["replayability"]["replayable"],
        true
    );
    assert_eq!(response["privacy_state"]["state"], "redacted");
    Ok(())
}

#[sinex_test]
async fn mcp_privacy_status_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(&client, "sinex_privacy_status", json!({})).await?;

    assert_eq!(response["source_surface"], "sinex_privacy_status");
    assert_eq!(response["payload"]["result"]["state"]["enabled"], true);
    assert_eq!(
        response["payload"]["result"]["state"]["affected_source_classes"],
        json!(["terminal"])
    );
    assert_eq!(response["privacy_state"]["state"], "redacted");
    assert!(
        !response.to_string().contains("secret_fixture_value"),
        "MCP privacy status leaked raw sensitive sample text"
    );
    Ok(())
}

#[sinex_test]
async fn mcp_system_health_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(&client, "sinex_system_health", json!({})).await?;

    assert_eq!(response["source_surface"], "sinex_system_health");
    assert_eq!(response["payload"]["result"]["status"], "degraded");
    assert_eq!(response["payload"]["result"]["healthy"], false);
    assert_eq!(
        response["payload"]["result"]["components"]["sse_confirmation"]["status"],
        "degraded"
    );
    assert_eq!(response["privacy_state"]["state"], "redacted");
    Ok(())
}

#[sinex_test]
async fn mcp_tasks_list_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(
        &client,
        "sinex_tasks_list",
        json!({
            "query": "mcp",
            "status": "started",
            "project_id": "sinex",
            "tag": "mcp",
            "due_from": "2026-05-19T00:00:00Z",
            "due_until": "2026-05-20T00:00:00Z",
            "limit": 10
        }),
    )
    .await?;

    assert_eq!(response["source_surface"], "sinex_tasks_list");
    assert_eq!(response["query_echo"]["status"], "started");
    assert_eq!(response["payload"]["result"]["total"], 1);
    assert_eq!(
        response["payload"]["result"]["tasks"][0]["title"],
        "Expose MCP task list"
    );
    assert_eq!(
        response["payload"]["result"]["tasks"][0]["project_id"],
        "sinex"
    );
    assert_eq!(
        response["payload"]["result"]["tasks"][0]["tags"],
        json!(["mcp"])
    );
    assert_eq!(response["privacy_state"]["state"], "redacted");
    Ok(())
}

#[sinex_test]
async fn mcp_task_state_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;
    let task_id = fixture_task_id();

    let response = call_tool(&client, "sinex_task_state", json!({ "task_id": task_id })).await?;

    assert_eq!(response["source_surface"], "sinex_task_state");
    assert_eq!(response["query_echo"]["task_id"], task_id);
    assert_eq!(response["payload"]["result"]["task_id"], task_id);
    assert_eq!(response["payload"]["result"]["event_count"], 3);
    assert_eq!(
        response["payload"]["result"]["state"]["title"],
        "Expose MCP task list"
    );
    assert_eq!(response["privacy_state"]["state"], "redacted");
    Ok(())
}

#[sinex_test]
async fn mcp_replay_operations_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(
        &client,
        "sinex_replay_operations",
        json!({
            "state": "Planning",
            "module": "terminal.atuin-history",
            "limit": 5
        }),
    )
    .await?;

    assert_eq!(response["source_surface"], "sinex_replay_operations");
    assert_eq!(response["query_echo"]["state"], "Planning");
    assert_eq!(
        response["payload"]["operations"][0]["operation_id"],
        fixture_operation_id()
    );
    assert_eq!(response["payload"]["operations"][0]["state"], "Planning");
    assert_eq!(response["privacy_state"]["state"], "redacted");
    Ok(())
}

#[sinex_test]
async fn mcp_replay_status_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(
        &client,
        "sinex_replay_status",
        json!({ "operation_id": fixture_operation_id() }),
    )
    .await?;

    assert_eq!(response["source_surface"], "sinex_replay_status");
    assert_eq!(
        response["payload"]["operation"]["operation_id"],
        fixture_operation_id()
    );
    assert_eq!(response["payload"]["operation"]["state"], "Previewed");
    assert_eq!(response["privacy_state"]["state"], "redacted");
    Ok(())
}

#[sinex_test]
async fn mcp_documents_search_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(
        &client,
        "sinex_documents_search",
        json!({
            "query": "secret plan",
            "kind": "markdown",
            "natural_key_prefix": "notes/",
            "limit": 5
        }),
    )
    .await?;

    assert_eq!(response["source_surface"], "sinex_documents_search");
    assert_eq!(response["query_echo"]["query"], "secret plan");
    assert_eq!(response["payload"]["result"]["search_mode"], "fts");
    assert_eq!(
        response["payload"]["result"]["results"][0]["document_id"],
        fixture_document_id()
    );
    assert_eq!(
        response["payload"]["result"]["results"][0]["text"]["reason"],
        "mcp_document_text_disabled"
    );
    assert_eq!(
        response["payload"]["result"]["results"][0]["headline"]["reason"],
        "mcp_document_text_disabled"
    );
    assert_eq!(
        response["payload"]["result"]["results"][0]["side_data"]["reason"],
        "mcp_document_side_data_disabled"
    );
    assert!(
        !response.to_string().contains("secret_fixture_value"),
        "MCP document search leaked raw document text, headline, or side data"
    );
    Ok(())
}

#[sinex_test]
async fn mcp_documents_get_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(
        &client,
        "sinex_documents_get",
        json!({ "document_id": fixture_document_id() }),
    )
    .await?;

    assert_eq!(response["source_surface"], "sinex_documents_get");
    assert_eq!(response["query_echo"]["document_id"], fixture_document_id());
    assert_eq!(response["payload"]["result"]["id"], fixture_document_id());
    assert_eq!(
        response["payload"]["result"]["side_data"]["reason"],
        "mcp_document_side_data_disabled"
    );
    assert!(
        !response.to_string().contains("secret_fixture_value"),
        "MCP document get leaked raw document side data"
    );
    Ok(())
}

#[sinex_test]
async fn mcp_documents_chunks_call_uses_redacted_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(
        &client,
        "sinex_documents_chunks",
        json!({ "document_id": fixture_document_id(), "limit": 2 }),
    )
    .await?;

    assert_eq!(response["source_surface"], "sinex_documents_chunks");
    assert_eq!(response["query_echo"]["document_id"], fixture_document_id());
    assert_eq!(
        response["payload"]["result"]["chunks"][0]["document_id"],
        fixture_document_id()
    );
    assert_eq!(
        response["payload"]["result"]["chunks"][0]["redaction_reason"],
        "mcp_document_chunk_text_redacted"
    );
    assert_eq!(
        response["payload"]["result"]["chunks"][0]["text_redacted"],
        true
    );
    assert!(response["payload"]["result"]["chunks"][0]["text"].is_null());
    assert!(
        !response.to_string().contains("secret_fixture_value"),
        "MCP document chunks leaked raw document chunk text"
    );
    Ok(())
}

#[sinex_test]
async fn mcp_semantic_epochs_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(&client, "sinex_semantic_epochs", json!({ "limit": 5 })).await?;

    assert_eq!(response["source_surface"], "sinex_semantic_epochs");
    assert_eq!(response["query_echo"]["limit"], 5);
    assert_eq!(
        response["payload"]["result"]["epochs"][0]["id"],
        fixture_semantic_epoch_id()
    );
    Ok(())
}

#[sinex_test]
async fn mcp_semantic_lanes_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(
        &client,
        "sinex_semantic_lanes",
        json!({ "status": "planned", "limit": 5 }),
    )
    .await?;

    assert_eq!(response["source_surface"], "sinex_semantic_lanes");
    assert_eq!(response["query_echo"]["status"], "planned");
    assert_eq!(
        response["payload"]["result"]["lanes"][0]["id"],
        fixture_semantic_lane_id()
    );
    Ok(())
}

#[sinex_test]
async fn mcp_semantic_lane_outputs_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(
        &client,
        "sinex_semantic_lane_outputs",
        json!({ "lane_id": fixture_semantic_lane_id(), "limit": 5 }),
    )
    .await?;

    assert_eq!(response["source_surface"], "sinex_semantic_lane_outputs");
    assert_eq!(
        response["query_echo"]["lane_id"],
        fixture_semantic_lane_id()
    );
    assert_eq!(
        response["payload"]["result"]["outputs"][0]["output_key"],
        "entity:fixture"
    );
    Ok(())
}

#[sinex_test]
async fn mcp_semantic_lane_diffs_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(
        &client,
        "sinex_semantic_lane_diffs",
        json!({ "lane_id": fixture_semantic_lane_id(), "limit": 5 }),
    )
    .await?;

    assert_eq!(response["source_surface"], "sinex_semantic_lane_diffs");
    assert_eq!(
        response["query_echo"]["lane_id"],
        fixture_semantic_lane_id()
    );
    assert_eq!(
        response["payload"]["result"]["diffs"][0]["id"],
        fixture_semantic_diff_id()
    );
    Ok(())
}

#[sinex_test]
async fn mcp_automata_status_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(
        &client,
        "sinex_automata_status",
        json!({ "stale_after_secs": 120, "recent_window_secs": 60 }),
    )
    .await?;

    assert_eq!(response["source_surface"], "sinex_automata_status");
    assert_eq!(response["query_echo"]["stale_after_secs"], 120);
    assert_eq!(response["payload"]["result"]["stale_after_secs"], 120);
    assert_eq!(
        response["payload"]["result"]["automata"][0]["module_name"],
        "session-detector"
    );
    assert_eq!(
        response["payload"]["result"]["automata"][0]["event_lag_p99_ms"],
        42.0
    );
    assert_eq!(response["privacy_state"]["state"], "redacted");
    Ok(())
}

#[sinex_test]
async fn mcp_sources_status_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(
        &client,
        "sinex_sources_status",
        json!({ "stale_after_secs": 120, "recent_window_secs": 60 }),
    )
    .await?;

    assert_eq!(response["source_surface"], "sinex_sources_status");
    assert_eq!(response["query_echo"]["recent_window_secs"], 60);
    assert_eq!(response["payload"]["result"]["recent_window_secs"], 60);
    assert_eq!(
        response["payload"]["result"]["sources"][0]["module_name"],
        "terminal.atuin-history"
    );
    assert_eq!(
        response["payload"]["result"]["sources"][0]["current_health"],
        "healthy"
    );
    assert_eq!(response["privacy_state"]["state"], "redacted");
    Ok(())
}

#[sinex_test]
async fn mcp_runtime_health_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(
        &client,
        "sinex_source_health",
        json!({ "stale_after_secs": 120 }),
    )
    .await?;

    assert_eq!(response["source_surface"], "sinex_source_health");
    assert_eq!(response["query_echo"]["stale_after_secs"], 120);
    assert_eq!(response["payload"]["result"]["active_count"], 2);
    assert_eq!(response["payload"]["result"]["inactive_count"], 1);
    assert_eq!(response["payload"]["result"]["unique_modules"], 3);
    assert_eq!(response["privacy_state"]["state"], "redacted");
    Ok(())
}

#[sinex_test]
async fn mcp_query_call_uses_descriptor_executor_and_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(
        &client,
        "sinex_query",
        json!({ "query": "runtime-health limit 1" }),
    )
    .await?;

    assert_eq!(response["source_surface"], "sinex_query");
    assert_eq!(response["query_echo"]["unit"], "runtime-health");
    assert_eq!(
        response["payload"]["rows"][0]["object_kind"],
        "runtime_module"
    );
    assert_eq!(
        response["payload"]["rows"][0]["ref"]["kind"],
        "runtime_module"
    );
    assert_eq!(response["payload"]["rows"][0]["fields"]["active_count"], 2);
    Ok(())
}

#[sinex_test]
async fn mcp_query_accepts_single_quoted_rfc3339_event_bounds() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(
        &client,
        "sinex_query",
        json!({
            "query": "events where ts_orig >= '2026-07-02T12:00:00Z' and ts_orig < '2026-07-02T13:00:00Z' limit 2"
        }),
    )
    .await?;
    let query_echo = response["query_echo"].to_string();

    assert_eq!(response["source_surface"], "sinex_query");
    assert_eq!(response["query_echo"]["unit"], "events");
    assert_eq!(response["payload"]["rows"][0]["object_kind"], "event");
    assert!(query_echo.contains("2026-07-02T12:00:00Z"));
    assert!(query_echo.contains("2026-07-02T13:00:00Z"));
    assert!(
        !query_echo.contains("'2026-07-02"),
        "single quote delimiters must not survive query parsing"
    );
    Ok(())
}

#[sinex_test]
async fn mcp_runtime_active_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(
        &client,
        "sinex_sources_active",
        json!({ "stale_after_secs": 120 }),
    )
    .await?;

    assert_eq!(response["source_surface"], "sinex_sources_active");
    assert_eq!(response["query_echo"]["stale_after_secs"], 120);
    assert_eq!(
        response["payload"]["result"]["modules"][0]["module_name"],
        "terminal.atuin-history"
    );
    assert_eq!(
        response["payload"]["result"]["modules"][0]["heartbeat_source"],
        "run"
    );
    assert_eq!(response["privacy_state"]["state"], "redacted");
    Ok(())
}

#[sinex_test]
async fn mcp_runtime_active_degrades_when_gateway_unreachable() -> TestResult<()> {
    let client = unreachable_gateway_client()?;

    let response = call_tool(
        &client,
        "sinex_sources_active",
        json!({ "stale_after_secs": 120 }),
    )
    .await?;

    assert_eq!(response["source_surface"], "sinex_sources_active");
    assert_eq!(response["query_echo"]["stale_after_secs"], 120);
    assert_eq!(response["payload"]["status"], "degraded");
    assert_eq!(response["payload"]["reason"], "gateway_unreachable");
    assert_eq!(response["payload"]["target_url"], "http://127.0.0.1:9");
    assert!(response["payload"]["result"].is_null());
    assert!(
        response["caveats"]
            .as_array()
            .expect("caveats must be an array")
            .iter()
            .any(|caveat| caveat["id"] == "mcp.gateway_unreachable")
    );
    Ok(())
}

#[sinex_test]
async fn mcp_runtime_registry_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(&client, "sinex_sources_registry", json!({})).await?;

    assert_eq!(response["source_surface"], "sinex_sources_registry");
    assert_eq!(
        response["payload"]["result"]["modules"][0]["module_name"],
        "terminal.atuin-history"
    );
    assert_eq!(
        response["payload"]["result"]["modules"][0]["state"],
        "running"
    );
    assert_eq!(response["privacy_state"]["state"], "redacted");
    Ok(())
}

#[sinex_test]
async fn mcp_event_engine_validation_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(&client, "sinex_event_engine_validation", json!({})).await?;

    assert_eq!(response["source_surface"], "sinex_event_engine_validation");
    assert_eq!(response["payload"]["snapshot"]["batch_size"], 12);
    assert_eq!(response["payload"]["snapshot"]["validation_invalid"], 0);
    assert_eq!(
        response["payload"]["snapshot"]["validation_coverage_pct"],
        100.0
    );
    assert_eq!(response["privacy_state"]["state"], "redacted");
    Ok(())
}

#[sinex_test]
async fn mcp_event_engine_batch_stats_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(
        &client,
        "sinex_event_engine_batch_stats",
        json!({
            "from": "2026-05-19T00:00:00Z",
            "to": "2026-05-19T01:00:00Z",
            "limit": 5
        }),
    )
    .await?;

    assert_eq!(response["source_surface"], "sinex_event_engine_batch_stats");
    assert_eq!(response["query_echo"]["limit"], 5);
    assert_eq!(response["payload"]["buckets"][0]["batch_count"], 3);
    assert_eq!(response["payload"]["buckets"][0]["validation_invalid"], 0);
    assert_eq!(
        response["payload"]["buckets"][0]["avg_validation_coverage_pct"],
        100.0
    );
    assert_eq!(response["privacy_state"]["state"], "redacted");
    Ok(())
}

#[sinex_test]
async fn mcp_throughput_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(&client, "sinex_throughput", json!({})).await?;

    assert_eq!(response["source_surface"], "sinex_throughput");
    assert_eq!(
        response["payload"]["result"]["per_source"][0]["source"],
        "terminal"
    );
    assert_eq!(
        response["payload"]["result"]["per_source"][0]["events_last_1h"],
        120
    );
    assert_eq!(
        response["payload"]["result"]["per_component"][0]["component"],
        "event_engine"
    );
    assert_eq!(response["privacy_state"]["state"], "redacted");
    Ok(())
}

#[sinex_test]
async fn mcp_recent_activity_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(&client, "sinex_recent_activity", json!({ "limit": 7 })).await?;

    assert_eq!(response["source_surface"], "sinex_recent_activity");
    assert_eq!(response["query_echo"]["limit"], 7);
    assert_eq!(
        response["payload"]["entries"][0]["activity_type"],
        "command"
    );
    assert_eq!(response["payload"]["entries"][0]["context"], "terminal");
    assert_eq!(response["privacy_state"]["state"], "redacted");
    Ok(())
}

#[sinex_test]
async fn mcp_command_frequency_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(
        &client,
        "sinex_command_frequency",
        json!({
            "from": "2026-05-19T00:00:00Z",
            "to": "2026-05-19T01:00:00Z",
            "limit": 3
        }),
    )
    .await?;

    assert_eq!(response["source_surface"], "sinex_command_frequency");
    assert_eq!(response["query_echo"]["limit"], 3);
    assert_eq!(response["payload"]["entries"][0]["command"], "xtask");
    assert_eq!(response["payload"]["entries"][0]["total_executions"], 12);
    assert_eq!(response["privacy_state"]["state"], "redacted");
    Ok(())
}

#[sinex_test]
async fn mcp_file_activity_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(
        &client,
        "sinex_file_activity",
        json!({
            "from": "2026-05-19T00:00:00Z",
            "to": "2026-05-19T01:00:00Z",
            "limit": 3
        }),
    )
    .await?;

    assert_eq!(response["source_surface"], "sinex_file_activity");
    assert_eq!(response["query_echo"]["limit"], 3);
    assert_eq!(
        response["payload"]["entries"][0]["directory"],
        "/realm/project/sinex"
    );
    assert_eq!(response["payload"]["entries"][0]["total_events"], 9);
    assert_eq!(response["privacy_state"]["state"], "redacted");
    Ok(())
}

#[sinex_test]
async fn mcp_system_state_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(
        &client,
        "sinex_system_state",
        json!({
            "from": "2026-05-19T00:00:00Z",
            "to": "2026-05-19T01:00:00Z",
            "limit": 3
        }),
    )
    .await?;

    assert_eq!(response["source_surface"], "sinex_system_state");
    assert_eq!(response["query_echo"]["limit"], 3);
    assert_eq!(response["payload"]["buckets"][0]["sample_count"], 5);
    assert_eq!(
        response["payload"]["buckets"][0]["avg_memory_percent"],
        42.5
    );
    assert_eq!(response["privacy_state"]["state"], "redacted");
    Ok(())
}

#[sinex_test]
async fn mcp_window_focus_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(
        &client,
        "sinex_window_focus",
        json!({
            "from": "2026-05-19T00:00:00Z",
            "to": "2026-05-19T01:00:00Z",
            "limit": 3
        }),
    )
    .await?;

    assert_eq!(response["source_surface"], "sinex_window_focus");
    assert_eq!(response["query_echo"]["limit"], 3);
    assert_eq!(response["payload"]["buckets"][0]["workspace"], "4");
    assert_eq!(response["payload"]["buckets"][0]["window_class"], "kitty");
    assert_eq!(response["payload"]["buckets"][0]["focus_event_count"], 6);
    assert_eq!(response["privacy_state"]["state"], "redacted");
    Ok(())
}

#[sinex_test]
async fn mcp_current_health_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(&client, "sinex_current_health", json!({ "limit": 4 })).await?;

    assert_eq!(response["source_surface"], "sinex_current_health");
    assert_eq!(response["query_echo"]["limit"], 4);
    assert_eq!(response["payload"]["entries"][0]["source"], "sinex");
    assert_eq!(response["payload"]["entries"][0]["event_type"], "health");
    assert_eq!(response["payload"]["entries"][0]["status"], "healthy");
    assert_eq!(response["privacy_state"]["state"], "redacted");
    Ok(())
}

#[sinex_test]
async fn mcp_current_device_state_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(&client, "sinex_current_device_state", json!({ "limit": 4 })).await?;

    assert_eq!(response["source_surface"], "sinex_current_device_state");
    assert_eq!(response["query_echo"]["limit"], 4);
    assert_eq!(response["payload"]["entries"][0]["unit_name"], "sinexd");
    assert_eq!(response["payload"]["entries"][0]["state"], "active");
    assert_eq!(response["privacy_state"]["state"], "redacted");
    Ok(())
}

#[sinex_test]
async fn mcp_gateway_stats_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(&client, "sinex_gateway_stats", telemetry_window_args()).await?;

    assert_eq!(response["source_surface"], "sinex_gateway_stats");
    assert_eq!(response["query_echo"]["limit"], 3);
    assert_eq!(response["payload"]["buckets"][0]["source"], "gateway");
    assert_eq!(response["payload"]["buckets"][0]["stat_events"], 4);
    assert_eq!(response["privacy_state"]["state"], "redacted");
    Ok(())
}

#[sinex_test]
async fn mcp_stream_stats_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(&client, "sinex_stream_stats", telemetry_window_args()).await?;

    assert_eq!(response["source_surface"], "sinex_stream_stats");
    assert_eq!(response["query_echo"]["limit"], 3);
    assert_eq!(response["payload"]["buckets"][0]["stream_name"], "EVENTS");
    assert_eq!(response["payload"]["buckets"][0]["sample_count"], 2);
    assert_eq!(response["privacy_state"]["state"], "redacted");
    Ok(())
}

#[sinex_test]
async fn mcp_assembly_stats_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(&client, "sinex_assembly_stats", telemetry_window_args()).await?;

    assert_eq!(response["source_surface"], "sinex_assembly_stats");
    assert_eq!(response["query_echo"]["limit"], 3);
    assert_eq!(response["payload"]["buckets"][0]["total_completed"], 7);
    assert_eq!(response["payload"]["buckets"][0]["sample_count"], 3);
    assert_eq!(response["privacy_state"]["state"], "redacted");
    Ok(())
}

#[sinex_test]
async fn mcp_source_stats_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(&client, "sinex_source_stats", telemetry_window_args()).await?;

    assert_eq!(response["source_surface"], "sinex_source_stats");
    assert_eq!(response["query_echo"]["limit"], 3);
    assert_eq!(response["payload"]["buckets"][0]["module_kind"], "source");
    assert_eq!(
        response["payload"]["buckets"][0]["total_events_processed"],
        42
    );
    assert_eq!(response["privacy_state"]["state"], "redacted");
    Ok(())
}

#[sinex_test]
async fn mcp_metric_counters_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(&client, "sinex_metric_counters", telemetry_window_args()).await?;

    assert_eq!(response["source_surface"], "sinex_metric_counters");
    assert_eq!(response["query_echo"]["limit"], 3);
    assert_eq!(
        response["payload"]["buckets"][0]["component"],
        "event_engine"
    );
    assert_eq!(response["payload"]["buckets"][0]["metric_name"], "events");
    assert_eq!(response["payload"]["buckets"][0]["total_value"], 99);
    assert_eq!(response["privacy_state"]["state"], "redacted");
    Ok(())
}

#[sinex_test]
async fn mcp_llm_prompts_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(
        &client,
        "sinex_llm_prompts",
        json!({ "status": "active", "limit": 2 }),
    )
    .await?;

    assert_eq!(response["source_surface"], "sinex_llm_prompts");
    assert_eq!(response["query_echo"]["status"], "active");
    assert_eq!(
        response["payload"]["result"]["events"][0]["payload"]["redacted"],
        true
    );
    assert_eq!(response["privacy_state"]["state"], "redacted");
    Ok(())
}

#[sinex_test]
async fn mcp_llm_route_explain_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(&client, "sinex_llm_route_explain", fixture_llm_route_args()).await?;

    assert_eq!(response["source_surface"], "sinex_llm_route_explain");
    assert_eq!(
        response["query_echo"]["request"]["task_kind"],
        "entity-extraction"
    );
    assert_eq!(
        response["payload"]["result"]["decision"]["model"],
        "fixture-model"
    );
    assert_eq!(response["privacy_state"]["state"], "redacted");
    Ok(())
}

#[sinex_test]
async fn mcp_llm_budget_report_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(&client, "sinex_llm_budget_report", json!({ "limit": 5 })).await?;

    assert_eq!(response["source_surface"], "sinex_llm_budget_report");
    assert_eq!(response["query_echo"]["limit"], 5);
    assert_eq!(response["payload"]["result"]["total_rows"], 1);
    assert_eq!(response["payload"]["result"]["prompt_tokens"], 12);
    assert_eq!(response["privacy_state"]["state"], "redacted");
    Ok(())
}

#[sinex_test]
async fn mcp_curation_proposals_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(
        &client,
        "sinex_curation_proposals",
        json!({ "status": "pending", "limit": 4 }),
    )
    .await?;

    assert_eq!(response["source_surface"], "sinex_curation_proposals");
    assert_eq!(response["query_echo"]["status"], "pending");
    assert_eq!(
        response["payload"]["result"]["events"][0]["payload"]["redacted"],
        true
    );
    assert_eq!(response["privacy_state"]["state"], "redacted");
    Ok(())
}

#[sinex_test]
async fn mcp_dlq_stats_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(&client, "sinex_dlq_stats", json!({})).await?;

    assert_eq!(response["source_surface"], "sinex_dlq_stats");
    assert_eq!(response["payload"]["result"]["total_messages"], 2);
    assert_eq!(response["payload"]["result"]["total_bytes"], 512);
    assert_eq!(response["privacy_state"]["state"], "redacted");
    Ok(())
}

#[sinex_test]
async fn mcp_dlq_peek_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(&client, "sinex_dlq_peek", json!({ "limit": 2 })).await?;

    assert_eq!(response["source_surface"], "sinex_dlq_peek");
    assert_eq!(response["query_echo"]["limit"], 2);
    assert_eq!(
        response["payload"]["result"]["messages"][0]["payload_redacted"],
        true
    );
    assert_eq!(
        response["payload"]["result"]["messages"][0]["privacy_caveats"][0]["id"],
        "policy.disclosure_applied"
    );
    assert_eq!(
        response["payload"]["result"]["messages"][0]["privacy_caveats"][0]["ref"]["id"],
        "secret_redacted"
    );
    assert_eq!(response["privacy_state"]["state"], "redacted");
    Ok(())
}

#[sinex_test]
async fn mcp_source_materials_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(
        &client,
        "sinex_source_materials",
        json!({ "status": "completed", "limit": 2 }),
    )
    .await?;

    assert_eq!(response["source_surface"], "sinex_source_materials");
    assert_eq!(response["query_echo"]["status"], "completed");
    assert_eq!(
        response["payload"]["result"]["materials"][0]["id"],
        fixture_material_id()
    );
    assert_eq!(response["privacy_state"]["state"], "redacted");
    Ok(())
}

#[sinex_test]
async fn mcp_source_material_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(
        &client,
        "sinex_source_material",
        json!({ "material_id": fixture_material_id() }),
    )
    .await?;

    assert_eq!(response["source_surface"], "sinex_source_material");
    assert_eq!(response["query_echo"]["material_id"], fixture_material_id());
    assert_eq!(
        response["payload"]["result"]["material"]["metadata"]["redacted"],
        true
    );
    assert_eq!(response["privacy_state"]["state"], "redacted");
    Ok(())
}

#[sinex_test]
async fn mcp_source_coverage_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(&client, "sinex_source_coverage", json!({})).await?;

    assert_eq!(response["source_surface"], "sinex_source_coverage");
    assert_eq!(
        response["payload"]["result"]["sources"][0]["event_count"],
        42
    );
    assert_eq!(
        response["payload"]["result"]["sources"][0]["material_count"],
        3
    );
    assert_eq!(
        response["payload"]["result"]["sources"][0]["recovered_partial_material_count"],
        1
    );
    assert_eq!(response["privacy_state"]["state"], "redacted");
    Ok(())
}

#[sinex_test]
async fn mcp_sources_status_view_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(&client, "sinex_sources_status_view", json!({})).await?;

    assert_eq!(response["source_surface"], "sinex_sources_status_view");
    assert_eq!(
        response["payload"]["schema_version"],
        "sinex.source-coverage-list/v1"
    );
    assert_eq!(response["payload"]["count"], 1);
    assert_eq!(
        response["payload"]["sources"][0]["source_id"],
        "terminal.atuin-history"
    );
    assert_eq!(response["payload"]["sources"][0]["event_count"], 42);
    assert!(response["payload"]["result"].is_null());
    assert_eq!(response["privacy_state"]["state"], "redacted");
    Ok(())
}

#[sinex_test]
async fn mcp_sources_status_view_degrades_when_gateway_unreachable() -> TestResult<()> {
    let client = unreachable_gateway_client()?;

    let response = call_tool(&client, "sinex_sources_status_view", json!({})).await?;

    assert_eq!(response["source_surface"], "sinex_sources_status_view");
    assert_eq!(response["payload"]["status"], "degraded");
    assert_eq!(response["payload"]["reason"], "gateway_unreachable");
    assert_eq!(response["payload"]["target_url"], "http://127.0.0.1:9");
    assert!(response["payload"]["result"].is_null());
    assert!(
        response["caveats"]
            .as_array()
            .expect("caveats must be an array")
            .iter()
            .any(|caveat| caveat["id"] == "mcp.gateway_unreachable")
    );
    Ok(())
}

#[sinex_test]
async fn mcp_source_presets_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(&client, "sinex_source_presets", json!({})).await?;

    assert_eq!(response["source_surface"], "sinex_source_presets");
    assert_eq!(
        response["payload"]["result"]["presets"][0]["name"],
        "terminal.atuin.default"
    );
    assert_eq!(response["privacy_state"]["state"], "redacted");
    Ok(())
}

#[sinex_test]
async fn mcp_source_bindings_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(
        &client,
        "sinex_source_bindings",
        json!({ "source_family": "terminal", "include_disabled": true }),
    )
    .await?;

    assert_eq!(response["source_surface"], "sinex_source_bindings");
    assert_eq!(response["query_echo"]["source_family"], "terminal");
    assert_eq!(response["query_echo"]["include_disabled"], true);
    assert_eq!(
        response["payload"]["result"]["bindings"][0]["id"],
        "terminal-atuin-history"
    );
    assert_eq!(response["privacy_state"]["state"], "redacted");
    Ok(())
}

#[sinex_test]
async fn mcp_ops_list_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(
        &client,
        "sinex_ops_list",
        json!({ "operation_type": "replay", "limit": 2 }),
    )
    .await?;

    assert_eq!(response["source_surface"], "sinex_ops_list");
    assert_eq!(response["query_echo"]["operation_type"], "replay");
    assert_eq!(response["payload"]["count"], 1);
    assert_eq!(response["payload"]["jobs"][0]["kind"], "replay");
    assert_eq!(
        response["payload"]["jobs"][0]["actions"][0]["id"],
        "ops.show"
    );
    assert_eq!(
        response["payload"]["jobs"][0]["actions"][0]["side_effect"],
        "read"
    );
    assert_eq!(
        response["payload"]["jobs"][0]["actions"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert!(response["payload"]["result"].is_null());
    assert_eq!(response["privacy_state"]["state"], "redacted");
    Ok(())
}

#[sinex_test]
async fn mcp_ops_get_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(
        &client,
        "sinex_ops_get",
        json!({ "operation_id": fixture_operation_id() }),
    )
    .await?;

    assert_eq!(response["source_surface"], "sinex_ops_get");
    assert_eq!(
        response["query_echo"]["operation_id"],
        fixture_operation_id()
    );
    assert_eq!(response["payload"]["id"], fixture_operation_id());
    assert_eq!(response["payload"]["kind"], "replay");
    assert_eq!(response["payload"]["actions"][0]["id"], "ops.show");
    assert_eq!(response["payload"]["actions"][0]["side_effect"], "read");
    assert_eq!(response["payload"]["actions"].as_array().unwrap().len(), 1);
    assert!(response["payload"]["result"].is_null());
    assert_eq!(response["privacy_state"]["state"], "redacted");
    Ok(())
}

#[sinex_test]
async fn mcp_lifecycle_status_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(&client, "sinex_lifecycle_status", json!({})).await?;

    assert_eq!(response["source_surface"], "sinex_lifecycle_status");
    assert_eq!(response["payload"]["result"]["total_events"], 42);
    assert_eq!(response["payload"]["result"]["tiers"][0]["tier"], "live");
    assert_eq!(response["privacy_state"]["state"], "redacted");
    Ok(())
}

#[sinex_test]
async fn mcp_audit_trail_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(
        &client,
        "sinex_audit_trail",
        json!({ "operation_id": fixture_operation_id() }),
    )
    .await?;

    assert_eq!(response["source_surface"], "sinex_audit_trail");
    assert_eq!(
        response["query_echo"]["operation_id"],
        fixture_operation_id()
    );
    assert_eq!(
        response["payload"]["result"]["audit_trail"]["operation"]["id"],
        fixture_operation_id()
    );
    assert_eq!(response["payload"]["result"]["event_count"], 1);
    assert_eq!(response["privacy_state"]["state"], "redacted");
    Ok(())
}

#[sinex_test]
async fn mcp_coordination_instances_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(
        &client,
        "sinex_coordination_instances",
        json!({ "module_kind": "service" }),
    )
    .await?;

    assert_eq!(response["source_surface"], "sinex_coordination_instances");
    assert_eq!(response["query_echo"]["module_kind"], "service");
    assert_eq!(
        response["payload"]["result"]["instances"][0]["instance_id"],
        fixture_instance_id()
    );
    assert_eq!(response["privacy_state"]["state"], "redacted");
    Ok(())
}

#[sinex_test]
async fn mcp_coordination_leader_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(
        &client,
        "sinex_coordination_leader",
        json!({ "module_kind": "service" }),
    )
    .await?;

    assert_eq!(response["source_surface"], "sinex_coordination_leader");
    assert_eq!(
        response["payload"]["result"]["leader"]["instance_id"],
        fixture_instance_id()
    );
    assert_eq!(response["privacy_state"]["state"], "redacted");
    Ok(())
}

#[sinex_test]
async fn mcp_coordination_instance_health_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(
        &client,
        "sinex_coordination_instance_health",
        json!({ "instance_id": fixture_instance_id() }),
    )
    .await?;

    assert_eq!(
        response["source_surface"],
        "sinex_coordination_instance_health"
    );
    assert_eq!(response["query_echo"]["instance_id"], fixture_instance_id());
    assert_eq!(response["payload"]["result"]["healthy"], true);
    assert_eq!(response["privacy_state"]["state"], "redacted");
    Ok(())
}

#[sinex_test]
async fn mcp_shadow_consumers_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(
        &client,
        "sinex_shadow_consumers",
        json!({ "prefix": "dev-fixture" }),
    )
    .await?;

    assert_eq!(response["source_surface"], "sinex_shadow_consumers");
    assert_eq!(response["query_echo"]["prefix"], "dev-fixture");
    assert_eq!(
        response["payload"]["result"]["consumers"][0]["consumer_name"],
        "dev-fixture"
    );
    assert_eq!(response["privacy_state"]["state"], "redacted");
    Ok(())
}

#[sinex_test]
async fn mcp_system_ping_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(&client, "sinex_system_ping", json!({})).await?;

    assert_eq!(response["source_surface"], "sinex_system_ping");
    assert_eq!(response["payload"]["result"], "pong");
    assert_eq!(response["privacy_state"]["state"], "redacted");
    Ok(())
}

#[sinex_test]
async fn mcp_system_version_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(&client, "sinex_system_version", json!({})).await?;

    assert_eq!(response["source_surface"], "sinex_system_version");
    assert_eq!(response["payload"]["result"], "0.4.2");
    assert_eq!(response["privacy_state"]["state"], "redacted");
    Ok(())
}

fn telemetry_window_args() -> Value {
    json!({
        "from": "2026-05-19T00:00:00Z",
        "to": "2026-05-19T01:00:00Z",
        "limit": 3
    })
}

fn fixture_llm_route_args() -> Value {
    json!({
        "request": {
            "task_kind": "entity-extraction",
            "prompt_id": "extract-entities",
            "input_hash": "input-hash",
            "privacy_route": "remote_allowed",
            "bucket_key": "fixture"
        },
        "policy": {
            "policy_id": "entity-policy",
            "task_kind": "entity-extraction",
            "prompt_id": "extract-entities",
            "prompt_version": "2026-05-19",
            "fallback_order": [
                {
                    "provider": "fixture-provider",
                    "model": "fixture-model",
                    "tier": null,
                    "is_local": false
                }
            ],
            "replay_policy": "record",
            "privacy_policy_ref": "privacy.llm.fixture",
            "rollout": null,
            "active": true
        }
    })
}

async fn mount_mcp_gateway_fixture() -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/"))
        .respond_with(|request: &wiremock::Request| {
            let Ok(body) = serde_json::from_slice::<Value>(&request.body) else {
                return ResponseTemplate::new(400).set_body_json(json!({
                    "jsonrpc": "2.0",
                    "error": {
                        "code": -32700,
                        "message": "invalid JSON fixture request"
                    },
                    "id": null
                }));
            };
            let Some(method) = body["method"].as_str() else {
                return ResponseTemplate::new(400).set_body_json(json!({
                    "jsonrpc": "2.0",
                    "error": {
                        "code": -32600,
                        "message": "missing fixture RPC method"
                    },
                    "id": body["id"]
                }));
            };
            let result = match method {
                "events.query" => json!({
                    "type": "events",
                    "events": [
                        fixture_sensitive_query_event()
                    ],
                    "next_cursor": null,
                    "total_estimate": null
                }),
                "events.cards" => fixture_event_card_list(),
                "events.lineage" => {
                    let event_id = match body["params"]["event_id"].as_str() {
                        Some(value) => value.to_string(),
                        None => fixture_event_id().to_string(),
                    };
                    json!({
                        "root": fixture_event(&event_id),
                        "ancestors": [],
                        "descendants": [],
                        "material_links": [
                            {
                                "from_material_id": fixture_material_id(),
                                "to_material_id": fixture_material_id(),
                                "relation_type": "fixture",
                                "metadata": {
                                    "raw_sample": "lineage secret_fixture_value should not leak"
                                },
                                "created_at": "2026-05-18T12:00:00Z"
                            }
                        ]
                    })
                }
                "events.relation_evidence" => fixture_relation_evidence_envelope(),
                "sources.readiness.list" => json!({
                    "sources": [
                        {
                            "source_family": "terminal",
                            "source_id": "terminal.atuin-history",
                            "source_identifier": "atuin-history",
                            "status": "available",
                            "cost": "local_fast",
                            "freshness_seconds": 12,
                            "material_count": 3,
                            "parsed_event_count": 3,
                            "caveats": [
                                {
                                    "code": "fixture_redacted",
                                    "message": "secret-like sample suppressed",
                                    "severity": "info"
                                }
                            ],
                            "evidence": {
                                "sample": "[REDACTED]"
                            }
                        },
                        {
                            "source_family": "terminal",
                            "source_id": "terminal.text-history",
                            "source_identifier": "text-history",
                            "status": "stale",
                            "cost": "local_fast",
                            "material_count": 1
                        }
                    ]
                }),
                "sources.readiness.get" => json!({
                    "readiness": {
                        "source_family": "terminal",
                        "source_id": "terminal.atuin-history",
                        "source_identifier": "atuin-history",
                        "status": "available",
                        "cost": "local_fast",
                        "material_count": 3
                    }
                }),
                "sources.continuity" => json!({
                    "source_identifier": body["params"]["source_identifier"].as_str().unwrap_or("/realm/data/captures/fixture.jsonl"),
                    "coverage_gaps": [
                        {
                            "gap_start": "2026-05-19T10:00:00Z",
                            "gap_end": "2026-05-19T10:30:00Z",
                            "gap_duration_seconds": 1800,
                            "gap_type": "temporal"
                        }
                    ],
                    "contract_status": {
                        "has_coverage_contract": true,
                        "expected_interval_seconds": 60,
                        "actual_coverage_percent": 99.1,
                        "breaches": []
                    },
                    "replayability": {
                        "replayable": true,
                        "reason": null,
                        "material_count": 3,
                        "events_count": 42
                    }
                }),
                "sources.continuity.list" => json!({
                    "reports": [
                        fixture_continuity_report()
                    ]
                }),
                "sources.continuity.get" => json!({
                    "report": fixture_continuity_report()
                }),
                "sources.drift.list" => json!({
                    "drifts": [
                        {
                            "checkpoint_key": "source.default.fixture",
                            "source_id": body["params"]["source_id"].as_str().unwrap_or("browser.history"),
                            "consumer_group": "default",
                            "consumer_name": "fixture",
                            "previous_hash": "shape-old",
                            "current_hash": "shape-new",
                            "format": "sqlite",
                            "added_keys": ["visit_duration"],
                            "removed_keys": [],
                            "type_changes": [
                                {
                                    "key": "visit_time",
                                    "previous_type": "number",
                                    "current_type": "string"
                                }
                            ],
                            "observed_at": "2026-05-21T07:00:00Z"
                        }
                    ]
                }),
                "sources.continuity.explain_gap" => json!({
                    "source_family": body["params"]["source_family"].as_str().unwrap_or("terminal"),
                    "at": body["params"]["at"].as_str().unwrap_or("2026-05-19T12:05:00Z"),
                    "gap": {
                        "from_ts": "2026-05-19T10:00:00Z",
                        "to_ts": "2026-05-19T10:30:00Z",
                        "kind": "private_mode",
                        "attribution": "fixture private mode"
                    },
                    "explanation": "At 2026-05-19T12:05:00Z, terminal was inside a coverage gap: fixture private mode"
                }),
                "privacy.private_mode.status" => json!({
                    "state": {
                        "enabled": true,
                        "reason_class": "operator_private",
                        "actor": "operator",
                        "started_at": "2026-05-19T10:00:00Z",
                        "expires_at": null,
                        "affected_source_classes": ["terminal"],
                        "updated_by_operation_id": "op-private"
                    }
                }),
                "system.health" => json!({
                    "status": "degraded",
                    "healthy": false,
                    "serving": true,
                    "degradation_reasons": [
                        "confirmation fan-out degraded",
                        "raw-ingest DLQ pressure: 3 pending message(s), sequence span 3"
                    ],
                    "components": {
                        "database": {
                            "status": "healthy",
                            "connected": true,
                            "latency_ms": 1.5,
                            "detail": null
                        },
                        "nats": {
                            "status": "healthy",
                            "connected": true,
                            "latency_ms": 2.0,
                            "detail": null
                        },
                        "raw_ingest_dlq": {
                            "status": "degraded",
                            "connected": true,
                            "latency_ms": null,
                            "detail": "raw-ingest DLQ pressure: 3 pending message(s), sequence span 3"
                        },
                        "confirmation_buffer": {
                            "status": "degraded",
                            "connected": true,
                            "latency_ms": null,
                            "detail": "confirmation buffers: observed=1, pending=12, timed_out_retained=4"
                        },
                        "replay_control": {
                            "status": "healthy",
                            "enabled": true,
                            "connected": true,
                            "last_error": null
                        },
                        "sse_confirmation": {
                            "status": "degraded",
                            "connected": true,
                            "latency_ms": null,
                            "detail": "pending_retry=2 dropped=1"
                        }
                    }
                }),
                "tasks.list" => json!({
                    "tasks": [
                        fixture_task_state()
                    ],
                    "total": 1,
                    "event_count": 3,
                    "limit": 10
                }),
                "tasks.state.get" => json!({
                    "task_id": body["params"]["task_id"].as_str().unwrap_or(fixture_task_id()),
                    "state": fixture_task_state(),
                    "event_count": 3
                }),
                "replay.list_operations" => json!({
                    "operations": [
                        fixture_replay_operation(
                            body["params"]["state"].as_str().unwrap_or("Planning")
                        )
                    ]
                }),
                "replay.operation_status" => json!({
                    "operation": fixture_replay_operation("Previewed")
                }),
                "documents.search" => json!({
                    "results": [
                        {
                            "document_id": fixture_document_id(),
                            "kind": "markdown",
                            "natural_key": "notes/fixture.md",
                            "chunk_index": 0,
                            "headline": "<mark>secret</mark> secret_fixture_value",
                            "text": "full document secret_fixture_value should not leak",
                            "score": 0.875,
                            "byte_offset_start": 0,
                            "byte_offset_end": 48,
                            "extraction_version": 1,
                            "side_data": {
                                "sample": "side secret_fixture_value should not leak"
                            },
                            "updated_at": "2026-05-19T11:45:00Z"
                        }
                    ],
                    "search_mode": "fts"
                }),
                "documents.get" => json!({
                    "id": fixture_document_id(),
                    "kind": "markdown",
                    "natural_key": "notes/fixture.md",
                    "extraction_version": 1,
                    "chunk_count": 2,
                    "text_byte_len": 128,
                    "side_data": {
                        "sample": "document side secret_fixture_value should not leak"
                    },
                    "created_at": "2026-05-19T11:00:00Z",
                    "updated_at": "2026-05-19T11:45:00Z"
                }),
                "documents.get_chunks_redacted" => json!({
                    "document_id": body["params"]["document_id"].as_str().unwrap_or(fixture_document_id()),
                    "chunks": [
                        {
                            "document_id": body["params"]["document_id"].as_str().unwrap_or(fixture_document_id()),
                            "chunk_index": 0,
                            "byte_offset_start": 0,
                            "byte_offset_end": 48,
                            "source_anchor_start": 10,
                            "source_anchor_end": 58,
                            "text_redacted": true,
                            "redaction_reason": "mcp_document_chunk_text_redacted",
                            "text_byte_len": 48
                        }
                    ]
                }),
                "semantic.epochs.list" => json!({
                    "epochs": [
                        {
                            "id": fixture_semantic_epoch_id(),
                            "name": "fixture-epoch",
                            "scope": {
                                "kind": "event_set",
                                "input_ids": ["event:fixture"],
                                "input_set_hash": "fixture-input-hash"
                            },
                            "code_ref": "fixture@sha",
                            "config_hash": "fixture-config",
                            "components": [],
                            "prompt_set_hash": null,
                            "model_config_hash": null,
                            "created_by": "mcp-fixture",
                            "operation_id": null,
                            "created_at": "2026-05-19T11:30:00Z",
                            "supersedes_epoch_id": null
                        }
                    ]
                }),
                "semantic.lanes.list" => json!({
                    "lanes": [
                        fixture_semantic_lane(body["params"]["status"].as_str().unwrap_or("planned"))
                    ]
                }),
                "semantic.lane_outputs.list" => json!({
                    "lane_id": body["params"]["lane_id"].as_str().unwrap_or(fixture_semantic_lane_id()),
                    "outputs": [
                        {
                            "lane_id": fixture_semantic_lane_id(),
                            "output_kind": "entity",
                            "output_key": "entity:fixture",
                            "source_event_id": null,
                            "source_material_id": null,
                            "source_anchor": null,
                            "output_hash": "fixture-output-hash",
                            "payload": {
                                "entity_key": "entity:fixture",
                                "canonical_name": "Fixture Entity",
                                "entity_type": "project",
                                "metadata": null
                            },
                            "metadata": {},
                            "created_at": "2026-05-19T11:40:00Z"
                        }
                    ]
                }),
                "semantic.lane_diffs.list" => json!({
                    "lane_id": body["params"]["lane_id"].as_str().unwrap_or(fixture_semantic_lane_id()),
                    "diffs": [
                        {
                            "id": fixture_semantic_diff_id(),
                            "baseline_lane_id": fixture_semantic_lane_id(),
                            "candidate_lane_id": fixture_semantic_candidate_lane_id(),
                            "diff_kind": "entity_relation",
                            "counts": { "entity_new": 1 },
                            "examples": [],
                            "report_hash": "fixture-report-hash",
                            "created_at": "2026-05-19T11:45:00Z"
                        }
                    ]
                }),
                "automata.status" => json!({
                    "generated_at": "2026-05-19T12:00:00Z",
                    "stale_after_secs": body["params"]["stale_after_secs"].as_u64().unwrap_or(300),
                    "recent_window_secs": body["params"]["recent_window_secs"].as_u64().unwrap_or(300),
                    "automata": [
                        {
                            "module_name": "session-detector",
                            "version": "0.4.2",
                            "description": "fixture automaton",
                            "manifest_status": "registered",
                            "live": true,
                            "service_name": "sinex-session-detector.service",
                            "instance_id": "session-detector-1",
                            "module_run_id": null,
                            "host": "test-host",
                            "run_status": "healthy",
                            "started_at": "2026-05-19T11:00:00Z",
                            "last_heartbeat_at": "2026-05-19T11:59:59Z",
                            "events_processed_current_run": 12,
                            "checkpoint_kind": "nats_kv",
                            "checkpoint_position": "seq:12",
                            "checkpoint_revision": 2,
                            "checkpoint_recorded_at": "2026-05-19T11:59:50Z",
                            "pending_invalidation_count": 0,
                            "error_rate_5m": 0.0,
                            "event_lag_p50_ms": 12.0,
                            "event_lag_p99_ms": 42.0,
                            "tick_runtime_p99_ms": 7.5,
                            "throughput_eps": 1.25,
                            "recent_output_count": 4,
                            "last_output_at": "2026-05-19T11:59:40Z",
                            "last_replay_at": null
                        }
                    ]
                }),
                "sources.status" => json!({
                    "generated_at": "2026-05-19T12:00:00Z",
                    "stale_after_secs": body["params"]["stale_after_secs"].as_u64().unwrap_or(300),
                    "recent_window_secs": body["params"]["recent_window_secs"].as_u64().unwrap_or(300),
                    "sources": [
                        {
                            "module_name": "terminal.atuin-history",
                            "version": "0.4.2",
                            "description": "fixture source",
                            "manifest_status": "registered",
                            "live": true,
                            "service_name": "source-driver-terminal.atuin-history.service",
                            "instance_id": "terminal-atuin-1",
                            "module_run_id": null,
                            "host": "test-host",
                            "run_status": "healthy",
                            "started_at": "2026-05-19T11:00:00Z",
                            "last_heartbeat_at": "2026-05-19T11:59:59Z",
                            "current_health": "healthy",
                            "health_changed_at": "2026-05-19T11:55:00Z",
                            "health_reason": "fixture",
                            "recent_output_count": 8,
                            "last_output_at": "2026-05-19T11:59:45Z"
                        }
                    ]
                }),
                "runtime.health" => json!({
                    "active_count": 2,
                    "inactive_count": 1,
                    "unique_modules": 3,
                    "active_run_count": 2,
                    "oldest_heartbeat": "2026-05-19T11:50:00Z"
                }),
                "runtime.list_active" => json!({
                    "modules": [
                        {
                            "module_name": "terminal.atuin-history",
                            "module_kind": "source",
                            "version": "0.4.2",
                            "description": "fixture source host",
                            "service_name": "source-driver-terminal.atuin-history.service",
                            "instance_id": "terminal-atuin-1",
                            "module_run_id": null,
                            "host": "test-host",
                            "status": "healthy",
                            "last_heartbeat_at": "2026-05-19T11:59:59Z",
                            "started_at": "2026-05-19T11:00:00Z",
                            "heartbeat_source": "run"
                        }
                    ]
                }),
                "runtime.list" => json!({
                    "modules": [
                        {
                            "module_name": "terminal.atuin-history",
                            "state": "running",
                            "last_heartbeat": "2026-05-19T11:59:59Z",
                            "processing_horizon": "2026-05-19T12:00:00Z"
                        }
                    ]
                }),
                "telemetry.event_engine_validation" => json!({
                    "snapshot": {
                        "observed_at": "2026-05-19T11:59:59Z",
                        "batch_size": 12,
                        "fetch_to_ack_ms": 18,
                        "events_deferred": 0,
                        "events_failed": 0,
                        "had_derived": false,
                        "insert_path": "values",
                        "validation_valid": 12,
                        "validation_skipped": 0,
                        "validation_no_schema": 0,
                        "validation_schema_not_found": 0,
                        "validation_invalid": 0,
                        "validation_coverage_pct": 100.0,
                        "suspicious_future_ts_orig": 0
                    }
                }),
                "telemetry.event_engine_batch_stats" => json!({
                    "buckets": [
                        {
                            "bucket": "2026-05-19T00:00:00Z",
                            "avg_batch_size": 12.0,
                            "max_batch_size": 18,
                            "avg_latency_ms": 14.5,
                            "max_latency_ms": 19.0,
                            "total_deferred": 0,
                            "total_failed": 0,
                            "derived_batches": 0,
                            "batch_count": 3,
                            "validation_valid": 36,
                            "validation_skipped": 0,
                            "validation_no_schema": 0,
                            "validation_schema_not_found": 0,
                            "validation_invalid": 0,
                            "avg_validation_coverage_pct": 100.0
                        }
                    ]
                }),
                "telemetry.throughput" => json!({
                    "per_source": [
                        {
                            "source": "terminal",
                            "events_last_1h": 120,
                            "events_last_24h": 1440,
                            "eps_1h": 0.0333333333,
                            "eps_24h": 0.0166666667
                        }
                    ],
                    "per_component": [
                        {
                            "component": "event_engine",
                            "eps_1h": 0.05,
                            "eps_24h": 0.02
                        }
                    ]
                }),
                "telemetry.recent_activity" => json!({
                    "entries": [
                        {
                            "activity_type": "command",
                            "context": "terminal",
                            "detail": "fixture recent activity",
                            "timestamp": "2026-05-19T12:00:00Z"
                        }
                    ]
                }),
                "telemetry.command_frequency" => json!({
                    "entries": [
                        {
                            "command": "xtask",
                            "shell": "zsh",
                            "total_executions": 12,
                            "successful_executions": 11,
                            "failed_executions": 1,
                            "avg_duration_ms": 42.0
                        }
                    ]
                }),
                "telemetry.file_activity" => json!({
                    "entries": [
                        {
                            "bucket": "2026-05-19T12:00:00Z",
                            "directory": "/realm/project/sinex",
                            "event_type": "modified",
                            "total_events": 9,
                            "unique_files": 4
                        }
                    ]
                }),
                "telemetry.system_state" => json!({
                    "buckets": [
                        {
                            "bucket": "2026-05-19T12:00:00Z",
                            "avg_cpu_percent": 12.5,
                            "max_cpu_percent": 25.0,
                            "avg_memory_percent": 42.5,
                            "max_memory_percent": 50.0,
                            "avg_disk_percent": 60.0,
                            "current_active_units": 8,
                            "sample_count": 5
                        }
                    ]
                }),
                "telemetry.window_focus" => json!({
                    "buckets": [
                        {
                            "bucket": "2026-05-19T12:00:00Z",
                            "workspace": "4",
                            "window_class": "kitty",
                            "window_title": "sinex",
                            "window_id": "0xabc",
                            "last_focus_time": "2026-05-19T12:00:00Z",
                            "focus_event_count": 6
                        }
                    ]
                }),
                "telemetry.current_health" => json!({
                    "entries": [
                        {
                            "source": "sinex",
                            "event_type": "health",
                            "component": "gateway",
                            "status": "healthy",
                            "reason": "fixture",
                            "last_update": "2026-05-19T12:00:00Z"
                        }
                    ]
                }),
                "telemetry.current_device_state" => json!({
                    "entries": [
                        {
                            "unit_name": "sinexd",
                            "unit_type": "service",
                            "state": "active",
                            "sub_state": "running",
                            "last_update": "2026-05-19T12:00:00Z"
                        }
                    ]
                }),
                "telemetry.gateway_stats" => json!({
                    "buckets": [
                        {
                            "bucket": "2026-05-19T12:00:00Z",
                            "source": "gateway",
                            "stat_events": 4,
                            "avg_total_requests": 12.0,
                            "total_rate_limited": 1,
                            "avg_latency_ms": 3.5,
                            "max_p99_latency_ms": 9.0
                        }
                    ]
                }),
                "telemetry.stream_stats" => json!({
                    "buckets": [
                        {
                            "bucket": "2026-05-19T12:00:00Z",
                            "stream_name": "EVENTS",
                            "avg_fill_pct": 12.0,
                            "max_fill_pct": 20.0,
                            "max_message_fill_pct": 15.0,
                            "max_byte_fill_pct": 90.0,
                            "max_pressure_level": "warning",
                            "limiting_dimension": "bytes",
                            "avg_messages": 128.0,
                            "max_messages": 256,
                            "sample_count": 2
                        }
                    ]
                }),
                "telemetry.assembly_stats" => json!({
                    "buckets": [
                        {
                            "bucket": "2026-05-19T12:00:00Z",
                            "max_active_assemblies": 2,
                            "total_completed": 7,
                            "total_cancelled": 0,
                            "total_failed": 1,
                            "total_timed_out": 0,
                            "avg_duration_ms": 22.0,
                            "sample_count": 3
                        }
                    ]
                }),
                "telemetry.source_stats" => json!({
                    "buckets": [
                        {
                            "bucket": "2026-05-19T12:00:00Z",
                            "module_kind": "source",
                            "total_events_processed": 42,
                            "total_events_dropped": 0,
                            "avg_latency_ms": 5.5,
                            "max_queue_depth": 1,
                            "total_errors": 0,
                            "sample_count": 4
                        }
                    ]
                }),
                "telemetry.metric_counters" => json!({
                    "buckets": [
                        {
                            "bucket": "2026-05-19T12:00:00Z",
                            "component": "event_engine",
                            "metric_name": "events",
                            "total_value": 99,
                            "max_value": 20,
                            "sample_count": 5
                        }
                    ]
                }),
                "llm.prompts.list" => json!({
                    "type": "events",
                    "events": [
                        {
                            "id": fixture_event_id(),
                            "source": "llm",
                            "event_type": "llm.prompt_template.registered",
                            "payload": {
                                "prompt_id": "extract-entities",
                                "version": "2026-05-19",
                                "body_storage_ref": "prompt body secret_fixture_value should not leak"
                            },
                            "ts_orig": "2026-05-19T12:00:00Z",
                            "host": "test-host",
                            "payload_schema_id": null,
                            "source_material_id": fixture_material_id(),
                            "anchor_byte": 0,
                            "offset_start": 0,
                            "offset_end": 12,
                            "offset_kind": "byte",
                            "associated_blob_ids": null
                        }
                    ],
                    "next_cursor": null,
                    "total_estimate": null
                }),
                "llm.route.explain" => json!({
                    "decision": {
                        "routing_decision_id": "018f4b6b-6a4d-7c80-8000-000000000010",
                        "policy_id": "entity-policy",
                        "task_kind": "entity-extraction",
                        "prompt_id": "extract-entities",
                        "prompt_version": "2026-05-19",
                        "provider": "fixture-provider",
                        "model": "fixture-model",
                        "experiment_id": null,
                        "bucket_key": "fixture",
                        "decision_reason": "fixture route"
                    }
                }),
                "llm.budget.report" => json!({
                    "rows": [
                        {
                            "budget_ledger_id": "018f4b6b-6a4d-7c80-8000-000000000011",
                            "routing_decision_id": "018f4b6b-6a4d-7c80-8000-000000000010",
                            "caller": "fixture",
                            "task_kind": "entity-extraction",
                            "provider": "fixture-provider",
                            "model": "fixture-model",
                            "prompt_tokens": 12,
                            "completion_tokens": 8,
                            "cost_estimate_microusd": 100,
                            "runtime_ms": 25,
                            "status": "success",
                            "failure_class": null,
                            "recorded_at": "2026-05-19T12:00:00Z"
                        }
                    ],
                    "total_rows": 1,
                    "success_count": 1,
                    "failure_count": 0,
                    "rejected_count": 0,
                    "prompt_tokens": 12,
                    "completion_tokens": 8,
                    "cost_estimate_microusd": 100,
                    "runtime_ms": 25
                }),
                "curation.proposals.list" => json!({
                    "type": "events",
                    "events": [
                        {
                            "id": fixture_event_id(),
                            "source": "curation",
                            "event_type": "curation.proposal",
                            "payload": {
                                "proposal_kind": "entity_relation",
                                "status": "pending",
                                "candidate": "fixture relation secret_fixture_value should not leak"
                            },
                            "ts_orig": "2026-05-19T12:00:00Z",
                            "host": "test-host",
                            "payload_schema_id": null,
                            "source_material_id": fixture_material_id(),
                            "anchor_byte": 0,
                            "offset_start": 0,
                            "offset_end": 12,
                            "offset_kind": "byte",
                            "associated_blob_ids": null
                        }
                    ],
                    "next_cursor": null,
                    "total_estimate": null
                }),
                "dlq.list" => json!({
                    "total_messages": 2,
                    "total_bytes": 512,
                    "first_seq": 10,
                    "last_seq": 11
                }),
                "dlq.peek" => json!({
                    "messages": [
                        {
                            "subject": "sinex.events.dlq",
                            "sequence": 10,
                            "retry_count": 3,
                            "original_subject": "sinex.events.raw.fixture",
                            "payload_preview": "[REDACTED]",
                            "payload_redacted": true,
                            "privacy_caveats": [
                                {
                                    "id": "policy.disclosure_applied",
                                    "message": "operator privacy policy changed content for dlq disclosure; raw stored data was not silently removed",
                                    "ref": {
                                        "kind": "policy",
                                        "id": "secret_redacted",
                                        "label": "privacy policy",
                                        "command_hint": "sinexctl privacy policy list",
                                        "rpc_method": "privacy.policy.list"
                                    }
                                }
                            ]
                        }
                    ]
                }),
                "sources.list" => json!({
                    "materials": [
                        {
                            "id": fixture_material_id(),
                            "material_kind": "local_cas",
                            "source_identifier": "/realm/data/captures/fixture.jsonl",
                            "status": "completed",
                            "timing_info_type": "realtime",
                            "format": "jsonl",
                            "contract_version": 1,
                            "staged_at": "2026-05-19T12:00:00Z",
                            "staged_by": "fixture",
                            "size_bytes": 1024,
                            "mime_type": "application/jsonl"
                        }
                    ]
                }),
                "sources.show" => json!({
                    "material": {
                        "id": fixture_material_id(),
                        "material_kind": "local_cas",
                        "source_identifier": "/realm/data/captures/fixture.jsonl",
                        "status": "completed",
                        "timing_info_type": "realtime",
                        "metadata": {
                            "secret_note": "secret_fixture_value should not leak"
                        },
                        "contract": null,
                        "temporal_evidence": {
                            "ledger_entries": 1,
                            "source_types": ["fixture"]
                        },
                        "staged_at": "2026-05-19T12:00:00Z",
                        "start_time": "2026-05-19T12:00:00Z",
                        "end_time": "2026-05-19T12:01:00Z",
                        "staged_by": "fixture",
                        "staged_on_host": "test-host",
                        "optional_blob_id": null,
                        "total_bytes": 1024,
                        "event_count": 42
                    }
                }),
                "sources.coverage" => json!({
                    "sources": [
                        {
                            "source_identifier": "/realm/data/captures/fixture.jsonl",
                            "material_kind": "local_cas",
                            "earliest_ts": "2026-05-19T12:00:00Z",
                            "latest_ts": "2026-05-19T12:30:00Z",
                            "event_count": 42,
                            "material_count": 3,
                            "completed_material_count": 2,
                            "failed_material_count": 0,
                            "recovered_partial_material_count": 1,
                            "sensing_material_count": 0,
                            "cancelled_material_count": 0,
                            "total_bytes": 1024
                        }
                    ]
                }),
                "sources.status.view" => fixture_source_status_view_envelope(),
                "sources.presets.list" => json!({
                    "presets": [
                        {
                            "name": "terminal.atuin.default",
                            "description": "Fixture Atuin history preset",
                            "source_family": "terminal",
                            "input_shape_kind": "sqlite",
                            "material_format_hint": "sqlite",
                            "resolver_preset": "atuin-history"
                        }
                    ]
                }),
                "sources.bindings.list" => json!({
                    "bindings": [
                        {
                            "id": "terminal-atuin-history",
                            "name": "Atuin history",
                            "source_family": "terminal",
                            "binding_mode": "nix",
                            "input_shape_kind": "sqlite",
                            "enabled": true,
                            "status": "configured",
                            "last_error": null,
                            "created_at": "2026-05-19T12:00:00Z"
                        }
                    ]
                }),
                "ops.list" => json!({
                    "operations": [
                        fixture_operation()
                    ]
                }),
                "ops.get" => json!({
                    "operation": fixture_operation()
                }),
                "lifecycle.status" => json!({
                    "tiers": [
                        {
                            "tier": "live",
                            "event_count": 42,
                            "oldest_ts": "2026-05-19T12:00:00Z",
                            "newest_ts": "2026-05-19T12:30:00Z",
                            "distinct_sources": 7
                        }
                    ],
                    "total_events": 42
                }),
                "audit.get" => json!({
                    "audit_trail": {
                        "operation": fixture_operation(),
                        "affected_events": [
                            {
                                "id": fixture_event_id(),
                                "source": "fixture",
                                "event_type": "fixture.event",
                                "ts_orig": "2026-05-19T12:00:00Z",
                                "ts_coided": "2026-05-19T12:00:01Z",
                                "tier": "live",
                                "provenance_operation_id": fixture_operation_id()
                            }
                        ]
                    },
                    "event_count": 1,
                    "next_cursor": null,
                    "has_more": false
                }),
                "coordination.list_instances" => json!({
                    "instances": [
                        fixture_instance(true)
                    ]
                }),
                "coordination.get_leader" => json!({
                    "leader": fixture_instance(true)
                }),
                "coordination.instance_health" => json!({
                    "instance": fixture_instance(true),
                    "healthy": true,
                    "last_error": null
                }),
                "shadow.list" => json!({
                    "consumers": [
                        {
                            "consumer_name": "dev-fixture",
                            "stream_name": "SINEX_RAW_EVENTS",
                            "subject_filter": "sinex.events.raw.fixture",
                            "num_pending": 2,
                            "first_sequence": 10
                        }
                    ]
                }),
                "system.ping" => json!("pong"),
                "system.version" => json!("0.4.2"),
                other => {
                    return ResponseTemplate::new(400).set_body_json(json!({
                        "jsonrpc": "2.0",
                        "error": {
                            "code": -32601,
                            "message": format!("unexpected fixture RPC method: {other}")
                        },
                        "id": body["id"]
                    }));
                }
            };

            ResponseTemplate::new(200).set_body_json(json!({
                "jsonrpc": "2.0",
                "result": result,
                "id": body["id"]
            }))
        })
        .mount(&server)
        .await;
    server
}

fn fixture_gateway_client(server: &MockServer) -> color_eyre::Result<GatewayClient> {
    GatewayClient::new(ClientConfig {
        url: server.uri(),
        token: Some("test-token".to_string()),
        insecure: true,
        ..Default::default()
    })
}

fn unreachable_gateway_client() -> color_eyre::Result<GatewayClient> {
    GatewayClient::new(ClientConfig {
        url: "http://127.0.0.1:9".to_string(),
        token: Some("test-token".to_string()),
        insecure: true,
        timeout: 1,
        retry_config: RetryConfig::builder()
            .max_attempts(1)
            .initial_delay(StdDuration::from_millis(1))
            .max_delay(StdDuration::from_millis(1))
            .build(),
        ..Default::default()
    })
}

fn fixture_event_id() -> &'static str {
    "018f4b6b-6a4d-7c80-8000-000000000001"
}

fn fixture_material_id() -> &'static str {
    "018f4b6b-6a4d-7c80-8000-000000000002"
}

fn fixture_task_id() -> &'static str {
    "018f4b6b-6a4d-7c80-8000-000000000003"
}

fn fixture_operation_id() -> &'static str {
    "018f4b6b-6a4d-7c80-8000-000000000004"
}

fn fixture_document_id() -> &'static str {
    "018f4b6b-6a4d-7c80-8000-000000000005"
}

fn fixture_semantic_epoch_id() -> &'static str {
    "018f4b6b-6a4d-7c80-8000-000000000006"
}

fn fixture_semantic_lane_id() -> &'static str {
    "018f4b6b-6a4d-7c80-8000-000000000007"
}

fn fixture_semantic_candidate_lane_id() -> &'static str {
    "018f4b6b-6a4d-7c80-8000-000000000008"
}

fn fixture_semantic_diff_id() -> &'static str {
    "018f4b6b-6a4d-7c80-8000-000000000009"
}

fn fixture_relation_evidence_envelope() -> Value {
    json!({
        "schema_version": "sinex.view-envelope/v3",
        "view_id": "018f4b6b-6a4d-7c80-8000-000000000010",
        "generated_at": "2026-05-19T12:00:00Z",
        "source_surface": "events.relation_evidence",
        "freshness": {
            "generated_at": "2026-05-19T12:00:00Z",
            "stale_after_secs": null
        },
        "filters": null,
        "caveats": [],
        "payload": {
            "seed_refs": [
                {
                    "kind": "event",
                    "id": fixture_event_id(),
                    "label": "fixture seed"
                }
            ],
            "support_refs": [
                {
                    "object": {
                        "kind": "event",
                        "id": fixture_event_id(),
                        "label": "fixture support"
                    },
                    "role": "support",
                    "observed_range": {
                        "start": "2026-05-19T12:00:00Z",
                        "end": "2026-05-19T12:00:00Z",
                        "basis": "source_intrinsic",
                        "quality": "exact"
                    },
                    "rationale": "fixture relation evidence"
                }
            ],
            "contradiction_refs": [],
            "caveats": [],
            "observed_range": {
                "start": "2026-05-19T12:00:00Z",
                "end": "2026-05-19T12:00:00Z",
                "basis": "source_intrinsic",
                "quality": "exact"
            },
            "expansion_trace": {
                "steps": []
            },
            "generated_at": "2026-05-19T12:00:00Z",
            "query": {
                "relation": "within",
                "within_secs": 300
            }
        }
    })
}

fn fixture_source_status_view_envelope() -> Value {
    json!({
        "schema_version": "sinex.view-envelope/v3",
        "view_id": "018f4b6b-6a4d-7c80-8000-000000000011",
        "generated_at": "2026-05-19T12:00:00Z",
        "source_surface": "sources.status",
        "freshness": {
            "generated_at": "2026-05-19T12:00:00Z",
            "stale_after_secs": null
        },
        "filters": null,
        "caveats": [],
        "payload": {
            "schema_version": "sinex.source-coverage-list/v1",
            "count": 1,
            "sources": [
                {
                    "source_id": "terminal.atuin-history",
                    "namespace": "terminal",
                    "event_types": ["command.executed"],
                    "readiness": "ready",
                    "continuity": "active",
                    "last_material_at": "2026-05-19T11:59:00Z",
                    "last_event_at": "2026-05-19T12:00:00Z",
                    "material_count": 3,
                    "event_count": 42,
                    "binding_count": 1,
                    "live_binding_count": 1,
                    "proposed_binding_count": 0,
                    "gaps": [],
                    "caveats": [],
                    "privacy": {
                        "tier": "sensitive",
                        "context": "command"
                    },
                    "actions": []
                }
            ]
        }
    })
}

fn fixture_instance_id() -> &'static str {
    "gateway-fixture"
}

fn fixture_instance(is_leader: bool) -> Value {
    json!({
        "instance_id": fixture_instance_id(),
        "module_kind": "service",
        "hostname": "test-host",
        "last_heartbeat": "2026-05-19T12:00:00Z",
        "is_leader": is_leader
    })
}

fn fixture_operation() -> Value {
    json!({
        "id": fixture_operation_id(),
        "operation_type": "replay",
        "operator": "fixture",
        "scope": {
            "source_name": "terminal.atuin-history"
        },
        "result_status": "running",
        "result_message": null,
        "preview_summary": {
            "total_events": 12
        },
        "duration_ms": 42
    })
}

fn fixture_replay_operation(state: &str) -> Value {
    json!({
        "operation_id": fixture_operation_id(),
        "state": state,
        "scope": {
            "source_name": "terminal.atuin-history",
            "time_window": null,
            "material_filter": null,
            "filters": {}
        },
        "preview_summary": {
            "total_events": 12,
            "time_window": {
                "start": "2026-05-19T10:00:00Z",
                "end": "2026-05-19T12:00:00Z"
            }
        },
        "checkpoint": {
            "processed_events": 3,
            "total_events": 12,
            "last_event_id": null,
            "batch_number": 1,
            "savepoint_id": null,
            "updated_at": "2026-05-19T11:30:00Z"
        },
        "actor": "operator",
        "created_at": "2026-05-19T10:00:00Z",
        "approved_by": null,
        "approved_at": null,
        "executor_module": null,
        "started_at": null,
        "finished_at": null,
        "outcome": null,
        "error_details": null
    })
}

fn fixture_semantic_lane(status: &str) -> Value {
    json!({
        "id": fixture_semantic_lane_id(),
        "name": "fixture-lane",
        "kind": "shadow",
        "base_epoch_id": null,
        "candidate_epoch_id": fixture_semantic_epoch_id(),
        "scope": {
            "kind": "event_set",
            "input_ids": ["event:fixture"],
            "input_set_hash": "fixture-input-hash"
        },
        "status": status,
        "purpose": "MCP fixture",
        "operation_id": null,
        "created_at": "2026-05-19T11:35:00Z",
        "completed_at": null,
        "expires_at": null
    })
}

fn fixture_continuity_report() -> Value {
    json!({
        "source_family": "terminal",
        "coverage_contract": "continuous",
        "is_declared": true,
        "replayability": {
            "raw_bytes_preserved": true,
            "timing_quality": true,
            "anchor_stability": true,
            "parser_determinism": true,
            "privacy_safe_replay": true,
            "weak_points": []
        },
        "seams": [],
        "gaps": [
            {
                "from_ts": "2026-05-19T10:00:00Z",
                "to_ts": "2026-05-19T10:30:00Z",
                "kind": "private_mode",
                "attribution": "fixture private mode"
            }
        ],
        "earliest_ts": "2026-05-19T09:00:00Z",
        "latest_ts": "2026-05-19T12:00:00Z",
        "material_count": 3,
        "event_count": 42
    })
}

fn fixture_event(event_id: &str) -> Value {
    json!({
        "id": event_id,
        "source": "fixture",
        "event_type": "fixture.event",
        "payload": { "summary": "raw lineage secret_fixture_value should not leak" },
        "ts_orig": "2026-05-18T12:00:00Z",
        "host": "test-host",
        "payload_schema_id": null,
        "source_material_id": fixture_material_id(),
        "anchor_byte": 0,
        "offset_start": 0,
        "offset_end": 12,
        "offset_kind": "byte",
        "associated_blob_ids": null
    })
}

fn fixture_task_state() -> Value {
    json!({
        "task_id": fixture_task_id(),
        "status": "started",
        "title": "Expose MCP task list",
        "body": "Fixture operator task",
        "project_id": "sinex",
        "tags": ["mcp"],
        "due_at": "2026-05-19T18:00:00Z",
        "priority": "high",
        "external_refs": [
            {
                "system": "github",
                "external_id": "1105",
                "version": null
            }
        ],
        "last_event_id": "018f4b6b-6a4d-7c80-8000-000000000004",
        "state_hash": "fixture-task-state",
        "updated_at": "2026-05-19T12:00:00Z"
    })
}

fn fixture_sensitive_query_event() -> Value {
    let mut event = fixture_event(fixture_event_id());
    let Some(fields) = event.as_object_mut() else {
        return event;
    };
    fields.insert(
        "snippet".to_string(),
        json!("search snippet secret_fixture_value should not leak"),
    );
    event
}

fn fixture_event_card_list() -> Value {
    json!({
        "schema_version": "event_card_list.v1",
        "count": 1,
        "cards": [
            {
                "ref": {
                    "kind": "event",
                    "id": fixture_event_id()
                },
                "timestamp": {
                    "original": "2026-05-18T12:00:00Z",
                    "ingested": "2026-05-18T12:00:00Z",
                    "quality": "original_timestamp"
                },
                "source": {
                    "family": "fixture",
                    "raw": "fixture",
                    "source_ref": {
                        "kind": "source_driver",
                        "id": "fixture",
                        "label": "fixture"
                    }
                },
                "event_type": "fixture.event",
                "origin_kind": "material",
                "summary": "disclosed fixture event",
                "payload_preview": {
                    "reason": "server_disclosed"
                },
                "material_refs": [],
                "privacy_state": {
                    "state": "redacted",
                    "reason": "fixture disclosure policy"
                },
                "caveats": [
                    {
                        "id": "event.payload.disclosed",
                        "message": "fixture payload was disclosed by the event card endpoint"
                    }
                ],
                "trace_refs": [],
                "trace_links": [],
                "projection_badges": [],
                "actions": []
            }
        ],
        "next_cursor": null,
        "total_estimate": 1
    })
}
