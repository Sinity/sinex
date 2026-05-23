use serde_json::{Value, json};
use sinex_primitives::rpc::{RpcMutability, RpcRole, method_catalog};
use sinex_primitives::temporal::{Duration, Timestamp};
use sinexctl::client::{ClientConfig, GatewayClient};
use sinexctl::mcp::{
    MCP_PROTOCOL_VERSION, McpSurfaceKind, assert_read_only_tool_names, call_tool, tool_catalog,
    tools,
};
use sinexctl::validation::{parse_time_input, parse_time_input_with_now, validate_time_range};
use std::collections::BTreeSet;
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
async fn mcp_lists_first_slice_read_only_tools() -> TestResult<()> {
    let tools = tools();
    let names = tools.iter().map(|tool| tool.name).collect::<Vec<_>>();

    assert_eq!(
        names,
        vec![
            "sinex.search_events",
            "sinex.trace_lineage",
            "sinex.source_readiness",
            "sinex.source_continuity",
            "sinex.privacy_status",
            "sinex.system_health",
            "sinex.tasks_list",
            "sinex.task_state",
            "sinex.replay_operations",
            "sinex.replay_status",
            "sinex.documents_search",
            "sinex.documents_get",
            "sinex.semantic_epochs",
            "sinex.semantic_lanes",
            "sinex.semantic_lane_outputs",
            "sinex.semantic_lane_diffs",
            "sinex.automata_status",
            "sinex.ingestors_status",
            "sinex.nodes_health",
            "sinex.nodes_active",
            "sinex.ingestd_validation",
            "sinex.ingestd_batch_stats"
        ]
    );
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
            !entry.backing_rpc_methods.is_empty(),
            "MCP entry `{}` must declare backing RPC descriptors",
            entry.name
        );
    }
    Ok(())
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
    assert_eq!(MCP_PROTOCOL_VERSION, "2024-11-05");
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
async fn mcp_search_events_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(
        &client,
        "sinex.search_events",
        json!({ "sources": ["fixture"], "limit": 1 }),
    )
    .await?;

    assert_eq!(response["tool"], "sinex.search_events");
    assert_eq!(response["items"]["result"]["type"], "events");
    assert_eq!(
        response["items"]["result"]["events"][0]["payload"]["reason"],
        "mcp_raw_samples_disabled"
    );
    assert_eq!(
        response["items"]["result"]["events"][0]["snippet"],
        "[REDACTED]"
    );
    assert_eq!(response["redaction"]["raw_samples"], false);
    assert!(
        !response.to_string().contains("ghp_fixture_secret"),
        "MCP event search leaked raw payload or snippet text"
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
        "sinex.trace_lineage",
        json!({ "event_id": event_id, "direction": "ancestors" }),
    )
    .await?;

    assert_eq!(response["tool"], "sinex.trace_lineage");
    assert_eq!(response["items"]["result"]["root"]["id"], event_id);
    assert_eq!(
        response["items"]["result"]["root"]["payload"]["reason"],
        "mcp_raw_samples_disabled"
    );
    assert_eq!(response["items"]["result"]["ancestors"], json!([]));
    assert_eq!(
        response["items"]["result"]["material_links"][0]["metadata"]["reason"],
        "mcp_raw_samples_disabled"
    );
    assert_eq!(response["provenance_refs"], json!([]));
    assert!(
        !response.to_string().contains("ghp_fixture_secret"),
        "MCP lineage leaked raw payload or material metadata text"
    );
    Ok(())
}

#[sinex_test]
async fn mcp_source_readiness_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(
        &client,
        "sinex.source_readiness",
        json!({
            "source_family": "terminal",
            "source_unit_id": "terminal.atuin-history",
            "include_caveats": false
        }),
    )
    .await?;

    assert_eq!(response["tool"], "sinex.source_readiness");
    let Some(sources) = response["items"]["result"]["sources"].as_array() else {
        return Err(color_eyre::eyre::eyre!(
            "source readiness response did not contain a sources array"
        ));
    };
    assert_eq!(sources.len(), 1);
    assert_eq!(sources[0]["source_unit_id"], "terminal.atuin-history");
    assert_eq!(sources[0]["evidence"]["sample"], "[REDACTED]");
    assert_eq!(response["items"]["caveats"], "suppressed_by_request");
    assert!(
        !response.to_string().contains("ghp_fixture_secret"),
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
        "sinex.source_continuity",
        json!({ "since": "2026-05-19T00:00:00Z" }),
    )
    .await?;

    assert_eq!(response["tool"], "sinex.source_continuity");
    assert_eq!(response["query"]["since"], "2026-05-19T00:00:00Z");
    assert_eq!(
        response["items"]["result"]["reports"][0]["source_family"],
        "terminal"
    );
    assert_eq!(
        response["items"]["result"]["reports"][0]["gaps"][0]["kind"],
        "private_mode"
    );
    assert_eq!(response["redaction"]["raw_samples"], false);
    Ok(())
}

#[sinex_test]
async fn mcp_source_continuity_get_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(
        &client,
        "sinex.source_continuity",
        json!({ "source_family": "terminal" }),
    )
    .await?;

    assert_eq!(response["tool"], "sinex.source_continuity");
    assert_eq!(response["query"]["source_family"], "terminal");
    assert_eq!(
        response["items"]["result"]["report"]["source_family"],
        "terminal"
    );
    assert_eq!(
        response["items"]["result"]["report"]["replayability"]["raw_bytes_preserved"],
        true
    );
    assert_eq!(response["redaction"]["raw_samples"], false);
    Ok(())
}

#[sinex_test]
async fn mcp_privacy_status_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(&client, "sinex.privacy_status", json!({})).await?;

    assert_eq!(response["tool"], "sinex.privacy_status");
    assert_eq!(response["items"]["result"]["state"]["enabled"], true);
    assert_eq!(
        response["items"]["result"]["state"]["affected_source_classes"],
        json!(["terminal"])
    );
    assert_eq!(response["redaction"]["raw_samples"], false);
    assert!(
        !response.to_string().contains("ghp_fixture_secret"),
        "MCP privacy status leaked raw sensitive sample text"
    );
    Ok(())
}

#[sinex_test]
async fn mcp_system_health_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(&client, "sinex.system_health", json!({})).await?;

    assert_eq!(response["tool"], "sinex.system_health");
    assert_eq!(response["items"]["result"]["status"], "degraded");
    assert_eq!(response["items"]["result"]["healthy"], false);
    assert_eq!(
        response["items"]["result"]["components"]["sse_confirmation"]["status"],
        "degraded"
    );
    assert_eq!(response["redaction"]["raw_samples"], false);
    Ok(())
}

#[sinex_test]
async fn mcp_tasks_list_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(
        &client,
        "sinex.tasks_list",
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

    assert_eq!(response["tool"], "sinex.tasks_list");
    assert_eq!(response["query"]["status"], "started");
    assert_eq!(response["items"]["result"]["total"], 1);
    assert_eq!(
        response["items"]["result"]["tasks"][0]["title"],
        "Expose MCP task list"
    );
    assert_eq!(
        response["items"]["result"]["tasks"][0]["project_id"],
        "sinex"
    );
    assert_eq!(
        response["items"]["result"]["tasks"][0]["tags"],
        json!(["mcp"])
    );
    assert_eq!(response["redaction"]["raw_samples"], false);
    Ok(())
}

#[sinex_test]
async fn mcp_task_state_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;
    let task_id = fixture_task_id();

    let response = call_tool(&client, "sinex.task_state", json!({ "task_id": task_id })).await?;

    assert_eq!(response["tool"], "sinex.task_state");
    assert_eq!(response["query"]["task_id"], task_id);
    assert_eq!(response["items"]["result"]["task_id"], task_id);
    assert_eq!(response["items"]["result"]["event_count"], 3);
    assert_eq!(
        response["items"]["result"]["state"]["title"],
        "Expose MCP task list"
    );
    assert_eq!(response["redaction"]["raw_samples"], false);
    Ok(())
}

#[sinex_test]
async fn mcp_replay_operations_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(
        &client,
        "sinex.replay_operations",
        json!({
            "state": "Planning",
            "node": "terminal.atuin-history",
            "limit": 5
        }),
    )
    .await?;

    assert_eq!(response["tool"], "sinex.replay_operations");
    assert_eq!(response["query"]["state"], "Planning");
    assert_eq!(
        response["items"]["operations"][0]["operation_id"],
        fixture_operation_id()
    );
    assert_eq!(response["items"]["operations"][0]["state"], "Planning");
    assert_eq!(response["redaction"]["raw_samples"], false);
    Ok(())
}

#[sinex_test]
async fn mcp_replay_status_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(
        &client,
        "sinex.replay_status",
        json!({ "operation_id": fixture_operation_id() }),
    )
    .await?;

    assert_eq!(response["tool"], "sinex.replay_status");
    assert_eq!(
        response["items"]["operation"]["operation_id"],
        fixture_operation_id()
    );
    assert_eq!(response["items"]["operation"]["state"], "Previewed");
    assert_eq!(response["redaction"]["raw_samples"], false);
    Ok(())
}

#[sinex_test]
async fn mcp_documents_search_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(
        &client,
        "sinex.documents_search",
        json!({
            "query": "secret plan",
            "kind": "markdown",
            "natural_key_prefix": "notes/",
            "limit": 5
        }),
    )
    .await?;

    assert_eq!(response["tool"], "sinex.documents_search");
    assert_eq!(response["query"]["query"], "secret plan");
    assert_eq!(response["items"]["result"]["search_mode"], "fts");
    assert_eq!(
        response["items"]["result"]["results"][0]["document_id"],
        fixture_document_id()
    );
    assert_eq!(
        response["items"]["result"]["results"][0]["text"]["reason"],
        "mcp_document_text_disabled"
    );
    assert_eq!(
        response["items"]["result"]["results"][0]["headline"]["reason"],
        "mcp_document_text_disabled"
    );
    assert_eq!(
        response["items"]["result"]["results"][0]["side_data"]["reason"],
        "mcp_document_side_data_disabled"
    );
    assert!(
        !response.to_string().contains("ghp_fixture_secret"),
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
        "sinex.documents_get",
        json!({ "document_id": fixture_document_id() }),
    )
    .await?;

    assert_eq!(response["tool"], "sinex.documents_get");
    assert_eq!(response["query"]["document_id"], fixture_document_id());
    assert_eq!(response["items"]["result"]["id"], fixture_document_id());
    assert_eq!(
        response["items"]["result"]["side_data"]["reason"],
        "mcp_document_side_data_disabled"
    );
    assert!(
        !response.to_string().contains("ghp_fixture_secret"),
        "MCP document get leaked raw document side data"
    );
    Ok(())
}

#[sinex_test]
async fn mcp_semantic_epochs_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(&client, "sinex.semantic_epochs", json!({ "limit": 5 })).await?;

    assert_eq!(response["tool"], "sinex.semantic_epochs");
    assert_eq!(response["query"]["limit"], 5);
    assert_eq!(
        response["items"]["result"]["epochs"][0]["id"],
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
        "sinex.semantic_lanes",
        json!({ "status": "planned", "limit": 5 }),
    )
    .await?;

    assert_eq!(response["tool"], "sinex.semantic_lanes");
    assert_eq!(response["query"]["status"], "planned");
    assert_eq!(
        response["items"]["result"]["lanes"][0]["id"],
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
        "sinex.semantic_lane_outputs",
        json!({ "lane_id": fixture_semantic_lane_id(), "limit": 5 }),
    )
    .await?;

    assert_eq!(response["tool"], "sinex.semantic_lane_outputs");
    assert_eq!(response["query"]["lane_id"], fixture_semantic_lane_id());
    assert_eq!(
        response["items"]["result"]["outputs"][0]["output_key"],
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
        "sinex.semantic_lane_diffs",
        json!({ "lane_id": fixture_semantic_lane_id(), "limit": 5 }),
    )
    .await?;

    assert_eq!(response["tool"], "sinex.semantic_lane_diffs");
    assert_eq!(response["query"]["lane_id"], fixture_semantic_lane_id());
    assert_eq!(
        response["items"]["result"]["diffs"][0]["id"],
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
        "sinex.automata_status",
        json!({ "stale_after_secs": 120, "recent_window_secs": 60 }),
    )
    .await?;

    assert_eq!(response["tool"], "sinex.automata_status");
    assert_eq!(response["query"]["stale_after_secs"], 120);
    assert_eq!(response["items"]["result"]["stale_after_secs"], 120);
    assert_eq!(
        response["items"]["result"]["automata"][0]["node_name"],
        "session-detector"
    );
    assert_eq!(
        response["items"]["result"]["automata"][0]["event_lag_p99_ms"],
        42.0
    );
    assert_eq!(response["redaction"]["raw_samples"], false);
    Ok(())
}

#[sinex_test]
async fn mcp_ingestors_status_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(
        &client,
        "sinex.ingestors_status",
        json!({ "stale_after_secs": 120, "recent_window_secs": 60 }),
    )
    .await?;

    assert_eq!(response["tool"], "sinex.ingestors_status");
    assert_eq!(response["query"]["recent_window_secs"], 60);
    assert_eq!(response["items"]["result"]["recent_window_secs"], 60);
    assert_eq!(
        response["items"]["result"]["ingestors"][0]["node_name"],
        "terminal.atuin-history"
    );
    assert_eq!(
        response["items"]["result"]["ingestors"][0]["current_health"],
        "healthy"
    );
    assert_eq!(response["redaction"]["raw_samples"], false);
    Ok(())
}

#[sinex_test]
async fn mcp_nodes_health_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(
        &client,
        "sinex.nodes_health",
        json!({ "stale_after_secs": 120 }),
    )
    .await?;

    assert_eq!(response["tool"], "sinex.nodes_health");
    assert_eq!(response["query"]["stale_after_secs"], 120);
    assert_eq!(response["items"]["result"]["active_count"], 2);
    assert_eq!(response["items"]["result"]["inactive_count"], 1);
    assert_eq!(response["items"]["result"]["unique_nodes"], 3);
    assert_eq!(response["redaction"]["raw_samples"], false);
    Ok(())
}

#[sinex_test]
async fn mcp_nodes_active_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(
        &client,
        "sinex.nodes_active",
        json!({ "stale_after_secs": 120 }),
    )
    .await?;

    assert_eq!(response["tool"], "sinex.nodes_active");
    assert_eq!(response["query"]["stale_after_secs"], 120);
    assert_eq!(
        response["items"]["result"]["nodes"][0]["node_name"],
        "terminal.atuin-history"
    );
    assert_eq!(
        response["items"]["result"]["nodes"][0]["heartbeat_source"],
        "run"
    );
    assert_eq!(response["redaction"]["raw_samples"], false);
    Ok(())
}

#[sinex_test]
async fn mcp_ingestd_validation_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(&client, "sinex.ingestd_validation", json!({})).await?;

    assert_eq!(response["tool"], "sinex.ingestd_validation");
    assert_eq!(response["items"]["snapshot"]["batch_size"], 12);
    assert_eq!(response["items"]["snapshot"]["validation_invalid"], 0);
    assert_eq!(
        response["items"]["snapshot"]["validation_coverage_pct"],
        100.0
    );
    assert_eq!(response["redaction"]["raw_samples"], false);
    Ok(())
}

#[sinex_test]
async fn mcp_ingestd_batch_stats_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(
        &client,
        "sinex.ingestd_batch_stats",
        json!({
            "from": "2026-05-19T00:00:00Z",
            "to": "2026-05-19T01:00:00Z",
            "limit": 5
        }),
    )
    .await?;

    assert_eq!(response["tool"], "sinex.ingestd_batch_stats");
    assert_eq!(response["query"]["limit"], 5);
    assert_eq!(response["items"]["buckets"][0]["batch_count"], 3);
    assert_eq!(response["items"]["buckets"][0]["validation_invalid"], 0);
    assert_eq!(
        response["items"]["buckets"][0]["avg_validation_coverage_pct"],
        100.0
    );
    assert_eq!(response["redaction"]["raw_samples"], false);
    Ok(())
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
                                    "raw_sample": "lineage ghp_fixture_secret should not leak"
                                },
                                "created_at": "2026-05-18T12:00:00Z"
                            }
                        ]
                    })
                }
                "sources.readiness.list" => json!({
                    "sources": [
                        {
                            "source_family": "terminal",
                            "source_unit_id": "terminal.atuin-history",
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
                            "source_unit_id": "terminal.text-history",
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
                        "source_unit_id": "terminal.atuin-history",
                        "source_identifier": "atuin-history",
                        "status": "available",
                        "cost": "local_fast",
                        "material_count": 3
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
                    "degradation_reasons": ["confirmation fan-out degraded"],
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
                            "headline": "<mark>secret</mark> ghp_fixture_secret",
                            "text": "full document ghp_fixture_secret should not leak",
                            "score": 0.875,
                            "byte_offset_start": 0,
                            "byte_offset_end": 48,
                            "extraction_version": 1,
                            "side_data": {
                                "sample": "side ghp_fixture_secret should not leak"
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
                        "sample": "document side ghp_fixture_secret should not leak"
                    },
                    "created_at": "2026-05-19T11:00:00Z",
                    "updated_at": "2026-05-19T11:45:00Z"
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
                            "node_name": "session-detector",
                            "version": "0.4.2",
                            "description": "fixture automaton",
                            "manifest_status": "registered",
                            "live": true,
                            "service_name": "sinex-process-session-detector.service",
                            "instance_id": "session-detector-1",
                            "source_run_id": null,
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
                "ingestors.status" => json!({
                    "generated_at": "2026-05-19T12:00:00Z",
                    "stale_after_secs": body["params"]["stale_after_secs"].as_u64().unwrap_or(300),
                    "recent_window_secs": body["params"]["recent_window_secs"].as_u64().unwrap_or(300),
                    "ingestors": [
                        {
                            "node_name": "terminal.atuin-history",
                            "version": "0.4.2",
                            "description": "fixture ingestor",
                            "manifest_status": "registered",
                            "live": true,
                            "service_name": "sinex-source-worker@terminal.atuin-history.service",
                            "instance_id": "terminal-atuin-1",
                            "source_run_id": null,
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
                "nodes.health" => json!({
                    "active_count": 2,
                    "inactive_count": 1,
                    "unique_nodes": 3,
                    "active_run_count": 2,
                    "oldest_heartbeat": "2026-05-19T11:50:00Z"
                }),
                "nodes.list_active" => json!({
                    "nodes": [
                        {
                            "node_name": "terminal.atuin-history",
                            "node_type": "ingestor",
                            "version": "0.4.2",
                            "description": "fixture source worker",
                            "service_name": "sinex-source-worker@terminal.atuin-history.service",
                            "instance_id": "terminal-atuin-1",
                            "source_run_id": null,
                            "host": "test-host",
                            "status": "healthy",
                            "last_heartbeat_at": "2026-05-19T11:59:59Z",
                            "started_at": "2026-05-19T11:00:00Z",
                            "heartbeat_source": "run"
                        }
                    ]
                }),
                "telemetry.ingestd_validation" => json!({
                    "snapshot": {
                        "observed_at": "2026-05-19T11:59:59Z",
                        "batch_size": 12,
                        "fetch_to_ack_ms": 18,
                        "events_deferred": 0,
                        "events_failed": 0,
                        "had_synthesis": false,
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
                "telemetry.ingestd_batch_stats" => json!({
                    "buckets": [
                        {
                            "bucket": "2026-05-19T00:00:00Z",
                            "avg_batch_size": 12.0,
                            "max_batch_size": 18,
                            "avg_latency_ms": 14.5,
                            "max_latency_ms": 19.0,
                            "total_deferred": 0,
                            "total_failed": 0,
                            "synthesis_batches": 0,
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

fn fixture_replay_operation(state: &str) -> Value {
    json!({
        "operation_id": fixture_operation_id(),
        "state": state,
        "scope": {
            "node_id": "terminal.atuin-history",
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
        "executor_node": null,
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
        "payload": { "summary": "raw lineage ghp_fixture_secret should not leak" },
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
        json!("search snippet ghp_fixture_secret should not leak"),
    );
    event
}
