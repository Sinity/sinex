use serde_json::{Value, json};
use sinex_primitives::rpc::{RpcMutability, RpcRole, method_catalog, methods};
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
            line.strip_prefix("| `sinex.")
                .and_then(|rest| rest.split_once('`'))
                .map(|(suffix, _)| format!("sinex.{suffix}"))
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
            !entry.backing_rpc_methods.is_empty(),
            "MCP entry `{}` must declare backing RPC descriptors",
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

    assert_eq!(response["source_surface"], "sinex.search_events");
    assert_eq!(response["query_echo"]["sources"][0], "fixture");
    assert_eq!(response["payload"]["result"]["type"], "events");
    assert_eq!(
        response["payload"]["result"]["events"][0]["payload"]["reason"],
        "mcp_raw_samples_disabled"
    );
    assert_eq!(
        response["payload"]["result"]["events"][0]["snippet"],
        "[REDACTED]"
    );
    assert_eq!(response["privacy_state"]["state"], "redacted");
    assert_eq!(response["caveats"][0]["id"], "mcp.raw_samples_redacted");
    assert!(
        !response.to_string().contains("secret_fixture_value"),
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

    assert_eq!(response["source_surface"], "sinex.trace_lineage");
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
async fn mcp_source_readiness_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(
        &client,
        "sinex.source_readiness",
        json!({
            "source_family": "terminal",
            "source_id": "terminal.atuin-history",
            "include_caveats": false
        }),
    )
    .await?;

    assert_eq!(response["source_surface"], "sinex.source_readiness");
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
        "sinex.source_continuity",
        json!({ "since": "2026-05-19T00:00:00Z" }),
    )
    .await?;

    assert_eq!(response["source_surface"], "sinex.source_continuity");
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
        "sinex.source_drift",
        json!({
            "source_id": "browser.history",
            "limit": 5
        }),
    )
    .await?;

    assert_eq!(response["source_surface"], "sinex.source_drift");
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
        "sinex.source_continuity",
        json!({ "source_family": "terminal" }),
    )
    .await?;

    assert_eq!(response["source_surface"], "sinex.source_continuity");
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
        "sinex.source_gap_explain",
        json!({
            "source_family": "terminal",
            "at": "2026-05-19T12:05:00Z"
        }),
    )
    .await?;

    assert_eq!(response["source_surface"], "sinex.source_gap_explain");
    assert_eq!(response["query_echo"]["source_family"], "terminal");
    assert_eq!(response["query_echo"]["at"], "2026-05-19T12:05:00Z");
    assert_eq!(response["payload"]["result"]["gap"]["kind"], "private_mode");
    assert!(
        response["payload"]["result"]["explanation"]
            .as_str()
            .unwrap_or_default()
            .contains("coverage gap")
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
        "sinex.source_identifier_continuity",
        json!({
            "source_identifier": "/realm/data/captures/fixture.jsonl",
            "material_kind": "local_cas"
        }),
    )
    .await?;

    assert_eq!(response["source_surface"], "sinex.source_identifier_continuity");
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

    let response = call_tool(&client, "sinex.privacy_status", json!({})).await?;

    assert_eq!(response["tool"], "sinex.privacy_status");
    assert_eq!(response["items"]["result"]["state"]["enabled"], true);
    assert_eq!(
        response["items"]["result"]["state"]["affected_source_classes"],
        json!(["terminal"])
    );
    assert_eq!(response["redaction"]["raw_samples"], false);
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
            "module": "terminal.atuin-history",
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
        "sinex.documents_chunks",
        json!({ "document_id": fixture_document_id(), "limit": 2 }),
    )
    .await?;

    assert_eq!(response["tool"], "sinex.documents_chunks");
    assert_eq!(response["query"]["document_id"], fixture_document_id());
    assert_eq!(
        response["items"]["result"]["chunks"][0]["document_id"],
        fixture_document_id()
    );
    assert_eq!(
        response["items"]["result"]["chunks"][0]["redaction_reason"],
        "mcp_document_chunk_text_redacted"
    );
    assert_eq!(
        response["items"]["result"]["chunks"][0]["text_redacted"],
        true
    );
    assert!(response["items"]["result"]["chunks"][0]["text"].is_null());
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
        response["items"]["result"]["automata"][0]["module_name"],
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
async fn mcp_sources_status_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(
        &client,
        "sinex.sources_status",
        json!({ "stale_after_secs": 120, "recent_window_secs": 60 }),
    )
    .await?;

    assert_eq!(response["tool"], "sinex.sources_status");
    assert_eq!(response["query"]["recent_window_secs"], 60);
    assert_eq!(response["items"]["result"]["recent_window_secs"], 60);
    assert_eq!(
        response["items"]["result"]["sources"][0]["module_name"],
        "terminal.atuin-history"
    );
    assert_eq!(
        response["items"]["result"]["sources"][0]["current_health"],
        "healthy"
    );
    assert_eq!(response["redaction"]["raw_samples"], false);
    Ok(())
}

#[sinex_test]
async fn mcp_runtime_health_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(
        &client,
        "sinex.source_health",
        json!({ "stale_after_secs": 120 }),
    )
    .await?;

    assert_eq!(response["tool"], "sinex.source_health");
    assert_eq!(response["query"]["stale_after_secs"], 120);
    assert_eq!(response["items"]["result"]["active_count"], 2);
    assert_eq!(response["items"]["result"]["inactive_count"], 1);
    assert_eq!(response["items"]["result"]["unique_modules"], 3);
    assert_eq!(response["redaction"]["raw_samples"], false);
    Ok(())
}

#[sinex_test]
async fn mcp_runtime_active_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(
        &client,
        "sinex.sources_active",
        json!({ "stale_after_secs": 120 }),
    )
    .await?;

    assert_eq!(response["tool"], "sinex.sources_active");
    assert_eq!(response["query"]["stale_after_secs"], 120);
    assert_eq!(
        response["items"]["result"]["modules"][0]["module_name"],
        "terminal.atuin-history"
    );
    assert_eq!(
        response["items"]["result"]["modules"][0]["heartbeat_source"],
        "run"
    );
    assert_eq!(response["redaction"]["raw_samples"], false);
    Ok(())
}

#[sinex_test]
async fn mcp_runtime_registry_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(&client, "sinex.sources_registry", json!({})).await?;

    assert_eq!(response["tool"], "sinex.sources_registry");
    assert_eq!(
        response["items"]["result"]["modules"][0]["module_name"],
        "terminal.atuin-history"
    );
    assert_eq!(
        response["items"]["result"]["modules"][0]["state"],
        "running"
    );
    assert_eq!(response["redaction"]["raw_samples"], false);
    Ok(())
}

#[sinex_test]
async fn mcp_event_engine_validation_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(&client, "sinex.event_engine_validation", json!({})).await?;

    assert_eq!(response["tool"], "sinex.event_engine_validation");
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
async fn mcp_event_engine_batch_stats_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(
        &client,
        "sinex.event_engine_batch_stats",
        json!({
            "from": "2026-05-19T00:00:00Z",
            "to": "2026-05-19T01:00:00Z",
            "limit": 5
        }),
    )
    .await?;

    assert_eq!(response["tool"], "sinex.event_engine_batch_stats");
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

#[sinex_test]
async fn mcp_throughput_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(&client, "sinex.throughput", json!({})).await?;

    assert_eq!(response["tool"], "sinex.throughput");
    assert_eq!(
        response["items"]["result"]["per_source"][0]["source"],
        "terminal"
    );
    assert_eq!(
        response["items"]["result"]["per_source"][0]["events_last_1h"],
        120
    );
    assert_eq!(
        response["items"]["result"]["per_component"][0]["component"],
        "event_engine"
    );
    assert_eq!(response["redaction"]["raw_samples"], false);
    Ok(())
}

#[sinex_test]
async fn mcp_recent_activity_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(&client, "sinex.recent_activity", json!({ "limit": 7 })).await?;

    assert_eq!(response["tool"], "sinex.recent_activity");
    assert_eq!(response["query"]["limit"], 7);
    assert_eq!(response["items"]["entries"][0]["activity_type"], "command");
    assert_eq!(response["items"]["entries"][0]["context"], "terminal");
    assert_eq!(response["redaction"]["raw_samples"], false);
    Ok(())
}

#[sinex_test]
async fn mcp_command_frequency_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(
        &client,
        "sinex.command_frequency",
        json!({
            "from": "2026-05-19T00:00:00Z",
            "to": "2026-05-19T01:00:00Z",
            "limit": 3
        }),
    )
    .await?;

    assert_eq!(response["tool"], "sinex.command_frequency");
    assert_eq!(response["query"]["limit"], 3);
    assert_eq!(response["items"]["entries"][0]["command"], "xtask");
    assert_eq!(response["items"]["entries"][0]["total_executions"], 12);
    assert_eq!(response["redaction"]["raw_samples"], false);
    Ok(())
}

#[sinex_test]
async fn mcp_file_activity_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(
        &client,
        "sinex.file_activity",
        json!({
            "from": "2026-05-19T00:00:00Z",
            "to": "2026-05-19T01:00:00Z",
            "limit": 3
        }),
    )
    .await?;

    assert_eq!(response["tool"], "sinex.file_activity");
    assert_eq!(response["query"]["limit"], 3);
    assert_eq!(
        response["items"]["entries"][0]["directory"],
        "/realm/project/sinex"
    );
    assert_eq!(response["items"]["entries"][0]["total_events"], 9);
    assert_eq!(response["redaction"]["raw_samples"], false);
    Ok(())
}

#[sinex_test]
async fn mcp_system_state_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(
        &client,
        "sinex.system_state",
        json!({
            "from": "2026-05-19T00:00:00Z",
            "to": "2026-05-19T01:00:00Z",
            "limit": 3
        }),
    )
    .await?;

    assert_eq!(response["tool"], "sinex.system_state");
    assert_eq!(response["query"]["limit"], 3);
    assert_eq!(response["items"]["buckets"][0]["sample_count"], 5);
    assert_eq!(response["items"]["buckets"][0]["avg_memory_percent"], 42.5);
    assert_eq!(response["redaction"]["raw_samples"], false);
    Ok(())
}

#[sinex_test]
async fn mcp_window_focus_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(
        &client,
        "sinex.window_focus",
        json!({
            "from": "2026-05-19T00:00:00Z",
            "to": "2026-05-19T01:00:00Z",
            "limit": 3
        }),
    )
    .await?;

    assert_eq!(response["tool"], "sinex.window_focus");
    assert_eq!(response["query"]["limit"], 3);
    assert_eq!(response["items"]["buckets"][0]["workspace"], "4");
    assert_eq!(response["items"]["buckets"][0]["window_class"], "kitty");
    assert_eq!(response["items"]["buckets"][0]["focus_event_count"], 6);
    assert_eq!(response["redaction"]["raw_samples"], false);
    Ok(())
}

#[sinex_test]
async fn mcp_current_health_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(&client, "sinex.current_health", json!({ "limit": 4 })).await?;

    assert_eq!(response["tool"], "sinex.current_health");
    assert_eq!(response["query"]["limit"], 4);
    assert_eq!(response["items"]["entries"][0]["source"], "sinex");
    assert_eq!(response["items"]["entries"][0]["event_type"], "health");
    assert_eq!(response["items"]["entries"][0]["status"], "healthy");
    assert_eq!(response["redaction"]["raw_samples"], false);
    Ok(())
}

#[sinex_test]
async fn mcp_current_device_state_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(&client, "sinex.current_device_state", json!({ "limit": 4 })).await?;

    assert_eq!(response["tool"], "sinex.current_device_state");
    assert_eq!(response["query"]["limit"], 4);
    assert_eq!(response["items"]["entries"][0]["unit_name"], "sinexd");
    assert_eq!(response["items"]["entries"][0]["state"], "active");
    assert_eq!(response["redaction"]["raw_samples"], false);
    Ok(())
}

#[sinex_test]
async fn mcp_gateway_stats_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(&client, "sinex.gateway_stats", telemetry_window_args()).await?;

    assert_eq!(response["tool"], "sinex.gateway_stats");
    assert_eq!(response["query"]["limit"], 3);
    assert_eq!(response["items"]["buckets"][0]["source"], "gateway");
    assert_eq!(response["items"]["buckets"][0]["stat_events"], 4);
    assert_eq!(response["redaction"]["raw_samples"], false);
    Ok(())
}

#[sinex_test]
async fn mcp_stream_stats_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(&client, "sinex.stream_stats", telemetry_window_args()).await?;

    assert_eq!(response["tool"], "sinex.stream_stats");
    assert_eq!(response["query"]["limit"], 3);
    assert_eq!(response["items"]["buckets"][0]["stream_name"], "EVENTS");
    assert_eq!(response["items"]["buckets"][0]["sample_count"], 2);
    assert_eq!(response["redaction"]["raw_samples"], false);
    Ok(())
}

#[sinex_test]
async fn mcp_assembly_stats_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(&client, "sinex.assembly_stats", telemetry_window_args()).await?;

    assert_eq!(response["tool"], "sinex.assembly_stats");
    assert_eq!(response["query"]["limit"], 3);
    assert_eq!(response["items"]["buckets"][0]["total_completed"], 7);
    assert_eq!(response["items"]["buckets"][0]["sample_count"], 3);
    assert_eq!(response["redaction"]["raw_samples"], false);
    Ok(())
}

#[sinex_test]
async fn mcp_source_stats_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(&client, "sinex.source_stats", telemetry_window_args()).await?;

    assert_eq!(response["tool"], "sinex.source_stats");
    assert_eq!(response["query"]["limit"], 3);
    assert_eq!(response["items"]["buckets"][0]["module_kind"], "source");
    assert_eq!(
        response["items"]["buckets"][0]["total_events_processed"],
        42
    );
    assert_eq!(response["redaction"]["raw_samples"], false);
    Ok(())
}

#[sinex_test]
async fn mcp_metric_counters_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(&client, "sinex.metric_counters", telemetry_window_args()).await?;

    assert_eq!(response["tool"], "sinex.metric_counters");
    assert_eq!(response["query"]["limit"], 3);
    assert_eq!(response["items"]["buckets"][0]["component"], "event_engine");
    assert_eq!(response["items"]["buckets"][0]["metric_name"], "events");
    assert_eq!(response["items"]["buckets"][0]["total_value"], 99);
    assert_eq!(response["redaction"]["raw_samples"], false);
    Ok(())
}

#[sinex_test]
async fn mcp_llm_prompts_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(
        &client,
        "sinex.llm_prompts",
        json!({ "status": "active", "limit": 2 }),
    )
    .await?;

    assert_eq!(response["tool"], "sinex.llm_prompts");
    assert_eq!(response["query"]["status"], "active");
    assert_eq!(
        response["items"]["result"]["events"][0]["payload"]["redacted"],
        true
    );
    assert_eq!(response["redaction"]["raw_samples"], false);
    Ok(())
}

#[sinex_test]
async fn mcp_llm_route_explain_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(&client, "sinex.llm_route_explain", fixture_llm_route_args()).await?;

    assert_eq!(response["tool"], "sinex.llm_route_explain");
    assert_eq!(
        response["query"]["request"]["task_kind"],
        "entity-extraction"
    );
    assert_eq!(
        response["items"]["result"]["decision"]["model"],
        "fixture-model"
    );
    assert_eq!(response["redaction"]["raw_samples"], false);
    Ok(())
}

#[sinex_test]
async fn mcp_llm_budget_report_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(&client, "sinex.llm_budget_report", json!({ "limit": 5 })).await?;

    assert_eq!(response["tool"], "sinex.llm_budget_report");
    assert_eq!(response["query"]["limit"], 5);
    assert_eq!(response["items"]["result"]["total_rows"], 1);
    assert_eq!(response["items"]["result"]["prompt_tokens"], 12);
    assert_eq!(response["redaction"]["raw_samples"], false);
    Ok(())
}

#[sinex_test]
async fn mcp_curation_proposals_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(
        &client,
        "sinex.curation_proposals",
        json!({ "status": "pending", "limit": 4 }),
    )
    .await?;

    assert_eq!(response["tool"], "sinex.curation_proposals");
    assert_eq!(response["query"]["status"], "pending");
    assert_eq!(
        response["items"]["result"]["events"][0]["payload"]["redacted"],
        true
    );
    assert_eq!(response["redaction"]["raw_samples"], false);
    Ok(())
}

#[sinex_test]
async fn mcp_dlq_stats_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(&client, "sinex.dlq_stats", json!({})).await?;

    assert_eq!(response["tool"], "sinex.dlq_stats");
    assert_eq!(response["items"]["result"]["total_messages"], 2);
    assert_eq!(response["items"]["result"]["total_bytes"], 512);
    assert_eq!(response["redaction"]["raw_samples"], false);
    Ok(())
}

#[sinex_test]
async fn mcp_dlq_peek_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(&client, "sinex.dlq_peek", json!({ "limit": 2 })).await?;

    assert_eq!(response["tool"], "sinex.dlq_peek");
    assert_eq!(response["query"]["limit"], 2);
    assert_eq!(
        response["items"]["result"]["messages"][0]["payload_redacted"],
        true
    );
    assert_eq!(
        response["items"]["result"]["messages"][0]["privacy_caveats"][0],
        "secret_redacted"
    );
    assert_eq!(response["redaction"]["raw_samples"], false);
    Ok(())
}

#[sinex_test]
async fn mcp_source_materials_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(
        &client,
        "sinex.source_materials",
        json!({ "status": "completed", "limit": 2 }),
    )
    .await?;

    assert_eq!(response["source_surface"], "sinex.source_materials");
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
        "sinex.source_material",
        json!({ "material_id": fixture_material_id() }),
    )
    .await?;

    assert_eq!(response["source_surface"], "sinex.source_material");
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

    let response = call_tool(&client, "sinex.source_coverage", json!({})).await?;

    assert_eq!(response["source_surface"], "sinex.source_coverage");
    assert_eq!(response["payload"]["result"]["sources"][0]["event_count"], 42);
    assert_eq!(
        response["payload"]["result"]["sources"][0]["material_count"],
        3
    );
    assert_eq!(response["privacy_state"]["state"], "redacted");
    Ok(())
}

#[sinex_test]
async fn mcp_source_presets_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(&client, "sinex.source_presets", json!({})).await?;

    assert_eq!(response["source_surface"], "sinex.source_presets");
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
        "sinex.source_bindings",
        json!({ "source_family": "terminal", "include_disabled": true }),
    )
    .await?;

    assert_eq!(response["source_surface"], "sinex.source_bindings");
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
        "sinex.ops_list",
        json!({ "operation_type": "replay", "limit": 2 }),
    )
    .await?;

    assert_eq!(response["tool"], "sinex.ops_list");
    assert_eq!(response["query"]["operation_type"], "replay");
    assert_eq!(
        response["items"]["result"]["operations"][0]["operation_type"],
        "replay"
    );
    assert_eq!(response["redaction"]["raw_samples"], false);
    Ok(())
}

#[sinex_test]
async fn mcp_ops_get_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(
        &client,
        "sinex.ops_get",
        json!({ "operation_id": fixture_operation_id() }),
    )
    .await?;

    assert_eq!(response["tool"], "sinex.ops_get");
    assert_eq!(response["query"]["operation_id"], fixture_operation_id());
    assert_eq!(
        response["items"]["result"]["operation"]["id"],
        fixture_operation_id()
    );
    assert_eq!(response["redaction"]["raw_samples"], false);
    Ok(())
}

#[sinex_test]
async fn mcp_lifecycle_status_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(&client, "sinex.lifecycle_status", json!({})).await?;

    assert_eq!(response["tool"], "sinex.lifecycle_status");
    assert_eq!(response["items"]["result"]["total_events"], 42);
    assert_eq!(response["items"]["result"]["tiers"][0]["tier"], "live");
    assert_eq!(response["redaction"]["raw_samples"], false);
    Ok(())
}

#[sinex_test]
async fn mcp_audit_trail_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(
        &client,
        "sinex.audit_trail",
        json!({ "operation_id": fixture_operation_id() }),
    )
    .await?;

    assert_eq!(response["tool"], "sinex.audit_trail");
    assert_eq!(response["query"]["operation_id"], fixture_operation_id());
    assert_eq!(
        response["items"]["result"]["audit_trail"]["operation"]["id"],
        fixture_operation_id()
    );
    assert_eq!(response["items"]["result"]["event_count"], 1);
    assert_eq!(response["redaction"]["raw_samples"], false);
    Ok(())
}

#[sinex_test]
async fn mcp_coordination_instances_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(
        &client,
        "sinex.coordination_instances",
        json!({ "module_kind": "service" }),
    )
    .await?;

    assert_eq!(response["tool"], "sinex.coordination_instances");
    assert_eq!(response["query"]["module_kind"], "service");
    assert_eq!(
        response["items"]["result"]["instances"][0]["instance_id"],
        fixture_instance_id()
    );
    assert_eq!(response["redaction"]["raw_samples"], false);
    Ok(())
}

#[sinex_test]
async fn mcp_coordination_leader_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(
        &client,
        "sinex.coordination_leader",
        json!({ "module_kind": "service" }),
    )
    .await?;

    assert_eq!(response["tool"], "sinex.coordination_leader");
    assert_eq!(
        response["items"]["result"]["leader"]["instance_id"],
        fixture_instance_id()
    );
    assert_eq!(response["redaction"]["raw_samples"], false);
    Ok(())
}

#[sinex_test]
async fn mcp_coordination_instance_health_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(
        &client,
        "sinex.coordination_instance_health",
        json!({ "instance_id": fixture_instance_id() }),
    )
    .await?;

    assert_eq!(response["tool"], "sinex.coordination_instance_health");
    assert_eq!(response["query"]["instance_id"], fixture_instance_id());
    assert_eq!(response["items"]["result"]["healthy"], true);
    assert_eq!(response["redaction"]["raw_samples"], false);
    Ok(())
}

#[sinex_test]
async fn mcp_shadow_consumers_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(
        &client,
        "sinex.shadow_consumers",
        json!({ "prefix": "dev-fixture" }),
    )
    .await?;

    assert_eq!(response["tool"], "sinex.shadow_consumers");
    assert_eq!(response["query"]["prefix"], "dev-fixture");
    assert_eq!(
        response["items"]["result"]["consumers"][0]["consumer_name"],
        "dev-fixture"
    );
    assert_eq!(response["redaction"]["raw_samples"], false);
    Ok(())
}

#[sinex_test]
async fn mcp_system_ping_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(&client, "sinex.system_ping", json!({})).await?;

    assert_eq!(response["tool"], "sinex.system_ping");
    assert_eq!(response["items"]["result"], "pong");
    assert_eq!(response["redaction"]["raw_samples"], false);
    Ok(())
}

#[sinex_test]
async fn mcp_system_version_call_uses_gateway_fixture() -> TestResult<()> {
    let server = mount_mcp_gateway_fixture().await;
    let client = fixture_gateway_client(&server)?;

    let response = call_tool(&client, "sinex.system_version", json!({})).await?;

    assert_eq!(response["tool"], "sinex.system_version");
    assert_eq!(response["items"]["result"], "0.4.2");
    assert_eq!(response["redaction"]["raw_samples"], false);
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
                            "privacy_caveats": ["secret_redacted"]
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
                            "material_count": 3
                        }
                    ]
                }),
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
                            "stream_name": "EVENTS",
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
