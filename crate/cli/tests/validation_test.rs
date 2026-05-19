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
            "sinex.privacy_status",
            "sinex.system_health"
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
