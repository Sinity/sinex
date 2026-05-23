use serde_json::{Value, json};
use sinex_primitives::temporal::{Duration, Timestamp};
use sinexctl::client::{ClientConfig, GatewayClient};
use sinexctl::mcp::{MCP_PROTOCOL_VERSION, assert_read_only_tool_names, call_tool, tools};
use sinexctl::validation::{parse_time_input, parse_time_input_with_now, validate_time_range};
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
            "sinex.source_readiness"
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
async fn mcp_protocol_version_is_pinned() -> TestResult<()> {
    assert_eq!(MCP_PROTOCOL_VERSION, "2024-11-05");
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
    assert_eq!(response["items"]["result"]["type"], "count");
    assert_eq!(response["items"]["result"]["count"], 1);
    assert_eq!(response["redaction"]["raw_samples"], false);
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
    assert_eq!(response["items"]["result"]["ancestors"], json!([]));
    assert_eq!(response["provenance_refs"], json!([]));
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
    let sources = response["items"]["result"]["sources"].as_array().unwrap();
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

async fn mount_mcp_gateway_fixture() -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/"))
        .respond_with(|request: &wiremock::Request| {
            let body: Value = serde_json::from_slice(&request.body).unwrap();
            let result = match body["method"].as_str().unwrap() {
                "events.query" => json!({
                    "type": "count",
                    "count": 1
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
                        "material_links": []
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
                other => panic!("unexpected fixture RPC method: {other}"),
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
        "payload": { "summary": "[REDACTED]" },
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
