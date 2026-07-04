use std::collections::BTreeSet;

use serde_json::json;
use xtask::sandbox::prelude::*;

use super::*;

#[sinex_test]
async fn mcp_catalog_and_tool_list_names_stay_in_sync() -> TestResult<()> {
    let catalog_names = tool_catalog()
        .into_iter()
        .map(|entry| entry.name)
        .collect::<BTreeSet<_>>();
    let tool_names = tools()
        .into_iter()
        .map(|tool| tool.name)
        .collect::<BTreeSet<_>>();

    assert_eq!(
        catalog_names, tool_names,
        "MCP tool catalog and tools/list surface must enumerate the same tools"
    );
    Ok(())
}

#[sinex_test]
async fn mcp_catalog_declares_view_envelope_contract_for_read_tools() -> TestResult<()> {
    let catalog = tool_catalog();
    assert!(
        !catalog.is_empty(),
        "MCP catalog must not be empty; the envelope contract would be unenforced"
    );

    for entry in catalog {
        assert_eq!(entry.kind, McpSurfaceKind::Tool);
        assert!(entry.read_only, "{} must remain read-only", entry.name);
        assert_eq!(
            entry.output_contract,
            McpOutputContract::ViewEnvelope,
            "{} must declare the ViewEnvelope output contract",
            entry.name
        );
    }
    Ok(())
}

#[sinex_test]
async fn mcp_standard_envelope_shape_carries_caveat_and_privacy_state() -> TestResult<()> {
    let response = envelope(
        "sinex_test_tool",
        &json!({ "limit": 3 }),
        &json!({ "result": [] }),
    );

    assert_eq!(response["source_surface"], "sinex_test_tool");
    assert_eq!(response["query_echo"], json!({ "limit": 3 }));
    assert_eq!(response["payload"], json!({ "result": [] }));
    assert_eq!(response["privacy_state"]["state"], "redacted");
    assert!(
        response["caveats"]
            .as_array()
            .expect("caveats must be an array")
            .iter()
            .any(|caveat| caveat["id"] == "mcp.raw_samples_redacted"),
        "MCP envelopes must carry the raw-sample redaction caveat: {response:?}"
    );
    assert!(
        response["caveats"]
            .as_array()
            .expect("caveats must be an array")
            .iter()
            .any(|caveat| caveat["id"] == ReadinessCaveatId::CoverageUnmeasurable.as_str()
                && caveat["message"].as_str().is_some_and(|message| message.contains("$.result"))),
        "MCP envelopes must explain empty result collections: {response:?}"
    );
    Ok(())
}

#[sinex_test]
async fn mcp_standard_envelope_shape_explains_null_result_slots() -> TestResult<()> {
    let response = envelope(
        "sinex_event_engine_validation",
        &json!({}),
        &json!({ "snapshot": null }),
    );

    assert!(
        response["caveats"]
            .as_array()
            .expect("caveats must be an array")
            .iter()
            .any(|caveat| caveat["id"] == ReadinessCaveatId::SourceAbsent.as_str()
                && caveat["message"].as_str().is_some_and(|message| message.contains("$.snapshot"))),
        "MCP envelopes must explain null result slots: {response:?}"
    );
    Ok(())
}

#[sinex_test]
async fn mcp_view_envelope_with_caveats_preserves_server_caveats() -> TestResult<()> {
    let response = mcp_view_envelope_with_caveats(
        "sinex_sources_status_view",
        &json!({}),
        &json!({ "sources": [] }),
        vec![CaveatView {
            id: ReadinessCaveatId::SourceAbsent.as_str().to_string(),
            message: "source status server caveat".to_string(),
            ref_: None,
        }],
    )?;

    let caveats = response["caveats"]
        .as_array()
        .expect("caveats must be an array");
    assert!(
        caveats
            .iter()
            .any(|caveat| caveat["id"] == "mcp.raw_samples_redacted"),
        "MCP redaction caveat must still be present: {response:?}"
    );
    assert!(
        caveats.iter().any(|caveat| caveat["id"]
            == ReadinessCaveatId::CoverageUnmeasurable.as_str()
            && caveat["message"].as_str().is_some_and(|message| message.contains("$.sources"))),
        "automatic empty-source caveat must be present: {response:?}"
    );
    assert!(
        caveats.iter().any(|caveat| caveat["id"]
            == ReadinessCaveatId::SourceAbsent.as_str()
            && caveat["message"] == "source status server caveat"),
        "server caveat must survive MCP re-enveloping: {response:?}"
    );
    Ok(())
}

#[sinex_test]
async fn mcp_gateway_unavailable_response_is_still_a_view_envelope() -> TestResult<()> {
    let response = gateway_unavailable_envelope(
        "sinex_sources_status",
        &json!({ "stale_after_secs": 60 }),
        "https://127.0.0.1:19086",
    )?;

    assert_eq!(response["source_surface"], "sinex_sources_status");
    assert_eq!(response["payload"]["status"], "degraded");
    assert_eq!(response["payload"]["reason"], "gateway_unreachable");
    assert_eq!(response["privacy_state"]["state"], "redacted");
    let caveats = response["caveats"]
        .as_array()
        .expect("degraded envelope caveats must be an array");
    assert!(
        caveats
            .iter()
            .any(|caveat| caveat["id"] == "mcp.gateway_unreachable"),
        "degraded gateway response must explain gateway reachability: {response:?}"
    );
    assert!(
        caveats
            .iter()
            .any(|caveat| caveat["id"] == "mcp.raw_samples_redacted"),
        "degraded gateway response must preserve MCP redaction caveat: {response:?}"
    );
    Ok(())
}

#[sinex_test]
async fn context_pack_project_path_returns_unscoped_with_scope_caveat() -> TestResult<()> {
    let args = ContextPackArgs {
        project_path: Some("/realm/project/sinex".to_string()),
        limit: 7,
    };

    let (query, scope, caveats) = context_pack_query_and_scope(&args)?;

    assert_eq!(query.limit, 7);
    assert!(query.sources.is_empty());
    assert_eq!(
        scope,
        ContextPackScope::ProjectPathUnavailable {
            requested_project_path: "/realm/project/sinex".to_string()
        }
    );
    let caveat = caveats
        .iter()
        .find(|caveat| caveat.id == "context_pack.project_scope_unavailable")
        .expect("real project path must expose scope limitation caveat");
    assert!(caveat.message.contains("project_path scoping is unavailable"));
    assert!(caveat.message.contains("sinex-a4w.3.3"));
    assert_eq!(
        caveat.ref_.as_ref().map(|object_ref| object_ref.id.as_str()),
        Some("sinex_context_pack.project_path")
    );

    let envelope = envelope_with_caveats(
        "sinex_context_pack",
        &json!(args),
        &json!({ "pack": { "scope": scope, "events": [] } }),
        caveats,
    );
    assert!(
        envelope["caveats"]
            .as_array()
            .expect("envelope caveats are an array")
            .iter()
            .any(|caveat| caveat["id"] == "context_pack.project_scope_unavailable"),
        "context-pack envelope must carry the scope limitation caveat: {envelope:?}"
    );
    Ok(())
}

#[sinex_test]
async fn context_pack_source_like_project_path_returns_source_hint_with_caveat(
) -> TestResult<()> {
    let args = ContextPackArgs {
        project_path: Some("terminal.atuin-history".to_string()),
        limit: 3,
    };

    let (query, scope, caveats) = context_pack_query_and_scope(&args)?;

    assert_eq!(query.limit, 3);
    assert_eq!(query.sources.len(), 1);
    assert_eq!(query.sources[0].to_string(), "terminal.atuin-history");
    assert_eq!(
        scope,
        ContextPackScope::SourceHint {
            requested_project_path: "terminal.atuin-history".to_string(),
            source: "terminal.atuin-history".to_string(),
        }
    );
    let caveat = caveats
        .iter()
        .find(|caveat| caveat.id == "context_pack.project_scope_unavailable")
        .expect("source-hint fallback must still expose scope limitation caveat");
    assert!(caveat.message.contains("source-hint filtered"));
    assert!(caveat.message.contains("source_hint=terminal.atuin-history"));
    Ok(())
}

#[sinex_test]
async fn context_pack_without_project_path_has_no_scope_caveat() -> TestResult<()> {
    let args = ContextPackArgs {
        project_path: None,
        limit: 5,
    };

    let (query, scope, caveats) = context_pack_query_and_scope(&args)?;

    assert_eq!(query.limit, 5);
    assert!(query.sources.is_empty());
    assert_eq!(scope, ContextPackScope::Unscoped);
    assert!(caveats.is_empty());
    Ok(())
}
