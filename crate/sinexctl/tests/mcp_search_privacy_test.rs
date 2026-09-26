use serde_json::json;
use sinexctl::{GatewayClient, Result, client::ClientConfig, mcp::call_tool};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test]
async fn search_events_tool_does_not_claim_redaction_without_running_it() -> Result<()> {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/"))
        .respond_with(|request: &wiremock::Request| {
            let body: serde_json::Value =
                serde_json::from_slice(&request.body).expect("valid JSON-RPC request body");
            assert_eq!(body["method"], "events.cards");
            ResponseTemplate::new(200).set_body_json(json!({
                "jsonrpc": "2.0",
                "id": body["id"],
                "result": {
                    "schema_version": "1",
                    "count": 0,
                    "cards": []
                }
            }))
        })
        .mount(&server)
        .await;

    let client = GatewayClient::new(ClientConfig {
        url: server.uri(),
        token: Some("test-token".to_string()),
        insecure: true,
        ..ClientConfig::default()
    })?;
    let response = call_tool(&client, "sinex_search_events", json!({})).await?;

    assert_eq!(response["privacy_state"]["state"], "transformation_unknown");
    assert!(!response["caveats"].as_array().is_some_and(|caveats| {
        caveats
            .iter()
            .any(|caveat| caveat["id"] == "mcp.raw_samples_redacted")
    }));
    Ok(())
}
