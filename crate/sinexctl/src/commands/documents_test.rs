use super::*;
use sinex_primitives::views::VIEW_ENVELOPE_SCHEMA_VERSION;
use xtask::sandbox::prelude::*;

fn fixture_timestamp() -> Timestamp {
    Timestamp::from_unix_timestamp(1_700_000_000).expect("valid timestamp")
}

fn fixture_document_id() -> Uuid {
    Uuid::nil()
}

#[sinex_test]
async fn documents_search_empty_results_emit_coverage_caveat() -> TestResult<()> {
    let request = DocumentsSearchRequest {
        query: "missing context".to_string(),
        kind: None,
        document_ids: None,
        natural_key_prefix: None,
        updated_after: None,
        updated_before: None,
        limit: Some(20),
        offset: None,
    };
    let response = DocumentsSearchResponse {
        results: Vec::new(),
        search_mode: "fts".to_string(),
        has_more: false,
        next_offset: None,
        empty_reason: Some("no_match".to_string()),
    };

    let envelope = documents_search_envelope(response, &request)?;
    let output = crate::fmt::render_finite_envelope(&envelope, OutputFormat::Json)?
        .expect("json must render");
    let parsed: serde_json::Value = serde_json::from_str(&output)?;

    assert_eq!(parsed["schema_version"], VIEW_ENVELOPE_SCHEMA_VERSION);
    assert_eq!(parsed["source_surface"], "sinexctl.docs.search");
    assert_eq!(parsed["payload"]["results"].as_array().map(Vec::len), Some(0));
    assert_eq!(parsed["query_echo"]["query"], "missing context");
    assert_eq!(parsed["caveats"][0]["id"], "coverage.unmeasurable");
    assert!(
        parsed["caveats"][0]["message"]
            .as_str()
            .is_some_and(|message| message.contains("empty_reason=no_match"))
    );
    Ok(())
}

#[sinex_test]
async fn documents_get_envelope_preserves_query_echo_without_caveat() -> TestResult<()> {
    let document_id = fixture_document_id();
    let request = DocumentsGetRequest { id: document_id };
    let response = DocumentsGetResponse {
        id: document_id,
        kind: "dendron_markdown".to_string(),
        natural_key: "projects/sinex/example".to_string(),
        extraction_version: 1,
        chunk_count: 2,
        text_byte_len: 128,
        side_data: serde_json::json!({}),
        created_at: fixture_timestamp(),
        updated_at: fixture_timestamp(),
    };

    let envelope = documents_get_envelope(response, &request)?;
    let output = crate::fmt::render_finite_envelope(&envelope, OutputFormat::Json)?
        .expect("json must render");
    let parsed: serde_json::Value = serde_json::from_str(&output)?;

    assert_eq!(parsed["source_surface"], "sinexctl.docs.get");
    assert_eq!(parsed["payload"]["natural_key"], "projects/sinex/example");
    assert_eq!(parsed["query_echo"]["id"], document_id.to_string());
    assert!(
        parsed.get("caveats").is_none(),
        "successful document metadata lookup should not invent caveats"
    );
    Ok(())
}

#[sinex_test]
async fn documents_chunks_empty_results_emit_coverage_caveat() -> TestResult<()> {
    let document_id = fixture_document_id();
    let request = DocumentsGetChunksRequest {
        document_id,
        limit: Some(20),
        offset: None,
    };
    let response = DocumentsGetChunksResponse {
        document_id,
        chunks: Vec::new(),
    };

    let envelope = documents_chunks_envelope(response, &request)?;
    let output = crate::fmt::render_finite_envelope(&envelope, OutputFormat::Json)?
        .expect("json must render");
    let parsed: serde_json::Value = serde_json::from_str(&output)?;

    assert_eq!(parsed["source_surface"], "sinexctl.docs.chunks");
    assert_eq!(parsed["payload"]["chunks"].as_array().map(Vec::len), Some(0));
    assert_eq!(parsed["query_echo"]["document_id"], document_id.to_string());
    assert_eq!(parsed["caveats"][0]["id"], "coverage.unmeasurable");
    assert!(
        parsed["caveats"][0]["message"]
            .as_str()
            .is_some_and(|message| message.contains("text coverage"))
    );
    Ok(())
}
