use super::{
    PayloadInfo, calculate_schema_content_hash, generate_schema_bundle_from_payloads,
    schema_bundle_major_version,
};
use serde_json::json;
use xtask::sandbox::prelude::*;

#[allow(clippy::unnecessary_wraps)]
fn schema_ok() -> crate::error::Result<serde_json::Value> {
    Ok(json!({"type": "object"}))
}

fn schema_err() -> crate::error::Result<serde_json::Value> {
    Err(crate::error::SinexError::serialization(
        "failed to serialize event payload schema",
    ))
}

#[sinex_test]
async fn generate_schemas_collects_entries() -> TestResult<()> {
    let payloads = [PayloadInfo {
        type_name: "test::Payload",
        source: "test-source",
        event_type: "test.event",
        version: "1.0.0",
        schema_fn: schema_ok,
    }];

    let bundle = generate_schema_bundle_from_payloads(payloads.iter())?;
    assert_eq!(bundle.entries().len(), 1);
    assert_eq!(
        bundle.entries()[0].sync_key(),
        (
            "test-source".to_string(),
            "test.event".to_string(),
            "1.0.0".to_string()
        )
    );
    assert_eq!(bundle.entries()[0].schema_content["type"], "object");
    assert_eq!(
        bundle.entries()[0].schema_content["x-sinex-source"],
        "test-source"
    );
    Ok(())
}

#[sinex_test]
async fn generate_schemas_surfaces_schema_generation_failures() -> TestResult<()> {
    let payloads = [PayloadInfo {
        type_name: "test::BrokenPayload",
        source: "test-source",
        event_type: "test.broken",
        version: "1.0.0",
        schema_fn: schema_err,
    }];

    let error = generate_schema_bundle_from_payloads(payloads.iter())
        .expect_err("schema generation failures must stay explicit");
    let rendered = format!("{error:#}");
    assert!(rendered.contains("failed to serialize event payload schema"));
    assert!(rendered.contains("test::BrokenPayload"));
    assert!(rendered.contains("test.broken"));
    Ok(())
}

#[sinex_test]
async fn schema_hash_changes_when_metadata_changes() -> TestResult<()> {
    let schema = json!({"type": "object", "properties": {}});
    let a = calculate_schema_content_hash("one", "evt", "1.0.0", &schema)?;
    let b = calculate_schema_content_hash("two", "evt", "1.0.0", &schema)?;
    assert_ne!(a, b);
    Ok(())
}

#[sinex_test]
async fn schema_bundle_major_version_reads_first_segment() -> TestResult<()> {
    assert_eq!(schema_bundle_major_version("7.3.9")?, 7);
    Ok(())
}
