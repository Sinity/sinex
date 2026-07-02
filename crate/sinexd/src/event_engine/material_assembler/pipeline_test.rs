use super::{
    decode_begin_message, parse_material_id, parse_slice_material_id, parse_slice_offset,
};
use async_nats::HeaderMap;
use serde_json::json;
use sinex_primitives::SinexError;
use uuid::Uuid;
use xtask::sandbox::sinex_test;

const SUBJECT: &str =
    "dev.source_material.frames.slices.test.00000000-0000-7000-8000-000000000001";

// Inline because these exercise private malformed-slice parsing helpers.
#[sinex_test]
async fn parse_slice_offset_accepts_valid_header() -> TestResult<()> {
    let mut headers = HeaderMap::new();
    headers.insert("Offset", "42");
    let offset = parse_slice_offset(SUBJECT, Some(&headers)).map_err(SinexError::validation)?;
    assert_eq!(offset, 42);
    Ok(())
}

#[sinex_test]
async fn parse_slice_offset_rejects_missing_header() -> TestResult<()> {
    let error =
        parse_slice_offset(SUBJECT, None).expect_err("missing offset header should fail");
    assert!(error.contains("missing Offset header"));
    Ok(())
}

#[sinex_test]
async fn parse_slice_offset_rejects_non_numeric_header() -> TestResult<()> {
    let mut headers = HeaderMap::new();
    headers.insert("Offset", "nope");
    let error = parse_slice_offset(SUBJECT, Some(&headers))
        .expect_err("non-numeric offset should fail");
    assert!(error.contains("invalid Offset header"));
    Ok(())
}

#[sinex_test]
async fn parse_slice_offset_rejects_negative_header() -> TestResult<()> {
    let mut headers = HeaderMap::new();
    headers.insert("Offset", "-1");
    let error =
        parse_slice_offset(SUBJECT, Some(&headers)).expect_err("negative offset should fail");
    assert!(error.contains("negative Offset header"));
    Ok(())
}

#[sinex_test]
async fn parse_material_id_reports_context() -> TestResult<()> {
    let error = parse_material_id("not-a-uuid", "test material_id")
        .expect_err("invalid material id should fail");
    assert!(error.contains("test material_id"));
    assert!(error.contains("not-a-uuid"));
    Ok(())
}

#[sinex_test]
async fn decode_begin_message_rejects_invalid_payload() -> TestResult<()> {
    let error = decode_begin_message(br#"{"material_id":"oops""#)
        .expect_err("invalid begin payload should fail");
    assert!(error.contains("invalid begin payload"));
    Ok(())
}

#[sinex_test]
async fn decode_begin_message_rejects_invalid_material_id() -> TestResult<()> {
    let error = decode_begin_message(
        serde_json::to_vec(&json!({
            "material_id": "not-a-uuid",
            "material_kind": "shell-history",
            "source_identifier": "history.db",
            "metadata": {},
            "started_at": "2026-03-28T08:00:00Z"
        }))?
        .as_slice(),
    )
    .expect_err("invalid begin material id should fail");
    assert!(error.contains("begin material_id"));
    Ok(())
}

#[sinex_test]
async fn decode_begin_message_accepts_valid_payload() -> TestResult<()> {
    let material_id = "00000000-0000-7000-8000-000000000001";
    let (begin, parsed_material_id) = decode_begin_message(
        serde_json::to_vec(&json!({
            "material_id": material_id,
            "material_kind": "shell-history",
            "source_identifier": "history.db",
            "metadata": {},
            "started_at": "2026-03-28T08:00:00Z"
        }))?
        .as_slice(),
    )
    .map_err(SinexError::validation)?;
    assert_eq!(begin.material_kind, "shell-history");
    assert_eq!(parsed_material_id, material_id.parse::<Uuid>()?);
    Ok(())
}

#[sinex_test]
async fn parse_slice_material_id_rejects_invalid_subject() -> TestResult<()> {
    let error = parse_slice_material_id("dev.source_material.frames.slices.test.not-a-uuid")
        .expect_err("invalid slice subject material id should fail");
    assert!(error.contains("slice subject material_id"));
    Ok(())
}
