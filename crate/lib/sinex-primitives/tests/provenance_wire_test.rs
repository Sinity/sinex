use serde_json::json;
use sinex_primitives::events::Provenance;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn material_provenance_requires_anchor_byte_in_wire_format() -> TestResult<()> {
    let err = serde_json::from_value::<Provenance>(json!({
        "source_material_id": uuid::Uuid::now_v7(),
    }))
    .expect_err("missing anchor_byte should fail");

    assert!(err.to_string().contains("missing anchor_byte"));
    Ok(())
}

#[sinex_test]
async fn material_provenance_requires_known_offset_kind_in_wire_format() -> TestResult<()> {
    let err = serde_json::from_value::<Provenance>(json!({
        "source_material_id": uuid::Uuid::now_v7(),
        "anchor_byte": 7,
        "offset_start": 1,
        "offset_end": 3,
        "offset_kind": "mystery",
    }))
    .expect_err("unknown offset_kind should fail");

    assert!(err.to_string().contains("invalid offset kind"));
    Ok(())
}

#[sinex_test]
async fn material_provenance_offsets_require_offset_kind_in_wire_format() -> TestResult<()> {
    let err = serde_json::from_value::<Provenance>(json!({
        "source_material_id": uuid::Uuid::now_v7(),
        "anchor_byte": 7,
        "offset_start": 1,
        "offset_end": 3,
    }))
    .expect_err("offset_kind should be required when offsets are present");

    assert!(err.to_string().contains("offsets require offset_kind"));
    Ok(())
}
