use sinex_primitives::parser::SourceUnitId;
use sinex_primitives::rpc::sources::{
    CaveatSeverity, caveat_codes, source_shape_drift_readiness_caveats,
    source_shape_drift_readiness_caveats_with_required_fields,
};
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn required_input_shape_removal_blocks_readiness() -> TestResult<()> {
    let source_unit = SourceUnitId::from_static("test.parser");
    let removed = vec!["/id".to_string(), "/optional".to_string()];
    let required = vec!["/id".to_string()];

    let caveats = source_shape_drift_readiness_caveats_with_required_fields(
        &source_unit,
        "shape-new",
        0,
        &removed,
        0,
        &required,
    );

    assert_eq!(caveats.len(), 1);
    assert_eq!(caveats[0].code, caveat_codes::PARSER_REQUIRED_FIELD_MISSING);
    assert_eq!(caveats[0].severity, CaveatSeverity::Blocking);
    assert!(caveats[0].message.contains("/id"));
    assert_eq!(caveats[0].evidence_ref.as_deref(), Some("drift:shape-new"));
    Ok(())
}

#[sinex_test]
async fn observed_only_shape_removal_stays_degraded() -> TestResult<()> {
    let source_unit = SourceUnitId::from_static("test.parser");
    let removed = vec!["/optional".to_string()];
    let required = vec!["/id".to_string()];

    let caveats = source_shape_drift_readiness_caveats_with_required_fields(
        &source_unit,
        "shape-new",
        0,
        &removed,
        0,
        &required,
    );

    assert_eq!(caveats.len(), 1);
    assert_eq!(caveats[0].code, caveat_codes::PARSER_REQUIRED_FIELD_MISSING);
    assert_eq!(caveats[0].severity, CaveatSeverity::Degraded);
    Ok(())
}

#[sinex_test]
async fn count_only_shape_policy_preserves_existing_degraded_semantics() -> TestResult<()> {
    let source_unit = SourceUnitId::from_static("test.parser");

    let caveats = source_shape_drift_readiness_caveats(&source_unit, "shape-new", 0, 1, 0);

    assert_eq!(caveats.len(), 1);
    assert_eq!(caveats[0].code, caveat_codes::PARSER_REQUIRED_FIELD_MISSING);
    assert_eq!(caveats[0].severity, CaveatSeverity::Degraded);
    Ok(())
}
