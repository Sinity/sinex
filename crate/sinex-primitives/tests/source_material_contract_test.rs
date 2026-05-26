use std::str::FromStr;

use serde_json::json;
use sinex_primitives::domain::{
    SourceMaterialFormat, SourceMaterialTimingInfoType, TemporalSourceType,
};
use sinex_primitives::rpc::sources::{
    SourceAnnotations, SourceMaterialMetadataContract, SourceMaterialStatistics, SourceOrigin,
};
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn source_material_contract_roundtrips_from_metadata() -> TestResult<()> {
    let mut contract = SourceMaterialMetadataContract::new(
        SourceMaterialFormat::Sqlite,
        SourceMaterialTimingInfoType::Intrinsic,
    );
    contract.origin = Some(SourceOrigin {
        source_uri: Some("/realm/data/captures/shell/atuin.db".to_string()),
        ..SourceOrigin::default()
    });
    contract.annotations = Some(SourceAnnotations {
        reason: Some("continuous shell history".to_string()),
        tags: vec!["shell".to_string(), "history".to_string()],
        ..SourceAnnotations::default()
    });
    contract.statistics = Some(SourceMaterialStatistics {
        total_bytes: Some(4096),
        record_count: Some(12),
        ..SourceMaterialStatistics::default()
    });

    let metadata = contract.metadata_patch();
    let parsed = SourceMaterialMetadataContract::from_metadata(&metadata)
        .expect("metadata patch should carry contract v1");

    assert_eq!(parsed.version, SourceMaterialMetadataContract::VERSION);
    assert_eq!(parsed.format, SourceMaterialFormat::Sqlite);
    assert_eq!(parsed.timing, SourceMaterialTimingInfoType::Intrinsic);
    assert_eq!(
        parsed
            .annotations
            .as_ref()
            .and_then(|annotations| annotations.reason.as_deref()),
        Some("continuous shell history")
    );
    assert_eq!(
        parsed
            .statistics
            .as_ref()
            .and_then(|statistics| statistics.total_bytes),
        Some(4096)
    );

    let atemporal = SourceMaterialMetadataContract::new(
        SourceMaterialFormat::Markdown,
        SourceMaterialTimingInfoType::Atemporal,
    );
    let parsed = SourceMaterialMetadataContract::from_metadata(&atemporal.metadata_patch())
        .expect("atemporal contract should roundtrip through metadata");
    assert_eq!(parsed.format, SourceMaterialFormat::Markdown);
    assert_eq!(parsed.timing, SourceMaterialTimingInfoType::Atemporal);
    Ok(())
}

#[sinex_test]
async fn source_material_format_infers_common_staged_shapes() -> TestResult<()> {
    assert_eq!(
        SourceMaterialFormat::infer_from_path("history.sqlite3"),
        SourceMaterialFormat::Sqlite
    );
    assert_eq!(
        SourceMaterialFormat::infer_from_path("events.ndjson"),
        SourceMaterialFormat::Jsonl
    );
    assert_eq!(
        SourceMaterialFormat::infer_from_path("notes.tar.zst"),
        SourceMaterialFormat::Archive
    );
    assert_eq!(
        SourceMaterialFormat::from_str("git").map_err(|error| color_eyre::eyre::eyre!(error))?,
        SourceMaterialFormat::Repository
    );
    Ok(())
}

#[sinex_test]
async fn source_material_timing_accepts_registry_vocabulary() -> TestResult<()> {
    for value in [
        "realtime",
        "intrinsic",
        "inferred",
        "declared",
        "atemporal",
        "staged_at",
    ] {
        let parsed = SourceMaterialTimingInfoType::from_str(value)
            .map_err(|error| color_eyre::eyre::eyre!(error))?;
        let serialized = serde_json::to_value(parsed)?;
        assert_eq!(serialized, json!(value));
    }
    assert_eq!(
        SourceMaterialTimingInfoType::from_temporal_source(TemporalSourceType::InferredMtime),
        SourceMaterialTimingInfoType::Inferred
    );
    Ok(())
}
