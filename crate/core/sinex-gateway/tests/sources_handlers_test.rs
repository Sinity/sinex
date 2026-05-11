//! Source material RPC handler tests.

use serde_json::json;
use sinex_db::repositories::DbPoolExt;
use sinex_db::repositories::source_materials::TemporalLedgerEntry;
use sinex_gateway::handlers;
use sinex_gateway::rpc_server::RpcAuthContext;
use sinex_gateway::service_container::ServiceContainer;
use sinex_primitives::Timestamp;
use sinex_primitives::domain::{SourceMaterialFormat, SourceMaterialTimingInfoType};
use sinex_primitives::rpc::sources::{
    SourcesListResponse, SourcesShowResponse, SourcesStageResponse,
};
use tempfile::tempdir;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn sources_stage_list_and_show_surface_contract_metadata(
    ctx: TestContext,
) -> TestResult<()> {
    let services = ServiceContainer::from_database_url(ctx.database_url().to_string()).await?;
    let auth = RpcAuthContext::system();

    let dir = tempdir()?;
    let file_path = dir.path().join("atuin-history.sqlite3");
    std::fs::write(&file_path, b"sqlite bytes")?;
    let file_path = file_path.to_string_lossy().to_string();

    let stage_value = handlers::handle_sources_stage(
        json!({
            "file_path": file_path,
            "format": "sqlite",
            "timing_info_type": "intrinsic",
            "reason": "continuous atuin history",
            "tags": ["shell", "history"]
        }),
        &services,
        &auth,
    )
    .await?;
    let stage: SourcesStageResponse = serde_json::from_value(stage_value)?;

    assert_eq!(stage.total_bytes, Some(12));
    assert_eq!(stage.contract.version, 1);
    assert_eq!(stage.contract.format, SourceMaterialFormat::Sqlite);
    assert_eq!(
        stage.contract.timing,
        SourceMaterialTimingInfoType::Intrinsic
    );
    assert_eq!(
        stage
            .contract
            .annotations
            .as_ref()
            .and_then(|annotations| annotations.reason.as_deref()),
        Some("continuous atuin history")
    );

    let material_id = uuid::Uuid::parse_str(&stage.material_id)?;
    ctx.pool()
        .source_materials()
        .append_temporal_ledger(TemporalLedgerEntry::intrinsic_content(
            material_id,
            12,
            Timestamp::now(),
        ))
        .await?;

    let list_value = handlers::handle_sources_list(ctx.pool(), json!({})).await?;
    let list: SourcesListResponse = serde_json::from_value(list_value)?;
    let summary = list
        .materials
        .iter()
        .find(|material| material.id == stage.material_id)
        .expect("staged material should appear in sources.list");
    assert_eq!(summary.format, Some(SourceMaterialFormat::Sqlite));
    assert_eq!(summary.contract_version, Some(1));
    assert_eq!(summary.timing_info_type, "intrinsic");

    let show_value =
        handlers::handle_sources_show(ctx.pool(), json!({ "material_id": stage.material_id }))
            .await?;
    let show: SourcesShowResponse = serde_json::from_value(show_value)?;
    let contract = show
        .material
        .contract
        .as_ref()
        .expect("sources.show should surface contract metadata");
    assert_eq!(contract.format, SourceMaterialFormat::Sqlite);
    assert_eq!(
        contract
            .statistics
            .as_ref()
            .and_then(|statistics| statistics.total_bytes),
        Some(12)
    );
    let evidence = show
        .material
        .temporal_evidence
        .as_ref()
        .expect("sources.show should summarize temporal evidence");
    assert_eq!(evidence.ledger_entries, 1);
    assert_eq!(evidence.source_types, vec!["intrinsic_content".to_string()]);

    Ok(())
}

#[sinex_test]
async fn sources_stage_rejects_non_file_material_formats(ctx: TestContext) -> TestResult<()> {
    let services = ServiceContainer::from_database_url(ctx.database_url().to_string()).await?;
    let auth = RpcAuthContext::system();

    let dir = tempdir()?;
    let file_path = dir.path().join("material.txt");
    std::fs::write(&file_path, b"material")?;
    let file_path = file_path.to_string_lossy().to_string();

    let error = handlers::handle_sources_stage(
        json!({
            "file_path": file_path,
            "format": "directory"
        }),
        &services,
        &auth,
    )
    .await
    .expect_err("file-only sources.stage must reject directory material format");

    assert!(
        error
            .to_string()
            .contains("sources.stage only accepts regular-file material formats"),
        "unexpected error: {error}"
    );
    Ok(())
}

#[sinex_test]
async fn sources_list_respects_limit(ctx: TestContext) -> TestResult<()> {
    let services = ServiceContainer::from_database_url(ctx.database_url().to_string()).await?;
    let auth = RpcAuthContext::system();

    let dir = tempdir()?;
    for name in ["one.jsonl", "two.jsonl"] {
        let file_path = dir.path().join(name);
        std::fs::write(&file_path, b"{}")?;
        handlers::handle_sources_stage(
            json!({
                "file_path": file_path.to_string_lossy().to_string(),
                "format": "jsonl"
            }),
            &services,
            &auth,
        )
        .await?;
    }

    let list_value = handlers::handle_sources_list(ctx.pool(), json!({ "limit": 1 })).await?;
    let list: SourcesListResponse = serde_json::from_value(list_value)?;
    assert_eq!(list.materials.len(), 1);
    Ok(())
}
