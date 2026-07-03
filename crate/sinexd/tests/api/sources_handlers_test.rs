//! Source material RPC handler tests.

use serde_json::json;
use sinex_db::repositories::DbPoolExt;
use sinex_db::repositories::source_materials::TemporalLedgerEntry;
use sinex_primitives::Timestamp;
use sinex_primitives::domain::{SourceMaterialFormat, SourceMaterialTimingInfoType};
use sinex_primitives::events::DynamicPayload;
use sinex_primitives::privacy::MaterialCaptureClass;
use sinex_primitives::rpc::sources::{
    SourceAdmissionDecision, SourcesArchiveRequest, SourcesArchiveResponse, SourcesListRequest,
    SourcesShowRequest, SourcesStageRequest,
};
use sinexd::api::handlers;
use sinexd::api::rpc_server::RpcAuthContext;
use sinexd::api::service_container::ServiceContainer;
use std::path::PathBuf;
use xtask::sandbox::prelude::*;

fn durable_material_dir(label: &str) -> TestResult<PathBuf> {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join(".test-materials")
        .join(format!("{label}-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&dir)?;
    Ok(dir)
}

async fn live_event_count(ctx: &TestContext, event_id: &str) -> TestResult<i64> {
    Ok(sqlx::query_scalar!(
        r#"SELECT COUNT(*)::bigint as "count!" FROM core.events WHERE id = $1::uuid"#,
        event_id.parse::<uuid::Uuid>()?
    )
    .fetch_one(ctx.pool())
    .await?)
}

async fn archived_event_count(ctx: &TestContext, event_id: &str) -> TestResult<i64> {
    Ok(sqlx::query_scalar!(
        r#"SELECT COUNT(*)::bigint as "count!" FROM audit.archived_events WHERE id = $1::uuid"#,
        event_id.parse::<uuid::Uuid>()?
    )
    .fetch_one(ctx.pool())
    .await?)
}

#[sinex_test]
async fn sources_stage_list_and_show_surface_contract_metadata(ctx: TestContext) -> TestResult<()> {
    let services = ServiceContainer::from_database_url(ctx.database_url().to_string()).await?;
    let auth = RpcAuthContext::system();

    let dir = durable_material_dir("stage-list-show")?;
    let file_path = dir.join("atuin-history.sqlite3");
    std::fs::write(&file_path, b"sqlite bytes")?;
    let file_path = file_path.to_string_lossy().to_string();

    let stage = handlers::handle_sources_stage(
        &services,
        SourcesStageRequest {
            file_path,
            format: Some(SourceMaterialFormat::Sqlite),
            timing_info_type: Some(SourceMaterialTimingInfoType::Intrinsic),
            reason: Some("continuous atuin history".to_string()),
            tags: vec!["shell".to_string(), "history".to_string()],
            binding_name: Some("source:terminal.activity.atuin-sqlite-live".to_string()),
            with_bytes: true,
        },
        &auth,
    )
    .await?;

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
    assert_eq!(
        stage
            .contract
            .origin
            .as_ref()
            .and_then(|origin| origin.binding_id.as_deref()),
        Some("source:terminal.activity.atuin-sqlite-live")
    );
    assert_eq!(
        stage
            .contract
            .policy
            .as_ref()
            .and_then(|policy| policy.capture_class),
        Some(MaterialCaptureClass::AllowedPlaintext)
    );
    assert_eq!(
        stage
            .contract
            .policy
            .as_ref()
            .and_then(|policy| policy.admission_decision),
        Some(SourceAdmissionDecision::Admitted)
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

    let list = handlers::handle_sources_list(ctx.pool(), SourcesListRequest::default()).await?;
    let summary = list
        .materials
        .iter()
        .find(|material| material.id == stage.material_id)
        .expect("staged material should appear in sources.list");
    assert_eq!(summary.format, Some(SourceMaterialFormat::Sqlite));
    assert_eq!(summary.contract_version, Some(1));
    assert_eq!(
        summary.timing_info_type,
        SourceMaterialTimingInfoType::Intrinsic
    );

    let show = handlers::handle_sources_show(
        ctx.pool(),
        SourcesShowRequest {
            material_id: stage.material_id,
        },
    )
    .await?;
    let contract = show
        .material
        .contract
        .as_ref()
        .expect("sources.show should surface contract metadata");
    assert_eq!(contract.format, SourceMaterialFormat::Sqlite);
    assert_eq!(
        contract
            .origin
            .as_ref()
            .and_then(|origin| origin.binding_id.as_deref()),
        Some("source:terminal.activity.atuin-sqlite-live")
    );
    assert_eq!(
        contract
            .statistics
            .as_ref()
            .and_then(|statistics| statistics.total_bytes),
        Some(12)
    );
    assert_eq!(
        contract
            .policy
            .as_ref()
            .and_then(|policy| policy.capture_class),
        Some(MaterialCaptureClass::AllowedPlaintext)
    );
    assert_eq!(
        contract
            .policy
            .as_ref()
            .and_then(|policy| policy.admission_decision),
        Some(SourceAdmissionDecision::Admitted)
    );
    let evidence = show
        .material
        .temporal_evidence
        .as_ref()
        .expect("sources.show should summarize temporal evidence");
    assert_eq!(evidence.ledger_entries, 1);
    assert_eq!(evidence.source_types, vec!["intrinsic_content".to_string()]);

    std::fs::remove_dir_all(&dir)?;
    Ok(())
}

#[sinex_test]
async fn sources_archive_scopes_to_requested_material_and_derived_cascade(
    ctx: TestContext,
) -> TestResult<()> {
    let target_material = ctx
        .create_source_material(Some("test.sources.archive.target"))
        .await?;
    let other_material = ctx
        .create_source_material(Some("test.sources.archive.other"))
        .await?;

    let target_event = ctx
        .pool()
        .events()
        .insert(
            DynamicPayload::new(
                "test.sources.archive.target",
                "test.sources.archive.root",
                json!({ "material": "target" }),
            )
            .from_material(target_material)
            .build()?,
        )
        .await?;
    let target_event_id = target_event.id.expect("target event id");
    let target_event_id_string = target_event_id.to_string();

    let derived_event = ctx
        .pool()
        .events()
        .insert(
            DynamicPayload::new(
                "test.sources.archive.derived",
                "test.sources.archive.child",
                json!({ "parent": target_event_id_string }),
            )
            .from_parents([target_event_id])?
            .build()?,
        )
        .await?;
    let derived_event_id = derived_event.id.expect("derived event id").to_string();

    let other_event = ctx
        .pool()
        .events()
        .insert(
            DynamicPayload::new(
                "test.sources.archive.other",
                "test.sources.archive.root",
                json!({ "material": "other" }),
            )
            .from_material(other_material)
            .build()?,
        )
        .await?;
    let other_event_id = other_event.id.expect("other event id").to_string();

    let dry_run = handlers::handle_sources_archive(
        ctx.pool(),
        SourcesArchiveRequest {
            material_id: target_material.to_string(),
            dry_run: true,
            reason: Some("source material archive scope regression".to_string()),
        },
    )
    .await?;
    assert_eq!(dry_run.cascade_count, 2);
    assert_eq!(dry_run.operation_id, None);
    assert_eq!(
        dry_run
            .preview
            .as_ref()
            .and_then(|preview| preview["root_event_count"].as_i64()),
        Some(1)
    );

    let archive: SourcesArchiveResponse = handlers::handle_sources_archive(
        ctx.pool(),
        SourcesArchiveRequest {
            material_id: target_material.to_string(),
            dry_run: false,
            reason: Some("source material archive scope regression".to_string()),
        },
    )
    .await?;

    assert_eq!(archive.cascade_count, 2);
    assert!(
        archive.operation_id.is_some(),
        "execution should surface lifecycle operation id"
    );
    assert_eq!(live_event_count(&ctx, &target_event_id_string).await?, 0);
    assert_eq!(archived_event_count(&ctx, &target_event_id_string).await?, 1);
    assert_eq!(live_event_count(&ctx, &derived_event_id).await?, 0);
    assert_eq!(archived_event_count(&ctx, &derived_event_id).await?, 1);
    assert_eq!(live_event_count(&ctx, &other_event_id).await?, 1);
    assert_eq!(archived_event_count(&ctx, &other_event_id).await?, 0);

    Ok(())
}

#[sinex_test]
async fn sources_stage_rejects_non_file_material_formats(ctx: TestContext) -> TestResult<()> {
    let services = ServiceContainer::from_database_url(ctx.database_url().to_string()).await?;
    let auth = RpcAuthContext::system();

    let dir = durable_material_dir("stage-reject-format")?;
    let file_path = dir.join("material.txt");
    std::fs::write(&file_path, b"material")?;
    let file_path = file_path.to_string_lossy().to_string();

    let error = handlers::handle_sources_stage(
        &services,
        SourcesStageRequest {
            file_path,
            format: Some(SourceMaterialFormat::Directory),
            timing_info_type: None,
            reason: None,
            tags: Vec::new(),
            binding_name: None,
            with_bytes: true,
        },
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
    std::fs::remove_dir_all(&dir)?;
    Ok(())
}

#[sinex_test]
async fn sources_list_respects_limit(ctx: TestContext) -> TestResult<()> {
    let services = ServiceContainer::from_database_url(ctx.database_url().to_string()).await?;
    let auth = RpcAuthContext::system();

    let dir = durable_material_dir("list-limit")?;
    for name in ["one.jsonl", "two.jsonl"] {
        let file_path = dir.join(name);
        std::fs::write(&file_path, b"{}")?;
        handlers::handle_sources_stage(
            &services,
            SourcesStageRequest {
                file_path: file_path.to_string_lossy().to_string(),
                format: Some(SourceMaterialFormat::Jsonl),
                timing_info_type: None,
                reason: None,
                tags: Vec::new(),
                binding_name: None,
                with_bytes: true,
            },
            &auth,
        )
        .await?;
    }

    let list = handlers::handle_sources_list(
        ctx.pool(),
        SourcesListRequest {
            status: None,
            source_identifier: None,
            limit: Some(1),
        },
    )
    .await?;
    assert_eq!(list.materials.len(), 1);
    std::fs::remove_dir_all(&dir)?;
    Ok(())
}
