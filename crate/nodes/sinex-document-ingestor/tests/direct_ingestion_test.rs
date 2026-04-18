use sinex_document_ingestor::{DocumentIngestorConfig, DocumentNode};
use sinex_node_sdk::prelude::DbPoolExt;
use sinex_node_sdk::runtime::stream::{Checkpoint, NodeInitContext, ScanArgs, TimeHorizon};
use sinex_node_sdk::{ExplorationProvider, IngestorNodeAdapter, Node};
use sinex_primitives::Id;
use tempfile::{Builder, NamedTempFile, tempdir};
use tokio::time::{Duration, timeout};
use xtask::sandbox::{node_runtime::TestRuntimeBuilder, sinex_test};

#[sinex_test]
async fn document_node_emits_events_for_targets(ctx: TestContext) -> TestResult<()> {
    let mut runtime = TestRuntimeBuilder::new(&ctx, "document-ingestor")
        .with_dry_run(false)
        .build()
        .await?;
    let (service_info, handles, raw_config, work_dir) = runtime.runtime.clone().into_parts();

    let mut temp = Builder::new().suffix(".txt").tempfile()?;
    use std::io::Write;
    writeln!(temp, "sample document payload")?;

    let mut config = DocumentIngestorConfig::default();
    config.allowed_roots = vec![
        temp.path()
            .parent()
            .expect("temp file should have a parent")
            .to_string_lossy()
            .into_owned(),
    ];
    let init_ctx = NodeInitContext::new(config, raw_config, service_info, handles, work_dir);

    // Use the wrapper to bridge IngestorNode to Node
    let mut node = IngestorNodeAdapter::<DocumentNode>::default();
    node.initialize(init_ctx).await?;

    let mut scan_args = ScanArgs::default();
    scan_args.targets = vec![temp.path().to_string_lossy().into_owned()];

    node.scan(Checkpoint::None, TimeHorizon::Snapshot, scan_args)
        .await?;

    let event = timeout(Duration::from_secs(1), runtime.event_rx.recv())
        .await
        .ok()
        .flatten()
        .expect("document ingestor should emit a document.ingested event");

    assert_eq!(event.event_type.as_str(), "document.ingested");
    assert!(event.payload["_source_material_id"].as_str().is_some());
    assert!(event.payload["file_path"].as_str().is_some());
    assert!(event.payload["source_material_id"].as_str().is_some());

    // NOTE: the AcquisitionManager is JetStream-first; ingestd is the sole database writer for
    // `raw.source_material_registry`. This test runs the node directly (no ingestd), so the
    // material should not appear in the database.
    let material_id = match event.provenance() {
        sinex_primitives::Provenance::Material { id, .. } => *id.as_uuid(),
        _ => panic!("expected material provenance"),
    };

    let record = ctx
        .pool
        .source_materials()
        .get_by_id(Id::from_uuid(material_id))
        .await?;
    assert!(
        record.is_none(),
        "material unexpectedly persisted; ingestd should be the sole DB writer"
    );

    Ok(())
}

#[sinex_test]
async fn document_node_rejects_unsupported_mime_targets(ctx: TestContext) -> TestResult<()> {
    let mut runtime = TestRuntimeBuilder::new(&ctx, "document-ingestor")
        .with_dry_run(false)
        .build()
        .await?;
    let (service_info, handles, raw_config, work_dir) = runtime.runtime.clone().into_parts();

    let mut temp = NamedTempFile::new()?;
    use std::io::Write;
    writeln!(temp, "{{\"json\":true}}")?;

    let mut config = DocumentIngestorConfig::default();
    config.supported_mime_types = vec!["text/plain".to_string()];
    config.allowed_roots = vec![
        temp.path()
            .parent()
            .expect("temp file should have a parent")
            .to_string_lossy()
            .into_owned(),
    ];
    let init_ctx = NodeInitContext::new(config, raw_config, service_info, handles, work_dir);

    let mut node = IngestorNodeAdapter::<DocumentNode>::default();
    node.initialize(init_ctx).await?;

    let mut scan_args = ScanArgs::default();
    scan_args.targets = vec![temp.path().to_string_lossy().into_owned()];

    let report = node
        .scan(Checkpoint::None, TimeHorizon::Snapshot, scan_args)
        .await?;

    assert_eq!(report.events_processed, 0);
    assert_eq!(report.failed_targets.len(), 1);
    assert!(
        report.failed_targets[0].1.contains("Unsupported MIME type"),
        "unexpected failure: {:?}",
        report.failed_targets
    );
    assert!(
        timeout(Duration::from_millis(200), runtime.event_rx.recv())
            .await
            .is_err(),
        "unsupported MIME target must not emit an event"
    );

    Ok(())
}

#[sinex_test]
async fn document_node_source_state_reports_initialized_readiness(
    ctx: TestContext,
) -> TestResult<()> {
    let runtime = TestRuntimeBuilder::new(&ctx, "document-ingestor")
        .with_dry_run(false)
        .build()
        .await?;
    let (service_info, handles, raw_config, work_dir) = runtime.runtime.clone().into_parts();

    let mut temp = Builder::new().suffix(".txt").tempfile()?;
    use std::io::Write;
    writeln!(temp, "document readiness probe")?;

    let mut config = DocumentIngestorConfig::default();
    config.allowed_roots = vec![
        temp.path()
            .parent()
            .expect("temp file should have a parent")
            .to_string_lossy()
            .into_owned(),
    ];
    let init_ctx =
        NodeInitContext::new(config.clone(), raw_config, service_info, handles, work_dir);

    let mut node = IngestorNodeAdapter::<DocumentNode>::default();
    node.initialize(init_ctx).await?;

    let state = node.ingestor().get_source_state()?;
    assert!(state.is_connected);
    assert!(state.healthy);
    assert!(state.description.contains("ready"));
    assert_eq!(
        state.metadata.get("initialized"),
        Some(&serde_json::json!(true))
    );
    assert_eq!(
        state.metadata.get("allowed_roots"),
        Some(&serde_json::json!(config.allowed_roots))
    );

    Ok(())
}

#[sinex_test]
async fn document_node_historical_dry_run_stays_side_effect_free(
    ctx: TestContext,
) -> TestResult<()> {
    let mut runtime = TestRuntimeBuilder::new(&ctx, "document-ingestor")
        .with_dry_run(true)
        .build()
        .await?;
    let (service_info, handles, raw_config, work_dir) = runtime.runtime.clone().into_parts();

    let mut temp = NamedTempFile::new()?;
    use std::io::Write;
    writeln!(temp, "sample document payload")?;

    let mut config = DocumentIngestorConfig::default();
    config.allowed_roots = vec![
        temp.path()
            .parent()
            .expect("temp file should have a parent")
            .to_string_lossy()
            .into_owned(),
    ];
    let init_ctx = NodeInitContext::new(config, raw_config, service_info, handles, work_dir);

    let mut node = IngestorNodeAdapter::<DocumentNode>::default();
    node.initialize(init_ctx).await?;

    let mut scan_args = ScanArgs::default();
    scan_args.dry_run = true;
    scan_args.targets = vec![temp.path().to_string_lossy().into_owned()];

    let report = node
        .scan(
            Checkpoint::None,
            TimeHorizon::Historical {
                end_time: sinex_primitives::Timestamp::now(),
            },
            scan_args,
        )
        .await?;

    assert_eq!(report.events_processed, 0);
    assert!(report.successful_targets.is_empty());
    assert!(report.failed_targets.is_empty());
    assert_eq!(
        report.warnings,
        vec!["Dry-run mode enabled; skipped 1 document target(s)".to_string()]
    );
    assert!(
        timeout(Duration::from_millis(200), runtime.event_rx.recv())
            .await
            .is_err(),
        "historical dry-run must not emit events"
    );

    Ok(())
}

#[sinex_test]
async fn document_node_skipped_targets_are_not_reported_as_success(
    ctx: TestContext,
) -> TestResult<()> {
    let mut runtime = TestRuntimeBuilder::new(&ctx, "document-ingestor")
        .with_dry_run(false)
        .build()
        .await?;
    let (service_info, handles, raw_config, work_dir) = runtime.runtime.clone().into_parts();

    let mut temp = NamedTempFile::new()?;
    use std::io::Write;
    temp.write_all(&vec![b'x'; 2048])?;

    let mut config = DocumentIngestorConfig::default();
    config.max_document_size = 1024;
    config.allowed_roots = vec![
        temp.path()
            .parent()
            .expect("temp file should have a parent")
            .to_string_lossy()
            .into_owned(),
    ];
    let init_ctx = NodeInitContext::new(config, raw_config, service_info, handles, work_dir);

    let mut node = IngestorNodeAdapter::<DocumentNode>::default();
    node.initialize(init_ctx).await?;

    let mut scan_args = ScanArgs::default();
    scan_args.targets = vec![temp.path().to_string_lossy().into_owned()];

    let report = node
        .scan(Checkpoint::None, TimeHorizon::Snapshot, scan_args)
        .await?;

    assert_eq!(report.events_processed, 0);
    assert!(report.successful_targets.is_empty());
    assert!(report.failed_targets.is_empty());
    assert!(
        report
            .warnings
            .iter()
            .any(|warning| warning.contains("larger than the configured size limit")),
        "expected oversized-document warning, got {:?}",
        report.warnings
    );
    assert!(
        timeout(Duration::from_millis(200), runtime.event_rx.recv())
            .await
            .is_err(),
        "skipped oversized target must not emit an event"
    );

    Ok(())
}

#[sinex_test]
async fn document_node_scans_configured_roots_when_targets_are_omitted(
    ctx: TestContext,
) -> TestResult<()> {
    let mut runtime = TestRuntimeBuilder::new(&ctx, "document-ingestor")
        .with_dry_run(false)
        .build()
        .await?;
    let (service_info, handles, raw_config, work_dir) = runtime.runtime.clone().into_parts();

    let temp = tempdir()?;
    let docs_dir = temp.path().join("docs");
    std::fs::create_dir_all(&docs_dir)?;
    let document_path = docs_dir.join("notes.md");
    std::fs::write(&document_path, "# captured\n")?;

    let mut config = DocumentIngestorConfig::default();
    config.allowed_roots = vec![temp.path().to_string_lossy().into_owned()];
    let init_ctx = NodeInitContext::new(config, raw_config, service_info, handles, work_dir);

    let mut node = IngestorNodeAdapter::<DocumentNode>::default();
    node.initialize(init_ctx).await?;

    let report = node
        .scan(Checkpoint::None, TimeHorizon::Snapshot, ScanArgs::default())
        .await?;

    assert_eq!(report.events_processed, 1);
    assert_eq!(
        report.successful_targets,
        vec![document_path.to_string_lossy().into_owned()]
    );

    let event = timeout(Duration::from_secs(1), runtime.event_rx.recv())
        .await
        .ok()
        .flatten()
        .expect("document ingestor should emit for files discovered under allowed roots");
    assert_eq!(event.event_type.as_str(), "document.ingested");
    Ok(())
}

#[sinex_test]
async fn document_node_skips_unchanged_roots_and_reingests_after_modification(
    ctx: TestContext,
) -> TestResult<()> {
    let mut runtime = TestRuntimeBuilder::new(&ctx, "document-ingestor")
        .with_dry_run(false)
        .build()
        .await?;
    let (service_info, handles, raw_config, work_dir) = runtime.runtime.clone().into_parts();

    let temp = tempdir()?;
    let document_path = temp.path().join("notes.md");
    std::fs::write(&document_path, "# first pass\n")?;

    let mut config = DocumentIngestorConfig::default();
    config.allowed_roots = vec![temp.path().to_string_lossy().into_owned()];
    let init_ctx = NodeInitContext::new(config, raw_config, service_info, handles, work_dir);

    let mut node = IngestorNodeAdapter::<DocumentNode>::default();
    node.initialize(init_ctx).await?;

    let first_report = node
        .scan(Checkpoint::None, TimeHorizon::Snapshot, ScanArgs::default())
        .await?;
    assert_eq!(first_report.events_processed, 1);
    timeout(Duration::from_secs(1), runtime.event_rx.recv())
        .await
        .ok()
        .flatten()
        .expect("first scan should emit document.ingested");

    let second_report = node
        .scan(Checkpoint::None, TimeHorizon::Snapshot, ScanArgs::default())
        .await?;
    assert_eq!(second_report.events_processed, 0);
    assert!(
        second_report
            .warnings
            .iter()
            .any(|warning| warning.contains("Skipped 1 unchanged document")),
        "expected unchanged-file warning, got {:?}",
        second_report.warnings
    );
    assert!(
        timeout(Duration::from_millis(200), runtime.event_rx.recv())
            .await
            .is_err(),
        "unchanged root scan must not emit a duplicate event"
    );

    std::fs::write(&document_path, "# second pass\n")?;

    let third_report = node
        .scan(Checkpoint::None, TimeHorizon::Snapshot, ScanArgs::default())
        .await?;
    assert_eq!(third_report.events_processed, 1);
    timeout(Duration::from_secs(1), runtime.event_rx.recv())
        .await
        .ok()
        .flatten()
        .expect("modified document should be re-ingested");

    Ok(())
}
