use sinex_core::{DbPoolExt, Id};
use sinex_document_ingestor::{DocumentIngestorConfig, DocumentProcessor};
use sinex_node_sdk::stream_processor::{Checkpoint, NodeInitContext, ScanArgs, TimeHorizon};
use sinex_node_sdk::{Node, SimpleIngestorWrapper};
use sinex_test_utils::{node_runtime::TestRuntimeBuilder, sinex_test, TestContext};
use tempfile::NamedTempFile;
use tokio::time::{timeout, Duration};

#[sinex_test]
async fn document_processor_emits_events_for_targets(ctx: TestContext) -> TestResult<()> {
    ctx.ensure_clean().await?;

    let mut runtime = TestRuntimeBuilder::new(&ctx, "document-ingestor")
        .with_dry_run(false)
        .build()
        .await?;
    let (service_info, handles, raw_config, work_dir) = runtime.runtime.clone().into_parts();

    let mut temp = NamedTempFile::new()?;
    use std::io::Write;
    writeln!(temp, "sample document payload")?;

    let mut config = DocumentIngestorConfig::default();
    config.allowed_roots = vec![temp
        .path()
        .parent()
        .expect("temp file should have a parent")
        .to_string_lossy()
        .into_owned()];
    let init_ctx = NodeInitContext::new(config, raw_config, service_info, handles, work_dir);

    // Use the wrapper to bridge SimpleIngestor to Node
    let mut processor = SimpleIngestorWrapper::<DocumentProcessor>::default();
    processor.initialize(init_ctx).await?;

    let mut scan_args = ScanArgs::default();
    scan_args.targets = vec![temp.path().to_string_lossy().into_owned()];

    processor
        .scan(Checkpoint::None, TimeHorizon::Snapshot, scan_args)
        .await?;

    let event = timeout(Duration::from_secs(1), runtime.event_rx.recv())
        .await
        .ok()
        .flatten()
        .expect("document ingestor should emit a document.ingested event");

    assert_eq!(event.event_type.as_str(), "document.ingested");
    assert_eq!(
        event.payload["_source_material_id"].as_str().is_some(),
        true
    );
    assert_eq!(event.payload["file_path"].as_str().is_some(), true);
    assert_eq!(event.payload["source_material_id"].as_str().is_some(), true);

    // NOTE: the AcquisitionManager is JetStream-first; ingestd is the sole database writer for
    // `raw.source_material_registry`. This test runs the processor directly (no ingestd), so the
    // material should not appear in the database.
    let material_id = match event.provenance() {
        sinex_core::Provenance::Material { id, .. } => *id.as_ulid(),
        _ => panic!("expected material provenance"),
    };

    let record = ctx
        .pool
        .source_materials()
        .get_by_id(Id::from_ulid(material_id))
        .await?;
    assert!(
        record.is_none(),
        "material unexpectedly persisted; ingestd should be the sole DB writer"
    );

    Ok(())
}
