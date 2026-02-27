use sinex_document_ingestor::{DocumentIngestorConfig, DocumentNode};
use sinex_node_sdk::prelude::DbPoolExt;
use sinex_node_sdk::runtime::stream::{Checkpoint, NodeInitContext, ScanArgs, TimeHorizon};
use sinex_node_sdk::{Node, IngestorNodeAdapter};
use sinex_primitives::Id;
use tempfile::NamedTempFile;
use tokio::time::{timeout, Duration};
use xtask::sandbox::{node_runtime::TestRuntimeBuilder, sinex_test};

#[sinex_test]
async fn document_node_emits_events_for_targets(ctx: TestContext) -> TestResult<()> {
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

    // Use the wrapper to bridge IngestorNode to Node
    let mut node = IngestorNodeAdapter::<DocumentNode>::default();
    node.initialize(init_ctx).await?;

    let mut scan_args = ScanArgs::default();
    scan_args.targets = vec![temp.path().to_string_lossy().into_owned()];

    node
        .scan(Checkpoint::None, TimeHorizon::Snapshot, scan_args)
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
        sinex_primitives::Provenance::Material { id, .. } => *id.as_ulid(),
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
