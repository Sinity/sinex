use sinex_document_ingestor::{DocumentIngestorConfig, DocumentProcessor};
use sinex_satellite_sdk::stream_processor::{
    Checkpoint, ProcessorInitContext, ScanArgs, TimeHorizon,
};
use sinex_satellite_sdk::StatefulStreamProcessor;
use sinex_test_utils::{satellite_runtime::TestRuntimeBuilder, sinex_test, TestContext};
use tempfile::NamedTempFile;
use tokio::time::{timeout, Duration};

#[sinex_test]
async fn document_processor_emits_events_for_targets(ctx: TestContext) -> color_eyre::Result<()> {
    let mut runtime = TestRuntimeBuilder::new(&ctx, "document-ingestor")
        .with_dry_run(false)
        .build()
        .await?;
    let (service_info, handles, raw_config, work_dir) = runtime.runtime.clone().into_parts();

    let config = DocumentIngestorConfig::default();
    let init_ctx = ProcessorInitContext::new(config, raw_config, service_info, handles, work_dir);

    let mut processor = DocumentProcessor::new();
    processor.initialize(init_ctx).await?;

    let mut temp = NamedTempFile::new()?;
    use std::io::Write;
    writeln!(temp, "sample document payload")?;

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

    Ok(())
}
