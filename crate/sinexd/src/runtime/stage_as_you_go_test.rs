use super::StageAsYouGoContext;
use crate::runtime::SinexError;
use crate::runtime::acquisition_manager::{AcquisitionManager, SOURCE_MATERIAL_END_SUBJECT};
use crate::runtime::stream::EventEmitter;
use sinex_primitives::environment::environment;
use sinex_primitives::{DynamicPayload, Id, events::Provenance};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio::time::{Duration, timeout};
use tokio_stream::StreamExt;
use uuid::Uuid;
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn emit_event_assigns_id_and_anchor() -> TestResult<()> {
    let (tx, mut rx) = mpsc::channel(1);
    let emitter = EventEmitter::new(tx, false);
    let context = StageAsYouGoContext::from_optional_emitter(emitter);

    let material_id = Uuid::now_v7();
    let event = DynamicPayload::new(
        "stage.test",
        "line.captured",
        serde_json::json!({"line": "hello"}),
    )
    .from_material_at(material_id, 12)
    .with_offset_start(12)?
    .with_offset_end(34)?
    .at_time(
        sinex_primitives::Timestamp::from_unix_timestamp_millis(1_710_000_000_123)
            .ok_or_else(|| SinexError::processing("test timestamp should be valid"))?,
    )
    .build()
    .expect("infallible: test provenance set");
    let emitted_id = context
        .emit_event_with_provenance(event, material_id, Some(12), Some(34))
        .await?;

    let emitted = timeout(Duration::from_secs(1), rx.recv())
        .await?
        .ok_or_else(|| SinexError::processing("event channel closed"))?;

    let stored_id = emitted
        .id
        .ok_or_else(|| SinexError::processing("event ID should be assigned"))?;
    // The returned id and the stored id must agree (single mint, carried through).
    assert_eq!(*stored_id.as_uuid(), emitted_id);
    // Event id is a random UUIDv7 (interpretation identity, not occurrence identity).
    assert_eq!(stored_id.as_uuid().get_version_num(), 7);
    assert_eq!(stored_id.as_uuid().get_variant(), uuid::Variant::RFC4122);

    match emitted.provenance() {
        Provenance::Material { anchor_byte, .. } => {
            assert_eq!(*anchor_byte, 12);
        }
        other => {
            return Err(SinexError::validation(format!(
                "unexpected provenance variant: {other:?}"
            ))
            .into());
        }
    }
    assert!(
        emitted.payload.get("_source_material_id").is_none(),
        "source material identity belongs in provenance, not payload metadata"
    );

    Ok(())
}

#[sinex_test]
async fn emit_event_rejects_synthesis_provenance() -> TestResult<()> {
    let (tx, _rx) = mpsc::channel(1);
    let emitter = EventEmitter::new(tx, false);
    let context = StageAsYouGoContext::from_optional_emitter(emitter);

    let material_id = Uuid::now_v7();
    let event = DynamicPayload::new(
        "stage.test",
        "line.captured",
        serde_json::json!({"line": "hello"}),
    )
    .from_parents([Id::from_uuid(Uuid::now_v7())])?
    .build()
    .expect("infallible: test provenance set");

    let err = context
        .emit_event_with_provenance(event, material_id, Some(12), Some(34))
        .await
        .expect_err("stage-as-you-go should not rewrite derived provenance");

    assert!(err.to_string().contains("material-provenance events"));
    Ok(())
}

#[sinex_test]
async fn emit_event_rejects_offset_mismatch() -> TestResult<()> {
    let (tx, _rx) = mpsc::channel(1);
    let emitter = EventEmitter::new(tx, false);
    let context = StageAsYouGoContext::from_optional_emitter(emitter);

    let material_id = Uuid::now_v7();
    let event = DynamicPayload::new(
        "stage.test",
        "line.captured",
        serde_json::json!({"line": "hello"}),
    )
    .from_material_at(material_id, 1)
    .with_offset_start(1)?
    .with_offset_end(2)?
    .build()
    .expect("infallible: test provenance set");

    let err = context
        .emit_event_with_provenance(event, material_id, Some(12), Some(34))
        .await
        .expect_err("stage-as-you-go should not rewrite material offsets");

    assert!(err.to_string().contains("offsets do not match"));
    Ok(())
}

#[sinex_test]
async fn reconciliation_config_is_retained_without_manager() -> TestResult<()> {
    let (tx, _rx) = mpsc::channel(1);
    let emitter = EventEmitter::new(tx, false);
    let context = StageAsYouGoContext::from_optional_emitter(emitter)
        .with_reconciliation(Duration::from_secs(5), Duration::from_secs(1));

    assert!(context.cleanup_config.is_some());
    assert!(context.reconciliation_task.is_none());
    Ok(())
}

#[sinex_test]
async fn signal_reconciliation_shutdown_reports_dropped_receiver() -> TestResult<()> {
    let (tx, rx) = watch::channel(false);
    drop(rx);

    assert!(!super::signal_reconciliation_shutdown(&tx));
    Ok(())
}

#[sinex_test]
async fn signal_reconciliation_shutdown_delivers_to_receiver() -> TestResult<()> {
    let (tx, mut rx) = watch::channel(false);

    assert!(super::signal_reconciliation_shutdown(&tx));
    rx.changed().await?;
    assert!(*rx.borrow());
    Ok(())
}

#[sinex_test]
async fn finalize_source_material_resumes_from_already_staged_bytes(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let work_dir = tempfile::tempdir()?;
    let acquisition = Arc::new(
        AcquisitionManager::with_defaults(ctx.nats_client(), "stage-retry-test")
            .with_work_dir(work_dir.path()),
    );
    let (tx, _rx) = mpsc::channel(1);
    let context = StageAsYouGoContext::from_sender(acquisition.clone(), tx, false);
    let material_id = context
        .register_in_flight("log_file", Some("test://resume"), serde_json::json!({}))
        .await?;
    let end_subject =
        environment().nats_subject_with_namespace(None, SOURCE_MATERIAL_END_SUBJECT.as_str());
    let mut end_sub = ctx.nats_client().subscribe(end_subject).await?;

    let mut handle = context
        .acquisition_handles
        .lock()
        .await
        .remove(&material_id)
        .expect("registered material should have an acquisition handle");
    acquisition.append_slice(&mut handle, b"abc").await?;
    context
        .acquisition_handles
        .lock()
        .await
        .insert(material_id, handle);

    context
        .finalize_source_material(material_id, b"abcdef", Some("text/plain"), Some("utf-8"))
        .await?;

    let end = timeout(Duration::from_secs(1), end_sub.next())
        .await?
        .ok_or_else(|| SinexError::processing("missing material end message"))?;
    let payload: serde_json::Value = serde_json::from_slice(&end.payload)?;
    if payload["material_id"] != material_id.to_string() {
        let end = timeout(Duration::from_secs(1), end_sub.next())
            .await?
            .ok_or_else(|| SinexError::processing("missing material end message"))?;
        let payload: serde_json::Value = serde_json::from_slice(&end.payload)?;
        assert_eq!(payload["material_id"], material_id.to_string());
        assert_eq!(payload["total_size_bytes"], 6);
        assert_eq!(payload["total_slices"], 2);
        return Ok(());
    }

    assert_eq!(payload["total_size_bytes"], 6);
    assert_eq!(payload["total_slices"], 2);
    Ok(())
}
