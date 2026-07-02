// Inline because these tests exercise private bootstrap coordination state;
// extracting them would require widening the test surface of AcquisitionManager.
use super::{
    AcquisitionManager, AppendStreamAcquirer, BufferedAppendStreamWriter,
    BufferedAppendStreamWriterConfig, RotationPolicy,
};
use serde_json::json;
use sinex_primitives::{Bytes, Seconds, Uuid, temporal::Timestamp};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tokio::sync::oneshot;
use tokio::time::{Duration, sleep};
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn concurrent_stream_bootstrap_waits_for_completion(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let manager = Arc::new(AcquisitionManager::with_defaults(
        ctx.nats_client(),
        "bootstrap-test",
    ));
    let attempts = Arc::new(AtomicUsize::new(0));
    let (started_tx, started_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();

    let first = {
        let manager = manager.clone();
        let attempts = attempts.clone();
        tokio::spawn(async move {
            manager
                .ensure_streams_ready_with(|| async move {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    let _ = started_tx.send(());
                    let _ = release_rx.await;
                    Ok(())
                })
                .await
        })
    };

    started_rx.await?;

    let second = {
        let manager = manager.clone();
        let attempts = attempts.clone();
        tokio::spawn(async move {
            manager
                .ensure_streams_ready_with(|| async move {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
                .await
        })
    };

    sleep(Duration::from_millis(100)).await;
    assert!(
        !second.is_finished(),
        "concurrent callers must wait for stream bootstrap to finish"
    );

    let _ = release_tx.send(());
    first.await??;
    second.await??;

    assert_eq!(
        attempts.load(Ordering::SeqCst),
        1,
        "only the first caller should perform bootstrap work"
    );
    Ok(())
}

#[sinex_test]
async fn failed_stream_bootstrap_remains_retryable(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let manager = AcquisitionManager::with_defaults(ctx.nats_client(), "retry-test");
    let attempts = Arc::new(AtomicUsize::new(0));

    let err = manager
        .ensure_streams_ready_with({
            let attempts = attempts.clone();
            || async move {
                attempts.fetch_add(1, Ordering::SeqCst);
                Err(sinex_primitives::error::SinexError::messaging(
                    "bootstrap failed",
                ))
            }
        })
        .await
        .expect_err("failed bootstrap should surface immediately");
    assert!(
        err.to_string().contains("bootstrap failed"),
        "unexpected error: {err}"
    );

    manager
        .ensure_streams_ready_with({
            let attempts = attempts.clone();
            || async move {
                attempts.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        })
        .await?;

    assert_eq!(
        attempts.load(Ordering::SeqCst),
        2,
        "failed bootstrap should not poison future retries"
    );
    Ok(())
}

#[sinex_test]
async fn oversized_material_begin_frame_is_rejected_before_nats(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let namespace = format!("oversized-material-begin-{}", Uuid::now_v7());
    let manager = AcquisitionManager::new_with_namespace(
        ctx.nats_client(),
        RotationPolicy::default(),
        "oversized-material-begin-test".to_string(),
        Some(namespace),
    );

    let error = manager
        .publish_begin(
            Uuid::now_v7(),
            "test://oversized-material-begin",
            json!({
                "oversized": "x".repeat(
                    crate::runtime::nats_payload::NATS_PUBLISH_PAYLOAD_HARD_LIMIT_BYTES + 1
                ),
            }),
            Timestamp::now(),
        )
        .await
        .expect_err("oversized begin metadata should fail before NATS publish");

    let error_text = error.to_string();
    assert!(error_text.contains("NATS payload exceeds configured hard limit"));
    assert!(error_text.contains("begin"));
    Ok(())
}

#[sinex_test]
async fn oversized_logical_record_is_chunked_without_losing_anchor(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let work_dir = tempfile::tempdir()?;
    let manager = AcquisitionManager::with_defaults(ctx.nats_client(), "oversized-test")
        .with_work_dir(work_dir.path());
    let mut handle = manager.begin_material("test://oversized").await?;
    let oversized = vec![0u8; AcquisitionManager::MAX_NATS_PAYLOAD_BYTES + 1];

    let anchors = manager
        .append_record_batch(&mut handle, &[&oversized])
        .await?;

    assert_eq!(anchors.len(), 1);
    assert_eq!(anchors[0].offset_start, 0);
    assert_eq!(anchors[0].offset_end, oversized.len() as i64);
    assert_eq!(handle.bytes_written(), oversized.len() as i64);
    assert_eq!(
        handle.slice_count, 3,
        "a 512KiB+1 logical record should publish as three 256KiB transport slices"
    );
    assert_eq!(
        handle.hasher.clone().finalize().to_hex().to_string(),
        blake3::hash(&oversized).to_hex().to_string()
    );

    let metadata = tokio::fs::metadata(handle.temp_path()).await?;
    assert_eq!(
        metadata.len(),
        oversized.len() as u64,
        "logical record bytes should be mirrored exactly once"
    );
    Ok(())
}

#[sinex_test]
async fn append_record_batch_returns_per_record_anchors(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let work_dir = tempfile::tempdir()?;
    let manager = AcquisitionManager::with_defaults(ctx.nats_client(), "record-batch-test")
        .with_work_dir(work_dir.path());
    let mut handle = manager.begin_material("test://record-batch").await?;
    let records = vec![
        b"alpha".to_vec(),
        b"beta".to_vec(),
        Vec::new(),
        b"gamma".to_vec(),
    ];

    let anchors = manager.append_record_batch(&mut handle, &records).await?;

    assert_eq!(anchors.len(), 4);
    assert_eq!(anchors[0].offset_start, 0);
    assert_eq!(anchors[0].offset_end, 5);
    assert_eq!(anchors[1].offset_start, 5);
    assert_eq!(anchors[1].offset_end, 9);
    assert_eq!(anchors[2].offset_start, 9);
    assert_eq!(anchors[2].offset_end, 9);
    assert_eq!(anchors[3].offset_start, 9);
    assert_eq!(anchors[3].offset_end, 14);
    assert!(
        anchors
            .iter()
            .all(|anchor| anchor.material_id == handle.material_id)
    );
    assert_eq!(handle.bytes_written(), 14);
    assert_eq!(handle.slice_count, 1);
    assert_eq!(
        tokio::fs::read(handle.temp_path()).await?,
        b"alphabetagamma"
    );
    Ok(())
}

#[sinex_test]
async fn append_stream_returns_contiguous_anchors(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let work_dir = tempfile::tempdir()?;
    let manager = Arc::new(
        AcquisitionManager::with_defaults(ctx.nats_client(), "append-stream-test")
            .with_work_dir(work_dir.path()),
    );
    let mut stream = AppendStreamAcquirer::new(manager);

    let first = stream
        .append_json_line(&json!({ "row": 1, "value": "alpha" }), "test://history")
        .await?;
    let second = stream
        .append_json_line(&json!({ "row": 2, "value": "beta" }), "test://history")
        .await?;
    let third = stream
        .append_json_line(
            &json!({ "row": 1, "value": "gamma" }),
            "test://other-history",
        )
        .await?;
    stream.finalize("test-complete").await?;

    assert_eq!(
        first.material_id, second.material_id,
        "one logical source stream should use one material until rotation"
    );
    assert_ne!(
        second.material_id, third.material_id,
        "changing logical sources must rotate instead of mixing records"
    );
    assert_eq!(
        first.offset_end, second.offset_start,
        "source record anchors should be contiguous byte ranges"
    );
    assert_eq!(
        third.offset_start, 0,
        "a rotated source material should restart byte anchors at zero"
    );
    assert!(
        first.offset_start < first.offset_end,
        "first record must occupy a non-empty range"
    );
    assert!(
        second.offset_start < second.offset_end,
        "second record must occupy a non-empty range"
    );
    Ok(())
}

#[sinex_test]
async fn append_stream_batches_records_into_one_slice(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let work_dir = tempfile::tempdir()?;
    let manager = Arc::new(
        AcquisitionManager::with_defaults(ctx.nats_client(), "append-stream-batch-test")
            .with_work_dir(work_dir.path()),
    );
    let mut stream = AppendStreamAcquirer::new(manager);
    let records = vec![b"one\n".to_vec(), b"two\n".to_vec(), b"three\n".to_vec()];

    let anchors = stream
        .append_many_with_anchors(&records, "test://batched-history")
        .await?;

    assert_eq!(anchors.len(), records.len());
    assert_eq!(anchors[0].offset_start, 0);
    assert_eq!(anchors[0].offset_end, 4);
    assert_eq!(anchors[1].offset_start, 4);
    assert_eq!(anchors[1].offset_end, 8);
    assert_eq!(anchors[2].offset_start, 8);
    assert_eq!(anchors[2].offset_end, 14);
    let handle = stream
        .current_handle
        .as_ref()
        .ok_or_else(|| SinexError::invalid_state("stream material should be active"))?;
    assert_eq!(handle.slice_count, 1);
    assert_eq!(
        tokio::fs::read(handle.temp_path()).await?,
        b"one\ntwo\nthree\n"
    );
    stream.finalize("test-complete").await?;
    Ok(())
}

#[sinex_test]
async fn append_stream_publishes_begin_before_exposing_material_id(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let namespace = format!("append-stream-eager-begin-{}", Uuid::now_v7());
    let work_dir = tempfile::tempdir()?;
    let manager = Arc::new(
        AcquisitionManager::new_with_namespace(
            ctx.nats_client(),
            RotationPolicy {
                max_bytes: Bytes::from(4),
                max_age_seconds: Seconds::from_secs(3600),
            },
            "append-stream-eager-begin-test".to_string(),
            Some(namespace),
        )
        .with_work_dir(work_dir.path()),
    );
    let mut stream = AppendStreamAcquirer::new(manager);

    let first = stream
        .append_with_anchor(b"1234", "test://eager-begin")
        .await?;
    let first_handle = stream
        .current_handle
        .as_ref()
        .ok_or_else(|| SinexError::invalid_state("stream material should be active"))?;
    assert!(
        first_handle.pending_begin.is_none(),
        "stream material BEGIN must be acked before callers can observe the material ID"
    );
    let first_material_id = first.material_id;

    let second = stream
        .append_with_anchor(b"5", "test://eager-begin")
        .await?;
    let rotated_handle = stream
        .current_handle
        .as_ref()
        .ok_or_else(|| SinexError::invalid_state("rotated stream material should be active"))?;
    assert_ne!(
        second.material_id, first_material_id,
        "append after a full material should rotate to a fresh source material"
    );
    assert_eq!(
        rotated_handle.material_id, second.material_id,
        "current handle should be the rotated material used for the second anchor"
    );
    assert!(
        rotated_handle.pending_begin.is_none(),
        "rotated stream material BEGIN must be acked before event anchors use it"
    );

    stream.finalize("test-complete").await?;
    Ok(())
}

#[sinex_test]
async fn append_stream_can_start_from_active_handle(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let namespace = format!("append-stream-existing-{}", Uuid::now_v7());
    let work_dir = tempfile::tempdir()?;
    let manager = Arc::new(
        AcquisitionManager::new_with_namespace(
            ctx.nats_client(),
            RotationPolicy {
                max_bytes: Bytes::from(8),
                max_age_seconds: Seconds::from_secs(3600),
            },
            "append-stream-existing-test".to_string(),
            Some(namespace),
        )
        .with_work_dir(work_dir.path()),
    );
    let handle = manager.begin_material("test://existing-stream").await?;
    let first_material_id = handle.material_id;
    let mut stream =
        AppendStreamAcquirer::from_active_handle(manager, handle, "test://existing-stream");

    let first = stream
        .append_with_anchor(b"1234", "test://existing-stream")
        .await?;
    let second = stream
        .append_with_anchor(b"56789", "test://existing-stream")
        .await?;

    assert_eq!(
        first.material_id, first_material_id,
        "first append should use the provided active material"
    );
    assert_ne!(
        second.material_id, first_material_id,
        "append that would exceed the rotation policy should rotate"
    );
    assert_eq!(second.offset_start, 0);

    stream.finalize("test-complete").await?;
    Ok(())
}

#[sinex_test]
async fn buffered_append_writer_preserves_record_offsets(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let namespace = format!("buffered-append-writer-{}", Uuid::now_v7());
    let work_dir = tempfile::tempdir()?;
    let manager = Arc::new(
        AcquisitionManager::new_with_namespace(
            ctx.nats_client(),
            RotationPolicy::default(),
            "buffered-writer-test".to_string(),
            Some(namespace),
        )
        .with_work_dir(work_dir.path()),
    );
    let writer = BufferedAppendStreamWriter::from_manager(
        manager,
        "test://buffered-writer",
        BufferedAppendStreamWriterConfig {
            batch_coalesce_window: std::time::Duration::from_millis(1),
            ..BufferedAppendStreamWriterConfig::default()
        },
    );

    let first = writer.append(b"one".to_vec()).await?;
    let second = writer.append(b"two".to_vec()).await?;
    writer.finalize("test-complete").await?;

    assert_eq!((first.offset_start, first.offset_end), (0, 3));
    assert_eq!((second.offset_start, second.offset_end), (3, 6));
    assert_eq!(first.material_id, second.material_id);
    Ok(())
}

#[sinex_test]
async fn buffered_append_writer_flushes_material_without_stopping(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let namespace = format!("buffered-append-flush-{}", Uuid::now_v7());
    let work_dir = tempfile::tempdir()?;
    let manager = Arc::new(
        AcquisitionManager::new_with_namespace(
            ctx.nats_client(),
            RotationPolicy::default(),
            "buffered-writer-flush-test".to_string(),
            Some(namespace),
        )
        .with_work_dir(work_dir.path()),
    );
    let writer = BufferedAppendStreamWriter::from_manager(
        manager,
        "test://buffered-writer-flush",
        BufferedAppendStreamWriterConfig {
            batch_coalesce_window: std::time::Duration::from_millis(1),
            ..BufferedAppendStreamWriterConfig::default()
        },
    );

    let first = writer.append(b"one".to_vec()).await?;
    writer.flush("snapshot-evidence-boundary").await?;
    let second = writer.append(b"two".to_vec()).await?;
    writer.finalize("test-complete").await?;

    assert_ne!(first.material_id, second.material_id);
    assert_eq!((first.offset_start, first.offset_end), (0, 3));
    assert_eq!((second.offset_start, second.offset_end), (0, 3));
    Ok(())
}

#[sinex_test]
async fn buffered_append_writer_flushes_after_max_open_duration(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let namespace = format!("buffered-append-max-open-{}", Uuid::now_v7());
    let work_dir = tempfile::tempdir()?;
    let manager = Arc::new(
        AcquisitionManager::new_with_namespace(
            ctx.nats_client(),
            RotationPolicy::default(),
            "buffered-writer-max-open-test".to_string(),
            Some(namespace),
        )
        .with_work_dir(work_dir.path()),
    );
    let writer = BufferedAppendStreamWriter::from_manager(
        manager,
        "test://buffered-writer-max-open",
        BufferedAppendStreamWriterConfig {
            batch_coalesce_window: std::time::Duration::from_millis(1),
            max_open_duration: Some(std::time::Duration::from_millis(50)),
            ..BufferedAppendStreamWriterConfig::default()
        },
    );

    let first = writer.append(b"one".to_vec()).await?;
    tokio::time::sleep(std::time::Duration::from_millis(120)).await;
    let second = writer.append(b"two".to_vec()).await?;
    writer.finalize("test-complete").await?;

    assert_ne!(first.material_id, second.material_id);
    assert_eq!((first.offset_start, first.offset_end), (0, 3));
    assert_eq!((second.offset_start, second.offset_end), (0, 3));
    Ok(())
}

#[sinex_test]
async fn prime_begins_material_without_staging_content(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let namespace = format!("buffered-append-prime-{}", Uuid::now_v7());
    let work_dir = tempfile::tempdir()?;
    let manager = Arc::new(
        AcquisitionManager::new_with_namespace(
            ctx.nats_client(),
            RotationPolicy::default(),
            "buffered-writer-prime-test".to_string(),
            Some(namespace),
        )
        .with_work_dir(work_dir.path()),
    );
    let writer = BufferedAppendStreamWriter::from_manager(
        manager,
        "test://buffered-writer-prime",
        BufferedAppendStreamWriterConfig {
            batch_coalesce_window: std::time::Duration::from_millis(1),
            ..BufferedAppendStreamWriterConfig::default()
        },
    );

    // Prime publishes BEGIN eagerly, then the first real record must anchor at
    // offset 0 — proving no placeholder byte was staged (#2184 prong E: this is
    // what stopped the ~30K degenerate 1-byte self-observation materials).
    writer.prime().await?;
    let first = writer.append(b"first-record\n".to_vec()).await?;
    writer.finalize("test-complete").await?;

    assert_eq!(first.offset_start, 0);
    assert_eq!(first.offset_end, b"first-record\n".len() as i64);
    Ok(())
}

// Preserved from the pre-existing split test file during inline extraction.
#[sinex_test]
async fn oversized_slice_rejection_does_not_mutate_local_stage(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let work_dir = tempfile::tempdir()?;
    let manager = AcquisitionManager::with_defaults(ctx.nats_client(), "oversized-test")
        .with_work_dir(work_dir.path());
    let mut handle = manager.begin_material("test://oversized").await?;
    let oversized = vec![0u8; AcquisitionManager::MAX_NATS_PAYLOAD_BYTES + 1];

    let error = manager
        .append_slice(&mut handle, &oversized)
        .await
        .expect_err("oversized slice should be rejected before mutating local state");

    assert!(
        error.to_string().contains("exceeds NATS max payload"),
        "unexpected error: {error}"
    );
    assert_eq!(handle.bytes_written(), 0);
    assert_eq!(
        handle.hasher.clone().finalize().to_hex().to_string(),
        blake3::Hasher::new().finalize().to_hex().to_string()
    );

    let metadata = tokio::fs::metadata(handle.temp_path()).await?;
    assert_eq!(
        metadata.len(),
        0,
        "oversized rejection must not stage bytes locally"
    );
    Ok(())
}
