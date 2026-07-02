use super::*;
use crate::event_engine::material_assembler::state::MaterialEndMessage;
use serde_json::json;
#[cfg(unix)]
use std::os::unix::ffi::OsStringExt;
use std::sync::Arc;
use tokio::time::timeout;
use tokio_stream::StreamExt;
use xtask::sandbox::prelude::*;

async fn test_assembler(
    ctx: &TestContext,
) -> TestResult<(MaterialAssembler, tempfile::TempDir, tempfile::TempDir)> {
    super::super::test_support::build_test_assembler(ctx, "io-test").await
}

async fn test_assembler_with_config(
    ctx: &TestContext,
    slice_timeout_secs: u64,
) -> TestResult<(MaterialAssembler, tempfile::TempDir, tempfile::TempDir)> {
    super::super::test_support::TestAssemblerBuilder::new("io-test")
        .slice_timeout_secs(slice_timeout_secs)
        .build(ctx)
        .await
}

async fn write_wal_entry(wal_path: &std::path::Path, entry: WalEntry) -> TestResult<()> {
    let entry_json = serde_json::to_vec(&entry)?;
    let envelope = WalEntryEnvelope {
        seq: 0,
        crc: crc32fast::hash(&entry_json),
        entry,
    };
    let mut wal = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(wal_path)
        .await?;
    wal.write_all(format!("{}\n", serde_json::to_string(&envelope)?).as_bytes())
        .await?;
    Ok(())
}

#[sinex_test]
async fn import_into_content_store_preserves_staging_file_until_cleanup(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let (assembler, _content_store_dir, state_dir) = test_assembler(&ctx).await?;
    let temp_path = state_dir.path().join("assembled.bin");
    tokio::fs::write(&temp_path, b"staged-content").await?;

    let final_state = FinalizationState {
        material_id: Uuid::now_v7(),
        temp_path: temp_path.clone(),
        expected_offset: 14,
        slice_count: 1,
        buffered_count: 0,
        metadata: json!({}),
        material_kind: "test".to_string(),
        source_identifier: "test://content-store".to_string(),
        started_at: Timestamp::now(),
    };

    let content_key = import_into_content_store(&assembler, &final_state).await?;
    assert!(!content_key.key.is_empty());
    assert!(
        temp_path.exists(),
        "content-store import should preserve the staging file until cleanup succeeds"
    );
    Ok(())
}

#[sinex_test]
async fn buffered_slice_file_len_bytes_rejects_unrepresentable_lengths() -> TestResult<()> {
    let error = buffered_slice_file_len_bytes(Path::new("/tmp/oversized-slice"), u64::MAX)
        .expect_err("oversized buffered slices must fail honestly");

    assert!(
        error
            .to_string()
            .contains("buffered slice length exceeds i64 range")
    );
    Ok(())
}

#[sinex_test]
async fn append_slice_data_batches_staged_and_wal_sync(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let (assembler, _content_store_dir, _state_dir) = test_assembler(&ctx).await?;
    let material_id = Uuid::now_v7();
    let mut state = assembler.create_placeholder_state(material_id).await?;
    state.phase = AssemblyPhase::Accumulating;

    append_slice_data(&assembler, &mut state, material_id, b"small-record").await?;

    assert_eq!(state.expected_offset, "small-record".len() as i64);
    assert_eq!(state.staged_bytes_since_sync, "small-record".len() as i64);
    assert!(
        state.wal_entries_since_sync > 0,
        "per-slice WAL writes should stay buffered instead of forced durable"
    );

    sync_staged_file_for_finalization(&assembler, &mut state, material_id).await?;

    assert_eq!(state.staged_bytes_since_sync, 0);
    Ok(())
}

#[sinex_test]
async fn handle_slice_releases_state_lock_before_staging_io(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let (assembler, _content_store_dir, _state_dir) = test_assembler(&ctx).await?;
    let material_id = Uuid::now_v7();
    let mut state = assembler.create_placeholder_state(material_id).await?;
    state.phase = AssemblyPhase::Accumulating;
    let state_handle = assembler.insert_state_handle(material_id, state);
    let assembler = Arc::new(assembler);

    let hook = Arc::new(SliceStagingIoHook {
        entered: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
        pause_next: std::sync::atomic::AtomicBool::new(true),
    });
    {
        let mut slot = SLICE_STAGING_IO_HOOK
            .lock()
            .map_err(|_| SinexError::invalid_state("slice staging hook mutex poisoned"))?;
        *slot = Some(hook.clone());
    }

    let entered = hook.entered.notified();
    let task_assembler = assembler.clone();
    let join = tokio::spawn(async move {
        handle_slice(&task_assembler, material_id, 0, b"first".to_vec()).await
    });

    timeout(Duration::from_secs(Timeouts::SHORT), entered).await?;
    {
        let guard = state_handle
            .try_lock()
            .expect("state mutex should be free while staged file I/O is pending");
        assert_eq!(
            guard.pending_write.as_ref().map(|write| write.offset),
            Some(0)
        );
        assert_eq!(guard.expected_offset, 0);
    }

    hook.release.notify_waiters();
    join.await??;
    {
        let mut slot = SLICE_STAGING_IO_HOOK
            .lock()
            .map_err(|_| SinexError::invalid_state("slice staging hook mutex poisoned"))?;
        *slot = None;
    }

    let state = state_handle.lock().await;
    assert_eq!(state.expected_offset, 5);
    assert!(state.pending_write.is_none());
    Ok(())
}

#[sinex_test]
async fn handle_slice_serializes_duplicate_staging_io(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let (assembler, _content_store_dir, _state_dir) = test_assembler(&ctx).await?;
    let material_id = Uuid::now_v7();
    let mut state = assembler.create_placeholder_state(material_id).await?;
    state.phase = AssemblyPhase::Accumulating;
    let temp_path = state.temp_path.clone();
    let state_handle = assembler.insert_state_handle(material_id, state);
    let assembler = Arc::new(assembler);

    let hook = Arc::new(SliceStagingIoHook {
        entered: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
        pause_next: std::sync::atomic::AtomicBool::new(true),
    });
    {
        let mut slot = SLICE_STAGING_IO_HOOK
            .lock()
            .map_err(|_| SinexError::invalid_state("slice staging hook mutex poisoned"))?;
        *slot = Some(hook.clone());
    }

    let first_entered = hook.entered.notified();
    let first_assembler = assembler.clone();
    let first = tokio::spawn(async move {
        handle_slice(&first_assembler, material_id, 0, b"first".to_vec()).await
    });
    timeout(Duration::from_secs(Timeouts::SHORT), first_entered).await?;

    let second_assembler = assembler.clone();
    let second = tokio::spawn(async move {
        handle_slice(&second_assembler, material_id, 0, b"first".to_vec()).await
    });
    tokio::task::yield_now().await;
    {
        let guard = state_handle
            .try_lock()
            .expect("state mutex should stay free while duplicate waits on I/O lock");
        assert_eq!(guard.expected_offset, 0);
        assert!(guard.pending_write.is_some());
    }

    hook.release.notify_waiters();
    first.await??;
    second.await??;
    {
        let mut slot = SLICE_STAGING_IO_HOOK
            .lock()
            .map_err(|_| SinexError::invalid_state("slice staging hook mutex poisoned"))?;
        *slot = None;
    }

    let state = state_handle.lock().await;
    assert_eq!(state.expected_offset, 5);
    assert_eq!(state.slice_count, 1);
    assert!(state.pending_write.is_none());
    drop(state);
    let bytes = fs::read(&temp_path).await?;
    assert_eq!(bytes, b"first");
    Ok(())
}

#[sinex_test]
async fn material_end_wal_entry_forces_sync(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let (assembler, _content_store_dir, _state_dir) = test_assembler(&ctx).await?;
    let material_id = Uuid::now_v7();
    let mut state = assembler.create_placeholder_state(material_id).await?;

    append_wal_entry(
        &assembler,
        &mut state,
        WalEntry::Slice { offset: 0, len: 1 },
    )
    .await?;
    assert_eq!(state.wal_entries_since_sync, 1);

    append_wal_entry(
        &assembler,
        &mut state,
        WalEntry::End(MaterialEndMessage {
            material_id: material_id.to_string(),
            ended_at: Timestamp::now().format_rfc3339(),
            content_hash: blake3::hash(b"x").to_hex().to_string(),
            total_slices: 1,
            total_size_bytes: 1,
            metadata: json!({}),
        }),
    )
    .await?;

    assert_eq!(state.wal_entries_since_sync, 0);
    assert_eq!(state.wal_bytes_since_sync, 0);
    Ok(())
}

#[sinex_test]
async fn checked_buffered_slice_total_rejects_overflow() -> TestResult<()> {
    let error = checked_buffered_slice_total(i64::MAX, 1, Path::new("/tmp/overflow-slice"))
        .expect_err("buffered slice byte totals must not silently overflow");

    assert!(
        error
            .to_string()
            .contains("buffered slice byte total overflowed")
    );
    Ok(())
}

#[sinex_test]
async fn handle_slice_ignores_duplicate_buffered_offset_without_growing_state(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let (assembler, _content_store_dir, _state_dir) = test_assembler(&ctx).await?;

    let material_id = Uuid::now_v7();
    handle_slice(&assembler, material_id, 4, b"late".to_vec()).await?;
    handle_slice(&assembler, material_id, 4, b"late".to_vec()).await?;

    let state = assembler
        .get_state_handle(&material_id)
        .ok_or_else(|| SinexError::invalid_state("missing assembler state"))?;
    let state = state.lock().await;
    assert_eq!(state.buffered_slices.len(), 1);
    assert_eq!(state.buffered_bytes, 4);
    assert_eq!(state.total_staged_bytes(), 4);
    Ok(())
}

#[sinex_test]
async fn handle_slice_rejects_material_that_exceeds_size_limit(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let (assembler, _content_store_dir, _state_dir) =
        super::super::test_support::TestAssemblerBuilder::new("io-test")
            .max_material_size_bytes(8)
            .build(&ctx)
            .await?;

    let material_id = Uuid::now_v7();
    handle_slice(&assembler, material_id, 0, b"12345".to_vec()).await?;
    handle_slice(&assembler, material_id, 5, b"6789".to_vec()).await?;

    assert!(
        assembler.get_state_handle(&material_id).is_none(),
        "oversized material should be failed and cleaned up"
    );
    Ok(())
}

#[sinex_test]
async fn handle_slice_routes_buffered_slice_limit_overflow_to_dlq(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let (assembler, _content_store_dir, _state_dir) =
        super::super::test_support::TestAssemblerBuilder::new("io-test")
            .buffered_slice_limit(1)
            .build(&ctx)
            .await?;

    let dlq_subject = ctx.pipeline_namespace().subject("events.dlq.event_engine");
    let mut dlq_sub = ctx.nats_client().subscribe(dlq_subject).await?;
    let material_id = Uuid::now_v7();

    handle_slice(&assembler, material_id, 4, b"late".to_vec()).await?;
    handle_slice(&assembler, material_id, 8, b"later".to_vec()).await?;

    let msg = timeout(Duration::from_secs(Timeouts::SHORT), dlq_sub.next())
        .await?
        .ok_or_else(|| SinexError::invalid_state("missing DLQ message"))?;
    let payload: JsonValue = serde_json::from_slice(&msg.payload)?;
    assert_eq!(payload["error"], "buffered_slice_limit_exceeded");
    assert_eq!(payload["material_id"], material_id.to_string());
    assert_eq!(payload["context"]["offset"], 8);
    assert_eq!(payload["context"]["buffered_count"], 1);

    assert!(
        assembler.get_state_handle(&material_id).is_none(),
        "buffered slice overflow should fail the material instead of leaving retry state behind"
    );
    Ok(())
}

#[sinex_test]
async fn prune_stale_buffered_slices_removes_replayed_offsets() -> TestResult<()> {
    let dir = tempfile::tempdir()?;
    let stale_path = dir.path().join("0.bin");
    let future_path = dir.path().join("8.bin");
    tokio::fs::write(&stale_path, b"stale").await?;
    tokio::fs::write(&future_path, b"future").await?;

    let mut buffered = BTreeMap::from([(0, stale_path.clone()), (8, future_path.clone())]);
    prune_stale_buffered_slices(Uuid::now_v7(), 4, &mut buffered).await?;

    assert_eq!(buffered.keys().copied().collect::<Vec<_>>(), vec![8]);
    assert!(!stale_path.exists());
    assert!(future_path.exists());
    Ok(())
}

#[sinex_test]
async fn parse_material_state_folder_accepts_uuid_name() -> TestResult<()> {
    let material_id = Uuid::now_v7();
    let path = std::path::Path::new("/tmp").join(material_id.to_string());

    let parsed = parse_material_state_folder(&path)?;

    assert_eq!(parsed, material_id);
    Ok(())
}

#[sinex_test]
async fn parse_material_state_folder_rejects_non_uuid_name() -> TestResult<()> {
    let path = std::path::Path::new("/tmp").join("notes");

    let error = parse_material_state_folder(&path)
        .expect_err("non-UUID material state folders must surface explicit errors");

    assert!(error.to_string().contains("invalid material id"));
    assert!(error.to_string().contains("notes"));
    Ok(())
}

#[sinex_test]
async fn parse_wal_envelope_line_reports_error_and_preview() -> TestResult<()> {
    let error = parse_wal_envelope_line("{\"invalid\":")
        .expect_err("invalid WAL envelope JSON must surface parse context");

    assert!(error.contains("failed to parse WAL envelope JSON"));
    assert!(error.contains("wal_line={\"invalid\":"));
    Ok(())
}

#[sinex_test]
async fn restore_state_uses_wal_activity_for_last_slice_received(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let (assembler, _content_store_dir, state_dir) = test_assembler(&ctx).await?;
    let material_id = Uuid::now_v7();
    let material_dir = state_dir.path().join(material_id.to_string());
    tokio::fs::create_dir_all(&material_dir).await?;

    let wal_path = material_dir.join(WAL_FILE_NAME);
    write_wal_entry(
        &wal_path,
        WalEntry::Begin(super::super::state::MaterialBeginMessage {
            material_id: material_id.to_string(),
            material_kind: "test".to_string(),
            source_identifier: "test://restore".to_string(),
            metadata: json!({}),
            started_at: Timestamp::now().format_rfc3339(),
        }),
    )
    .await?;
    let wal_modified = Timestamp::from(tokio::fs::metadata(&wal_path).await?.modified()?);

    restore_state(&assembler).await?;

    let state = assembler
        .get_state_handle(&material_id)
        .expect("valid WAL state must restore");
    assert_eq!(state.lock().await.last_slice_received, wal_modified);
    Ok(())
}

#[sinex_test]
async fn restore_state_prefers_checkpoint_last_slice_received_over_wal_mtime(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let (assembler, _content_store_dir, state_dir) = test_assembler(&ctx).await?;
    let material_id = Uuid::now_v7();
    let material_dir = state_dir.path().join(material_id.to_string());
    tokio::fs::create_dir_all(&material_dir).await?;
    tokio::fs::write(material_dir.join(TEMP_FILE_NAME), &[]).await?;

    let persisted_last_slice_received = Timestamp::now();
    let stale_wal_mtime = std::time::SystemTime::now() - std::time::Duration::from_mins(2);
    write_wal_entry(
        &material_dir.join(WAL_FILE_NAME),
        WalEntry::Checkpoint(PersistedState {
            material_id: material_id.to_string(),
            expected_offset: 0,
            slice_count: 0,
            started_at: Timestamp::now().format_rfc3339(),
            last_slice_received: Some(persisted_last_slice_received.format_rfc3339()),
            material_kind: "test".to_string(),
            source_identifier: "test://restore".to_string(),
            metadata: json!({}),
            pending_write: None,
            pending_end: None,
            phase: AssemblyPhase::Accumulating,
        }),
    )
    .await?;
    std::fs::File::options()
        .append(true)
        .open(material_dir.join(WAL_FILE_NAME))?
        .set_modified(stale_wal_mtime)?;

    restore_state(&assembler).await?;

    let state = assembler
        .get_state_handle(&material_id)
        .expect("checkpoint-backed WAL state must restore");
    assert_eq!(
        state.lock().await.last_slice_received,
        persisted_last_slice_received
    );
    Ok(())
}

#[sinex_test]
async fn restore_state_promotes_fully_staged_pending_write(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let (assembler, _content_store_dir, state_dir) = test_assembler(&ctx).await?;
    let material_id = Uuid::now_v7();
    let material_dir = state_dir.path().join(material_id.to_string());
    tokio::fs::create_dir_all(&material_dir).await?;
    tokio::fs::write(material_dir.join(TEMP_FILE_NAME), b"data").await?;

    write_wal_entry(
        &material_dir.join(WAL_FILE_NAME),
        WalEntry::Checkpoint(PersistedState {
            material_id: material_id.to_string(),
            expected_offset: 0,
            slice_count: 0,
            started_at: Timestamp::now().format_rfc3339(),
            last_slice_received: None,
            material_kind: "test".to_string(),
            source_identifier: "test://restore".to_string(),
            metadata: json!({}),
            pending_write: Some(PendingWrite {
                offset: 0,
                len: 4,
                slice_count_delta: 1,
            }),
            pending_end: None,
            phase: AssemblyPhase::Accumulating,
        }),
    )
    .await?;

    restore_state(&assembler).await?;

    let state = assembler
        .get_state_handle(&material_id)
        .expect("fully staged pending write should restore as committed");
    let state = state.lock().await;
    assert_eq!(state.expected_offset, 4);
    assert_eq!(state.slice_count, 1);
    assert!(state.pending_write.is_none());
    Ok(())
}

#[sinex_test]
async fn restore_state_cleans_up_terminal_material_even_with_pending_end(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let (assembler, _content_store_dir, state_dir) = test_assembler(&ctx).await?;
    let material_id = Uuid::now_v7();
    let material_id_typed = Id::from_uuid(material_id);
    let material_dir = state_dir.path().join(material_id.to_string());
    tokio::fs::create_dir_all(&material_dir).await?;

    ctx.pool
        .source_materials()
        .register_external_in_flight(
            material_id,
            "test",
            Some("test://terminal-restore"),
            json!({}),
            Timestamp::now(),
        )
        .await?;
    ctx.pool
        .source_materials()
        .finalize_in_flight(material_id_typed, None, None, None, Some(0))
        .await?;

    write_wal_entry(
        &material_dir.join(WAL_FILE_NAME),
        WalEntry::Begin(super::super::state::MaterialBeginMessage {
            material_id: material_id.to_string(),
            material_kind: "test".to_string(),
            source_identifier: "test://terminal-restore".to_string(),
            metadata: json!({}),
            started_at: Timestamp::now().format_rfc3339(),
        }),
    )
    .await?;
    write_wal_entry(
        &material_dir.join(WAL_FILE_NAME),
        WalEntry::End(MaterialEndMessage {
            material_id: material_id.to_string(),
            ended_at: Timestamp::now().format_rfc3339(),
            content_hash: blake3::hash(b"").to_hex().to_string(),
            total_slices: 0,
            total_size_bytes: 0,
            metadata: json!({}),
        }),
    )
    .await?;

    restore_state(&assembler).await?;

    assert!(
        !material_dir.exists(),
        "terminal materials must not be resurrected from persisted pending_end state"
    );
    assert!(
        assembler.get_state_handle(&material_id).is_none(),
        "terminal materials must not occupy the active assembler set after restore"
    );
    Ok(())
}

#[sinex_test]
async fn restore_state_finalizes_complete_pending_end(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let (assembler, _content_store_dir, state_dir) = test_assembler(&ctx).await?;
    let material_id = Uuid::now_v7();
    let material_dir = state_dir.path().join(material_id.to_string());
    tokio::fs::create_dir_all(&material_dir).await?;

    ctx.pool
        .source_materials()
        .register_external_in_flight(
            material_id,
            "test",
            Some("test://empty-pending-end-restore"),
            json!({}),
            Timestamp::now(),
        )
        .await?;

    write_wal_entry(
        &material_dir.join(WAL_FILE_NAME),
        WalEntry::Begin(super::super::state::MaterialBeginMessage {
            material_id: material_id.to_string(),
            material_kind: "test".to_string(),
            source_identifier: "test://empty-pending-end-restore".to_string(),
            metadata: json!({}),
            started_at: Timestamp::now().format_rfc3339(),
        }),
    )
    .await?;
    write_wal_entry(
        &material_dir.join(WAL_FILE_NAME),
        WalEntry::End(MaterialEndMessage {
            material_id: material_id.to_string(),
            ended_at: Timestamp::now().format_rfc3339(),
            content_hash: blake3::hash(b"").to_hex().to_string(),
            total_slices: 0,
            total_size_bytes: 0,
            metadata: json!({}),
        }),
    )
    .await?;

    restore_state(&assembler).await?;

    assert!(
        !material_dir.exists(),
        "complete pending_end state should finalize during restore, not stay active"
    );
    assert!(
        assembler.get_state_handle(&material_id).is_none(),
        "finalized restored pending_end state must not occupy the active set"
    );
    let material = ctx
        .pool
        .source_materials()
        .get_by_id(Id::from_uuid(material_id))
        .await?
        .expect("material should still be tracked");
    assert_eq!(material.status, sinex_primitives::MaterialStatus::Failed);
    Ok(())
}

#[sinex_test]
async fn restore_state_cleans_up_assemblies_already_past_slice_timeout(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let (assembler, _content_store_dir, state_dir) =
        test_assembler_with_config(&ctx, 1).await?;
    let material_id = Uuid::now_v7();
    let material_dir = state_dir.path().join(material_id.to_string());
    tokio::fs::create_dir_all(&material_dir).await?;

    write_wal_entry(
        &material_dir.join(WAL_FILE_NAME),
        WalEntry::Begin(super::super::state::MaterialBeginMessage {
            material_id: material_id.to_string(),
            material_kind: "test".to_string(),
            source_identifier: "test://restore".to_string(),
            metadata: json!({}),
            started_at: Timestamp::now().format_rfc3339(),
        }),
    )
    .await?;

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    restore_state(&assembler).await?;

    assert!(
        !material_dir.exists(),
        "startup restore should drop assemblies already past the slice timeout"
    );
    assert!(
        assembler.assembler_state.is_empty(),
        "stale restored assemblies must not occupy the active set"
    );
    Ok(())
}

#[sinex_test]
async fn restore_state_cleans_up_stale_incomplete_pending_end(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let (assembler, _content_store_dir, state_dir) =
        test_assembler_with_config(&ctx, 1).await?;
    let material_id = Uuid::now_v7();
    let material_dir = state_dir.path().join(material_id.to_string());
    tokio::fs::create_dir_all(&material_dir).await?;

    write_wal_entry(
        &material_dir.join(WAL_FILE_NAME),
        WalEntry::Begin(super::super::state::MaterialBeginMessage {
            material_id: material_id.to_string(),
            material_kind: "test".to_string(),
            source_identifier: "test://incomplete-pending-end-restore".to_string(),
            metadata: json!({}),
            started_at: Timestamp::now().format_rfc3339(),
        }),
    )
    .await?;
    write_wal_entry(
        &material_dir.join(WAL_FILE_NAME),
        WalEntry::End(MaterialEndMessage {
            material_id: material_id.to_string(),
            ended_at: Timestamp::now().format_rfc3339(),
            content_hash: blake3::hash(b"incomplete").to_hex().to_string(),
            total_slices: 1,
            total_size_bytes: 10,
            metadata: json!({}),
        }),
    )
    .await?;

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    restore_state(&assembler).await?;

    assert!(
        !material_dir.exists(),
        "stale incomplete pending_end state should not be restored indefinitely"
    );
    assert!(
        assembler.get_state_handle(&material_id).is_none(),
        "stale incomplete pending_end state must not occupy the active set"
    );
    Ok(())
}

#[sinex_test]
async fn wal_line_preview_truncates_long_lines() -> TestResult<()> {
    let preview = wal_line_preview(&"a".repeat(200));
    assert_eq!(preview.chars().count(), 161);
    assert!(preview.ends_with('…'));
    Ok(())
}

#[sinex_test]
async fn restore_state_cleans_up_invalid_started_at_in_wal(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let (assembler, _content_store_dir, state_dir) = test_assembler(&ctx).await?;
    let material_id = Uuid::now_v7();
    let material_dir = state_dir.path().join(material_id.to_string());
    tokio::fs::create_dir_all(&material_dir).await?;

    write_wal_entry(
        &material_dir.join(WAL_FILE_NAME),
        WalEntry::Begin(super::super::state::MaterialBeginMessage {
            material_id: material_id.to_string(),
            material_kind: "test".to_string(),
            source_identifier: "test://restore".to_string(),
            metadata: json!({}),
            started_at: "not-a-timestamp".to_string(),
        }),
    )
    .await?;

    restore_state(&assembler).await?;

    assert!(
        !material_dir.exists(),
        "invalid WAL started_at should be quarantined and cleaned up"
    );
    assert!(
        assembler.assembler_state.is_empty(),
        "invalid WAL started_at must not restore an in-memory assembly"
    );
    Ok(())
}

#[sinex_test]
async fn restore_state_cleans_up_invalid_buffered_slice_filename(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let (assembler, _content_store_dir, state_dir) = test_assembler(&ctx).await?;
    let material_id = Uuid::now_v7();
    let material_dir = state_dir.path().join(material_id.to_string());
    tokio::fs::create_dir_all(material_dir.join(BUFFER_DIR_NAME)).await?;

    write_wal_entry(
        &material_dir.join(WAL_FILE_NAME),
        WalEntry::Begin(super::super::state::MaterialBeginMessage {
            material_id: material_id.to_string(),
            material_kind: "test".to_string(),
            source_identifier: "test://restore".to_string(),
            metadata: json!({}),
            started_at: Timestamp::now().format_rfc3339(),
        }),
    )
    .await?;

    tokio::fs::write(
        material_dir.join(BUFFER_DIR_NAME).join("bad-offset.slice"),
        b"slice",
    )
    .await?;

    restore_state(&assembler).await?;

    assert!(
        !material_dir.exists(),
        "invalid buffered slice filenames should be quarantined and cleaned up"
    );
    assert!(
        assembler.assembler_state.is_empty(),
        "invalid buffered slice filenames must not restore an in-memory assembly"
    );
    Ok(())
}

#[sinex_test]
async fn restore_state_cleans_up_partial_replay_after_corrupt_wal_line(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let (assembler, _content_store_dir, state_dir) = test_assembler(&ctx).await?;
    let material_id = Uuid::now_v7();
    let material_dir = state_dir.path().join(material_id.to_string());
    tokio::fs::create_dir_all(&material_dir).await?;

    let wal_path = material_dir.join(WAL_FILE_NAME);
    write_wal_entry(
        &wal_path,
        WalEntry::Begin(super::super::state::MaterialBeginMessage {
            material_id: material_id.to_string(),
            material_kind: "test".to_string(),
            source_identifier: "test://restore".to_string(),
            metadata: json!({}),
            started_at: Timestamp::now().format_rfc3339(),
        }),
    )
    .await?;
    tokio::fs::write(
        &wal_path,
        format!(
            "{}{}\n",
            tokio::fs::read_to_string(&wal_path).await?,
            "{\"invalid\":"
        ),
    )
    .await?;
    tokio::fs::write(material_dir.join(TEMP_FILE_NAME), b"abc").await?;

    restore_state(&assembler).await?;

    assert!(
        !material_dir.exists(),
        "corrupt replay state should be cleaned up instead of partially restored"
    );
    assert!(
        assembler.assembler_state.is_empty(),
        "no in-memory assembly should be restored from a corrupt WAL"
    );
    Ok(())
}

#[cfg(unix)]
#[sinex_test]
async fn parse_material_state_folder_rejects_non_utf8_name() -> TestResult<()> {
    let path = std::path::PathBuf::from("/tmp")
        .join(std::ffi::OsString::from_vec(vec![0x66, 0x6f, 0x80]));

    let err = parse_material_state_folder(&path)
        .expect_err("non-UTF-8 material state folders must surface explicit errors");

    assert!(err.to_string().contains("not valid UTF-8"));
    Ok(())
}
