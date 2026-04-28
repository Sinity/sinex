//! I/O operations for `MaterialAssembler`.
//!
//! This module contains all file system operations, buffering logic, and git-annex
//! interactions for the material assembler. Extracted to keep the main module
//! focused on state management and orchestration.

use super::{
    MaterialAssembler,
    assembly_state_machine::{
        AssemblyInput, AssemblyLogicalState, AssemblyStateMachine, AssemblyTransition,
    },
    durability::DurabilityPolicy,
    finalize::PendingEndBehavior,
    restore_plan::{
        ReplayedState, RestoreClassification, RestorePlan, RestorePlanInput, derive_restore_plan,
    },
    state::{
        AssemblerState, AssemblyPhase, BUFFER_DIR_NAME, FinalizationState, PendingWrite,
        PersistedState, TEMP_FILE_NAME, WAL_FILE_NAME, WalEntry, WalEntryEnvelope,
        parse_material_started_at,
    },
};
use crate::{IngestdResult, SinexError};
use blake3::Hasher;
use camino::Utf8PathBuf;
use sinex_node_sdk::content_store::ContentStoreKey;
use sinex_primitives::Timestamp;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Instant;
use tokio::{fs, fs::File, io::AsyncReadExt, io::AsyncWriteExt};
use tracing::{debug, info, warn};
use uuid::Uuid;

/// Restore persisted assembler state on startup by replaying the WAL
///
/// # Edge Cases
///
/// - **Corrupt WAL entries**: If WAL replay encounters malformed or invalid envelope entries,
///   the persisted state is treated as incompatible and cleaned up instead of resuming from a
///   truncated replay.
/// - **Terminal materials with incomplete state**: If a material is marked terminal in the
///   database but the WAL shows incomplete assembly (missing end or buffered slices), the
///   state is cleaned up as stale.
pub(super) async fn restore_state(assembler: &MaterialAssembler) -> IngestdResult<()> {
    let mut entries = match fs::read_dir(&assembler.state_root).await {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(err) => {
            return Err(SinexError::io(format!(
                "Failed to read assembler state root {}",
                assembler.state_root.display()
            ))
            .with_source(err));
        }
    };

    while let Some(entry) = entries
        .next_entry()
        .await
        .map_err(|e| SinexError::io("Failed to iterate state directory").with_source(e))?
    {
        let path = entry.path();
        if !entry
            .file_type()
            .await
            .map_err(|e| SinexError::io("Failed to inspect state entry").with_source(e))?
            .is_dir()
        {
            continue;
        }

        let material_id = match parse_material_state_folder(&path) {
            Ok(material_id) => material_id,
            Err(error @ SinexError::InvalidState(_)) => {
                warn!(
                    path = %path.display(),
                    error = %error,
                    "Assembler state entry is invalid; cleaning it up and continuing"
                );
                cleanup_state_path(&path).await;
                continue;
            }
            Err(error) => return Err(error),
        };

        match restore_state_params(assembler, material_id, &path).await {
            Ok(Some(restored)) => {
                let state_handle = assembler.insert_state_handle(material_id, restored.state);
                info!(material_id = %material_id, "Restored in-flight material state from WAL");
                if restored.should_finalize_pending_end {
                    assembler
                        .try_finalize_pending_end(
                            material_id,
                            state_handle,
                            PendingEndBehavior::Ignore,
                        )
                        .await?;
                }
            }
            Ok(None) => {}
            Err(error @ SinexError::InvalidState(_)) => {
                warn!(
                    material_id = %material_id,
                    error = %error,
                    "Persisted material state is invalid; cleaning it up and continuing"
                );
                cleanup_state_path(&path).await;
            }
            Err(error) => return Err(error),
        }
    }

    Ok(())
}

struct RestoredAssemblerState {
    state: AssemblerState,
    should_finalize_pending_end: bool,
}

fn parse_material_state_folder(path: &std::path::Path) -> IngestdResult<Uuid> {
    let folder_name = path.file_name().ok_or_else(|| {
        SinexError::invalid_state(format!(
            "Assembler state folder {} is missing a file name",
            path.display()
        ))
    })?;

    let folder_name = folder_name.to_str().ok_or_else(|| {
        SinexError::invalid_state(format!(
            "Assembler state folder {} is not valid UTF-8",
            path.display()
        ))
    })?;

    Uuid::from_str(folder_name).map_err(|error| {
        SinexError::invalid_state("Assembler state folder has invalid material id")
            .with_context("path", path.display().to_string())
            .with_context("folder_name", folder_name.to_string())
            .with_std_error(&error)
    })
}

fn state_path_has_recoverable_artifacts(state_dir: &Path, temp_path: &Path) -> bool {
    temp_path.exists() || state_dir.join(BUFFER_DIR_NAME).exists()
}

fn log_restore_plan(plan: &RestorePlan) {
    let trace = plan
        .trace
        .iter()
        .map(|entry| format!("{}={}", entry.code, entry.detail))
        .collect::<Vec<_>>()
        .join("; ");

    info!(
        material_id = %plan.material_id,
        classification = %plan.classification,
        trace,
        "Derived material restore plan"
    );
}

async fn apply_non_restoring_plan(
    assembler: &MaterialAssembler,
    plan: &RestorePlan,
) -> IngestdResult<Option<RestoredAssemblerState>> {
    match &plan.classification {
        RestoreClassification::Discard { .. } if plan.cleanup_state() => {
            cleanup_state(assembler, plan.material_id).await;
        }
        RestoreClassification::Quarantine { reason } => {
            warn!(
                material_id = %plan.material_id,
                reason = ?reason,
                "Quarantining persisted material state for operator review"
            );
        }
        _ => {}
    }

    Ok(None)
}

async fn restore_state_params(
    assembler: &MaterialAssembler,
    material_id: Uuid,
    state_dir: &std::path::Path,
) -> IngestdResult<Option<RestoredAssemblerState>> {
    let wal_path = state_dir.join(WAL_FILE_NAME);
    let temp_path = state_dir.join(TEMP_FILE_NAME);

    if !wal_path.exists() {
        let plan = derive_restore_plan(RestorePlanInput {
            material_id,
            wal_present: false,
            has_state_artifacts: state_path_has_recoverable_artifacts(state_dir, &temp_path),
            replay_corrupted: false,
            has_envelope_entries: false,
            has_non_empty_lines: false,
            material_terminal: false,
            file_progress_error: None,
            stale: false,
            replayed_state: None,
        });
        log_restore_plan(&plan);
        return apply_non_restoring_plan(assembler, &plan).await;
    }

    // Open WAL for reading
    let mut wal_file = File::open(&wal_path).await.map_err(|e| {
        SinexError::io(format!("Failed to open WAL for {material_id}")).with_source(e)
    })?;

    // Replay WAL lines in envelope format (with CRC).
    let mut state_snapshot = ReplayedState::default();
    let mut content_buffer = Vec::new();
    wal_file
        .read_to_end(&mut content_buffer)
        .await
        .map_err(|e| {
            SinexError::io(format!("Failed to read WAL for {material_id}")).with_source(e)
        })?;

    let content = String::from_utf8_lossy(&content_buffer);
    let mut max_seq: u64 = 0;
    let mut has_envelope_entries = false;
    let mut has_non_empty_lines = false;
    let mut replay_corrupted = false;

    for (line_num, line) in content.lines().enumerate() {
        if line.is_empty() {
            continue;
        }
        has_non_empty_lines = true;

        // Extract the raw "entry" field bytes from the line for CRC verification
        // before parsing the full envelope. This avoids the round-trip serialization
        // vulnerability where a serde_json version change could alter key ordering
        // and invalidate all existing WAL entries.
        let entry_crc_ok = match extract_raw_entry_bytes(line) {
            Some(raw_bytes) => {
                let parsed_envelope: Result<WalEntryEnvelope, _> = serde_json::from_str(line);
                match parsed_envelope {
                    Ok(ref env) => crc32fast::hash(raw_bytes.as_bytes()) == env.crc,
                    Err(_) => false,
                }
            }
            None => false,
        };
        match parse_wal_envelope_line(line) {
            Ok(envelope) => {
                if !entry_crc_ok {
                    warn!(
                        material_id = %material_id,
                        line = line_num + 1,
                        seq = envelope.seq,
                        expected_crc = envelope.crc,
                        "WAL CRC mismatch — corruption detected, stopping replay"
                    );
                    replay_corrupted = true;
                    break;
                }
                if envelope.seq > max_seq {
                    max_seq = envelope.seq;
                }
                has_envelope_entries = true;
                state_snapshot.apply(envelope.entry);
            }
            Err(error) => {
                warn!(
                    material_id = %material_id,
                    line = line_num + 1,
                    error = %error,
                    "WAL replay error — invalid envelope entry, stopping replay"
                );
                replay_corrupted = true;
                break;
            }
        }
    }

    if replay_corrupted {
        warn!(
            material_id = %material_id,
            "WAL replay encountered corruption; cleaning up incompatible persisted state"
        );
        let plan = derive_restore_plan(RestorePlanInput {
            material_id,
            wal_present: true,
            has_state_artifacts: true,
            replay_corrupted: true,
            has_envelope_entries,
            has_non_empty_lines,
            material_terminal: false,
            file_progress_error: None,
            stale: false,
            replayed_state: None,
        });
        log_restore_plan(&plan);
        return apply_non_restoring_plan(assembler, &plan).await;
    }

    if !has_envelope_entries {
        if has_non_empty_lines {
            warn!(
                material_id = %material_id,
                "WAL contains no valid envelope entries; cleaning up incompatible or corrupt state"
            );
        }
        let plan = derive_restore_plan(RestorePlanInput {
            material_id,
            wal_present: true,
            has_state_artifacts: true,
            replay_corrupted: false,
            has_envelope_entries: false,
            has_non_empty_lines,
            material_terminal: false,
            file_progress_error: None,
            stale: false,
            replayed_state: None,
        });
        log_restore_plan(&plan);
        return apply_non_restoring_plan(assembler, &plan).await;
    }

    if assembler.material_is_terminal(material_id).await? {
        info!(
            material_id = %material_id,
            "Persisted assembler state belongs to an already terminal material; cleaning it up"
        );
        let plan = derive_restore_plan(RestorePlanInput {
            material_terminal: true,
            ..RestorePlanInput::from_replayed(material_id, &state_snapshot)
        });
        log_restore_plan(&plan);
        return apply_non_restoring_plan(assembler, &plan).await;
    }

    // Resume sequence numbering from where the WAL left off
    let next_seq = max_seq + 1;
    match reconcile_replayed_file_progress(material_id, &temp_path, &mut state_snapshot).await {
        Ok(()) => {}
        Err(error) => {
            let error_message = error.to_string();
            warn!(
                material_id = %material_id,
                error = %error,
                "Persisted material file progress is inconsistent with WAL; cleaning it up"
            );
            let plan = derive_restore_plan(RestorePlanInput {
                file_progress_error: Some(error_message),
                ..RestorePlanInput::from_replayed(material_id, &state_snapshot)
            });
            log_restore_plan(&plan);
            return apply_non_restoring_plan(assembler, &plan).await;
        }
    }

    // Reopen WAL in append mode for the live state
    let wal_append = fs::OpenOptions::new()
        .append(true)
        .create(true)
        .open(&wal_path)
        .await
        .map_err(|e| {
            SinexError::io(format!("Failed to open WAL for appending {material_id}")).with_source(e)
        })?;

    // Rebuild Hasher & Temp File handle
    let temp_file = if temp_path.exists() {
        Some(
            File::options()
                .create(true)
                .append(true)
                .open(&temp_path)
                .await
                .map_err(|e| SinexError::io("Failed to open temp file").with_source(e))?,
        )
    } else {
        None
    };

    let hasher = rebuild_hasher(&temp_path).await?;
    let mut buffered_slices = load_buffered_slices(&state_dir.join(BUFFER_DIR_NAME)).await?;
    prune_stale_buffered_slices(
        material_id,
        state_snapshot.expected_offset,
        &mut buffered_slices,
    )
    .await?;
    let buffered_bytes = buffered_slice_bytes(&buffered_slices).await?;
    let last_slice_received = restore_last_slice_received(
        material_id,
        &wal_path,
        state_snapshot.last_slice_received.as_deref(),
    )?;

    let stale = restored_state_is_stale(
        &state_snapshot,
        &buffered_slices,
        last_slice_received,
        assembler.slice_arrival_timeout,
    );
    let plan = derive_restore_plan(RestorePlanInput {
        stale,
        ..RestorePlanInput::from_replayed(material_id, &state_snapshot)
    });
    log_restore_plan(&plan);
    if !plan.restores_state() {
        info!(
            material_id = %material_id,
            elapsed_secs = (Timestamp::now() - last_slice_received).whole_seconds(),
            "Restored assembly does not resume from persisted state"
        );
        return apply_non_restoring_plan(assembler, &plan).await;
    }
    let should_finalize_pending_end =
        matches!(plan.classification, RestoreClassification::Finalize { .. });

    Ok(Some(RestoredAssemblerState {
        state: AssemblerState {
            material_id,
            temp_path,
            temp_file,
            wal_file: Some(wal_append),
            wal_seq: next_seq,
            expected_offset: state_snapshot.expected_offset,
            slice_count: state_snapshot.slice_count,
            buffered_slices,
            buffered_bytes,
            state_dir: state_dir.to_path_buf(),
            started_at: parse_material_started_at(
                material_id,
                &state_snapshot.started_at,
                "restored WAL state",
            )?,
            material_kind: state_snapshot.material_kind,
            source_identifier: state_snapshot.source_identifier,
            metadata: state_snapshot.metadata,
            phase: state_snapshot.phase,
            hasher,
            pending_write: state_snapshot.pending_write,
            pending_end: state_snapshot.pending_end,
            last_slice_received,
            staged_bytes_since_sync: 0,
            wal_entries_since_sync: 0,
            wal_bytes_since_sync: 0,
            last_staged_sync: Instant::now(),
            last_wal_sync: Instant::now(),
        },
        should_finalize_pending_end,
    }))
}

fn restore_last_slice_received(
    material_id: Uuid,
    wal_path: &Path,
    persisted_last_slice_received: Option<&str>,
) -> IngestdResult<Timestamp> {
    if let Some(last_slice_received) = persisted_last_slice_received {
        return Timestamp::parse_rfc3339(last_slice_received).map_err(|error| {
            SinexError::invalid_state(format!(
                "Failed to parse persisted last_slice_received during restore (material {material_id})"
            ))
            .with_std_error(&error)
        });
    }

    let modified = std::fs::metadata(wal_path)
        .and_then(|metadata| metadata.modified())
        .map_err(|error| {
            SinexError::io(format!(
                "Failed to read WAL modification time during restore (material {material_id})"
            ))
            .with_source(error)
        })?;
    Ok(Timestamp::from(modified))
}

fn restored_state_is_stale(
    state_snapshot: &ReplayedState,
    buffered_slices: &BTreeMap<i64, PathBuf>,
    last_slice_received: Timestamp,
    slice_arrival_timeout: std::time::Duration,
) -> bool {
    if state_snapshot.phase == AssemblyPhase::Finalizing {
        return false;
    }

    let elapsed = Timestamp::now() - last_slice_received;
    let pending_end_blocked = state_snapshot.pending_end.is_some()
        && !restored_pending_end_is_complete(state_snapshot, buffered_slices);
    elapsed.whole_seconds() > slice_arrival_timeout.as_secs() as i64
        && (state_snapshot.pending_end.is_none()
            || pending_end_blocked
            || !buffered_slices.is_empty()
            || state_snapshot.phase == AssemblyPhase::PendingBegin)
}

fn restored_pending_end_is_complete(
    state_snapshot: &ReplayedState,
    buffered_slices: &BTreeMap<i64, PathBuf>,
) -> bool {
    let Some(end) = &state_snapshot.pending_end else {
        return false;
    };

    state_snapshot.phase != AssemblyPhase::PendingBegin
        && buffered_slices.is_empty()
        && state_snapshot.expected_offset == end.total_size_bytes
        && state_snapshot.slice_count == end.total_slices
}

fn parse_wal_envelope_line(line: &str) -> Result<WalEntryEnvelope, String> {
    serde_json::from_str::<WalEntryEnvelope>(line).map_err(|error| {
        format!(
            "failed to parse WAL envelope JSON: {error}; wal_line={}",
            wal_line_preview(line)
        )
    })
}

/// Extract the raw JSON bytes of the "entry" field from a WAL line without
/// re-serializing. This preserves the original byte sequence for CRC verification,
/// avoiding sensitivity to `serde_json` key-ordering changes across versions.
fn extract_raw_entry_bytes(line: &str) -> Option<&str> {
    // The envelope format is: {"seq":N,"crc":C,"entry":{...}}
    // Find the "entry": key and extract everything from the opening { to the matching }.
    let entry_key = "\"entry\":";
    let key_pos = line.find(entry_key)?;
    let value_start = key_pos + entry_key.len();
    let rest = &line[value_start..];
    // Find the start of the entry value (skip whitespace)
    let trimmed = rest.trim_start();
    if !trimmed.starts_with('{') {
        return None;
    }
    // Track brace depth to find the matching closing brace
    let mut depth = 0i32;
    let mut in_string = false;
    let mut escape_next = false;
    for (i, ch) in trimmed.char_indices() {
        if escape_next {
            escape_next = false;
            continue;
        }
        match ch {
            '\\' if in_string => escape_next = true,
            '"' => in_string = !in_string,
            '{' if !in_string => depth += 1,
            '}' if !in_string => {
                depth -= 1;
                if depth == 0 {
                    return Some(&trimmed[..=i]);
                }
            }
            _ => {}
        }
    }
    None
}

fn wal_line_preview(line: &str) -> String {
    const MAX_PREVIEW_CHARS: usize = 160;
    let mut preview = line.chars().take(MAX_PREVIEW_CHARS).collect::<String>();
    if line.chars().count() > MAX_PREVIEW_CHARS {
        preview.push('…');
    }
    preview
}

async fn reconcile_replayed_file_progress(
    material_id: Uuid,
    temp_path: &Path,
    state_snapshot: &mut ReplayedState,
) -> IngestdResult<()> {
    let actual_size = staged_file_size_bytes(temp_path).await?;

    if let Some(pending_write) = state_snapshot.pending_write.clone() {
        let committed_size = pending_write
            .offset
            .checked_add(pending_write.len as i64)
            .ok_or_else(|| {
                SinexError::invalid_state("pending_write length overflowed restored material size")
                    .with_context("material_id", material_id.to_string())
                    .with_context("offset", pending_write.offset.to_string())
                    .with_context("len", pending_write.len.to_string())
            })?;

        if actual_size == state_snapshot.expected_offset {
            debug!(
                material_id = %material_id,
                offset = pending_write.offset,
                len = pending_write.len,
                "Dropped restored pending_write that never reached the staging file"
            );
            state_snapshot.pending_write = None;
            return Ok(());
        }

        if actual_size == committed_size {
            state_snapshot.expected_offset = committed_size;
            state_snapshot.slice_count = state_snapshot
                .slice_count
                .saturating_add(pending_write.slice_count_delta);
            state_snapshot.pending_write = None;
            debug!(
                material_id = %material_id,
                offset = pending_write.offset,
                len = pending_write.len,
                "Promoted restored pending_write into committed material progress"
            );
            return Ok(());
        }

        return Err(
            SinexError::invalid_state("pending_write does not match staged file progress")
                .with_context("material_id", material_id.to_string())
                .with_context(
                    "expected_offset",
                    state_snapshot.expected_offset.to_string(),
                )
                .with_context("pending_offset", pending_write.offset.to_string())
                .with_context("pending_len", pending_write.len.to_string())
                .with_context("actual_size", actual_size.to_string()),
        );
    }

    if actual_size != state_snapshot.expected_offset {
        return Err(SinexError::invalid_state(
            "staged file size does not match restored WAL progress",
        )
        .with_context("material_id", material_id.to_string())
        .with_context(
            "expected_offset",
            state_snapshot.expected_offset.to_string(),
        )
        .with_context("actual_size", actual_size.to_string()));
    }

    Ok(())
}

async fn staged_file_size_bytes(temp_path: &Path) -> IngestdResult<i64> {
    match fs::metadata(temp_path).await {
        Ok(metadata) => i64::try_from(metadata.len()).map_err(|error| {
            SinexError::invalid_state("staged file length exceeds i64 range")
                .with_context("path", temp_path.display().to_string())
                .with_std_error(&error)
        }),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(error) => Err(SinexError::io(format!(
            "Failed to stat staged file {}",
            temp_path.display()
        ))
        .with_source(error)),
    }
}

fn checkpoint_snapshot(state: &AssemblerState) -> PersistedState {
    PersistedState {
        material_id: state.material_id.to_string(),
        expected_offset: state.expected_offset,
        slice_count: state.slice_count,
        started_at: state.started_at.format_rfc3339(),
        last_slice_received: Some(state.last_slice_received.format_rfc3339()),
        material_kind: state.material_kind.clone(),
        source_identifier: state.source_identifier.clone(),
        metadata: state.metadata.clone(),
        pending_write: state.pending_write.clone(),
        pending_end: state.pending_end.clone(),
        phase: state.phase,
    }
}

async fn rebuild_hasher(temp_path: &PathBuf) -> IngestdResult<Hasher> {
    let mut hasher = Hasher::new();
    if temp_path.exists() {
        let mut file = fs::File::open(&temp_path).await.map_err(|e| {
            SinexError::io(format!(
                "Failed to open temp file for hasher rebuild {}: {}",
                temp_path.display(),
                e
            ))
        })?;
        // Stream in 64 KiB chunks instead of reading the entire file into memory.
        // A 512 MiB material would otherwise allocate a single 512 MiB buffer.
        let mut buf = vec![0u8; 64 * 1024];
        loop {
            let n = file.read(&mut buf).await.map_err(|e| {
                SinexError::io(format!(
                    "Failed to read temp file during hasher rebuild {}: {}",
                    temp_path.display(),
                    e
                ))
            })?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
        }
    }
    Ok(hasher)
}

async fn load_buffered_slices(buffers_dir: &PathBuf) -> IngestdResult<BTreeMap<i64, PathBuf>> {
    let mut buffered_slices = BTreeMap::new();
    if !buffers_dir.exists() {
        return Ok(buffered_slices);
    }

    let mut buffer_entries = fs::read_dir(&buffers_dir).await.map_err(|e| {
        SinexError::io(format!(
            "Failed to read buffer dir {}",
            buffers_dir.display()
        ))
        .with_source(e)
    })?;

    while let Some(buf_entry) = buffer_entries
        .next_entry()
        .await
        .map_err(|e| SinexError::io("Failed to iterate buffered slices").with_source(e))?
    {
        let buf_path = buf_entry.path();
        if !buf_entry
            .file_type()
            .await
            .map_err(|e| SinexError::io("Failed to inspect buffered slice").with_source(e))?
            .is_file()
        {
            continue;
        }

        let offset = parse_buffered_slice_offset(&buf_path)?;

        buffered_slices.insert(offset, buf_path);
    }

    Ok(buffered_slices)
}

fn parse_buffered_slice_offset(path: &std::path::Path) -> IngestdResult<i64> {
    let stem = path.file_stem().ok_or_else(|| {
        SinexError::invalid_state("Buffered slice is missing a file stem")
            .with_context("path", path.display().to_string())
    })?;
    let stem = stem.to_str().ok_or_else(|| {
        SinexError::invalid_state("Buffered slice file name is not valid UTF-8")
            .with_context("path", path.display().to_string())
    })?;
    stem.parse::<i64>().map_err(|error| {
        SinexError::invalid_state("Buffered slice file name has invalid offset")
            .with_context("path", path.display().to_string())
            .with_context("file_stem", stem.to_string())
            .with_std_error(&error)
    })
}

async fn buffered_slice_bytes(buffered_slices: &BTreeMap<i64, PathBuf>) -> IngestdResult<i64> {
    let mut total = 0i64;
    for path in buffered_slices.values() {
        let metadata = fs::metadata(path).await.map_err(|e| {
            SinexError::io(format!("Failed to stat buffered slice {}", path.display()))
                .with_source(e)
        })?;
        let slice_bytes = buffered_slice_file_len_bytes(path, metadata.len())?;
        total = checked_buffered_slice_total(total, slice_bytes, path)?;
    }
    Ok(total)
}

fn buffered_slice_file_len_bytes(path: &Path, len: u64) -> IngestdResult<i64> {
    i64::try_from(len).map_err(|error| {
        SinexError::processing("buffered slice length exceeds i64 range")
            .with_context("path", path.display().to_string())
            .with_context("slice_len_bytes", len.to_string())
            .with_std_error(&error)
    })
}

fn checked_buffered_slice_total(total: i64, slice_bytes: i64, path: &Path) -> IngestdResult<i64> {
    total.checked_add(slice_bytes).ok_or_else(|| {
        SinexError::processing("buffered slice byte total overflowed")
            .with_context("path", path.display().to_string())
            .with_context("current_total_bytes", total.to_string())
            .with_context("slice_len_bytes", slice_bytes.to_string())
    })
}

async fn prune_stale_buffered_slices(
    material_id: Uuid,
    expected_offset: i64,
    buffered_slices: &mut BTreeMap<i64, PathBuf>,
) -> IngestdResult<()> {
    let stale_offsets = buffered_slices
        .keys()
        .copied()
        .filter(|offset| *offset < expected_offset)
        .collect::<Vec<_>>();

    for offset in stale_offsets {
        if let Some(path) = buffered_slices.remove(&offset) {
            if let Err(error) = fs::remove_file(&path).await {
                warn!(
                    material_id = %material_id,
                    offset,
                    path = %path.display(),
                    error = %error,
                    "Failed to remove stale buffered slice recovered from disk"
                );
            } else {
                debug!(
                    material_id = %material_id,
                    offset,
                    path = %path.display(),
                    "Dropped stale buffered slice recovered from disk"
                );
            }
        }
    }

    Ok(())
}

/// Append an entry to the WAL, wrapped in a `WalEntryEnvelope` with CRC32 checksum.
///
/// Each entry is serialized as `{"seq":N,"crc":CHECKSUM,"entry":{...}}\n` and fsync'd.
/// The CRC is computed over the serialized `entry` JSON, allowing recovery to detect
/// corruption (bit-flips, partial writes) before applying the entry.
///
/// Error construction below uses `SinexError::io(msg).with_source(e)` inline throughout.
/// Each site carries a distinct message that identifies the precise failure point
/// (open, write, newline, sync), so extracting a shared helper would only obscure that
/// specificity. The pattern is intentionally repeated rather than abstracted.
pub(super) async fn append_wal_entry(
    assembler: &MaterialAssembler,
    state: &mut AssemblerState,
    entry: WalEntry,
) -> IngestdResult<()> {
    let force_sync = matches!(entry, WalEntry::Begin(_) | WalEntry::End(_));

    // Ensure WAL file is open
    if state.wal_file.is_none() {
        fs::create_dir_all(&state.state_dir)
            .await
            .map_err(|e| SinexError::io("Failed to ensure assembler state dir").with_source(e))?;

        let mut opts = fs::OpenOptions::new();
        opts.create(true).append(true).write(true);

        let file = opts
            .open(&state.state_dir.join(WAL_FILE_NAME))
            .await
            .map_err(|e| SinexError::io("Failed to open WAL file").with_source(e))?;
        state.wal_file = Some(file);
    }

    // Serialize the entry, compute CRC, wrap in envelope
    let entry_json = serde_json::to_vec(&entry).map_err(|e| {
        SinexError::serialization("failed to serialize WAL entry").with_std_error(&e)
    })?;
    let crc = crc32fast::hash(&entry_json);
    let seq = state.wal_seq;
    state.wal_seq += 1;

    let envelope = WalEntryEnvelope { seq, crc, entry };
    let serialized = serde_json::to_string(&envelope).map_err(|e| {
        SinexError::serialization("failed to serialize WAL envelope").with_std_error(&e)
    })?;
    let wal_bytes = serialized.len().saturating_add(1);

    if let Some(file) = state.wal_file.as_mut() {
        file.write_all(serialized.as_bytes())
            .await
            .map_err(|e| SinexError::io("WAL write failed").with_source(e))?;
        file.write_all(b"\n")
            .await
            .map_err(|e| SinexError::io("WAL write newline failed").with_source(e))?;
        assembler
            .durability_policy
            .flush_wal_after_append(file)
            .await?;
    }

    state.wal_entries_since_sync = state.wal_entries_since_sync.saturating_add(1);
    state.wal_bytes_since_sync = state.wal_bytes_since_sync.saturating_add(wal_bytes);
    assembler
        .durability_policy
        .sync_wal_if_needed(state, force_sync)
        .await?;

    Ok(())
}

pub(super) async fn sync_staged_file_for_finalization(
    assembler: &MaterialAssembler,
    state: &mut AssemblerState,
    material_id: Uuid,
) -> IngestdResult<()> {
    assembler
        .durability_policy
        .sync_staged_file_if_needed(state, material_id, true)
        .await
}

/// Remove the persisted state directory for a material
pub(super) async fn cleanup_state(assembler: &MaterialAssembler, material_id: Uuid) {
    let path = assembler.state_root.join(material_id.to_string());
    cleanup_state_path(&path).await;
}

async fn cleanup_state_path(path: &Path) {
    let temp_path = path.join(TEMP_FILE_NAME);
    let buffers_dir = path.join(BUFFER_DIR_NAME);

    if temp_path.exists()
        && let Err(e) = fs::remove_file(&temp_path).await
    {
        warn!(
            path = %temp_path.display(),
            "Failed to remove temp file: {}",
            e
        );
    }

    if buffers_dir.exists()
        && let Err(e) = fs::remove_dir_all(&buffers_dir).await
    {
        warn!(
            path = %buffers_dir.display(),
            "Failed to remove buffers directory: {}",
            e
        );
    }

    if let Err(e) = fs::remove_dir_all(&path).await {
        warn!(
            path = %path.display(),
            "Failed to remove assembler state directory: {}",
            e
        );
    }
}

/// Store a slice (in-order or buffered) for a material
///
/// # Edge Cases
///
/// - **Early slice arrival**: Slices may arrive before local begin state after WAL restore,
///   redelivery, or non-SDK publishers. A placeholder state is created to buffer slices until
///   begin arrives.
/// - **Race condition on placeholder creation**: Multiple slices arriving concurrently for
///   a new material may attempt to create placeholders. `insert_state_handle` handles this
///   via `DashMap`'s entry API, ensuring only one placeholder wins.
/// - **Dropped late slices**: If a material is already terminal (completed/failed), late-arriving
///   slices are silently dropped to avoid resurrection of completed assemblies.
#[tracing::instrument(skip(assembler, data), fields(data_len = data.len(), lock_acquire_ms, lock_hold_ms))]
pub(super) async fn handle_slice(
    assembler: &MaterialAssembler,
    material_id: Uuid,
    offset: i64,
    data: Vec<u8>,
) -> IngestdResult<()> {
    let state_handle = if let Some(existing) = assembler.get_state_handle(&material_id) {
        existing
    } else {
        let transition = if let Some(terminal_state) =
            assembler.material_terminal_state(material_id).await?
        {
            AssemblyStateMachine::transition(terminal_state, AssemblyInput::SliceFrame)
        } else {
            AssemblyStateMachine::transition(AssemblyLogicalState::Idle, AssemblyInput::SliceFrame)
        }
        .map_err(|error| error.into_sinex_error(material_id))?;

        if matches!(transition, AssemblyTransition::IgnoreTerminalFrame) {
            debug!(
                material_id = %material_id,
                offset,
                transition = ?transition,
                "Dropping slice for terminal material"
            );
            return Ok(());
        }
        debug!(
            material_id = %material_id,
            offset,
            transition = ?transition,
            "Assembly state machine accepted slice for new material state"
        );
        let placeholder = assembler.create_placeholder_state(material_id).await?;
        assembler.insert_state_handle(material_id, placeholder)
    };

    let acquire_start = std::time::Instant::now();
    let mut state = state_handle.lock().await;
    let acquire_ms = acquire_start.elapsed().as_millis() as u64;
    tracing::Span::current().record("lock_acquire_ms", acquire_ms);
    if acquire_ms > 50 {
        warn!(material_id = %material_id, acquire_ms, "Slow lock acquisition in handle_slice");
    }
    let hold_start = std::time::Instant::now();

    let transition = AssemblyStateMachine::transition_for_state(&state, AssemblyInput::SliceFrame)
        .map_err(|error| error.into_sinex_error(material_id))?;

    if matches!(transition, AssemblyTransition::IgnoreFinalizingFrame) {
        debug!(
            material_id = %material_id,
            offset,
            transition = ?transition,
            "Ignoring slice received while material is finalizing"
        );
        return Ok(());
    }
    debug!(
        material_id = %material_id,
        offset,
        transition = ?transition,
        "Assembly state machine accepted slice for existing material state"
    );

    // Update last slice received timestamp
    state.last_slice_received = Timestamp::now();

    use std::cmp::Ordering;
    match offset.cmp(&state.expected_offset) {
        Ordering::Equal => {
            let projected_total = state.total_staged_bytes().saturating_add(data.len() as i64);
            if projected_total > assembler.max_material_size_bytes {
                let current_total = state.total_staged_bytes();
                let buffered_count = state.buffered_slices.len();
                let expected_offset = state.expected_offset;
                let resume_phase = state.phase;
                state.phase = AssemblyPhase::Finalizing;
                drop(state);

                assembler
                    .route_material_error(
                        material_id,
                        "material_size_limit_exceeded",
                        serde_json::json!({
                            "offset": offset,
                            "incoming_bytes": data.len(),
                            "expected_offset": expected_offset,
                            "buffered_count": buffered_count,
                            "current_total_bytes": current_total,
                            "projected_total_bytes": projected_total,
                            "max_material_size_bytes": assembler.max_material_size_bytes,
                        }),
                    )
                    .await;
                assembler
                    .finalize_failed_material_claimed_checked(
                        material_id,
                        "material_size_limit_exceeded",
                        resume_phase,
                    )
                    .await?;
                return Ok(());
            }

            append_slice_data(assembler, &mut state, material_id, &data).await?;
            flush_buffered_slices(assembler, &mut state, material_id).await?;
        }
        Ordering::Greater => {
            if state.buffered_slices.contains_key(&offset) {
                debug!(
                    material_id = %material_id,
                    offset,
                    expected = state.expected_offset,
                    "Ignoring duplicate buffered slice"
                );
            } else if state.buffered_slices.len() >= assembler.max_buffered_slices {
                let buffered_count = state.buffered_slices.len();
                let expected_offset = state.expected_offset;
                let buffered_offsets: Vec<_> = state.buffered_slices.keys().copied().collect();
                let resume_phase = state.phase;
                state.phase = AssemblyPhase::Finalizing;
                drop(state);

                assembler
                    .route_material_error(
                        material_id,
                        "buffered_slice_limit_exceeded",
                        serde_json::json!({
                            "offset": offset,
                            "expected_offset": expected_offset,
                            "buffered_count": buffered_count,
                            "buffered_offsets": buffered_offsets,
                            "max_buffered_slices": assembler.max_buffered_slices
                        }),
                    )
                    .await;
                assembler
                    .finalize_failed_material_claimed_checked(
                        material_id,
                        "buffered_slice_limit_exceeded",
                        resume_phase,
                    )
                    .await?;
                return Ok(());
            } else {
                let projected_total = state.total_staged_bytes().saturating_add(data.len() as i64);
                if projected_total > assembler.max_material_size_bytes {
                    let current_total = state.total_staged_bytes();
                    let buffered_count = state.buffered_slices.len();
                    let expected_offset = state.expected_offset;
                    let buffered_offsets: Vec<_> = state.buffered_slices.keys().copied().collect();
                    let resume_phase = state.phase;
                    state.phase = AssemblyPhase::Finalizing;
                    drop(state);

                    assembler
                        .route_material_error(
                            material_id,
                            "material_size_limit_exceeded",
                            serde_json::json!({
                                "offset": offset,
                                "incoming_bytes": data.len(),
                                "expected_offset": expected_offset,
                                "buffered_count": buffered_count,
                                "buffered_offsets": buffered_offsets,
                                "current_total_bytes": current_total,
                                "projected_total_bytes": projected_total,
                                "max_material_size_bytes": assembler.max_material_size_bytes,
                            }),
                        )
                        .await;
                    assembler
                        .finalize_failed_material_claimed_checked(
                            material_id,
                            "material_size_limit_exceeded",
                            resume_phase,
                        )
                        .await?;
                    return Ok(());
                }

                let buffer_path = persist_buffered_slice(&mut state, offset, &data).await?;
                state.buffered_bytes = state.buffered_bytes.saturating_add(data.len() as i64);
                state.buffered_slices.insert(offset, buffer_path.clone());

                // Log buffering event
                append_wal_entry(
                    assembler,
                    &mut state,
                    WalEntry::BufferedSlice {
                        offset,
                        path: buffer_path,
                    },
                )
                .await?;

                debug!(
                    material_id = %material_id,
                    offset,
                    expected = state.expected_offset,
                    "Buffered out-of-order slice"
                );
            }
        }
        Ordering::Less => {
            debug!(material_id = %material_id, offset, expected = state.expected_offset, "Ignoring duplicate or overlapping slice");
        }
    }

    // No longer calling persist_state() here!
    // Slice application is logged inside append_slice_data via WAL

    let should_finalize = state.phase != AssemblyPhase::PendingBegin && state.pending_end.is_some();
    let hold_ms = hold_start.elapsed().as_millis() as u64;
    tracing::Span::current().record("lock_hold_ms", hold_ms);
    if hold_ms > 100 {
        warn!(material_id = %material_id, hold_ms, "Long lock hold in handle_slice");
    }
    drop(state);

    if should_finalize {
        assembler
            .try_finalize_pending_end(
                material_id,
                state_handle,
                super::finalize::PendingEndBehavior::Ignore,
            )
            .await?;
    }
    Ok(())
}

async fn append_slice_data(
    assembler: &MaterialAssembler,
    state: &mut AssemblerState,
    material_id: Uuid,
    data: &[u8],
) -> IngestdResult<()> {
    let pending_write = if let Some(existing) = state.pending_write.clone() {
        if existing.offset != state.expected_offset || existing.len != data.len() {
            return Err(
                SinexError::invalid_state("pending_write does not match retried slice")
                    .with_context("material_id", material_id.to_string())
                    .with_context("expected_offset", state.expected_offset.to_string())
                    .with_context("pending_offset", existing.offset.to_string())
                    .with_context("pending_len", existing.len.to_string())
                    .with_context("incoming_len", data.len().to_string()),
            );
        }
        existing
    } else {
        let pending_write = PendingWrite {
            offset: state.expected_offset,
            len: data.len(),
            slice_count_delta: 1,
        };
        state.pending_write = Some(pending_write.clone());
        append_wal_entry(
            assembler,
            state,
            WalEntry::Checkpoint(checkpoint_snapshot(state)),
        )
        .await?;
        pending_write
    };

    let expected_size_after_write = pending_write
        .offset
        .checked_add(pending_write.len as i64)
        .ok_or_else(|| {
            SinexError::invalid_state("slice write overflowed expected material size")
                .with_context("material_id", material_id.to_string())
                .with_context("offset", pending_write.offset.to_string())
                .with_context("len", pending_write.len.to_string())
        })?;

    let actual_size = staged_file_size_bytes(&state.temp_path).await?;
    match actual_size {
        size if size == pending_write.offset => {
            if state.temp_file.is_none() {
                state.temp_file = Some(
                    File::options()
                        .create(true)
                        .append(true)
                        .open(&state.temp_path)
                        .await
                        .map_err(|e| {
                            SinexError::io(format!(
                                "Failed to reopen staging file for {material_id}"
                            ))
                            .with_source(e)
                        })?,
                );
            }
            if let Some(file) = state.temp_file.as_mut() {
                file.write_all(data).await.map_err(|e| {
                    SinexError::io(format!("Failed to write slice for {material_id}"))
                        .with_source(e)
                })?;
                assembler
                    .durability_policy
                    .flush_staged_after_write(file, material_id)
                    .await?;
            }
        }
        size if size == expected_size_after_write => {
            debug!(
                material_id = %material_id,
                offset = pending_write.offset,
                len = pending_write.len,
                "Resuming slice commit from previously staged bytes"
            );
        }
        size => {
            return Err(SinexError::invalid_state(
                "slice staging file is inconsistent with pending_write",
            )
            .with_context("material_id", material_id.to_string())
            .with_context("pending_offset", pending_write.offset.to_string())
            .with_context("pending_len", pending_write.len.to_string())
            .with_context("actual_size", size.to_string()));
        }
    }

    state.staged_bytes_since_sync = state
        .staged_bytes_since_sync
        .saturating_add(pending_write.len as i64);
    assembler
        .durability_policy
        .sync_staged_file_if_needed(state, material_id, false)
        .await?;

    append_wal_entry(
        assembler,
        state,
        WalEntry::Slice {
            offset: pending_write.offset,
            len: pending_write.len,
        },
    )
    .await?;

    state.hasher.update(data);
    state.expected_offset = expected_size_after_write;
    state.slice_count = state
        .slice_count
        .saturating_add(pending_write.slice_count_delta);
    state.pending_write = None;

    Ok(())
}

async fn flush_buffered_slices(
    assembler: &MaterialAssembler,
    state: &mut AssemblerState,
    material_id: Uuid,
) -> IngestdResult<()> {
    while let Some(&next_offset) = state.buffered_slices.keys().next() {
        if next_offset != state.expected_offset {
            break;
        }

        let buf_path = state
            .buffered_slices
            .get(&next_offset)
            .cloned()
            .ok_or_else(|| {
                SinexError::service(format!(
                    "Missing buffered slice for {material_id} at offset {next_offset}"
                ))
            })?;

        let buffered_data = fs::read(&buf_path).await.map_err(|e| {
            SinexError::io(format!(
                "Failed to read buffered slice {next_offset} for {material_id}"
            ))
            .with_source(e)
        })?;

        append_slice_data(assembler, state, material_id, &buffered_data).await?;

        let removed_path = state.buffered_slices.remove(&next_offset).ok_or_else(|| {
            SinexError::service(format!(
                "Missing buffered slice for {material_id} at offset {next_offset} during removal"
            ))
        })?;
        state.buffered_bytes = state
            .buffered_bytes
            .saturating_sub(buffered_data.len() as i64);

        append_wal_entry(
            assembler,
            state,
            WalEntry::BufferedSliceTaken {
                offset: next_offset,
            },
        )
        .await?;

        if let Err(e) = fs::remove_file(&removed_path).await {
            warn!(path = %removed_path.display(), "Failed to remove buffered slice file: {}", e);
        }
    }
    Ok(())
}

async fn persist_buffered_slice(
    state: &mut AssemblerState,
    offset: i64,
    data: &[u8],
) -> IngestdResult<PathBuf> {
    let buffers_dir = state.buffers_dir();
    fs::create_dir_all(&buffers_dir)
        .await
        .map_err(|e| SinexError::io("Failed to create buffer dir").with_source(e))?;

    let buffer_path = buffers_dir.join(format!("{offset}.bin"));
    let temp_path = buffers_dir.join(format!("{}.{}.tmp", offset, Uuid::now_v7()));
    let mut file = fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temp_path)
        .await
        .map_err(|e| SinexError::io("Failed to persist buffered slice").with_source(e))?;
    file.write_all(data)
        .await
        .map_err(|e| SinexError::io("Failed to persist buffered slice").with_source(e))?;
    // PERF: No fsync on buffered slices — JetStream retransmits on loss, so these are
    // reconstructable. The WAL records that we're expecting this offset; if the buffer file
    // is corrupt/empty after crash, recovery re-requests from JetStream. Trade-off: higher
    // throughput vs. slightly longer recovery on crash during heavy out-of-order ingestion.
    fs::rename(&temp_path, &buffer_path)
        .await
        .map_err(|e| SinexError::io("Failed to persist buffered slice").with_source(e))?;

    Ok(buffer_path)
}

/// Import the assembled material into the SDK content store.
pub(super) async fn import_into_content_store(
    assembler: &MaterialAssembler,
    state: &FinalizationState,
) -> IngestdResult<ContentStoreKey> {
    let staging_path = Utf8PathBuf::from_path_buf(state.temp_path.clone()).map_err(|path| {
        SinexError::io(format!(
            "Staging path is not valid utf-8 for content-store import: {}",
            path.display()
        ))
    })?;

    assembler
        .content_store
        .store_file(&staging_path)
        .await
        .map_err(|e| SinexError::io("content-store import failed").with_source(e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::material_assembler::state::MaterialEndMessage;
    use serde_json::json;
    #[cfg(unix)]
    use std::os::unix::ffi::OsStringExt;
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
            .ok_or_else(|| color_eyre::eyre::eyre!("missing assembler state"))?;
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

        let dlq_subject = ctx.pipeline_namespace().subject("events.dlq.ingestd");
        let mut dlq_sub = ctx.nats_client().subscribe(dlq_subject).await?;
        let material_id = Uuid::now_v7();

        handle_slice(&assembler, material_id, 4, b"late".to_vec()).await?;
        handle_slice(&assembler, material_id, 8, b"later".to_vec()).await?;

        let msg = timeout(Duration::from_secs(Timeouts::SHORT), dlq_sub.next())
            .await?
            .ok_or_else(|| color_eyre::eyre::eyre!("missing DLQ message"))?;
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
        assert_eq!(
            material.status.as_str(),
            sinex_db::repositories::material_status::FAILED
        );
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
}
