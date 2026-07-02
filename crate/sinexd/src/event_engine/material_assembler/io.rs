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
use crate::event_engine::{EventEngineResult, SinexError};
use crate::runtime::content_store::ContentStoreKey;
use blake3::Hasher;
use camino::Utf8PathBuf;
use sinex_primitives::Timestamp;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Instant;
use tokio::{fs, fs::File, io::AsyncReadExt, io::AsyncWriteExt};
use tracing::{debug, info, warn};
use uuid::Uuid;

#[cfg(test)]
struct SliceStagingIoHook {
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
    pause_next: std::sync::atomic::AtomicBool,
}

#[cfg(test)]
static SLICE_STAGING_IO_HOOK: std::sync::Mutex<Option<std::sync::Arc<SliceStagingIoHook>>> =
    std::sync::Mutex::new(None);

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
pub(super) async fn restore_state(assembler: &MaterialAssembler) -> EventEngineResult<()> {
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

fn parse_material_state_folder(path: &std::path::Path) -> EventEngineResult<Uuid> {
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
) -> EventEngineResult<Option<RestoredAssemblerState>> {
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
) -> EventEngineResult<Option<RestoredAssemblerState>> {
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
) -> EventEngineResult<Timestamp> {
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
) -> EventEngineResult<()> {
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

async fn staged_file_size_bytes(temp_path: &Path) -> EventEngineResult<i64> {
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

async fn rebuild_hasher(temp_path: &PathBuf) -> EventEngineResult<Hasher> {
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

async fn load_buffered_slices(buffers_dir: &PathBuf) -> EventEngineResult<BTreeMap<i64, PathBuf>> {
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

fn parse_buffered_slice_offset(path: &std::path::Path) -> EventEngineResult<i64> {
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

async fn buffered_slice_bytes(buffered_slices: &BTreeMap<i64, PathBuf>) -> EventEngineResult<i64> {
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

fn buffered_slice_file_len_bytes(path: &Path, len: u64) -> EventEngineResult<i64> {
    i64::try_from(len).map_err(|error| {
        SinexError::processing("buffered slice length exceeds i64 range")
            .with_context("path", path.display().to_string())
            .with_context("slice_len_bytes", len.to_string())
            .with_std_error(&error)
    })
}

fn checked_buffered_slice_total(
    total: i64,
    slice_bytes: i64,
    path: &Path,
) -> EventEngineResult<i64> {
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
) -> EventEngineResult<()> {
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
) -> EventEngineResult<()> {
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
) -> EventEngineResult<()> {
    assembler
        .durability_policy
        .sync_staged_file_if_needed(state, material_id, true)
        .await
}

/// Remove the persisted state directory for a material
pub(super) async fn cleanup_state(assembler: &MaterialAssembler, material_id: Uuid) {
    let _ = assembler.slice_io_locks.remove(&material_id);
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
///   redelivery, or non-runtime publishers. A placeholder state is created to buffer slices until
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
) -> EventEngineResult<()> {
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

    enum DeferredSliceIo {
        None,
        StageInOrder {
            pending_write: PendingWrite,
            temp_path: PathBuf,
            staged_sync: bool,
        },
        BufferOutOfOrder {
            buffer_path: PathBuf,
            data: Vec<u8>,
        },
    }

    let mut deferred_io = DeferredSliceIo::None;

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

            let pending_write =
                prepare_pending_slice_write(assembler, &mut state, material_id, &data).await?;
            let staged_bytes_after_write = state
                .staged_bytes_since_sync
                .saturating_add(pending_write.len as i64);
            let staged_sync = assembler
                .durability_policy
                .staged_sync_decision(
                    super::durability::StagedDurabilityCounters {
                        bytes_since_sync: staged_bytes_after_write,
                        elapsed_since_sync: state.last_staged_sync.elapsed(),
                    },
                    false,
                )
                .should_sync();

            deferred_io = DeferredSliceIo::StageInOrder {
                pending_write,
                temp_path: state.temp_path.clone(),
                staged_sync,
            };
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

                let buffers_dir = state.buffers_dir();
                deferred_io = DeferredSliceIo::BufferOutOfOrder {
                    buffer_path: buffers_dir.join(format!("{offset}.bin")),
                    data: data.clone(),
                };
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

    match deferred_io {
        DeferredSliceIo::None => {}
        DeferredSliceIo::StageInOrder {
            pending_write,
            temp_path,
            staged_sync,
        } => {
            let slice_io_lock = assembler.slice_io_lock(material_id);
            let _slice_io_guard = slice_io_lock.lock().await;
            notify_slice_staging_io_for_tests().await;
            stage_slice_file(material_id, &temp_path, &pending_write, &data, staged_sync).await?;
            let mut state = state_handle.lock().await;
            commit_pending_slice_write(
                assembler,
                &mut state,
                material_id,
                &data,
                &pending_write,
                staged_sync,
            )
            .await?;
            flush_buffered_slices(assembler, &mut state, material_id).await?;
        }
        DeferredSliceIo::BufferOutOfOrder { buffer_path, data } => {
            let slice_io_lock = assembler.slice_io_lock(material_id);
            let _slice_io_guard = slice_io_lock.lock().await;
            {
                let state = state_handle.lock().await;
                if state.buffered_slices.contains_key(&offset) {
                    debug!(
                        material_id = %material_id,
                        offset,
                        expected = state.expected_offset,
                        "Ignoring duplicate buffered slice after I/O lock wait"
                    );
                    return Ok(());
                }
            }
            persist_buffered_slice_to_path(&buffer_path, offset, &data).await?;
            let mut state = state_handle.lock().await;
            if state.buffered_slices.contains_key(&offset) {
                if let Err(error) = fs::remove_file(&buffer_path).await {
                    warn!(
                        path = %buffer_path.display(),
                        error = %error,
                        "Failed to remove duplicate buffered slice file"
                    );
                }
            } else {
                state.buffered_bytes = state.buffered_bytes.saturating_add(data.len() as i64);
                state.buffered_slices.insert(offset, buffer_path.clone());

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
    }

    if should_finalize {
        // Decouple the slice-completed finalize from the ordered consumer too
        // (#2187): the last slice that completes a material must not run the heavy
        // finalize inline, or it head-of-line blocks every following frame exactly
        // like the END-driven path did. State is durable (staged bytes + WAL), so
        // dispatch onto the bounded finalize worker set and let the consumer ACK.
        assembler.dispatch_finalize(material_id, state_handle);
    }
    Ok(())
}

async fn append_slice_data(
    assembler: &MaterialAssembler,
    state: &mut AssemblerState,
    material_id: Uuid,
    data: &[u8],
) -> EventEngineResult<()> {
    let pending_write = prepare_pending_slice_write(assembler, state, material_id, data).await?;
    stage_slice_for_locked_state(assembler, state, material_id, data, &pending_write).await?;
    commit_pending_slice_write(assembler, state, material_id, data, &pending_write, false).await?;

    Ok(())
}

async fn prepare_pending_slice_write(
    assembler: &MaterialAssembler,
    state: &mut AssemblerState,
    material_id: Uuid,
    data: &[u8],
) -> EventEngineResult<PendingWrite> {
    if let Some(existing) = state.pending_write.clone() {
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
        Ok(existing)
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
        Ok(pending_write)
    }
}

fn checked_pending_write_end(
    material_id: Uuid,
    pending_write: &PendingWrite,
) -> EventEngineResult<i64> {
    pending_write
        .offset
        .checked_add(pending_write.len as i64)
        .ok_or_else(|| {
            SinexError::invalid_state("slice write overflowed expected material size")
                .with_context("material_id", material_id.to_string())
                .with_context("offset", pending_write.offset.to_string())
                .with_context("len", pending_write.len.to_string())
        })
}

async fn stage_slice_for_locked_state(
    assembler: &MaterialAssembler,
    state: &mut AssemblerState,
    material_id: Uuid,
    data: &[u8],
    pending_write: &PendingWrite,
) -> EventEngineResult<()> {
    let expected_size_after_write = checked_pending_write_end(material_id, pending_write)?;

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

    Ok(())
}

async fn stage_slice_file(
    material_id: Uuid,
    temp_path: &Path,
    pending_write: &PendingWrite,
    data: &[u8],
    sync_after_write: bool,
) -> EventEngineResult<()> {
    let expected_size_after_write = checked_pending_write_end(material_id, pending_write)?;
    let actual_size = staged_file_size_bytes(temp_path).await?;
    match actual_size {
        size if size == pending_write.offset => {
            let mut file = File::options()
                .create(true)
                .append(true)
                .open(temp_path)
                .await
                .map_err(|e| {
                    SinexError::io(format!("Failed to reopen staging file for {material_id}"))
                        .with_source(e)
                })?;
            file.write_all(data).await.map_err(|e| {
                SinexError::io(format!("Failed to write slice for {material_id}")).with_source(e)
            })?;
            file.flush().await.map_err(|e| {
                SinexError::io(format!("Failed to flush staged material for {material_id}"))
                    .with_source(e)
            })?;
            if sync_after_write {
                file.sync_data().await.map_err(|e| {
                    SinexError::io(format!("Failed to sync staged material for {material_id}"))
                        .with_source(e)
                })?;
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
    Ok(())
}

async fn commit_pending_slice_write(
    assembler: &MaterialAssembler,
    state: &mut AssemblerState,
    material_id: Uuid,
    data: &[u8],
    pending_write: &PendingWrite,
    staged_synced_after_write: bool,
) -> EventEngineResult<()> {
    let expected_size_after_write = checked_pending_write_end(material_id, pending_write)?;
    match state.pending_write.as_ref() {
        Some(current)
            if current.offset == pending_write.offset
                && current.len == pending_write.len
                && current.slice_count_delta == pending_write.slice_count_delta => {}
        None if state.expected_offset >= expected_size_after_write => {
            debug!(
                material_id = %material_id,
                offset = pending_write.offset,
                len = pending_write.len,
                expected_offset = state.expected_offset,
                "Ignoring duplicate slice whose pending write was already committed"
            );
            return Ok(());
        }
        other => {
            return Err(
                SinexError::invalid_state("pending_write changed before slice commit")
                    .with_context("material_id", material_id.to_string())
                    .with_context("pending_offset", pending_write.offset.to_string())
                    .with_context("pending_len", pending_write.len.to_string())
                    .with_context("state_pending_write", format!("{other:?}"))
                    .with_context("state_expected_offset", state.expected_offset.to_string()),
            );
        }
    }

    state.staged_bytes_since_sync = state
        .staged_bytes_since_sync
        .saturating_add(pending_write.len as i64);
    if staged_synced_after_write {
        state.staged_bytes_since_sync = 0;
        state.last_staged_sync = Instant::now();
    } else {
        assembler
            .durability_policy
            .sync_staged_file_if_needed(state, material_id, false)
            .await?;
    }

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
) -> EventEngineResult<()> {
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

async fn persist_buffered_slice_to_path(
    buffer_path: &Path,
    offset: i64,
    data: &[u8],
) -> EventEngineResult<()> {
    let buffers_dir = buffer_path
        .parent()
        .ok_or_else(|| SinexError::invalid_state("buffered slice path has no parent"))?;
    fs::create_dir_all(buffers_dir)
        .await
        .map_err(|e| SinexError::io("Failed to create buffer dir").with_source(e))?;

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
    fs::rename(&temp_path, buffer_path)
        .await
        .map_err(|e| SinexError::io("Failed to persist buffered slice").with_source(e))?;
    Ok(())
}

#[cfg(test)]
async fn notify_slice_staging_io_for_tests() {
    let hook = SLICE_STAGING_IO_HOOK
        .lock()
        .ok()
        .and_then(|guard| guard.clone());
    if let Some(hook) = hook {
        if hook
            .pause_next
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            hook.entered.notify_waiters();
            hook.release.notified().await;
        }
    }
}

#[cfg(not(test))]
async fn notify_slice_staging_io_for_tests() {}

/// Import the assembled material into the runtime content store.
pub(super) async fn import_into_content_store(
    assembler: &MaterialAssembler,
    state: &FinalizationState,
) -> EventEngineResult<ContentStoreKey> {
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
#[path = "io_test.rs"]
mod tests;
