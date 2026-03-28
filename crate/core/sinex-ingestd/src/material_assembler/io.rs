//! I/O operations for `MaterialAssembler`.
//!
//! This module contains all file system operations, buffering logic, and git-annex
//! interactions for the material assembler. Extracted to keep the main module
//! focused on state management and orchestration.

use super::{
    MaterialAssembler,
    state::{
        AssemblerState,
        AssemblyPhase,
        BUFFER_DIR_NAME,
        FinalizationState,

        // MaterialBeginMessage removed (unused)
        MaterialEndMessage,
        parse_material_started_at,
        TEMP_FILE_NAME,
        WAL_FILE_NAME,
        WalEntry,
        WalEntryEnvelope,
    },
};
use crate::{IngestdResult, SinexError};
use blake3::Hasher;
use camino::Utf8PathBuf;
use sinex_node_sdk::annex::AnnexKey;
use sinex_primitives::Timestamp;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::str::FromStr;
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

        let material_id = parse_material_state_folder(&path)?;

        if let Some(state) = restore_state_params(assembler, material_id, &path).await? {
            assembler.insert_state_handle(material_id, state).await;
            info!(material_id = %material_id, "Restored in-flight material state from WAL");
        }
    }

    Ok(())
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
            "Assembler state folder {:?} is not valid UTF-8",
            path
        ))
    })?;

    Uuid::from_str(folder_name).map_err(|error| {
        SinexError::invalid_state("Assembler state folder has invalid material id")
            .with_context("path", path.display().to_string())
            .with_context("folder_name", folder_name.to_string())
            .with_std_error(&error)
    })
}

async fn restore_state_params(
    assembler: &MaterialAssembler,
    material_id: Uuid,
    state_dir: &std::path::Path,
) -> IngestdResult<Option<AssemblerState>> {
    let wal_path = state_dir.join(WAL_FILE_NAME);
    let temp_path = state_dir.join(TEMP_FILE_NAME);

    if !wal_path.exists() {
        // If neither exists, verify if we should just clean up (e.g. empty dir)
        return Ok(None);
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

        match parse_wal_envelope_line(line) {
            Ok(envelope) => {
            // Verify CRC: re-serialize the entry and compare checksum
            let entry_json = match serde_json::to_vec(&envelope.entry) {
                Ok(json) => json,
                Err(e) => {
                    warn!(
                        material_id = %material_id,
                        line = line_num + 1,
                        "WAL entry re-serialization failed (stopping replay): {e}"
                    );
                    replay_corrupted = true;
                    break;
                }
            };
            let computed_crc = crc32fast::hash(&entry_json);
            if computed_crc != envelope.crc {
                warn!(
                    material_id = %material_id,
                    line = line_num + 1,
                    seq = envelope.seq,
                    expected_crc = envelope.crc,
                    computed_crc = computed_crc,
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
        cleanup_state(assembler, material_id).await;
        return Ok(None);
    }

    if !has_envelope_entries {
        if has_non_empty_lines {
            warn!(
                material_id = %material_id,
                "WAL contains no valid envelope entries; cleaning up incompatible or corrupt state"
            );
        }
        cleanup_state(assembler, material_id).await;
        return Ok(None);
    }

    // Resume sequence numbering from where the WAL left off
    let next_seq = max_seq + 1;
    // Validate temp file size matches WAL state — catches crash-during-write corruption
    // where the WAL recorded a slice but the temp file has incomplete data (or is missing).
    if state_snapshot.expected_offset > 0 {
        if !temp_path.exists() {
            warn!(
                material_id = %material_id,
                expected_bytes = state_snapshot.expected_offset,
                "WAL indicates assembled data but temp file is missing; cleaning up stale state"
            );
            cleanup_state(assembler, material_id).await;
            return Ok(None);
        }
        let actual_size = fs::metadata(&temp_path).await.map_or(0, |m| m.len() as i64);
        if actual_size != state_snapshot.expected_offset {
            warn!(
                material_id = %material_id,
                expected = state_snapshot.expected_offset,
                actual = actual_size,
                "Temp file size mismatch after WAL replay; cleaning up inconsistent state"
            );
            cleanup_state(assembler, material_id).await;
            return Ok(None);
        }
    }

    // Check terminal status
    let is_terminal = assembler.material_is_terminal(material_id).await?;
    if is_terminal
        && state_snapshot.phase != AssemblyPhase::Finalizing
        && state_snapshot.pending_end.is_none()
    {
        info!(material_id = %material_id, "Material is terminal but state incomplete; treating as stale and cleaning up");
        cleanup_state(assembler, material_id).await;
        return Ok(None);
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
    prune_stale_buffered_slices(material_id, state_snapshot.expected_offset, &mut buffered_slices)
        .await?;
    let buffered_bytes = buffered_slice_bytes(&buffered_slices).await?;

    // Acquire semaphore permit for restored assemblies (same as new assemblies)
    let permit = assembler
        .active_assemblies
        .clone()
        .try_acquire_owned()
        .map_err(|e| {
            SinexError::service(format!(
                "Too many active assemblies during restore (material {material_id})"
            ))
            .with_source(e)
        })?;

    Ok(Some(AssemblerState {
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
        pending_write: None,
        pending_end: state_snapshot.pending_end,
        last_slice_received: Timestamp::now(),
        _permit: Some(permit),
    }))
}

fn parse_wal_envelope_line(line: &str) -> Result<WalEntryEnvelope, String> {
    serde_json::from_str::<WalEntryEnvelope>(line).map_err(|error| {
        format!(
            "failed to parse WAL envelope JSON: {error}; wal_line={}",
            wal_line_preview(line)
        )
    })
}

fn wal_line_preview(line: &str) -> String {
    const MAX_PREVIEW_CHARS: usize = 160;
    let mut preview = line.chars().take(MAX_PREVIEW_CHARS).collect::<String>();
    if line.chars().count() > MAX_PREVIEW_CHARS {
        preview.push('…');
    }
    preview
}

#[derive(Default)]
struct ReplayedState {
    expected_offset: i64,
    slice_count: usize,
    started_at: String,
    material_kind: String,
    source_identifier: String,
    metadata: serde_json::Value,
    phase: AssemblyPhase,
    pending_end: Option<MaterialEndMessage>,
}

impl ReplayedState {
    fn apply(&mut self, entry: WalEntry) {
        match entry {
            WalEntry::Begin(msg) => {
                self.phase = AssemblyPhase::Accumulating;
                self.started_at = msg.started_at;
                self.material_kind = msg.material_kind;
                self.source_identifier = msg.source_identifier;
                self.metadata = msg.metadata;
            }
            WalEntry::Slice { offset: _, len } => {
                // WAL implies this slice was processed successfully (written to temp file)
                self.expected_offset += len as i64;
                self.slice_count += 1;
            }
            WalEntry::End(msg) => {
                self.pending_end = Some(msg);
            }
            WalEntry::Checkpoint(state) => {
                // Checkpoint overrides everything previous
                self.expected_offset = state.expected_offset;
                self.slice_count = state.slice_count;
                self.started_at = state.started_at;
                self.material_kind = state.material_kind;
                self.source_identifier = state.source_identifier;
                self.metadata = state.metadata;
                self.phase = state.phase;
                self.pending_end = state.pending_end;
            }
            _ => {} // Buffer events don't change core state reconstruction directly
        }
    }
}

async fn rebuild_hasher(temp_path: &PathBuf) -> IngestdResult<Hasher> {
    let mut hasher = Hasher::new();
    if temp_path.exists() {
        let contents = fs::read(&temp_path).await.map_err(|e| {
            SinexError::io(format!(
                "Failed to read temp file {}: {}",
                temp_path.display(),
                e
            ))
        })?;
        if !contents.is_empty() {
            hasher.update(&contents);
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
pub(super) async fn append_wal_entry(
    _assembler: &MaterialAssembler,
    state: &mut AssemblerState,
    entry: WalEntry,
) -> IngestdResult<()> {
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

    if let Some(file) = state.wal_file.as_mut() {
        file.write_all(serialized.as_bytes())
            .await
            .map_err(|e| SinexError::io("WAL write failed").with_source(e))?;
        file.write_all(b"\n")
            .await
            .map_err(|e| SinexError::io("WAL write newline failed").with_source(e))?;
        // fsync for durability
        file.sync_all()
            .await
            .map_err(|e| SinexError::io("WAL sync failed").with_source(e))?;
    }

    Ok(())
}

/// Remove the persisted state directory for a material
pub(super) async fn cleanup_state(assembler: &MaterialAssembler, material_id: Uuid) {
    let path = assembler.state_root.join(material_id.to_string());

    // Also clean up any orphaned temp files
    let temp_path = path.join(TEMP_FILE_NAME);
    if temp_path.exists()
        && let Err(e) = fs::remove_file(&temp_path).await
    {
        warn!(
            material_id = %material_id,
            path = %temp_path.display(),
            "Failed to remove temp file: {}",
            e
        );
    }

    // Clean up buffered slice files
    let buffers_dir = path.join(BUFFER_DIR_NAME);
    if buffers_dir.exists()
        && let Err(e) = fs::remove_dir_all(&buffers_dir).await
    {
        warn!(
            material_id = %material_id,
            path = %buffers_dir.display(),
            "Failed to remove buffers directory: {}",
            e
        );
    }

    // Finally remove the entire state directory
    if let Err(e) = fs::remove_dir_all(&path).await {
        warn!(
            material_id = %material_id,
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
/// - **Early slice arrival**: Slices may arrive before the begin message due to separate
///   `JetStream` subjects. A placeholder state is created to buffer slices until begin arrives.
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
    let state_handle = if let Some(existing) = assembler.get_state_handle(&material_id).await {
        existing
    } else {
        if assembler.material_is_terminal(material_id).await? {
            debug!(
                material_id = %material_id,
                offset,
                "Dropping slice for material already completed"
            );
            return Ok(());
        }
        let placeholder = assembler.create_placeholder_state(material_id).await?;
        assembler
            .insert_state_handle(material_id, placeholder)
            .await
    };

    let acquire_start = std::time::Instant::now();
    let mut state = state_handle.lock().await;
    let acquire_ms = acquire_start.elapsed().as_millis() as u64;
    tracing::Span::current().record("lock_acquire_ms", acquire_ms);
    if acquire_ms > 50 {
        warn!(material_id = %material_id, acquire_ms, "Slow lock acquisition in handle_slice");
    }
    let hold_start = std::time::Instant::now();

    if state.phase == AssemblyPhase::Finalizing {
        debug!(material_id = %material_id, offset, "Ignoring slice received while material is finalizing");
        return Ok(());
    }

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
                    .finalize_failed_material_claimed(
                        material_id,
                        "material_size_limit_exceeded",
                    )
                    .await;
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
                    .finalize_failed_material_claimed(
                        material_id,
                        "buffered_slice_limit_exceeded",
                    )
                    .await;
                return Ok(());
            } else {
                let projected_total = state.total_staged_bytes().saturating_add(data.len() as i64);
                if projected_total > assembler.max_material_size_bytes {
                    let current_total = state.total_staged_bytes();
                    let buffered_count = state.buffered_slices.len();
                    let expected_offset = state.expected_offset;
                    let buffered_offsets: Vec<_> = state.buffered_slices.keys().copied().collect();
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
                        .finalize_failed_material_claimed(
                            material_id,
                            "material_size_limit_exceeded",
                        )
                        .await;
                    return Ok(());
                }

                let buffer_path = persist_buffered_slice(&mut state, offset, &data).await?;
                state.buffered_bytes += data.len() as i64;
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
    if state.temp_file.is_some()
        && let Some(file) = state.temp_file.as_mut()
    {
        file.write_all(data).await.map_err(|e| {
            SinexError::io(format!("Failed to write slice for {material_id}")).with_source(e)
        })?;
        // fsync temp file BEFORE writing WAL entry. Without this, crash after WAL write
        // but before data reaches disk = WAL says "slice received" but temp file is incomplete
        // → hash mismatch on recovery → material marked failed.
        file.sync_all().await.map_err(|e| {
            SinexError::io(format!("Failed to sync slice for {material_id}")).with_source(e)
        })?;
    }

    state.hasher.update(data);

    // Log slice processing to WAL *after* temp file write succeeds
    append_wal_entry(
        assembler,
        state,
        WalEntry::Slice {
            offset: state.expected_offset,
            len: data.len(),
        },
    )
    .await?;

    state.expected_offset += data.len() as i64;
    state.slice_count += 1;
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

        let buf_path = state.buffered_slices.get(&next_offset).cloned().ok_or_else(|| {
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

/// Import the assembled material into git-annex
pub(super) async fn import_into_annex(
    assembler: &MaterialAssembler,
    state: &FinalizationState,
) -> IngestdResult<AnnexKey> {
    let staging_path = Utf8PathBuf::from_path_buf(state.temp_path.clone()).map_err(|path| {
        SinexError::io(format!(
            "Staging path is not valid utf-8 for annex import: {}",
            path.display()
        ))
    })?;

    assembler
        .annex
        .add_file(&staging_path)
        .await
        .map_err(|e| SinexError::io("git-annex add failed").with_source(e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MaterialReadySet;
    use camino::Utf8PathBuf;
    use serde_json::json;
    use sinex_node_sdk::annex::{AnnexConfig, GitAnnex};
    #[cfg(unix)]
    use std::os::unix::ffi::OsStringExt;
    use std::sync::Arc;
    use tokio::time::timeout;
    use tokio_stream::StreamExt;
    use xtask::sandbox::prelude::*;

    async fn test_assembler(
        ctx: &TestContext,
    ) -> TestResult<(MaterialAssembler, tempfile::TempDir, tempfile::TempDir)> {
        let annex_dir = tempfile::tempdir()?;
        let repo_path = Utf8PathBuf::from_path_buf(annex_dir.path().to_path_buf())
            .map_err(|_| color_eyre::eyre::eyre!("tempdir path is not valid utf-8"))?;
        GitAnnex::init(&repo_path, Some("io-test")).await?;
        let annex = Arc::new(GitAnnex::new(AnnexConfig {
            repo_path,
            num_copies: None,
            large_files: None,
        })?);

        let state_dir = tempfile::tempdir()?;
        let assembler = MaterialAssembler::new(
            ctx.nats_client(),
            ctx.pool.clone(),
            annex,
            state_dir.path().to_path_buf(),
            Some(ctx.pipeline_namespace().prefix().to_string()),
            1_000,
            50,
            Some(MaterialReadySet::default()),
            100,
            512 * 1024 * 1024,
            300,
            3_600,
            90,
        )?;

        Ok((assembler, annex_dir, state_dir))
    }

    async fn write_wal_entry(
        wal_path: &std::path::Path,
        entry: WalEntry,
    ) -> TestResult<()> {
        let entry_json = serde_json::to_vec(&entry)?;
        let envelope = WalEntryEnvelope {
            seq: 0,
            crc: crc32fast::hash(&entry_json),
            entry,
        };
        tokio::fs::write(wal_path, format!("{}\n", serde_json::to_string(&envelope)?)).await?;
        Ok(())
    }

    #[sinex_test]
    async fn import_into_annex_preserves_staging_file_until_cleanup(
        ctx: TestContext,
    ) -> TestResult<()> {
        let ctx = ctx.with_nats().shared().await?;
        let (assembler, _annex_dir, state_dir) = test_assembler(&ctx).await?;
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
            source_identifier: "test://annex".to_string(),
            started_at: Timestamp::now(),
        };

        let annex_key = import_into_annex(&assembler, &final_state).await?;
        assert!(!annex_key.key.is_empty());
        assert!(
            temp_path.exists(),
            "annex import should preserve the staging file until cleanup succeeds"
        );
        Ok(())
    }

    #[sinex_test]
    async fn buffered_slice_file_len_bytes_rejects_unrepresentable_lengths() -> TestResult<()> {
        let error = buffered_slice_file_len_bytes(Path::new("/tmp/oversized-slice"), u64::MAX)
            .expect_err("oversized buffered slices must fail honestly");

        assert!(error
            .to_string()
            .contains("buffered slice length exceeds i64 range"));
        Ok(())
    }

    #[sinex_test]
    async fn checked_buffered_slice_total_rejects_overflow() -> TestResult<()> {
        let error = checked_buffered_slice_total(i64::MAX, 1, Path::new("/tmp/overflow-slice"))
            .expect_err("buffered slice byte totals must not silently overflow");

        assert!(error
            .to_string()
            .contains("buffered slice byte total overflowed"));
        Ok(())
    }

    #[sinex_test]
    async fn handle_slice_ignores_duplicate_buffered_offset_without_growing_state(
        ctx: TestContext,
    ) -> TestResult<()> {
        let ctx = ctx.with_nats().shared().await?;
        let annex_dir = tempfile::tempdir()?;
        let repo_path = Utf8PathBuf::from_path_buf(annex_dir.path().to_path_buf())
            .map_err(|_| color_eyre::eyre::eyre!("tempdir path is not valid utf-8"))?;
        GitAnnex::init(&repo_path, Some("io-test")).await?;
        let annex = Arc::new(GitAnnex::new(AnnexConfig {
            repo_path,
            num_copies: None,
            large_files: None,
        })?);

        let state_dir = tempfile::tempdir()?;
        let assembler = MaterialAssembler::new(
            ctx.nats_client(),
            ctx.pool.clone(),
            annex,
            state_dir.path().to_path_buf(),
            Some(ctx.pipeline_namespace().prefix().to_string()),
            1_000,
            50,
            Some(MaterialReadySet::default()),
            100,
            512 * 1024 * 1024,
            300,
            3_600,
            90,
        )?;

        let material_id = Uuid::now_v7();
        handle_slice(&assembler, material_id, 4, b"late".to_vec()).await?;
        handle_slice(&assembler, material_id, 4, b"late".to_vec()).await?;

        let state = assembler
            .get_state_handle(&material_id)
            .await
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
        let annex_dir = tempfile::tempdir()?;
        let repo_path = Utf8PathBuf::from_path_buf(annex_dir.path().to_path_buf())
            .map_err(|_| color_eyre::eyre::eyre!("tempdir path is not valid utf-8"))?;
        GitAnnex::init(&repo_path, Some("io-test")).await?;
        let annex = Arc::new(GitAnnex::new(AnnexConfig {
            repo_path,
            num_copies: None,
            large_files: None,
        })?);

        let state_dir = tempfile::tempdir()?;
        let assembler = MaterialAssembler::new(
            ctx.nats_client(),
            ctx.pool.clone(),
            annex,
            state_dir.path().to_path_buf(),
            Some(ctx.pipeline_namespace().prefix().to_string()),
            1_000,
            50,
            Some(MaterialReadySet::default()),
            100,
            8,
            300,
            3_600,
            90,
        )?;

        let material_id = Uuid::now_v7();
        handle_slice(&assembler, material_id, 0, b"12345".to_vec()).await?;
        handle_slice(&assembler, material_id, 5, b"6789".to_vec()).await?;

        assert!(
            assembler.get_state_handle(&material_id).await.is_none(),
            "oversized material should be failed and cleaned up"
        );
        Ok(())
    }

    #[sinex_test]
    async fn handle_slice_routes_buffered_slice_limit_overflow_to_dlq(
        ctx: TestContext,
    ) -> TestResult<()> {
        let ctx = ctx.with_nats().shared().await?;
        let annex_dir = tempfile::tempdir()?;
        let repo_path = Utf8PathBuf::from_path_buf(annex_dir.path().to_path_buf())
            .map_err(|_| color_eyre::eyre::eyre!("tempdir path is not valid utf-8"))?;
        GitAnnex::init(&repo_path, Some("io-test")).await?;
        let annex = Arc::new(GitAnnex::new(AnnexConfig {
            repo_path,
            num_copies: None,
            large_files: None,
        })?);

        let state_dir = tempfile::tempdir()?;
        let assembler = MaterialAssembler::new(
            ctx.nats_client(),
            ctx.pool.clone(),
            annex,
            state_dir.path().to_path_buf(),
            Some(ctx.pipeline_namespace().prefix().to_string()),
            1_000,
            50,
            Some(MaterialReadySet::default()),
            1,
            512 * 1024 * 1024,
            300,
            3_600,
            90,
        )?;

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
            assembler.get_state_handle(&material_id).await.is_none(),
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
    async fn wal_line_preview_truncates_long_lines() -> TestResult<()> {
        let preview = wal_line_preview(&"a".repeat(200));
        assert_eq!(preview.chars().count(), 161);
        assert!(preview.ends_with('…'));
        Ok(())
    }

    #[sinex_test]
    async fn restore_state_rejects_invalid_started_at_in_wal(ctx: TestContext) -> TestResult<()> {
        let ctx = ctx.with_nats().shared().await?;
        let (assembler, _annex_dir, state_dir) = test_assembler(&ctx).await?;
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

        let error = restore_state(&assembler)
            .await
            .expect_err("invalid WAL started_at must fail honestly");
        assert!(error.to_string().contains("Invalid started_at"));
        assert!(error.to_string().contains("restored WAL state"));
        Ok(())
    }

    #[sinex_test]
    async fn restore_state_rejects_invalid_buffered_slice_filename(ctx: TestContext) -> TestResult<()> {
        let ctx = ctx.with_nats().shared().await?;
        let (assembler, _annex_dir, state_dir) = test_assembler(&ctx).await?;
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

        tokio::fs::write(material_dir.join(BUFFER_DIR_NAME).join("bad-offset.slice"), b"slice")
            .await?;

        let error = restore_state(&assembler)
            .await
            .expect_err("invalid buffered slice filenames must fail restore honestly");

        assert!(error.to_string().contains("invalid offset"));
        assert!(error.to_string().contains("bad-offset"));
        Ok(())
    }

    #[sinex_test]
    async fn restore_state_cleans_up_partial_replay_after_corrupt_wal_line(
        ctx: TestContext,
    ) -> TestResult<()> {
        let ctx = ctx.with_nats().shared().await?;
        let (assembler, _annex_dir, state_dir) = test_assembler(&ctx).await?;
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
        let path = std::path::PathBuf::from("/tmp").join(std::ffi::OsString::from_vec(vec![
            0x66, 0x6f, 0x80,
        ]));

        let err = parse_material_state_folder(&path)
            .expect_err("non-UTF-8 material state folders must surface explicit errors");

        assert!(err.to_string().contains("not valid UTF-8"));
        Ok(())
    }
}
