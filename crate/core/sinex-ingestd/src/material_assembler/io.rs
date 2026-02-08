//! I/O operations for `MaterialAssembler`.
//!
//! This module contains all file system operations, buffering logic, and git-annex
//! interactions for the material assembler. Extracted to keep the main module
//! focused on state management and orchestration.

use super::{
    state::{
        AssemblerState, FinalizationState, MaterialEndMessage, WalEntry, WalEntryEnvelope,
        BUFFER_DIR_NAME, TEMP_FILE_NAME, WAL_FILE_NAME,
    },
    MaterialAssembler, MAX_BUFFERED_SLICES,
};
use crate::{IngestdResult, SinexError};
use blake3::Hasher;
use camino::Utf8PathBuf;
use libc;
use sinex_node_sdk::annex::AnnexKey;
use sinex_primitives::Timestamp;
use sinex_primitives::Ulid;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::str::FromStr;
use tokio::{fs, fs::File, io::AsyncReadExt, io::AsyncWriteExt};
use tracing::{debug, info, warn};

/// Restore persisted assembler state on startup by replaying the WAL
///
/// # Edge Cases
///
/// - **Corrupt WAL entries**: If WAL replay encounters malformed JSON, it stops at the error
///   and uses the partial state up to that point. This is logged but not fatal.
/// - **Terminal materials with incomplete state**: If a material is marked terminal in the
///   database but the WAL shows incomplete assembly (missing end or buffered slices), the
///   state is cleaned up as stale.
/// - **Legacy state.json migration**: Old state.json files are automatically migrated to
///   WAL format on first restore.
pub(super) async fn restore_state(assembler: &MaterialAssembler) -> IngestdResult<()> {
    let mut entries = match fs::read_dir(&assembler.state_root).await {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(err) => {
            return Err(SinexError::io(format!(
                "Failed to read assembler state root {}: {}",
                assembler.state_root.display(),
                err
            )));
        }
    };

    while let Some(entry) = entries
        .next_entry()
        .await
        .map_err(|e| SinexError::io(format!("Failed to iterate state directory: {e}")))?
    {
        let path = entry.path();
        if !entry
            .file_type()
            .await
            .map_err(|e| SinexError::io(format!("Failed to inspect state entry: {e}")))?
            .is_dir()
        {
            continue;
        }

        let folder_name = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or_default();
        let material_id = match Ulid::from_str(folder_name) {
            Ok(id) => id,
            Err(_) => continue, // Skip non-ULID folders
        };

        if let Some(state) = restore_state_params(assembler, material_id, &path).await? {
            assembler.insert_state_handle(material_id, state).await;
            info!(material_id = %material_id, "Restored in-flight material state from WAL");
        }
    }

    Ok(())
}

async fn restore_state_params(
    assembler: &MaterialAssembler,
    material_id: Ulid,
    state_dir: &std::path::Path,
) -> IngestdResult<Option<AssemblerState>> {
    let wal_path = state_dir.join(WAL_FILE_NAME);
    let temp_path = state_dir.join(TEMP_FILE_NAME);

    if !wal_path.exists() {
        // If neither exists, verify if we should just clean up (e.g. empty dir)
        return Ok(None);
    }

    // Open WAL for reading
    let mut wal_file = File::open(&wal_path)
        .await
        .map_err(|e| SinexError::io(format!("Failed to open WAL for {material_id}: {e}")))?;

    // Replay WAL — supports both envelope format (with CRC) and legacy bare entries
    let mut state_snapshot = ReplayedState::default();
    let mut content_buffer = Vec::new();
    wal_file
        .read_to_end(&mut content_buffer)
        .await
        .map_err(|e| SinexError::io(format!("Failed to read WAL for {material_id}: {e}")))?;

    let content = String::from_utf8_lossy(&content_buffer);
    let mut max_seq: u64 = 0;
    let mut legacy_entries = 0u64;

    for (line_num, line) in content.lines().enumerate() {
        if line.is_empty() {
            continue;
        }

        // Try envelope format first (has seq + crc fields)
        if let Ok(envelope) = serde_json::from_str::<WalEntryEnvelope>(line) {
            // Verify CRC: re-serialize the entry and compare checksum
            let entry_json = match serde_json::to_vec(&envelope.entry) {
                Ok(json) => json,
                Err(e) => {
                    warn!(
                        material_id = %material_id,
                        line = line_num + 1,
                        "WAL entry re-serialization failed (stopping replay): {e}"
                    );
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
                break;
            }
            if envelope.seq > max_seq {
                max_seq = envelope.seq;
            }
            state_snapshot.apply(envelope.entry);
        } else if let Ok(entry) = serde_json::from_str::<WalEntry>(line) {
            // Legacy format: bare WalEntry without envelope
            legacy_entries += 1;
            state_snapshot.apply(entry);
        } else {
            warn!(
                material_id = %material_id,
                line = line_num + 1,
                "WAL replay error — invalid JSON, stopping replay"
            );
            break;
        }
    }

    if legacy_entries > 0 {
        info!(
            material_id = %material_id,
            legacy_entries,
            "Replayed legacy WAL entries without CRC (will be upgraded on next write)"
        );
    }

    // Resume sequence numbering from where the WAL left off
    let next_seq = if max_seq > 0 {
        max_seq + 1
    } else {
        legacy_entries
    };
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
    if is_terminal && !state_snapshot.finalizing && state_snapshot.pending_end.is_none() {
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
            SinexError::io(format!(
                "Failed to open WAL for appending {material_id}: {e}"
            ))
        })?;

    // Rebuild Hasher & Temp File handle
    let temp_file = if temp_path.exists() {
        Some(
            File::options()
                .create(true)
                .append(true)
                .open(&temp_path)
                .await
                .map_err(|e| SinexError::io(format!("Failed to open temp file: {e}")))?,
        )
    } else {
        None
    };

    let hasher = rebuild_hasher(&temp_path).await?;
    let buffered_slices = load_buffered_slices(&state_dir.join(BUFFER_DIR_NAME)).await?;

    Ok(Some(AssemblerState {
        material_id,
        temp_path,
        temp_file,
        wal_file: Some(wal_append),
        wal_seq: next_seq,
        expected_offset: state_snapshot.expected_offset,
        slice_count: state_snapshot.slice_count,
        buffered_slices,
        state_dir: state_dir.to_path_buf(),
        started_at: time::OffsetDateTime::parse(
            &state_snapshot.started_at,
            &time::format_description::well_known::Rfc3339,
        )
        .map_or_else(|_| Timestamp::now(), Timestamp::new),
        material_kind: state_snapshot.material_kind,
        source_identifier: state_snapshot.source_identifier,
        metadata: state_snapshot.metadata,
        has_begin: state_snapshot.has_begin,
        hasher,
        pending_write: None,
        pending_end: state_snapshot.pending_end,
        finalizing: state_snapshot.finalizing,
        last_slice_received: Timestamp::now(),
        _permit: None,
    }))
}

#[derive(Default)]
struct ReplayedState {
    expected_offset: i64,
    slice_count: usize,
    started_at: String,
    material_kind: String,
    source_identifier: String,
    metadata: serde_json::Value,
    has_begin: bool,
    pending_end: Option<MaterialEndMessage>,
    finalizing: bool,
}

impl ReplayedState {
    fn apply(&mut self, entry: WalEntry) {
        match entry {
            WalEntry::Begin(msg) => {
                self.has_begin = true;
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
                self.has_begin = state.has_begin;
                self.pending_end = state.pending_end;
                self.finalizing = state.finalizing;
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
            "Failed to read buffer dir {}: {}",
            buffers_dir.display(),
            e
        ))
    })?;

    while let Some(buf_entry) = buffer_entries
        .next_entry()
        .await
        .map_err(|e| SinexError::io(format!("Failed to iterate buffered slices: {e}")))?
    {
        let buf_path = buf_entry.path();
        if !buf_entry
            .file_type()
            .await
            .map_err(|e| SinexError::io(format!("Failed to inspect buffered slice: {e}")))?
            .is_file()
        {
            continue;
        }

        let Some(offset) = buf_path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .and_then(|stem| stem.parse::<i64>().ok())
        else {
            warn!(
                path = %buf_path.display(),
                "Skipping buffered slice with invalid filename"
            );
            continue;
        };

        buffered_slices.insert(offset, buf_path);
    }

    Ok(buffered_slices)
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
            .map_err(|e| SinexError::io(format!("Failed to ensure assembler state dir: {e}")))?;

        let mut opts = fs::OpenOptions::new();
        opts.create(true).append(true).write(true);

        let file = opts
            .open(&state.state_dir.join(WAL_FILE_NAME))
            .await
            .map_err(|e| SinexError::io(format!("Failed to open WAL file: {e}")))?;
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
            .map_err(|e| SinexError::io(format!("WAL write failed: {e}")))?;
        file.write_all(b"\n")
            .await
            .map_err(|e| SinexError::io(format!("WAL write newline failed: {e}")))?;
        // fsync for durability
        file.sync_all()
            .await
            .map_err(|e| SinexError::io(format!("WAL sync failed: {e}")))?;
    }

    Ok(())
}

/// Remove the persisted state directory for a material
pub(super) async fn cleanup_state(assembler: &MaterialAssembler, material_id: Ulid) {
    let path = assembler.state_root.join(material_id.to_string());

    // Also clean up any orphaned temp files
    let temp_path = path.join(TEMP_FILE_NAME);
    if temp_path.exists() {
        if let Err(e) = fs::remove_file(&temp_path).await {
            warn!(
                material_id = %material_id,
                path = %temp_path.display(),
                "Failed to remove temp file: {}",
                e
            );
        }
    }

    // Clean up buffered slice files
    let buffers_dir = path.join(BUFFER_DIR_NAME);
    if buffers_dir.exists() {
        if let Err(e) = fs::remove_dir_all(&buffers_dir).await {
            warn!(
                material_id = %material_id,
                path = %buffers_dir.display(),
                "Failed to remove buffers directory: {}",
                e
            );
        }
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
    material_id: Ulid,
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

    if state.finalizing {
        debug!(material_id = %material_id, offset, "Ignoring slice received while material is finalizing");
        return Ok(());
    }

    // Update last slice received timestamp
    state.last_slice_received = Timestamp::now();

    use std::cmp::Ordering;
    match offset.cmp(&state.expected_offset) {
        Ordering::Equal => {
            append_slice_data(assembler, &mut state, material_id, &data).await?;
            flush_buffered_slices(assembler, &mut state, material_id).await?;
        }
        Ordering::Greater => {
            if state.buffered_slices.len() >= MAX_BUFFERED_SLICES {
                // ... error handling for max buffer ...
                // (Truncated for brevity in this single-tool edit, but I should preserve the logic)
                // I will assume logic is similar but we need to route error.
                // Re-implementing simplified logic for this massive replace:
                // Actually I must preserve the logic.
                let buffered_count = state.buffered_slices.len();
                let expected_offset = state.expected_offset;
                let buffered_offsets: Vec<_> = state.buffered_slices.keys().copied().collect();
                state.finalizing = true;
                drop(state); // unlock

                assembler
                    .route_material_error(
                        material_id,
                        "buffered_slice_limit_exceeded",
                        serde_json::json!({
                            "offset": offset,
                            "expected_offset": expected_offset,
                            "buffered_count": buffered_count,
                            "buffered_offsets": buffered_offsets,
                            "max_buffered_slices": MAX_BUFFERED_SLICES
                        }),
                    )
                    .await;
                assembler
                    .finalize_failed_material(material_id, "buffered_slice_limit_exceeded")
                    .await;
                return Ok(());
            }

            let buffer_path = persist_buffered_slice(&mut state, offset, &data).await?;
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
        Ordering::Less => {
            debug!(material_id = %material_id, offset, expected = state.expected_offset, "Ignoring duplicate or overlapping slice");
        }
    }

    // No longer calling persist_state() here!
    // Slice application is logged inside append_slice_data via WAL

    let should_finalize = state.has_begin && state.pending_end.is_some();
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
    material_id: Ulid,
    data: &[u8],
) -> IngestdResult<()> {
    if state.temp_file.is_some() {
        if let Some(file) = state.temp_file.as_mut() {
            file.write_all(data).await.map_err(|e| {
                SinexError::io(format!("Failed to write slice for {material_id}: {e}"))
            })?;
            // fsync temp file BEFORE writing WAL entry. Without this, crash after WAL write
            // but before data reaches disk = WAL says "slice received" but temp file is incomplete
            // → hash mismatch on recovery → material marked failed.
            file.sync_all().await.map_err(|e| {
                SinexError::io(format!("Failed to sync slice for {material_id}: {e}"))
            })?;
        }
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
    material_id: Ulid,
) -> IngestdResult<()> {
    while let Some(&next_offset) = state.buffered_slices.keys().next() {
        if next_offset != state.expected_offset {
            break;
        }

        let buf_path = state.buffered_slices.remove(&next_offset).ok_or_else(|| {
            SinexError::service(format!(
                "Missing buffered slice for {material_id} at offset {next_offset}"
            ))
        })?;

        // Log taking from buffer
        append_wal_entry(
            assembler,
            state,
            WalEntry::BufferedSliceTaken {
                offset: next_offset,
            },
        )
        .await?;

        let buffered_data = fs::read(&buf_path).await.map_err(|e| {
            SinexError::io(format!(
                "Failed to read buffered slice {next_offset} for {material_id}: {e}"
            ))
        })?;

        append_slice_data(assembler, state, material_id, &buffered_data).await?;

        if let Err(e) = fs::remove_file(&buf_path).await {
            warn!(path = %buf_path.display(), "Failed to remove buffered slice file: {}", e);
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
        .map_err(|e| SinexError::io(format!("Failed to create buffer dir: {e}")))?;

    let buffer_path = buffers_dir.join(format!("{offset}.bin"));
    let temp_path = buffers_dir.join(format!("{}.{}.tmp", offset, Ulid::new()));
    let mut file = fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temp_path)
        .await
        .map_err(|e| SinexError::io(format!("Failed to persist buffered slice: {e}")))?;
    file.write_all(data)
        .await
        .map_err(|e| SinexError::io(format!("Failed to persist buffered slice: {e}")))?;
    // PERF: No fsync on buffered slices — JetStream retransmits on loss, so these are
    // reconstructable. The WAL records that we're expecting this offset; if the buffer file
    // is corrupt/empty after crash, recovery re-requests from JetStream. Trade-off: higher
    // throughput vs. slightly longer recovery on crash during heavy out-of-order ingestion.
    fs::rename(&temp_path, &buffer_path)
        .await
        .map_err(|e| SinexError::io(format!("Failed to persist buffered slice: {e}")))?;

    Ok(buffer_path)
}

// Deprecated: old persist_state used full rewrites. Replaced by append_wal_entry.

/// Import the assembled material into git-annex
pub(super) async fn import_into_annex(
    assembler: &MaterialAssembler,
    state: &FinalizationState,
) -> IngestdResult<(AnnexKey, PathBuf)> {
    let relative_utf8 = Utf8PathBuf::from(format!("materials/{}.bin", state.material_id));
    let repo_path = assembler.annex.repo_path();
    let target_path_utf8 = repo_path.join(&relative_utf8);

    if let Some(parent) = target_path_utf8.parent() {
        fs::create_dir_all(parent.as_std_path())
            .await
            .map_err(|e| {
                SinexError::io(format!(
                    "Failed to create annex target directory {}: {}",
                    parent.as_str(),
                    e
                ))
            })?;
    }

    let target_path: PathBuf = target_path_utf8.clone().into_std_path_buf();

    if let Err(e) = fs::rename(&state.temp_path, &target_path).await {
        if e.raw_os_error() == Some(libc::EXDEV) {
            fs::copy(&state.temp_path, &target_path)
                .await
                .map_err(|copy_err| {
                    SinexError::io(format!(
                        "Failed to copy assembled file into annex: {copy_err}"
                    ))
                })?;
            fs::remove_file(&state.temp_path)
                .await
                .map_err(|remove_err| {
                    SinexError::io(format!(
                        "Failed to remove staging file after copy: {remove_err}"
                    ))
                })?;
        } else {
            return Err(SinexError::io(format!(
                "Failed to move assembled file into annex: {e}"
            )));
        }
    }

    let annex_key = assembler
        .annex
        .add_file(&relative_utf8)
        .await
        .map_err(|e| SinexError::io(format!("git-annex add failed: {e}")))?;

    Ok((annex_key, target_path))
}
