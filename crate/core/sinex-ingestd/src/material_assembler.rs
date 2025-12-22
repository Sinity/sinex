//! Material Assembler for consuming material slices from NATS JetStream.
//!
//! The assembler is responsible for rebuilding source material streams from
//! begin/slice/end messages, persisting the assembled material into git-annex,
//! registering blobs in Postgres, updating the source material registry and
//! temporal ledger, and routing failures to the DLQ. State is persisted on disk
//! so that in-flight assemblies can survive process restarts.

use async_nats::{jetstream, Client as NatsClient};
use blake3::Hasher;
use camino::Utf8PathBuf;
use chrono::{DateTime, Utc};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map as JsonMap};
use sinex_core::db::models::blob::Blob;
use sinex_core::db::query_helpers::ulid_to_uuid;
use sinex_core::{
    db::{DbPool, DbPoolExt},
    environment::SinexEnvironment,
    types::Ulid,
    Id, JsonValue, SourceMaterialRecord,
};
use sinex_satellite_sdk::annex::{AnnexKey, GitAnnex};
use std::{
    collections::{BTreeMap, HashMap},
    path::PathBuf,
    str::FromStr,
    sync::Arc,
};

use libc;
use tokio::{
    fs,
    fs::File,
    io::AsyncWriteExt,
    sync::{Mutex, RwLock},
};
use tracing::{debug, error, info, warn};

use crate::{IngestdResult, SinexError};

const BUFFER_DIR_NAME: &str = "buffers";
const STATE_FILE_NAME: &str = "state.json";
const TEMP_FILE_NAME: &str = "material.bin";
const DLQ_CONSUMER: &str = "ingestd";

/// Message from `source_material.begin`
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct MaterialBeginMessage {
    material_id: String,
    material_kind: String,
    source_identifier: String,
    metadata: JsonValue,
    started_at: String,
}

/// Message from `source_material.end`
#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(dead_code)]
struct MaterialEndMessage {
    material_id: String,
    ended_at: String,
    content_hash: String,
    total_slices: usize,
    total_size_bytes: i64,
    #[serde(default)]
    metadata: JsonValue,
}

/// Persisted assembler state (stored on disk for restart recovery)
#[derive(Debug, Serialize, Deserialize)]
struct PersistedState {
    material_id: String,
    expected_offset: i64,
    slice_count: usize,
    started_at: String,
    material_kind: String,
    source_identifier: String,
    metadata: JsonValue,
    #[serde(default)]
    pending_end: Option<MaterialEndMessage>,
    #[serde(default)]
    finalizing: bool,
}

/// DLQ payload for material failures
#[derive(Debug, Serialize)]
struct MaterialDlqPayload {
    material_id: String,
    error: String,
    context: JsonValue,
    failed_at: DateTime<Utc>,
}

/// Assembler state held in memory
#[derive(Debug)]
struct AssemblerState {
    material_id: Ulid,
    temp_path: PathBuf,
    temp_file: Option<File>,
    expected_offset: i64,
    slice_count: usize,
    buffered_slices: BTreeMap<i64, PathBuf>,
    state_dir: PathBuf,
    started_at: DateTime<Utc>,
    material_kind: String,
    source_identifier: String,
    metadata: JsonValue,
    hasher: Hasher,
    pending_end: Option<MaterialEndMessage>,
    finalizing: bool,
}

#[derive(Clone)]
struct FinalizationState {
    material_id: Ulid,
    temp_path: PathBuf,
    expected_offset: i64,
    slice_count: usize,
    buffered_count: usize,
    metadata: JsonValue,
    material_kind: String,
    source_identifier: String,
    started_at: DateTime<Utc>,
}

impl AssemblerState {
    fn buffers_dir(&self) -> PathBuf {
        self.state_dir.join(BUFFER_DIR_NAME)
    }

    fn state_file(&self) -> PathBuf {
        self.state_dir.join(STATE_FILE_NAME)
    }

    fn finalization_view(&self) -> FinalizationState {
        FinalizationState {
            material_id: self.material_id,
            temp_path: self.temp_path.clone(),
            expected_offset: self.expected_offset,
            slice_count: self.slice_count,
            buffered_count: self.buffered_slices.len(),
            metadata: self.metadata.clone(),
            material_kind: self.material_kind.clone(),
            source_identifier: self.source_identifier.clone(),
            started_at: self.started_at,
        }
    }
}

fn take_buffered_slice(
    state: &mut AssemblerState,
    material_id: Ulid,
    offset: i64,
) -> IngestdResult<PathBuf> {
    state.buffered_slices.remove(&offset).ok_or_else(|| {
        SinexError::service(format!(
            "Missing buffered slice for {material_id} at offset {offset}"
        ))
    })
}

fn normalize_metadata(value: JsonValue) -> JsonValue {
    match value {
        JsonValue::Object(_) => value,
        JsonValue::Null => json!({}),
        other => {
            let mut map = JsonMap::new();
            map.insert("value".to_string(), other);
            JsonValue::Object(map)
        }
    }
}

fn merge_metadata(base: &JsonValue, updates: &JsonValue) -> JsonValue {
    let mut merged = normalize_metadata(base.clone());
    if let Some(target) = merged.as_object_mut() {
        match updates {
            JsonValue::Object(map) => {
                for (key, value) in map {
                    target.insert(key.clone(), value.clone());
                }
            }
            JsonValue::Null => {}
            other => {
                target.insert("value".to_string(), other.clone());
            }
        }
    }
    merged
}

fn build_finalize_metadata(
    state: &FinalizationState,
    end_metadata: &JsonValue,
    ended_at: DateTime<Utc>,
    total_bytes: i64,
    content_hash: &str,
) -> JsonValue {
    let mut merged = merge_metadata(&state.metadata, end_metadata);
    let map = merged
        .as_object_mut()
        .expect("metadata normalized to object during merge");
    map.insert(
        "finalize_reason".to_string(),
        JsonValue::String("jetstream-material".to_string()),
    );
    map.insert(
        "finalized_at".to_string(),
        JsonValue::String(ended_at.to_rfc3339()),
    );
    map.insert(
        "content_hash".to_string(),
        JsonValue::String(content_hash.to_string()),
    );
    map.insert(
        "total_slices".to_string(),
        JsonValue::Number(state.slice_count.into()),
    );
    map.insert(
        "total_bytes".to_string(),
        JsonValue::Number(total_bytes.into()),
    );
    map.entry("material_kind".to_string())
        .or_insert_with(|| JsonValue::String(state.material_kind.clone()));
    map.entry("source_identifier".to_string())
        .or_insert_with(|| JsonValue::String(state.source_identifier.clone()));
    merged
}

/// Material assembler service
pub struct MaterialAssembler {
    js: jetstream::Context,
    nats_client: NatsClient,
    pool: DbPool,
    env: SinexEnvironment,
    annex: Arc<GitAnnex>,
    assembler_state: Arc<RwLock<HashMap<Ulid, Arc<Mutex<AssemblerState>>>>>,
    state_root: PathBuf,
    dlq_subject: String,
}

struct MaterialConsumerHandles {
    begin: tokio::task::JoinHandle<IngestdResult<()>>,
    slices: tokio::task::JoinHandle<IngestdResult<()>>,
    end: tokio::task::JoinHandle<IngestdResult<()>>,
}

impl Drop for MaterialConsumerHandles {
    fn drop(&mut self) {
        self.begin.abort();
        self.slices.abort();
        self.end.abort();
    }
}

impl MaterialAssembler {
    /// Create a new material assembler
    pub fn new(
        nats_client: NatsClient,
        pool: DbPool,
        annex: Arc<GitAnnex>,
        state_root: PathBuf,
    ) -> IngestdResult<Self> {
        if let Err(e) = std::fs::create_dir_all(&state_root) {
            return Err(SinexError::io(format!(
                "Failed to create assembler state directory {}: {}",
                state_root.display(),
                e
            )));
        }

        let js = jetstream::new(nats_client.clone());
        let env = sinex_core::environment().clone();

        Ok(Self {
            js,
            nats_client,
            pool,
            env: env.clone(),
            annex,
            assembler_state: Arc::new(RwLock::new(HashMap::new())),
            state_root,
            dlq_subject: env.nats_subject(&format!("events.dlq.{DLQ_CONSUMER}")),
        })
    }

    /// Restore persisted assembler state on startup
    async fn restore_state(&self) -> IngestdResult<()> {
        let mut entries = match fs::read_dir(&self.state_root).await {
            Ok(entries) => entries,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(err) => {
                return Err(SinexError::io(format!(
                    "Failed to read assembler state root {}: {}",
                    self.state_root.display(),
                    err
                )));
            }
        };

        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|e| SinexError::io(format!("Failed to iterate state directory: {}", e)))?
        {
            let path = entry.path();
            if !entry
                .file_type()
                .await
                .map_err(|e| SinexError::io(format!("Failed to inspect state entry: {}", e)))?
                .is_dir()
            {
                continue;
            }

            let state_file = path.join(STATE_FILE_NAME);
            if !state_file.exists() {
                continue;
            }

            let data = fs::read(&state_file).await.map_err(|e| {
                SinexError::io(format!(
                    "Failed to read state file {}: {}",
                    state_file.display(),
                    e
                ))
            })?;

            let persisted: PersistedState = match serde_json::from_slice(&data) {
                Ok(state) => state,
                Err(e) => {
                    warn!(
                        path = %state_file.display(),
                        "Failed to decode persisted assembler state: {}",
                        e
                    );
                    continue;
                }
            };

            let material_id = match Ulid::from_str(&persisted.material_id) {
                Ok(id) => id,
                Err(e) => {
                    warn!(
                        material_id = %persisted.material_id,
                        "Invalid material_id in persisted state: {}",
                        e
                    );
                    continue;
                }
            };

            let temp_path = path.join(TEMP_FILE_NAME);
            let temp_file = File::options()
                .create(true)
                .append(true)
                .open(&temp_path)
                .await
                .map_err(|e| {
                    SinexError::io(format!(
                        "Failed to open temp file {}: {}",
                        temp_path.display(),
                        e
                    ))
                })?;

            // Recompute hasher from existing bytes
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

            // Load buffered slices
            let mut buffered_slices = BTreeMap::new();
            let buffers_dir = path.join(BUFFER_DIR_NAME);
            if buffers_dir.exists() {
                let mut buffer_entries = fs::read_dir(&buffers_dir).await.map_err(|e| {
                    SinexError::io(format!(
                        "Failed to read buffer dir {}: {}",
                        buffers_dir.display(),
                        e
                    ))
                })?;

                while let Some(buf_entry) = buffer_entries.next_entry().await.map_err(|e| {
                    SinexError::io(format!("Failed to iterate buffered slices: {}", e))
                })? {
                    let buf_path = buf_entry.path();
                    if !buf_entry
                        .file_type()
                        .await
                        .map_err(|e| {
                            SinexError::io(format!("Failed to inspect buffered slice: {}", e))
                        })?
                        .is_file()
                    {
                        continue;
                    }

                    let offset = match buf_path
                        .file_stem()
                        .and_then(|stem| stem.to_str())
                        .and_then(|stem| stem.parse::<i64>().ok())
                    {
                        Some(offset) => offset,
                        None => {
                            warn!(
                                path = %buf_path.display(),
                                "Skipping buffered slice with invalid filename"
                            );
                            continue;
                        }
                    };

                    buffered_slices.insert(offset, buf_path);
                }
            }

            let started_at = DateTime::parse_from_rfc3339(&persisted.started_at)
                .map(|dt| dt.with_timezone(&Utc))
                .unwrap_or_else(|_| Utc::now());

            let state = AssemblerState {
                material_id,
                temp_path,
                temp_file: Some(temp_file),
                expected_offset: persisted.expected_offset,
                slice_count: persisted.slice_count,
                buffered_slices,
                state_dir: path.clone(),
                started_at,
                material_kind: persisted.material_kind,
                source_identifier: persisted.source_identifier,
                metadata: normalize_metadata(persisted.metadata),
                hasher,
                pending_end: persisted.pending_end,
                finalizing: persisted.finalizing,
            };

            self.insert_state_handle(material_id, state).await;

            info!(material_id = %material_id, "Restored in-flight material state");
        }

        Ok(())
    }

    /// Persist assembler state to disk
    async fn persist_state(&self, state: &AssemblerState) -> IngestdResult<()> {
        let persisted = PersistedState {
            material_id: state.material_id.to_string(),
            expected_offset: state.expected_offset,
            slice_count: state.slice_count,
            started_at: state.started_at.to_rfc3339(),
            material_kind: state.material_kind.clone(),
            source_identifier: state.source_identifier.clone(),
            metadata: state.metadata.clone(),
            pending_end: state.pending_end.clone(),
            finalizing: state.finalizing,
        };

        let serialized = serde_json::to_vec_pretty(&persisted).map_err(|e| {
            SinexError::serialization(format!(
                "Failed to serialize assembler state for {}: {}",
                state.material_id, e
            ))
        })?;

        fs::create_dir_all(&state.state_dir).await.map_err(|e| {
            SinexError::io(format!(
                "Failed to ensure assembler state dir {}: {}",
                state.state_dir.display(),
                e
            ))
        })?;

        fs::write(state.state_file(), serialized)
            .await
            .map_err(|e| {
                SinexError::io(format!(
                    "Failed to persist assembler state for {}: {}",
                    state.material_id, e
                ))
            })?;

        Ok(())
    }

    /// Remove the persisted state directory for a material
    async fn cleanup_state(&self, material_id: Ulid) {
        let path = self.state_root.join(material_id.to_string());
        if let Err(e) = fs::remove_dir_all(&path).await {
            warn!(
                material_id = %material_id,
                path = %path.display(),
                "Failed to remove assembler state directory: {}",
                e
            );
        }
    }

    /// Fetch a handle to an existing assembler state for a material.
    async fn get_state_handle(&self, material_id: &Ulid) -> Option<Arc<Mutex<AssemblerState>>> {
        self.assembler_state.read().await.get(material_id).cloned()
    }

    /// Insert a new assembler state if one does not already exist.
    async fn insert_state_handle(
        &self,
        material_id: Ulid,
        state: AssemblerState,
    ) -> Arc<Mutex<AssemblerState>> {
        let state_handle = Arc::new(Mutex::new(state));

        let mut states = self.assembler_state.write().await;
        if let Some(existing) = states.get(&material_id) {
            existing.clone()
        } else {
            states.insert(material_id, state_handle.clone());
            state_handle
        }
    }

    /// Build a placeholder assembler state for materials whose slices arrive before the begin message.
    async fn create_placeholder_state(&self, material_id: Ulid) -> IngestdResult<AssemblerState> {
        let state_dir = self.state_root.join(material_id.to_string());
        fs::create_dir_all(&state_dir)
            .await
            .map_err(|e| SinexError::io(format!("Failed to create assembler state dir: {}", e)))?;

        let temp_path = state_dir.join(TEMP_FILE_NAME);
        let temp_file = File::create(&temp_path)
            .await
            .map_err(|e| SinexError::io(format!("Failed to create temp file: {}", e)))?;

        Ok(AssemblerState {
            material_id,
            temp_path,
            temp_file: Some(temp_file),
            expected_offset: 0,
            slice_count: 0,
            buffered_slices: BTreeMap::new(),
            state_dir,
            started_at: Utc::now(),
            material_kind: "unknown".to_string(),
            source_identifier: "unknown".to_string(),
            metadata: json!({}),
            hasher: Hasher::new(),
            pending_end: None,
            finalizing: false,
        })
    }

    /// Handle a begin message
    async fn handle_begin(&self, msg: jetstream::Message) -> IngestdResult<()> {
        let mut begin: MaterialBeginMessage =
            serde_json::from_slice(&msg.payload).map_err(|e| {
                SinexError::parse(format!("Failed to decode begin message payload: {}", e))
            })?;
        begin.metadata = normalize_metadata(begin.metadata);

        let material_id = Ulid::from_str(&begin.material_id).map_err(|e| {
            SinexError::parse(format!(
                "Invalid material_id '{}' in begin message: {}",
                begin.material_id, e
            ))
        })?;

        let started_at = DateTime::parse_from_rfc3339(&begin.started_at)
            .map(|dt| dt.with_timezone(&Utc))
            .unwrap_or_else(|_| Utc::now());

        if let Some(existing_handle) = self.get_state_handle(&material_id).await {
            {
                let mut existing = existing_handle.lock().await;
                // We may have created a placeholder state from slices arriving first; enrich it.
                existing.material_kind = begin.material_kind.clone();
                existing.source_identifier = begin.source_identifier.clone();
                existing.metadata = begin.metadata.clone();
                existing.started_at = started_at;
                self.persist_state(&existing).await?;
            }

            self.register_material_record(
                material_id,
                &begin.material_kind,
                &begin.source_identifier,
                begin.metadata.clone(),
                started_at,
            )
            .await?;

            debug!(
                material_id = %material_id,
                "Begin message updated existing assembler state"
            );
            return Ok(());
        }

        let state_dir = self.state_root.join(material_id.to_string());
        fs::create_dir_all(&state_dir)
            .await
            .map_err(|e| SinexError::io(format!("Failed to create assembler state dir: {}", e)))?;

        let temp_path = state_dir.join(TEMP_FILE_NAME);
        let temp_file = File::create(&temp_path)
            .await
            .map_err(|e| SinexError::io(format!("Failed to create temp file: {}", e)))?;

        let state = AssemblerState {
            material_id,
            temp_path,
            temp_file: Some(temp_file),
            expected_offset: 0,
            slice_count: 0,
            buffered_slices: BTreeMap::new(),
            state_dir,
            started_at,
            material_kind: begin.material_kind,
            source_identifier: begin.source_identifier,
            metadata: begin.metadata,
            hasher: Hasher::new(),
            pending_end: None,
            finalizing: false,
        };

        let register_metadata = state.metadata.clone();
        let register_kind = state.material_kind.clone();
        let register_identifier = state.source_identifier.clone();

        self.persist_state(&state).await?;
        self.insert_state_handle(material_id, state).await;
        self.register_material_record(
            material_id,
            &register_kind,
            &register_identifier,
            register_metadata,
            started_at,
        )
        .await?;
        info!(material_id = %material_id, "Initialized material assembler state");

        Ok(())
    }

    async fn register_material_record(
        &self,
        material_id: Ulid,
        material_kind: &str,
        source_identifier: &str,
        metadata: JsonValue,
        started_at: DateTime<Utc>,
    ) -> IngestdResult<()> {
        self.pool
            .source_materials()
            .register_external_in_flight(
                material_id,
                material_kind,
                Some(source_identifier),
                metadata,
                started_at,
            )
            .await
            .map(|_| ())
            .map_err(|e| {
                SinexError::database(format!(
                    "Failed to register source material {}: {}",
                    material_id, e
                ))
            })
    }

    /// Store a slice (in-order or buffered) for a material
    async fn handle_slice(
        &self,
        material_id: Ulid,
        offset: i64,
        data: Vec<u8>,
    ) -> IngestdResult<()> {
        let state_handle = if let Some(existing) = self.get_state_handle(&material_id).await {
            existing
        } else {
            // Slices may arrive before the begin message due to JetStream scheduling.
            // Create a placeholder state so we can buffer slices and let the later
            // begin message fill in metadata.
            let placeholder = self.create_placeholder_state(material_id).await?;
            self.insert_state_handle(material_id, placeholder).await
        };

        let mut state = state_handle.lock().await;

        if state.finalizing {
            debug!(
                material_id = %material_id,
                offset,
                "Ignoring slice received while material is finalizing"
            );
            return Ok(());
        }

        if offset == state.expected_offset {
            if let Some(file) = state.temp_file.as_mut() {
                file.write_all(&data).await.map_err(|e| {
                    SinexError::io(format!("Failed to write slice for {}: {}", material_id, e))
                })?;
                file.flush().await.map_err(|e| {
                    SinexError::io(format!("Failed to flush slice for {}: {}", material_id, e))
                })?;
            }

            state.hasher.update(&data);
            state.expected_offset += data.len() as i64;
            state.slice_count += 1;

            // Flush any buffered slices that are now in order
            loop {
                let next_offset = match state.buffered_slices.first_key_value() {
                    Some((&next_offset, _)) => next_offset,
                    None => break,
                };

                if next_offset != state.expected_offset {
                    break;
                }

                let buf_path = match take_buffered_slice(&mut state, material_id, next_offset) {
                    Ok(path) => path,
                    Err(err) => {
                        warn!(
                            material_id = %material_id,
                            offset = next_offset,
                            error = %err,
                            "Buffered slice vanished while flushing; aborting material assembly"
                        );
                        return Err(err);
                    }
                };

                let buffered_data = fs::read(&buf_path).await.map_err(|e| {
                    SinexError::io(format!(
                        "Failed to read buffered slice {} for {}: {}",
                        next_offset, material_id, e
                    ))
                })?;

                if let Some(file) = state.temp_file.as_mut() {
                    file.write_all(&buffered_data).await.map_err(|e| {
                        SinexError::io(format!(
                            "Failed to write buffered slice for {}: {}",
                            material_id, e
                        ))
                    })?;
                    file.flush().await.map_err(|e| {
                        SinexError::io(format!(
                            "Failed to flush buffered slice for {}: {}",
                            material_id, e
                        ))
                    })?;
                }

                state.hasher.update(&buffered_data);
                state.expected_offset += buffered_data.len() as i64;
                state.slice_count += 1;

                if let Err(e) = fs::remove_file(&buf_path).await {
                    warn!(
                        path = %buf_path.display(),
                        material_id = %material_id,
                        "Failed to remove buffered slice file: {}",
                        e
                    );
                }
            }
        } else if offset > state.expected_offset {
            fs::create_dir_all(state.buffers_dir())
                .await
                .map_err(|e| SinexError::io(format!("Failed to create buffer dir: {}", e)))?;

            let buffer_path = state.buffers_dir().join(format!("{}.bin", offset));
            fs::write(&buffer_path, &data)
                .await
                .map_err(|e| SinexError::io(format!("Failed to persist buffered slice: {}", e)))?;

            state.buffered_slices.insert(offset, buffer_path);
            debug!(
                material_id = %material_id,
                offset,
                expected = state.expected_offset,
                "Buffered out-of-order slice"
            );
        } else {
            debug!(
                material_id = %material_id,
                offset,
                expected = state.expected_offset,
                "Ignoring duplicate or overlapping slice"
            );
        }

        self.persist_state(&state).await?;
        Ok(())
    }

    /// Insert or fetch blob metadata for the assembled material
    async fn upsert_blob(
        &self,
        state: &FinalizationState,
        annex_key: &AnnexKey,
        content_hash: &str,
    ) -> IngestdResult<Id<Blob>> {
        let repo = self.pool.blobs();

        if let Some(existing) = repo
            .get_by_content(&annex_key.backend, &annex_key.hash, annex_key.size as i64)
            .await
            .map_err(|e| {
                error!(
                    material_id = %state.material_id,
                    backend = %annex_key.backend,
                    hash = %annex_key.hash,
                    size = annex_key.size,
                    error = %e,
                    error_debug = ?e,
                    "Failed to query blob store"
                );
                SinexError::database(format!("Failed to query blob store: {}", e))
            })?
        {
            return Ok(Id::from_ulid(existing.id.as_ulid().clone()));
        }

        let metadata = json!({
            "material_id": state.material_id.to_string(),
            "source_identifier": state.source_identifier,
            "material_kind": state.material_kind,
            "total_slices": state.slice_count,
        });

        let blob = Blob::builder()
            .annex_backend(annex_key.backend.clone())
            .content_hash(annex_key.hash.clone())
            .original_filename(state.source_identifier.clone())
            .size_bytes(annex_key.size as i64)
            .checksum_blake3(content_hash.to_string())
            .metadata(metadata)
            .build();

        let stored = repo.insert(blob).await.map_err(|e| {
            error!(
                material_id = %state.material_id,
                backend = %annex_key.backend,
                hash = %annex_key.hash,
                size = annex_key.size,
                error = %e,
                error_debug = ?e,
                "Failed to insert blob metadata"
            );
            SinexError::database(format!("Failed to insert blob metadata: {}", e))
        })?;

        Ok(Id::from_ulid(stored.id.as_ulid().clone()))
    }

    /// Finalize source material registry and ledger
    async fn finalize_material_record(
        &self,
        state: &FinalizationState,
        blob_id: Id<Blob>,
        total_size_bytes: i64,
        metadata: JsonValue,
    ) -> IngestdResult<()> {
        let repo = self.pool.source_materials();
        let id: Id<SourceMaterialRecord> = Id::from_ulid(state.material_id);

        repo.update_metadata(id, metadata.clone())
            .await
            .map_err(|e| {
                SinexError::database(format!("Failed to update material metadata: {}", e))
            })?;

        let encoding_hint = metadata
            .as_object()
            .and_then(|map| map.get("encoding"))
            .and_then(|value| value.as_str())
            .map(|s| s.to_string());
        let content_preview_hint = metadata
            .as_object()
            .and_then(|map| map.get("content_preview"))
            .and_then(|value| value.as_str())
            .map(|s| s.to_string());

        repo.finalize_in_flight(
            Id::from_ulid(state.material_id),
            Some(blob_id),
            encoding_hint.as_deref(),
            content_preview_hint.clone(),
            Some(total_size_bytes),
        )
        .await
        .map_err(|e| SinexError::database(format!("Failed to finalize material: {}", e)))
    }

    /// Append entry in raw.temporal_ledger
    async fn record_ledger_entry(&self, state: &FinalizationState) -> IngestdResult<()> {
        sqlx::query!(
            r#"
            INSERT INTO raw.temporal_ledger
                (source_material_id, offset_start, offset_end, offset_kind, ts_capture, precision, clock, source_type)
            VALUES (($1::uuid)::ulid, $2, $3, $4, $5, $6, $7, $8)
            "#,
            ulid_to_uuid(state.material_id),
            0_i64,
            state.expected_offset,
            "byte",
            state.started_at,
            "bounded",
            "wall",
            "realtime_capture"
        )
        .execute(&self.pool)
        .await
        .map_err(|e| SinexError::database(format!("Failed to append temporal ledger entry: {}", e)))?;

        Ok(())
    }

    /// Route material failure to DLQ
    async fn route_material_error(
        &self,
        material_id: Ulid,
        error: impl Into<String>,
        context: JsonValue,
    ) {
        let payload = MaterialDlqPayload {
            material_id: material_id.to_string(),
            error: error.into(),
            context,
            failed_at: Utc::now(),
        };

        match serde_json::to_vec(&payload) {
            Ok(bytes) => {
                if let Err(e) = self
                    .nats_client
                    .publish(self.dlq_subject.clone(), bytes.into())
                    .await
                {
                    error!(
                        material_id = %material_id,
                        "Failed to publish material DLQ entry: {}",
                        e
                    );
                } else {
                    debug!(material_id = %material_id, "Routed to DLQ");
                }
            }
            Err(e) => {
                error!(
                    material_id = %material_id,
                    "Failed to encode DLQ payload: {}",
                    e
                );
            }
        }
    }

    /// Import the assembled material into git-annex
    async fn import_into_annex(
        &self,
        state: &FinalizationState,
    ) -> IngestdResult<(AnnexKey, PathBuf)> {
        let relative_utf8 = Utf8PathBuf::from(format!("materials/{}.bin", state.material_id));
        let repo_path = self.annex.repo_path();
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
                            "Failed to copy assembled file into annex: {}",
                            copy_err
                        ))
                    })?;
                fs::remove_file(&state.temp_path)
                    .await
                    .map_err(|remove_err| {
                        SinexError::io(format!(
                            "Failed to remove staging file after copy: {}",
                            remove_err
                        ))
                    })?;
            } else {
                return Err(SinexError::io(format!(
                    "Failed to move assembled file into annex: {}",
                    e
                )));
            }
        }

        let annex_key = self
            .annex
            .add_file(&relative_utf8)
            .await
            .map_err(|e| SinexError::io(format!("git-annex add failed: {}", e)))?;

        Ok((annex_key, target_path))
    }

    /// Handle material finalization (end message)
    async fn handle_end(&self, mut end: MaterialEndMessage) -> IngestdResult<()> {
        end.metadata = normalize_metadata(end.metadata);
        let material_id = Ulid::from_str(&end.material_id).map_err(|e| {
            SinexError::parse(format!(
                "Invalid material_id '{}' in end message: {}",
                end.material_id, e
            ))
        })?;
        let ended_at = DateTime::parse_from_rfc3339(&end.ended_at)
            .map(|dt| dt.with_timezone(&Utc))
            .unwrap_or_else(|_| Utc::now());

        if self.pool.is_closed() {
            error!(
                material_id = %material_id,
                "Database pool closed before handling end message"
            );
            return Err(SinexError::database(
                "database pool closed before end processing".to_string(),
            ));
        }

        let state_handle = if let Some(existing) = self.get_state_handle(&material_id).await {
            existing
        } else {
            // End may arrive before begin/slices (separate streams). Create a placeholder so we can
            // record the end and finalize once the missing slices arrive.
            warn!(
                material_id = %material_id,
                "End message received before material state existed; creating placeholder"
            );
            let placeholder = self.create_placeholder_state(material_id).await?;
            self.insert_state_handle(material_id, placeholder).await
        };

        // Record end so we can tolerate out-of-order delivery across begin/slices/end streams.
        {
            let mut state = state_handle.lock().await;
            if state.finalizing {
                debug!(material_id = %material_id, "Ignoring end message while finalizing");
                return Ok(());
            }
            state.pending_end = Some(end);
            self.persist_state(&state).await?;
        }

        let (final_state, assembled_bytes, slice_count, computed_hash, end) = {
            let mut state = state_handle.lock().await;
            let end_preview = state
                .pending_end
                .clone()
                .expect("pending_end set immediately above");

            let view = state.finalization_view();
            let assembled_bytes = view.expected_offset;
            let slice_count = view.slice_count;

            // Not complete yet: keep the end in state and ask JetStream to redeliver later.
            let expected_slices = end_preview.total_slices;
            let expected_bytes = end_preview.total_size_bytes;
            let seen_slices = view.slice_count.saturating_add(view.buffered_count);

            // If the end metadata makes the current buffered state impossible to finalize, treat
            // it as corruption and route to DLQ instead of NAK-looping forever.
            //
            // Example: a slice arrives with an offset beyond the claimed total byte size, or we
            // have already seen as many slices as the end claims exist but still can't assemble.
            let has_invalid_offsets = state
                .buffered_slices
                .keys()
                .any(|off| *off < 0 || *off >= expected_bytes);

            if expected_bytes < 0
                || view.expected_offset > expected_bytes
                || has_invalid_offsets
                || (seen_slices >= expected_slices && view.expected_offset != expected_bytes)
            {
                let reason = if expected_bytes < 0 {
                    format!("invalid end.total_size_bytes={expected_bytes}")
                } else if view.expected_offset > expected_bytes {
                    format!(
                        "assembled_bytes={} exceeds expected_bytes={}",
                        view.expected_offset, expected_bytes
                    )
                } else if has_invalid_offsets {
                    format!(
                        "buffered slice offsets outside expected_bytes={expected_bytes} (buffered_offsets={:?})",
                        state.buffered_slices.keys().cloned().collect::<Vec<_>>()
                    )
                } else {
                    format!(
                        "cannot assemble full material: assembled_bytes={} expected_bytes={} slice_count={} buffered_count={} expected_slices={}",
                        view.expected_offset,
                        expected_bytes,
                        view.slice_count,
                        view.buffered_count,
                        expected_slices
                    )
                };

                let ctx = json!({
                    "reason": reason,
                    "assembled_bytes": view.expected_offset,
                    "slice_count": view.slice_count,
                    "buffered_offsets": state.buffered_slices.keys().cloned().collect::<Vec<_>>(),
                    "expected_bytes": expected_bytes,
                    "expected_slices": expected_slices,
                    "end": {
                        "ended_at": end_preview.ended_at,
                        "content_hash": end_preview.content_hash,
                    }
                });

                drop(state);
                self.route_material_error(
                    material_id,
                    "material assembly corruption detected",
                    ctx,
                )
                .await;
                self.cleanup_state(material_id).await;
                let _ = self.assembler_state.write().await.remove(&material_id);
                return Ok(());
            }

            if view.buffered_count > 0
                || view.expected_offset < expected_bytes
                || view.slice_count < expected_slices
            {
                return Err(SinexError::service(format!(
                    "end received before all slices were processed for {material_id}: assembled_bytes={} slice_count={} buffered={} expected_bytes={} expected_slices={}",
                    view.expected_offset,
                    view.slice_count,
                    view.buffered_count,
                    expected_bytes,
                    expected_slices
                )));
            }

            // Complete: transition into finalization. Prevent concurrent slice writes by taking
            // the file handle and marking finalizing.
            state.finalizing = true;
            let end = state
                .pending_end
                .take()
                .expect("pending_end must exist when finalizing");

            if let Some(mut file) = state.temp_file.take() {
                if let Err(e) = file.flush().await {
                    warn!(
                        material_id = %material_id,
                        "Failed to flush temp file during finalization: {}",
                        e
                    );
                }
            }

            let computed_hash = state.hasher.clone().finalize().to_hex().to_string();
            self.persist_state(&state).await?;

            (view, assembled_bytes, slice_count, computed_hash, end)
        };

        debug!(
            material_id = %material_id,
            assembled_bytes,
            slice_count,
            reported_total = end.total_size_bytes,
            temp_path = %final_state.temp_path.display(),
            "Processing end message"
        );

        // If the payload claims zero bytes, avoid annex/blob work and treat this as an empty
        // material. Persist a DLQ entry so publishers can diagnose.
        if end.total_size_bytes == 0 {
            warn!(
                material_id = %material_id,
                slices = slice_count,
                total_size = end.total_size_bytes,
                "Material ended with no content; skipping annex import and routing to DLQ"
            );

            self.route_material_error(
                material_id,
                "empty_material",
                json!({
                    "slice_count": slice_count,
                    "expected_size": end.total_size_bytes
                }),
            )
            .await;

            self.cleanup_state(material_id).await;
            let _ = self.assembler_state.write().await.remove(&material_id);
            return Ok(());
        }

        // Ensure the assembled file exists even if no slices were processed (e.g., out-of-order messages).
        if !final_state.temp_path.exists() {
            if let Some(parent) = final_state.temp_path.parent() {
                fs::create_dir_all(parent).await.map_err(|e| {
                    SinexError::io(format!(
                        "Failed to create temp file parent directory {}: {}",
                        parent.display(),
                        e
                    ))
                })?;
            }
            File::create(&final_state.temp_path).await.map_err(|e| {
                SinexError::io(format!(
                    "Failed to recreate missing assembled file {}: {}",
                    final_state.temp_path.display(),
                    e
                ))
            })?;
        }

        // Ensure no buffered slices remain
        if final_state.buffered_count > 0 {
            warn!(
                material_id = %material_id,
                buffered = final_state.buffered_count,
                "Buffered slices remain when end message arrived; forcing flush"
            );
        }

        // Sanity checks: ensure sizes line up before annex import.
        if end.total_size_bytes <= 0 {
            warn!(
                material_id = %material_id,
                assembled_bytes,
                reported_total = end.total_size_bytes,
                "Material ended empty; routing to DLQ instead of annex import"
            );
            self.route_material_error(
                material_id,
                "empty_material",
                json!({
                    "assembled_bytes": assembled_bytes,
                    "reported_total": end.total_size_bytes,
                    "slice_count": slice_count,
                    "buffered_slices": final_state.buffered_count
                }),
            )
            .await;
            self.cleanup_state(material_id).await;
            let _ = self.assembler_state.write().await.remove(&material_id);
            return Ok(());
        }

        if assembled_bytes != end.total_size_bytes {
            warn!(
                material_id = %material_id,
                assembled_bytes,
                reported_total = end.total_size_bytes,
                "Material size mismatch between assembled bytes and end message; routing to DLQ"
            );
            self.route_material_error(
                material_id,
                "material_size_mismatch",
                json!({
                    "assembled_bytes": assembled_bytes,
                    "reported_total": end.total_size_bytes,
                    "slice_count": slice_count,
                    "buffered_slices": final_state.buffered_count
                }),
            )
            .await;
            self.cleanup_state(material_id).await;
            let _ = self.assembler_state.write().await.remove(&material_id);
            return Ok(());
        }

        if computed_hash != end.content_hash {
            warn!(
                material_id = %material_id,
                expected = %end.content_hash,
                actual = %computed_hash,
                "Material hash mismatch detected"
            );

            self.route_material_error(
                material_id,
                "material_hash_mismatch",
                json!({
                    "expected_hash": end.content_hash,
                    "actual_hash": computed_hash,
                }),
            )
            .await;

            self.cleanup_state(material_id).await;
            let _ = self.assembler_state.write().await.remove(&material_id);
            return Ok(());
        }

        // Verify the staged file size matches expectations before annex import.
        let file_size = fs::metadata(&final_state.temp_path)
            .await
            .map(|m| m.len() as i64)
            .unwrap_or(0);
        if file_size != assembled_bytes {
            warn!(
                material_id = %material_id,
                file_size,
                assembled_bytes,
                "Assembled file size on disk does not match assembled bytes; routing to DLQ"
            );
            self.route_material_error(
                material_id,
                "material_size_mismatch_disk",
                json!({
                    "assembled_bytes": assembled_bytes,
                    "file_size": file_size,
                    "reported_total": end.total_size_bytes,
                }),
            )
            .await;
            self.cleanup_state(material_id).await;
            let _ = self.assembler_state.write().await.remove(&material_id);
            return Ok(());
        }

        let (annex_key, final_path) = match self.import_into_annex(&final_state).await {
            Ok(result) => result,
            Err(e) => {
                self.route_material_error(
                    material_id,
                    "annex_import_failed",
                    json!({ "error": e.to_string() }),
                )
                .await;
                {
                    let mut state = state_handle.lock().await;
                    state.finalizing = false;
                    state.pending_end = Some(end);
                    let _ = self.persist_state(&state).await;
                }
                return Err(e);
            }
        };

        let blob_id = match self
            .upsert_blob(&final_state, &annex_key, &end.content_hash)
            .await
        {
            Ok(id) => id,
            Err(e) => {
                self.route_material_error(
                    material_id,
                    "blob_registration_failed",
                    json!({ "error": e.to_string() }),
                )
                .await;
                {
                    let mut state = state_handle.lock().await;
                    state.finalizing = false;
                    state.pending_end = Some(end);
                    let _ = self.persist_state(&state).await;
                }
                return Err(e);
            }
        };

        let finalize_metadata = build_finalize_metadata(
            &final_state,
            &end.metadata,
            ended_at,
            end.total_size_bytes,
            &end.content_hash,
        );

        if let Err(e) = self
            .finalize_material_record(
                &final_state,
                blob_id,
                end.total_size_bytes,
                finalize_metadata,
            )
            .await
        {
            self.route_material_error(
                material_id,
                "material_finalize_failed",
                json!({ "error": e.to_string() }),
            )
            .await;
            {
                let mut state = state_handle.lock().await;
                state.finalizing = false;
                state.pending_end = Some(end);
                let _ = self.persist_state(&state).await;
            }
            return Err(e);
        }

        if let Err(e) = self.record_ledger_entry(&final_state).await {
            self.route_material_error(
                material_id,
                "ledger_append_failed",
                json!({ "error": e.to_string() }),
            )
            .await;
            {
                let mut state = state_handle.lock().await;
                state.finalizing = false;
                state.pending_end = Some(end);
                let _ = self.persist_state(&state).await;
            }
            return Err(e);
        }

        self.cleanup_state(material_id).await;
        let _ = self.assembler_state.write().await.remove(&material_id);

        info!(
            material_id = %material_id,
            annex_key = %annex_key.key,
            path = %final_path.display(),
            size_bytes = end.total_size_bytes,
            slices = slice_count,
            "Material assembly complete and persisted to annex"
        );

        Ok(())
    }

    /// Spawn consumer for begin messages
    fn spawn_begin_consumer(&self) -> tokio::task::JoinHandle<IngestdResult<()>> {
        let js = self.js.clone();
        let env = self.env.clone();
        let assembler = self.clone_for_task();

        tokio::spawn(async move {
            let stream_name = env.nats_stream_name("SOURCE_MATERIAL_BEGIN");
            let stream = js
                .get_stream(&stream_name)
                .await
                .map_err(|e| SinexError::network(format!("Failed to get begin stream: {}", e)))?;

            let consumer = stream
                .get_or_create_consumer(
                    "ingestd_material_begin",
                    jetstream::consumer::pull::Config {
                        durable_name: Some("ingestd_material_begin".to_string()),
                        ack_policy: jetstream::consumer::AckPolicy::Explicit,
                        // Critical for correctness: tests (and real systems) may publish before this
                        // consumer is created on first startup; don't silently skip earlier messages.
                        deliver_policy: jetstream::consumer::DeliverPolicy::All,
                        ..Default::default()
                    },
                )
                .await
                .map_err(|e| {
                    SinexError::network(format!("Failed to create begin consumer: {}", e))
                })?;

            loop {
                let mut messages =
                    consumer
                        .batch()
                        .max_messages(50)
                        .messages()
                        .await
                        .map_err(|e| {
                            SinexError::network(format!("Failed to fetch begin messages: {}", e))
                        })?;

                while let Some(message) = messages.next().await {
                    let message = match message {
                        Ok(msg) => msg,
                        Err(e) => {
                            warn!("Error receiving begin message: {}", e);
                            continue;
                        }
                    };

                    if let Err(err) = assembler.handle_begin(message.clone()).await {
                        error!("Failed to process begin message: {}", err);
                        let _ = message.ack_with(jetstream::AckKind::Nak(None)).await;
                        continue;
                    }

                    if let Err(e) = message.ack().await {
                        warn!("Failed to ack begin message: {}", e);
                    }
                }
            }
        })
    }

    /// Spawn consumer for slice messages
    fn spawn_slices_consumer(&self) -> tokio::task::JoinHandle<IngestdResult<()>> {
        let js = self.js.clone();
        let env = self.env.clone();
        let assembler = self.clone_for_task();

        tokio::spawn(async move {
            let stream_name = env.nats_stream_name("SOURCE_MATERIAL_SLICES");
            let stream = js
                .get_stream(&stream_name)
                .await
                .map_err(|e| SinexError::network(format!("Failed to get slices stream: {}", e)))?;

            let consumer = stream
                .get_or_create_consumer(
                    "ingestd_material_slices",
                    jetstream::consumer::pull::Config {
                        durable_name: Some("ingestd_material_slices".to_string()),
                        ack_policy: jetstream::consumer::AckPolicy::Explicit,
                        // Same reasoning as begin/end: don't skip slices published before consumer creation.
                        deliver_policy: jetstream::consumer::DeliverPolicy::All,
                        max_ack_pending: 1_000,
                        ..Default::default()
                    },
                )
                .await
                .map_err(|e| {
                    SinexError::network(format!("Failed to create slices consumer: {}", e))
                })?;

            loop {
                let mut messages = consumer
                    .batch()
                    .max_messages(200)
                    .messages()
                    .await
                    .map_err(|e| {
                        SinexError::network(format!("Failed to fetch slice messages: {}", e))
                    })?;

                while let Some(message) = messages.next().await {
                    let message = match message {
                        Ok(msg) => msg,
                        Err(e) => {
                            warn!("Error receiving slice message: {}", e);
                            continue;
                        }
                    };

                    let offset = message
                        .headers
                        .as_ref()
                        .and_then(|h| h.get("Offset"))
                        .and_then(|v| v.as_str().parse::<i64>().ok())
                        .unwrap_or(0);

                    let material_id = message
                        .subject
                        .split('.')
                        .last()
                        .and_then(|part| Ulid::from_str(part).ok());

                    let Some(material_id) = material_id else {
                        warn!(
                            "Slice message missing material id in subject {}",
                            message.subject
                        );
                        let _ = message.ack().await;
                        continue;
                    };

                    if let Err(err) = assembler
                        .handle_slice(material_id, offset, message.payload.to_vec())
                        .await
                    {
                        error!(
                            material_id = %material_id,
                            "Failed to process slice message: {}",
                            err
                        );
                        assembler
                            .route_material_error(
                                material_id,
                                "slice_processing_failed",
                                json!({ "error": err.to_string(), "offset": offset }),
                            )
                            .await;
                        let _ = message.ack_with(jetstream::AckKind::Nak(None)).await;
                        continue;
                    }

                    if let Err(e) = message.ack().await {
                        warn!("Failed to ack slice message: {}", e);
                    }
                }
            }
        })
    }

    /// Spawn consumer for end messages
    fn spawn_end_consumer(&self) -> tokio::task::JoinHandle<IngestdResult<()>> {
        let js = self.js.clone();
        let env = self.env.clone();
        let assembler = self.clone_for_task();

        tokio::spawn(async move {
            let stream_name = env.nats_stream_name("SOURCE_MATERIAL_END");
            let stream = js
                .get_stream(&stream_name)
                .await
                .map_err(|e| SinexError::network(format!("Failed to get end stream: {}", e)))?;

            let consumer = stream
                .get_or_create_consumer(
                    "ingestd_material_end",
                    jetstream::consumer::pull::Config {
                        durable_name: Some("ingestd_material_end".to_string()),
                        ack_policy: jetstream::consumer::AckPolicy::Explicit,
                        // Ensure end messages published before consumer creation are still processed.
                        deliver_policy: jetstream::consumer::DeliverPolicy::All,
                        ..Default::default()
                    },
                )
                .await
                .map_err(|e| {
                    SinexError::network(format!("Failed to create end consumer: {}", e))
                })?;

            loop {
                let mut messages =
                    consumer
                        .batch()
                        .max_messages(50)
                        .messages()
                        .await
                        .map_err(|e| {
                            SinexError::network(format!("Failed to fetch end messages: {}", e))
                        })?;

                while let Some(message) = messages.next().await {
                    let message = match message {
                        Ok(msg) => msg,
                        Err(e) => {
                            warn!("Error receiving end message: {}", e);
                            continue;
                        }
                    };

                    let end_message: MaterialEndMessage =
                        match serde_json::from_slice(&message.payload) {
                            Ok(msg) => msg,
                            Err(e) => {
                                warn!("Failed to decode end message payload: {}", e);
                                if let Err(ack_err) = message.ack().await {
                                    warn!("Failed to ack malformed end message: {}", ack_err);
                                }
                                continue;
                            }
                        };

                    if let Err(err) = assembler.handle_end(end_message).await {
                        error!("Failed to process end message: {}", err);
                        let _ = message
                            .ack_with(jetstream::AckKind::Nak(Some(
                                std::time::Duration::from_millis(200),
                            )))
                            .await;
                        continue;
                    }

                    if let Err(e) = message.ack().await {
                        warn!("Failed to ack end message: {}", e);
                    }
                }
            }
        })
    }

    /// Helper for cloning into async tasks
    fn clone_for_task(&self) -> Self {
        Self {
            js: self.js.clone(),
            nats_client: self.nats_client.clone(),
            pool: self.pool.clone(),
            env: self.env.clone(),
            annex: self.annex.clone(),
            assembler_state: self.assembler_state.clone(),
            state_root: self.state_root.clone(),
            dlq_subject: self.dlq_subject.clone(),
        }
    }

    /// Bootstrap JetStream streams for materials
    async fn bootstrap_streams(&self) -> IngestdResult<()> {
        info!("Bootstrapping material streams");

        self.js
            .get_or_create_stream(jetstream::stream::Config {
                name: self.env.nats_stream_name("SOURCE_MATERIAL_BEGIN"),
                subjects: vec![self.env.nats_subject("source_material.begin")],
                storage: jetstream::stream::StorageType::File,
                ..Default::default()
            })
            .await
            .map_err(|e| SinexError::network(format!("Failed to create begin stream: {}", e)))?;

        self.js
            .get_or_create_stream(jetstream::stream::Config {
                name: self.env.nats_stream_name("SOURCE_MATERIAL_SLICES"),
                subjects: vec![self.env.nats_subject("source_material.slices.>")],
                storage: jetstream::stream::StorageType::File,
                max_age: tokio::time::Duration::from_secs(7 * 24 * 60 * 60),
                max_message_size: 512 * 1024,
                ..Default::default()
            })
            .await
            .map_err(|e| SinexError::network(format!("Failed to create slices stream: {}", e)))?;

        self.js
            .get_or_create_stream(jetstream::stream::Config {
                name: self.env.nats_stream_name("SOURCE_MATERIAL_END"),
                subjects: vec![self.env.nats_subject("source_material.end")],
                storage: jetstream::stream::StorageType::File,
                ..Default::default()
            })
            .await
            .map_err(|e| SinexError::network(format!("Failed to create end stream: {}", e)))?;

        info!("Material streams bootstrapped successfully");
        Ok(())
    }

    /// Run the assembler service
    pub async fn run(self) -> IngestdResult<()> {
        info!("Starting Material Assembler");

        self.bootstrap_streams().await?;
        self.restore_state().await?;

        let mut consumers = MaterialConsumerHandles {
            begin: self.spawn_begin_consumer(),
            slices: self.spawn_slices_consumer(),
            end: self.spawn_end_consumer(),
        };

        tokio::select! {
            result = &mut consumers.begin => {
                return Self::handle_task_exit("material begin consumer", result);
            }
            result = &mut consumers.slices => {
                return Self::handle_task_exit("material slice consumer", result);
            }
            result = &mut consumers.end => {
                return Self::handle_task_exit("material end consumer", result);
            }
        }
    }

    fn handle_task_exit(
        task_name: &str,
        result: Result<IngestdResult<()>, tokio::task::JoinError>,
    ) -> IngestdResult<()> {
        match result {
            Ok(Ok(())) => Err(SinexError::service(format!(
                "{task_name} exited without signalling shutdown"
            ))),
            Ok(Err(err)) => Err(err),
            Err(join_err) if join_err.is_cancelled() => {
                Err(SinexError::cancelled(format!("{task_name} was cancelled")))
            }
            Err(join_err) => Err(SinexError::service(format!(
                "{task_name} panicked: {join_err}"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{collections::BTreeMap, str::FromStr};
    use tempfile::tempdir;

    fn test_state(material_id: Ulid) -> AssemblerState {
        let temp_dir = tempdir().expect("temp dir should be creatable");
        AssemblerState {
            material_id,
            temp_path: temp_dir.path().join(TEMP_FILE_NAME),
            temp_file: None,
            expected_offset: 0,
            slice_count: 0,
            buffered_slices: BTreeMap::new(),
            state_dir: temp_dir.path().to_path_buf(),
            started_at: Utc::now(),
            material_kind: "test".to_string(),
            source_identifier: "test".to_string(),
            metadata: JsonValue::Null,
            hasher: Hasher::new(),
            pending_end: None,
            finalizing: false,
        }
    }

    #[test]
    fn missing_buffered_slice_returns_error_instead_of_panic() {
        let material_id = Ulid::from_str("01J00000000000000000000000").unwrap();
        let mut state = test_state(material_id);

        let result = take_buffered_slice(&mut state, material_id, 42);

        assert!(result.is_err());
    }

    #[test]
    fn buffered_slice_is_removed_and_returned() {
        let material_id = Ulid::from_str("01J00000000000000000000000").unwrap();
        let mut state = test_state(material_id);
        let buffer_path = state.state_dir.join("buffers/42.bin");
        state.buffered_slices.insert(42, buffer_path.clone());

        let result = take_buffered_slice(&mut state, material_id, 42).unwrap();

        assert_eq!(result, buffer_path);
        assert!(state.buffered_slices.is_empty());
    }
}
