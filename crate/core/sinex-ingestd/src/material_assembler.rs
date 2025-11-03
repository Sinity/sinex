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
use serde_json::json;
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
use tokio::{fs, fs::File, io::AsyncWriteExt, sync::RwLock};
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
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct MaterialEndMessage {
    material_id: String,
    ended_at: String,
    content_hash: String,
    total_slices: usize,
    total_size_bytes: i64,
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
}

impl AssemblerState {
    fn buffers_dir(&self) -> PathBuf {
        self.state_dir.join(BUFFER_DIR_NAME)
    }

    fn state_file(&self) -> PathBuf {
        self.state_dir.join(STATE_FILE_NAME)
    }

    fn temp_file_path(&self) -> PathBuf {
        self.temp_path.clone()
    }
}

/// Material assembler service
pub struct MaterialAssembler {
    js: jetstream::Context,
    pool: DbPool,
    env: SinexEnvironment,
    annex: Arc<GitAnnex>,
    assembler_state: Arc<RwLock<HashMap<Ulid, AssemblerState>>>,
    state_root: PathBuf,
    dlq_subject: String,
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

        let js = jetstream::new(nats_client);
        let env = sinex_core::environment().clone();

        Ok(Self {
            js,
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
                metadata: persisted.metadata,
                hasher,
            };

            self.assembler_state
                .write()
                .await
                .insert(material_id, state);

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
        };

        let serialized = serde_json::to_vec_pretty(&persisted).map_err(|e| {
            SinexError::serialization(format!(
                "Failed to serialize assembler state for {}: {}",
                state.material_id, e
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

    /// Handle a begin message
    async fn handle_begin(&self, msg: jetstream::Message) -> IngestdResult<()> {
        let begin: MaterialBeginMessage = serde_json::from_slice(&msg.payload).map_err(|e| {
            SinexError::parse(format!("Failed to decode begin message payload: {}", e))
        })?;

        let material_id = Ulid::from_str(&begin.material_id).map_err(|e| {
            SinexError::parse(format!(
                "Invalid material_id '{}' in begin message: {}",
                begin.material_id, e
            ))
        })?;

        let mut states = self.assembler_state.write().await;
        if states.contains_key(&material_id) {
            debug!(
                material_id = %material_id,
                "Begin message received for material that already has assembler state"
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

        let started_at = DateTime::parse_from_rfc3339(&begin.started_at)
            .map(|dt| dt.with_timezone(&Utc))
            .unwrap_or_else(|_| Utc::now());

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
        };

        self.persist_state(&state).await?;
        states.insert(material_id, state);
        info!(material_id = %material_id, "Initialized material assembler state");

        Ok(())
    }

    /// Store a slice (in-order or buffered) for a material
    async fn handle_slice(
        &self,
        material_id: Ulid,
        offset: i64,
        data: Vec<u8>,
    ) -> IngestdResult<()> {
        let mut states = self.assembler_state.write().await;
        let state = match states.get_mut(&material_id) {
            Some(state) => state,
            None => {
                warn!(
                    material_id = %material_id,
                    "Slice received for unknown material (missing begin)"
                );
                return Ok(());
            }
        };

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

                let buf_path = state
                    .buffered_slices
                    .remove(&next_offset)
                    .expect("buffer entry must exist");

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

        self.persist_state(state).await?;
        Ok(())
    }

    /// Insert or fetch blob metadata for the assembled material
    async fn upsert_blob(
        &self,
        state: &AssemblerState,
        annex_key: &AnnexKey,
        content_hash: &str,
    ) -> IngestdResult<Id<Blob>> {
        let repo = self.pool.blobs();

        if let Some(existing) = repo
            .get_by_content(&annex_key.backend, &annex_key.hash, annex_key.size as i64)
            .await
            .map_err(|e| SinexError::database(format!("Failed to query blob store: {}", e)))?
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

        let stored = repo
            .insert(blob)
            .await
            .map_err(|e| SinexError::database(format!("Failed to insert blob metadata: {}", e)))?;

        Ok(Id::from_ulid(stored.id.as_ulid().clone()))
    }

    /// Finalize source material registry and ledger
    async fn finalize_material_record(
        &self,
        state: &AssemblerState,
        blob_id: Id<Blob>,
        total_size_bytes: i64,
        content_hash: &str,
    ) -> IngestdResult<()> {
        let repo = self.pool.source_materials();
        let id: Id<SourceMaterialRecord> = Id::from_ulid(state.material_id);

        let finalize_metadata = json!({
            "finalize_reason": "jetstream-material",
            "finalized_at": Utc::now().to_rfc3339(),
            "content_hash": content_hash,
            "total_slices": state.slice_count,
            "source_identifier": state.source_identifier,
        });

        repo.update_metadata(id, finalize_metadata)
            .await
            .map_err(|e| {
                SinexError::database(format!("Failed to update material metadata: {}", e))
            })?;

        repo.finalize_in_flight(
            Id::from_ulid(state.material_id),
            Some(blob_id),
            None,
            None,
            Some(total_size_bytes),
        )
        .await
        .map_err(|e| SinexError::database(format!("Failed to finalize material: {}", e)))
    }

    /// Append entry in raw.temporal_ledger
    async fn record_ledger_entry(&self, state: &AssemblerState) -> IngestdResult<()> {
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
            state.material_kind
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
                match self
                    .js
                    .publish(self.dlq_subject.clone(), bytes.into())
                    .await
                {
                    Ok(ack) => {
                        if let Err(e) = ack.await {
                            error!(
                                material_id = %material_id,
                                "Failed to confirm DLQ publish: {}",
                                e
                            );
                        } else {
                            debug!(material_id = %material_id, "Routed to DLQ");
                        }
                    }
                    Err(e) => {
                        error!(
                            material_id = %material_id,
                            "Failed to publish material DLQ entry: {}",
                            e
                        );
                    }
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
        state: &AssemblerState,
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
    async fn handle_end(&self, end: MaterialEndMessage) -> IngestdResult<()> {
        let material_id = Ulid::from_str(&end.material_id).map_err(|e| {
            SinexError::parse(format!(
                "Invalid material_id '{}' in end message: {}",
                end.material_id, e
            ))
        })?;

        let mut states = self.assembler_state.write().await;
        let mut state = match states.remove(&material_id) {
            Some(state) => state,
            None => {
                warn!(
                    material_id = %material_id,
                    "End message received for unknown material"
                );
                return Ok(());
            }
        };

        if let Some(mut file) = state.temp_file.take() {
            if let Err(e) = file.flush().await {
                warn!(
                    material_id = %material_id,
                    "Failed to flush temp file during finalization: {}",
                    e
                );
            }
        }

        // Ensure no buffered slices remain
        if !state.buffered_slices.is_empty() {
            warn!(
                material_id = %material_id,
                buffered = state.buffered_slices.len(),
                "Buffered slices remain when end message arrived; forcing flush"
            );
        }

        let computed_hash = state.hasher.clone().finalize().to_hex().to_string();
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
            return Ok(());
        }

        let (annex_key, final_path) = match self.import_into_annex(&state).await {
            Ok(result) => result,
            Err(e) => {
                self.route_material_error(
                    material_id,
                    "annex_import_failed",
                    json!({ "error": e.to_string() }),
                )
                .await;
                states.insert(material_id, state); // Reinsert state for potential retry
                return Err(e);
            }
        };

        let blob_id = match self
            .upsert_blob(&state, &annex_key, &end.content_hash)
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
                states.insert(material_id, state); // Reinsert for retry
                return Err(e);
            }
        };

        if let Err(e) = self
            .finalize_material_record(&state, blob_id, end.total_size_bytes, &end.content_hash)
            .await
        {
            self.route_material_error(
                material_id,
                "material_finalize_failed",
                json!({ "error": e.to_string() }),
            )
            .await;
            states.insert(material_id, state); // Reinsert for retry
            return Err(e);
        }

        if let Err(e) = self.record_ledger_entry(&state).await {
            self.route_material_error(
                material_id,
                "ledger_append_failed",
                json!({ "error": e.to_string() }),
            )
            .await;
            states.insert(material_id, state); // Reinsert for retry
            return Err(e);
        }

        self.cleanup_state(material_id).await;

        info!(
            material_id = %material_id,
            annex_key = %annex_key.key,
            path = %final_path.display(),
            size_bytes = end.total_size_bytes,
            slices = state.slice_count,
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
            let stream_name = env.nats_subject("source_material_begin");
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
            let stream_name = env.nats_subject("source_material_slices");
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
            let stream_name = env.nats_subject("source_material_end");
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
                        let _ = message.ack_with(jetstream::AckKind::Nak(None)).await;
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

        let mut begin_handle = self.spawn_begin_consumer();
        let mut slices_handle = self.spawn_slices_consumer();
        let mut end_handle = self.spawn_end_consumer();

        tokio::select! {
            result = &mut begin_handle => {
                slices_handle.abort();
                end_handle.abort();
                return Self::handle_task_exit("material begin consumer", result);
            }
            result = &mut slices_handle => {
                begin_handle.abort();
                end_handle.abort();
                return Self::handle_task_exit("material slice consumer", result);
            }
            result = &mut end_handle => {
                begin_handle.abort();
                slices_handle.abort();
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
