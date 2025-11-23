//! Material Acquisition Manager for Stage-as-You-Go pattern.
//!
//! Salvaged from sinex-sensd and adapted for JetStream-first architecture.
//! Handles material lifecycle: begin → append slices → finalize,
//! with rotation, hashing, and NATS publishing.

use crate::stream_processor::ProcessorHandles;
use crate::SatelliteResult;
use async_nats::{jetstream, Client as NatsClient};
use chrono::{DateTime, Utc};
use color_eyre::eyre::{Context, Result};
use serde::Serialize;
use serde_json::json;
use sinex_core::{
    db::{DbPool, DbPoolExt},
    environment::SinexEnvironment,
    types::Ulid,
    Id, SourceMaterialRecord,
};
use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio::sync::RwLock;
use tokio::time::sleep;
use tracing::{debug, info, warn};

/// Rotation policy configuration (salvaged from sensd)
#[derive(Debug, Clone)]
pub struct RotationPolicy {
    /// Maximum size in bytes before rotation
    pub max_bytes: i64,
    /// Maximum age before rotation (seconds)
    pub max_age_seconds: u64,
    /// Overlap period during rotation (milliseconds)
    pub overlap_duration_ms: u64,
}

impl Default for RotationPolicy {
    fn default() -> Self {
        Self {
            max_bytes: 100 * 1024 * 1024, // 100MB
            max_age_seconds: 3600,        // 1 hour
            overlap_duration_ms: 100,     // 100ms overlap
        }
    }
}

/// Material rotation state (salvaged from sensd)
#[derive(Debug, Clone)]
#[allow(dead_code)]
enum RotationState {
    Normal {
        material_id: Ulid,
        started_at: DateTime<Utc>,
        bytes_written: i64,
    },
    Rotating {
        old_material_id: Ulid,
        new_material_id: Ulid,
        rotation_started_at: DateTime<Utc>,
        overlap_deadline: DateTime<Utc>,
    },
}

/// Material acquisition manager
pub struct AcquisitionManager {
    nats_client: NatsClient,
    db_pool: DbPool,
    rotation_policy: RotationPolicy,
    env: SinexEnvironment,
    state: Arc<RwLock<RotationState>>,
    source_type: String,
    source_path: String,
    streams_ready: Arc<AtomicBool>,
}

/// Handle to an active source material being captured
pub struct SourceMaterialHandle {
    pub material_id: Ulid,
    temp_file: Option<File>,
    temp_path: PathBuf,
    hasher: blake3::Hasher,
    slice_count: usize,
    bytes_written: i64,
    started_at: DateTime<Utc>,
}

/// Message for source_material.begin subject
#[derive(Debug, Serialize)]
struct MaterialBeginMessage {
    material_id: String,
    material_kind: String,
    source_identifier: String,
    metadata: serde_json::Value,
    started_at: String,
}

/// Message for source_material.end subject
#[derive(Debug, Serialize)]
struct MaterialEndMessage {
    material_id: String,
    ended_at: String,
    content_hash: String,
    total_slices: usize,
    total_size_bytes: i64,
}

/// Ledger entry matching raw.temporal_ledger schema
#[derive(Debug, Clone)]
struct LedgerEntry {
    source_material_id: Ulid,
    offset_start: i64,
    offset_end: i64,
    offset_kind: String,
    ts_capture: DateTime<Utc>,
    precision: String,
    clock: String,
    source_type: String,
}

impl AcquisitionManager {
    /// Ensure JetStream streams required for material capture exist.
    pub async fn bootstrap_streams(nats_client: &NatsClient) -> Result<()> {
        let env = sinex_core::environment().clone();
        let js = jetstream::new(nats_client.clone());

        let mut attempt = 0;
        loop {
            match Self::ensure_streams_once(&js, &env).await {
                Ok(()) => return Ok(()),
                Err(err) => {
                    attempt += 1;
                    if attempt >= 5 {
                        return Err(err);
                    }
                    sleep(std::time::Duration::from_millis(100 * attempt as u64)).await;
                }
            }
        }
    }

    async fn ensure_streams_once(js: &jetstream::Context, env: &SinexEnvironment) -> Result<()> {
        js.get_or_create_stream(jetstream::stream::Config {
            name: env.nats_stream_name("SOURCE_MATERIAL_BEGIN"),
            subjects: vec![env.nats_subject("source_material.begin")],
            storage: jetstream::stream::StorageType::File,
            ..Default::default()
        })
        .await?;

        js.get_or_create_stream(jetstream::stream::Config {
            name: env.nats_stream_name("SOURCE_MATERIAL_SLICES"),
            subjects: vec![env.nats_subject("source_material.slices.>")],
            storage: jetstream::stream::StorageType::File,
            max_age: std::time::Duration::from_secs(7 * 24 * 60 * 60),
            max_message_size: 512 * 1024,
            ..Default::default()
        })
        .await?;

        js.get_or_create_stream(jetstream::stream::Config {
            name: env.nats_stream_name("SOURCE_MATERIAL_END"),
            subjects: vec![env.nats_subject("source_material.end")],
            storage: jetstream::stream::StorageType::File,
            ..Default::default()
        })
        .await?;

        Ok(())
    }

    /// Create new acquisition manager
    pub fn new(
        nats_client: NatsClient,
        db_pool: DbPool,
        rotation_policy: RotationPolicy,
        source_type: String,
        source_path: String,
    ) -> Self {
        let state = Arc::new(RwLock::new(RotationState::Normal {
            material_id: Ulid::nil(),
            started_at: Utc::now(),
            bytes_written: 0,
        }));

        let env = sinex_core::environment().clone();

        Self {
            nats_client,
            db_pool,
            rotation_policy,
            env,
            state,
            source_type,
            source_path,
            streams_ready: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Create an acquisition manager directly from processor handles
    pub fn from_handles(
        handles: &ProcessorHandles,
        rotation_policy: RotationPolicy,
        source_type: impl Into<String>,
        source_path: impl Into<String>,
    ) -> SatelliteResult<Self> {
        let nats_client = match handles.transport() {
            crate::event_processor::EventTransport::Nats(publisher) => {
                publisher.nats_client().clone()
            }
        };

        Ok(Self::new(
            nats_client,
            handles.db_pool().clone(),
            rotation_policy,
            source_type.into(),
            source_path.into(),
        ))
    }

    /// Begin capturing a new source material
    ///
    /// Ported from TemporalLedger::create_material + MaterialRotationManager logic
    pub async fn begin_material(&self, source_identifier: &str) -> Result<SourceMaterialHandle> {
        self.ensure_streams_ready().await?;

        // Register in-flight material in database
        let material_hint = "stream"; // Default, can be parameterized
        let metadata = json!({
            "source_type": &self.source_type,
            "source_identifier": source_identifier,
            "material_hint": material_hint,
        });

        let record = self
            .db_pool
            .source_materials()
            .register_in_flight(material_hint, Some(&self.source_path), metadata)
            .await
            .context("Failed to register in-flight source material")?;

        let material_id = record.id;

        // Create temporary file for local buffering
        let temp_dir = std::env::temp_dir();
        let temp_path = temp_dir.join(format!("sinex_material_{}.tmp", material_id));
        let temp_file = File::create(&temp_path)
            .await
            .context("Failed to create temp file")?;

        info!(
            material_id = %material_id,
            source_identifier = %source_identifier,
            temp_path = %temp_path.display(),
            "Created new source material"
        );

        // Publish begin message to NATS
        self.publish_begin(material_id, source_identifier).await?;

        // Update rotation state
        let mut state = self.state.write().await;
        *state = RotationState::Normal {
            material_id,
            started_at: Utc::now(),
            bytes_written: 0,
        };

        Ok(SourceMaterialHandle {
            material_id,
            temp_file: Some(temp_file),
            temp_path,
            hasher: blake3::Hasher::new(),
            slice_count: 0,
            bytes_written: 0,
            started_at: Utc::now(),
        })
    }

    async fn ensure_streams_ready(&self) -> Result<()> {
        if self.streams_ready.load(Ordering::SeqCst) {
            return Ok(());
        }

        AcquisitionManager::bootstrap_streams(&self.nats_client).await?;
        self.streams_ready.store(true, Ordering::SeqCst);
        Ok(())
    }

    /// Publish material begin event to NATS
    async fn publish_begin(&self, material_id: Ulid, source_identifier: &str) -> Result<()> {
        let msg = MaterialBeginMessage {
            material_id: material_id.to_string(),
            material_kind: self.source_type.clone(),
            source_identifier: source_identifier.to_string(),
            metadata: json!({}),
            started_at: Utc::now().to_rfc3339(),
        };

        let subject = self.env.nats_subject("source_material.begin");
        let payload = serde_json::to_vec(&msg)?;

        let js = async_nats::jetstream::new(self.nats_client.clone());
        js.publish(subject, payload.into())
            .await?
            .await
            .context("Failed to publish material begin")?;

        debug!(material_id = %material_id, "Published material begin");
        Ok(())
    }

    /// Append data slice to material
    ///
    /// Writes locally and publishes slice to NATS
    pub async fn append_slice(&self, handle: &mut SourceMaterialHandle, data: &[u8]) -> Result<()> {
        // Write to temp file
        if let Some(ref mut file) = handle.temp_file {
            file.write_all(data).await?;
        }

        // Update hash
        handle.hasher.update(data);

        // Publish slice to NATS
        let offset_start = handle.bytes_written;
        let offset_end = offset_start + data.len() as i64;

        self.publish_slice(handle.material_id, handle.slice_count, data, offset_start)
            .await?;

        handle.bytes_written = offset_end;
        handle.slice_count += 1;

        Ok(())
    }

    /// Publish material slice to NATS
    async fn publish_slice(
        &self,
        material_id: Ulid,
        slice_index: usize,
        data: &[u8],
        offset: i64,
    ) -> Result<()> {
        let subject = self
            .env
            .nats_subject(&format!("source_material.slices.{}", material_id));

        // Add headers
        let mut headers = async_nats::HeaderMap::new();
        headers.insert(
            "Nats-Msg-Id",
            format!("{}-{}", material_id, slice_index).as_str(),
        );
        headers.insert("Slice-Index", slice_index.to_string().as_str());
        headers.insert("Offset", offset.to_string().as_str());
        headers.insert("Chunk-Hash", blake3::hash(data).to_hex().as_str());

        let js = async_nats::jetstream::new(self.nats_client.clone());
        js.publish_with_headers(subject, headers, data.to_vec().into())
            .await?
            .await
            .context("Failed to publish material slice")?;

        debug!(material_id = %material_id, slice_index, offset, "Published material slice");
        Ok(())
    }

    /// Finalize material and publish end event
    ///
    /// Ported from TemporalLedger::finalize_material
    pub async fn finalize(&self, mut handle: SourceMaterialHandle, reason: &str) -> Result<()> {
        // Close temp file
        if let Some(mut file) = handle.temp_file.take() {
            file.flush().await?;
        }

        // Compute final hash
        let content_hash = handle.hasher.finalize();
        let hash_hex = content_hash.to_hex();

        // Update database
        let repo = self.db_pool.source_materials();

        let finalize_metadata = json!({
            "finalize_reason": reason,
            "finalized_at": Utc::now().to_rfc3339(),
            "content_hash": hash_hex.as_str(),
        });

        let id: Id<SourceMaterialRecord> = Id::from_ulid(handle.material_id);
        repo.update_metadata(id, finalize_metadata).await?;

        let id: Id<SourceMaterialRecord> = Id::from_ulid(handle.material_id);
        repo.finalize_in_flight(id, None, None, None, Some(handle.bytes_written))
            .await?;

        // Record ledger entry
        self.record_ledger_entry(LedgerEntry {
            source_material_id: handle.material_id,
            offset_start: 0,
            offset_end: handle.bytes_written,
            offset_kind: "byte".to_string(),
            ts_capture: handle.started_at,
            precision: "bounded".to_string(),
            clock: "wall".to_string(),
            source_type: "realtime_capture".to_string(),
        })
        .await?;

        // Publish end message
        self.publish_end(
            handle.material_id,
            handle.slice_count,
            handle.bytes_written,
            &hash_hex,
        )
        .await?;

        // Clean up temp file
        if let Err(e) = tokio::fs::remove_file(&handle.temp_path).await {
            warn!("Failed to remove temp file: {}", e);
        }

        info!(
            material_id = %handle.material_id,
            bytes_written = handle.bytes_written,
            slices = handle.slice_count,
            hash = %hash_hex,
            "Finalized source material"
        );

        Ok(())
    }

    /// Publish material end event to NATS
    async fn publish_end(
        &self,
        material_id: Ulid,
        total_slices: usize,
        total_bytes: i64,
        content_hash: &str,
    ) -> Result<()> {
        let msg = MaterialEndMessage {
            material_id: material_id.to_string(),
            ended_at: Utc::now().to_rfc3339(),
            content_hash: content_hash.to_string(),
            total_slices,
            total_size_bytes: total_bytes,
        };

        let subject = self.env.nats_subject("source_material.end");
        let payload = serde_json::to_vec(&msg)?;

        let js = async_nats::jetstream::new(self.nats_client.clone());
        js.publish(subject, payload.into())
            .await?
            .await
            .context("Failed to publish material end")?;

        debug!(material_id = %material_id, "Published material end");
        Ok(())
    }

    /// Record ledger entry (ported from TemporalLedger)
    async fn record_ledger_entry(&self, entry: LedgerEntry) -> Result<()> {
        sqlx::query!(
            r#"
            INSERT INTO raw.temporal_ledger
                (source_material_id, offset_start, offset_end, offset_kind,
                 ts_capture, precision, clock, source_type)
            VALUES ($1::uuid::ulid, $2, $3, $4, $5, $6, $7, $8)
            "#,
            entry.source_material_id.as_uuid(),
            entry.offset_start,
            entry.offset_end,
            &entry.offset_kind,
            entry.ts_capture,
            &entry.precision,
            &entry.clock,
            &entry.source_type
        )
        .execute(&self.db_pool)
        .await?;

        Ok(())
    }

    /// Check if rotation is needed (ported from MaterialRotationManager)
    pub async fn should_rotate(&self, handle: &SourceMaterialHandle) -> bool {
        let age_seconds = Utc::now()
            .signed_duration_since(handle.started_at)
            .num_seconds()
            .max(0) as u64;

        handle.bytes_written >= self.rotation_policy.max_bytes
            || age_seconds >= self.rotation_policy.max_age_seconds
    }
}

/// Helper: AppendStreamAcquirer for continuous streams (terminals, logs)
pub struct AppendStreamAcquirer {
    manager: Arc<AcquisitionManager>,
    current_handle: Option<SourceMaterialHandle>,
}

impl AppendStreamAcquirer {
    pub fn new(manager: Arc<AcquisitionManager>) -> Self {
        Self {
            manager,
            current_handle: None,
        }
    }

    /// Append data, automatically rotating if needed
    pub async fn append(&mut self, data: &[u8], source_identifier: &str) -> Result<()> {
        // Initialize if needed
        if self.current_handle.is_none() {
            self.current_handle = Some(self.manager.begin_material(source_identifier).await?);
        }

        let handle = self.current_handle.as_mut().unwrap();

        // Check rotation
        if self.manager.should_rotate(handle).await {
            info!("Rotating material due to size/age limits");
            let old_handle = self.current_handle.take().unwrap();
            self.manager.finalize(old_handle, "rotation").await?;
            self.current_handle = Some(self.manager.begin_material(source_identifier).await?);
        }

        // Append to current material
        let handle = self.current_handle.as_mut().unwrap();
        self.manager.append_slice(handle, data).await?;

        Ok(())
    }

    /// Finalize current material
    pub async fn finalize(&mut self, reason: &str) -> Result<()> {
        if let Some(handle) = self.current_handle.take() {
            self.manager.finalize(handle, reason).await?;
        }
        Ok(())
    }
}
