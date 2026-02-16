//! Material Acquisition Manager for Stage-as-You-Go pattern.
//!
//! Adapted for JetStream-first architecture.
//! Handles material lifecycle: begin → append slices → finalize,
//! with rotation, hashing, and NATS publishing.

use crate::stream_processor::NodeHandles;
use crate::{NodeResult, SinexError};
use async_nats::{jetstream, Client as NatsClient};
use serde::Serialize;
use serde_json::{json, Value as JsonValue};
use sinex_primitives::{
    environment::{environment, SinexEnvironment},
    temporal::Timestamp,
    units::{Bytes, Seconds},
    Ulid,
};
use std::{
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};
use tokio::fs::{File, OpenOptions};
use tokio::io::AsyncWriteExt;
use tokio::time::sleep;
use tracing::{debug, info, warn};
use uuid::Uuid;

/// Rotation policy configuration
#[derive(Debug, Clone)]
pub struct RotationPolicy {
    /// Maximum size in bytes before rotation
    pub max_bytes: Bytes,
    /// Maximum age before rotation (seconds)
    pub max_age_seconds: Seconds,
}

impl Default for RotationPolicy {
    fn default() -> Self {
        Self {
            max_bytes: Bytes::from_mebibytes(100),     // 100MB
            max_age_seconds: Seconds::from_secs(3600), // 1 hour
        }
    }
}

/// Material acquisition manager
pub struct AcquisitionManager {
    nats_client: NatsClient,
    rotation_policy: RotationPolicy,
    env: SinexEnvironment,
    namespace: Option<String>,
    source_type: String,
    _source_path: String,
    streams_ready: Arc<AtomicBool>,
    work_dir: Option<PathBuf>,
}

/// Handle to an active source material being captured
pub struct SourceMaterialHandle {
    pub material_id: Ulid,
    temp_file: Option<File>,
    temp_path: PathBuf,
    hasher: blake3::Hasher,
    slice_count: usize,
    bytes_written: i64,
    started_at: Timestamp,
}

impl SourceMaterialHandle {
    pub fn temp_path(&self) -> &Path {
        &self.temp_path
    }
}

impl Drop for SourceMaterialHandle {
    fn drop(&mut self) {
        // Clean up temp file to prevent disk leaks on panic or forgotten finalize()
        drop(self.temp_file.take());
        if self.temp_path.exists() {
            if let Err(e) = std::fs::remove_file(&self.temp_path) {
                tracing::warn!(
                    path = %self.temp_path.display(),
                    material_id = %self.material_id,
                    error = %e,
                    "Failed to clean up temp file in SourceMaterialHandle Drop"
                );
            }
        }
    }
}

/// Message for source_material.begin subject
#[derive(Debug, Serialize)]
struct MaterialBeginMessage {
    material_id: String,
    material_kind: String,
    source_identifier: String,
    metadata: JsonValue,
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
    metadata: JsonValue,
}

impl AcquisitionManager {
    /// Create an acquisition manager with default rotation policy.
    ///
    /// This is a convenience constructor for the common case where you don't need
    /// custom rotation settings. Defaults to 100MB max size and 1 hour max age.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use sinex_node_sdk::acquisition_manager::AcquisitionManager;
    /// # async fn example() {
    /// let nats_client = async_nats::connect("nats://localhost").await.unwrap();
    /// let manager = AcquisitionManager::with_defaults(
    ///     nats_client,
    ///     "terminal",
    ///     "/dev/pts/0"
    /// );
    /// # }
    /// ```
    pub fn with_defaults(
        nats_client: NatsClient,
        source_type: impl Into<String>,
        source_path: impl Into<String>,
    ) -> Self {
        Self::new(
            nats_client,
            RotationPolicy::default(),
            source_type.into(),
            source_path.into(),
        )
    }

    /// Create an acquisition manager from NodeHandles with default rotation.
    ///
    /// Convenience wrapper around `from_handles` that uses default rotation policy.
    pub fn from_handles_with_defaults(
        handles: &NodeHandles,
        source_type: impl Into<String>,
        source_path: impl Into<String>,
    ) -> NodeResult<Self> {
        Self::from_handles(handles, RotationPolicy::default(), source_type, source_path)
    }

    /// Ensure JetStream streams required for material capture exist.
    pub async fn bootstrap_streams(nats_client: &NatsClient) -> NodeResult<()> {
        Self::bootstrap_streams_with_namespace(nats_client, None).await
    }

    /// Ensure JetStream streams required for material capture exist for a namespace.
    pub async fn bootstrap_streams_with_namespace(
        nats_client: &NatsClient,
        namespace: Option<&str>,
    ) -> NodeResult<()> {
        let env = environment().clone();
        let js = jetstream::new(nats_client.clone());

        let mut attempt = 0;
        loop {
            match Self::ensure_streams_once(&js, &env, namespace).await {
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

    async fn ensure_streams_once(
        js: &jetstream::Context,
        env: &SinexEnvironment,
        namespace: Option<&str>,
    ) -> NodeResult<()> {
        js.get_or_create_stream(jetstream::stream::Config {
            name: env.nats_stream_name_with_namespace(namespace, "SOURCE_MATERIAL_BEGIN"),
            subjects: vec![env.nats_subject_with_namespace(namespace, "source_material.begin")],
            storage: jetstream::stream::StorageType::File,
            ..Default::default()
        })
        .await
        .map_err(|e| {
            SinexError::messaging("failed to create SOURCE_MATERIAL_BEGIN stream")
                .with_std_error(&e)
        })?;

        js.get_or_create_stream(jetstream::stream::Config {
            name: env.nats_stream_name_with_namespace(namespace, "SOURCE_MATERIAL_SLICES"),
            subjects: vec![env.nats_subject_with_namespace(namespace, "source_material.slices.>")],
            storage: jetstream::stream::StorageType::File,
            max_age: std::time::Duration::from_secs(7 * 24 * 60 * 60),
            max_message_size: 512 * 1024,
            ..Default::default()
        })
        .await
        .map_err(|e| {
            SinexError::messaging("failed to create SOURCE_MATERIAL_SLICES stream")
                .with_std_error(&e)
        })?;

        js.get_or_create_stream(jetstream::stream::Config {
            name: env.nats_stream_name_with_namespace(namespace, "SOURCE_MATERIAL_END"),
            subjects: vec![env.nats_subject_with_namespace(namespace, "source_material.end")],
            storage: jetstream::stream::StorageType::File,
            ..Default::default()
        })
        .await
        .map_err(|e| {
            SinexError::messaging("failed to create SOURCE_MATERIAL_END stream").with_std_error(&e)
        })?;

        Ok(())
    }

    /// Create new acquisition manager
    pub fn new(
        nats_client: NatsClient,
        rotation_policy: RotationPolicy,
        source_type: String,
        source_path: String,
    ) -> Self {
        Self::new_with_namespace(nats_client, rotation_policy, source_type, source_path, None)
    }

    /// Create new acquisition manager with an optional namespace.
    pub fn new_with_namespace(
        nats_client: NatsClient,
        rotation_policy: RotationPolicy,
        source_type: String,
        source_path: String,
        namespace: Option<String>,
    ) -> Self {
        let env = environment().clone();
        let work_dir = std::env::var("SINEX_WORK_DIR")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .map(PathBuf::from);

        Self {
            nats_client,
            rotation_policy,
            env,
            namespace,
            source_type,
            _source_path: source_path,
            streams_ready: Arc::new(AtomicBool::new(false)),
            work_dir,
        }
    }

    /// Create an acquisition manager directly from processor handles
    pub fn from_handles(
        handles: &NodeHandles,
        rotation_policy: RotationPolicy,
        source_type: impl Into<String>,
        source_path: impl Into<String>,
    ) -> NodeResult<Self> {
        let nats_client = match handles.transport() {
            crate::event_node::EventTransport::Nats(publisher) => publisher.nats_client().clone(),
        };

        Ok(Self::new(
            nats_client,
            rotation_policy,
            source_type.into(),
            source_path.into(),
        ))
    }

    /// Override the working directory for temporary material buffers.
    pub fn with_work_dir(mut self, work_dir: impl AsRef<Path>) -> Self {
        self.work_dir = Some(work_dir.as_ref().to_path_buf());
        self
    }

    /// Create a builder for new source material
    pub fn build_material(&self, source_identifier: impl Into<String>) -> MaterialBuilder<'_> {
        MaterialBuilder::new(self, source_identifier)
    }

    /// Begin capturing a new source material
    ///
    /// Ported from TemporalLedger::create_material + MaterialRotationManager logic
    pub async fn begin_material(
        &self,
        source_identifier: &str,
    ) -> NodeResult<SourceMaterialHandle> {
        self.build_material(source_identifier).begin().await
    }

    pub async fn begin_material_with_metadata(
        &self,
        source_identifier: &str,
        metadata: JsonValue,
    ) -> NodeResult<SourceMaterialHandle> {
        self.build_material(source_identifier)
            .with_metadata(metadata)
            .begin()
            .await
    }

    async fn ensure_streams_ready(&self) -> NodeResult<()> {
        // Use compare_exchange to avoid duplicate bootstrap from concurrent callers
        if self
            .streams_ready
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            // Another caller already set it to true (bootstrap done or in progress)
            return Ok(());
        }

        if let Err(e) = AcquisitionManager::bootstrap_streams_with_namespace(
            &self.nats_client,
            self.namespace.as_deref(),
        )
        .await
        {
            // Reset flag so next caller can retry
            self.streams_ready.store(false, Ordering::SeqCst);
            return Err(e);
        }

        Ok(())
    }

    /// Publish material begin event to NATS
    async fn publish_begin(
        &self,
        material_id: Ulid,
        source_identifier: &str,
        metadata: JsonValue,
    ) -> NodeResult<()> {
        let started_at = Timestamp::now().format_rfc3339();

        let msg = MaterialBeginMessage {
            material_id: material_id.to_string(),
            material_kind: self.source_type.clone(),
            source_identifier: source_identifier.to_string(),
            metadata,
            started_at,
        };

        let subject = self
            .env
            .nats_subject_with_namespace(self.namespace.as_deref(), "source_material.begin");
        let payload = serde_json::to_vec(&msg)?;

        let js = async_nats::jetstream::new(self.nats_client.clone());
        js.publish(subject, payload.into())
            .await
            .map_err(|e| SinexError::messaging(format!("Failed to publish material begin: {e}")))?
            .await
            .map_err(|e| {
                SinexError::messaging(format!("Failed to publish material begin (ack): {e}"))
            })?;

        debug!(material_id = %material_id, "Published material begin");
        Ok(())
    }

    /// Append data slice to material
    ///
    /// Writes locally and publishes slice to NATS
    pub async fn append_slice(
        &self,
        handle: &mut SourceMaterialHandle,
        data: &[u8],
    ) -> NodeResult<()> {
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

        debug!(
            material_id = %handle.material_id,
            slice_index = handle.slice_count - 1,
            bytes = data.len(),
            offset_start,
            offset_end,
            "Appended material slice"
        );

        Ok(())
    }

    /// NATS maximum message payload size. Messages exceeding this will be rejected.
    /// The actual NATS default is 1MB but we use a conservative limit to account for
    /// headers and protocol overhead.
    const MAX_NATS_PAYLOAD_BYTES: usize = 512 * 1024;

    /// Publish material slice to NATS
    async fn publish_slice(
        &self,
        material_id: Ulid,
        slice_index: usize,
        data: &[u8],
        offset: i64,
    ) -> NodeResult<()> {
        if data.len() > Self::MAX_NATS_PAYLOAD_BYTES {
            return Err(SinexError::validation(format!(
                "Material slice {} exceeds NATS max payload ({} bytes > {} bytes). \
                 Caller must split data into smaller chunks.",
                slice_index,
                data.len(),
                Self::MAX_NATS_PAYLOAD_BYTES
            )));
        }

        let subject = self.env.nats_subject_with_namespace(
            self.namespace.as_deref(),
            &format!("source_material.slices.{material_id}"),
        );

        // Add headers
        let mut headers = async_nats::HeaderMap::new();
        let msg_id = format!("{material_id}-{slice_index}");
        let slice_index_str = slice_index.to_string();
        let offset_str = offset.to_string();
        let chunk_hash = blake3::hash(data).to_hex();
        headers.insert("Nats-Msg-Id", msg_id.as_str());
        headers.insert("Slice-Index", slice_index_str.as_str());
        headers.insert("Offset", offset_str.as_str());
        headers.insert("Chunk-Hash", chunk_hash.as_str());

        let js = async_nats::jetstream::new(self.nats_client.clone());
        js.publish_with_headers(subject, headers, data.to_vec().into())
            .await
            .map_err(|e| SinexError::messaging(format!("Failed to publish material slice: {e}")))?
            .await
            .map_err(|e| {
                SinexError::messaging(format!("Failed to publish material slice (ack): {e}"))
            })?;

        debug!(
            material_id = %material_id,
            slice_index,
            offset,
            bytes = data.len(),
            "Published material slice"
        );
        Ok(())
    }

    /// Finalize material and publish end event
    ///
    /// Ported from TemporalLedger::finalize_material
    pub async fn finalize(&self, handle: SourceMaterialHandle, reason: &str) -> NodeResult<()> {
        self.finalize_with_metadata(handle, reason, json!({})).await
    }

    /// Cancel a material capture and finalize with cancellation metadata.
    pub async fn cancel(&self, handle: SourceMaterialHandle, reason: &str) -> NodeResult<()> {
        self.finalize_with_metadata(
            handle,
            reason,
            json!({
                "cancelled": true,
                "cancel_reason": reason,
            }),
        )
        .await
    }

    pub async fn finalize_with_metadata(
        &self,
        mut handle: SourceMaterialHandle,
        _reason: &str,
        metadata: JsonValue,
    ) -> NodeResult<()> {
        // Close temp file
        if let Some(mut file) = handle.temp_file.take() {
            file.flush().await?;
            file.sync_all().await?;
        }

        // Compute final hash
        let content_hash = handle.hasher.finalize();
        let hash_hex = content_hash.to_hex();

        // Publish end message
        self.publish_end(
            handle.material_id,
            handle.slice_count,
            handle.bytes_written,
            &hash_hex,
            metadata,
        )
        .await?;

        // Clean up temp file
        if let Err(e) = tokio::fs::remove_file(&handle.temp_path).await {
            warn!("Failed to remove temp file: {e}");
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
        metadata: JsonValue,
    ) -> NodeResult<()> {
        let ended_at = Timestamp::now().format_rfc3339();

        let msg = MaterialEndMessage {
            material_id: material_id.to_string(),
            ended_at,
            content_hash: content_hash.to_string(),
            total_slices,
            total_size_bytes: total_bytes,
            metadata,
        };

        let subject = self
            .env
            .nats_subject_with_namespace(self.namespace.as_deref(), "source_material.end");
        let payload = serde_json::to_vec(&msg)?;

        let js = async_nats::jetstream::new(self.nats_client.clone());
        js.publish(subject, payload.into())
            .await
            .map_err(|e| SinexError::messaging(format!("Failed to publish material end: {e}")))?
            .await
            .map_err(|e| {
                SinexError::messaging(format!("Failed to publish material end (ack): {e}"))
            })?;

        debug!(
            material_id = %material_id,
            total_slices,
            total_bytes,
            "Published material end"
        );
        Ok(())
    }

    /// Check if rotation is needed (ported from MaterialRotationManager)
    pub async fn should_rotate(&self, handle: &SourceMaterialHandle) -> bool {
        let age_seconds = (Timestamp::now() - handle.started_at)
            .whole_seconds()
            .max(0) as u64;

        handle.bytes_written >= self.rotation_policy.max_bytes.as_u64() as i64
            || age_seconds >= self.rotation_policy.max_age_seconds.as_secs()
    }
}

/// Builder for source material creation
pub struct MaterialBuilder<'a> {
    manager: &'a AcquisitionManager,
    source_identifier: String,
    metadata: JsonValue,
}

impl<'a> MaterialBuilder<'a> {
    pub fn new(manager: &'a AcquisitionManager, source_identifier: impl Into<String>) -> Self {
        Self {
            manager,
            source_identifier: source_identifier.into(),
            metadata: json!({}),
        }
    }

    pub fn with_metadata(mut self, metadata: JsonValue) -> Self {
        self.metadata = metadata;
        self
    }

    pub fn with_metadata_field(mut self, key: &str, value: JsonValue) -> Self {
        if !self.metadata.is_object() {
            self.metadata = json!({});
        }
        // SAFETY: We checked/initialized is_object() above, so this cannot fail
        if let Some(obj) = self.metadata.as_object_mut() {
            obj.insert(key.to_string(), value);
        }
        self
    }

    pub async fn begin(self) -> NodeResult<SourceMaterialHandle> {
        self.manager.ensure_streams_ready().await?;

        // Generate a new material id locally; ingestd is the sole database writer.
        let material_id = Ulid::new();

        // Create temporary file for local buffering
        let temp_dir = self
            .manager
            .work_dir
            .clone()
            .unwrap_or_else(|| self.manager.env.temp_dir());
        tokio::fs::create_dir_all(&temp_dir).await?;
        let temp_path = temp_dir.join(format!("sinex_material_{}.tmp", Uuid::new_v4()));
        let temp_file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temp_path)
            .await
            .map_err(SinexError::io)?;

        info!(
            material_id = %material_id,
            source_identifier = %self.source_identifier,
            temp_path = %temp_path.display(),
            "Created new source material"
        );

        // Publish begin message to NATS
        self.manager
            .publish_begin(material_id, &self.source_identifier, self.metadata)
            .await?;

        Ok(SourceMaterialHandle {
            material_id,
            temp_file: Some(temp_file),
            temp_path,
            hasher: blake3::Hasher::new(),
            slice_count: 0,
            bytes_written: 0,
            started_at: Timestamp::now(),
        })
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
    pub async fn append(&mut self, data: &[u8], source_identifier: &str) -> NodeResult<()> {
        // Initialize if needed
        if self.current_handle.is_none() {
            self.current_handle = Some(self.manager.begin_material(source_identifier).await?);
        }

        let handle = self
            .current_handle
            .as_mut()
            .ok_or_else(|| SinexError::invalid_state("current_handle should be initialized"))?;

        // Check rotation
        if self.manager.should_rotate(handle).await {
            info!("Rotating material due to size/age limits");
            let old_handle = self.current_handle.take().ok_or_else(|| {
                SinexError::invalid_state("current_handle should exist for rotation")
            })?;
            self.manager.finalize(old_handle, "rotation").await?;
            self.current_handle = Some(self.manager.begin_material(source_identifier).await?);
        }

        // Append to current material
        let handle = self.current_handle.as_mut().ok_or_else(|| {
            SinexError::invalid_state("current_handle should exist after rotation")
        })?;
        self.manager.append_slice(handle, data).await?;

        Ok(())
    }

    /// Finalize current material
    pub async fn finalize(&mut self, reason: &str) -> NodeResult<()> {
        if let Some(handle) = self.current_handle.take() {
            self.manager.finalize(handle, reason).await?;
        }
        Ok(())
    }
}
