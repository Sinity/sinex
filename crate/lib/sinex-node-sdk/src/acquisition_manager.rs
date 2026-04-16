//! Material Acquisition Manager for Stage-as-You-Go pattern.
//!
//! Adapted for JetStream-first architecture.
//! Handles material lifecycle: begin → append slices → finalize,
//! with rotation, hashing, and NATS publishing.

use crate::error_helpers::env_nonempty_string_optional;
use crate::runtime::stream::NodeHandles;
use crate::{NodeResult, SinexError};
use async_nats::{Client as NatsClient, jetstream};
use serde::Serialize;
use serde_json::{Value as JsonValue, json};
use sinex_primitives::{
    Uuid,
    environment::{SinexEnvironment, environment},
    temporal::Timestamp,
    units::{Bytes, Seconds},
};
use std::{
    future::Future,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::fs::{File, OpenOptions};
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;
use tokio::time::sleep;
use tracing::{debug, info, warn};

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
    js: async_nats::jetstream::Context,
    rotation_policy: RotationPolicy,
    env: SinexEnvironment,
    namespace: Option<String>,
    source_type: String,
    streams_ready: Arc<AtomicBool>,
    streams_bootstrap_lock: Arc<Mutex<()>>,
    work_dir: Option<PathBuf>,
}

/// Handle to an active source material being captured
pub struct SourceMaterialHandle {
    pub material_id: Uuid,
    temp_file: Option<File>,
    temp_path: PathBuf,
    hasher: blake3::Hasher,
    slice_count: usize,
    bytes_written: i64,
    started_at: Timestamp,
    pending_begin: Option<PendingMaterialBegin>,
    pending_published_slice: Option<PendingPublishedSlice>,
}

struct PendingMaterialBegin {
    source_identifier: String,
    metadata: JsonValue,
}

struct PendingPublishedSlice {
    offset: i64,
    slice_index: usize,
    data: Vec<u8>,
}

impl SourceMaterialHandle {
    pub fn temp_path(&self) -> &Path {
        &self.temp_path
    }

    pub fn bytes_written(&self) -> i64 {
        self.bytes_written
    }

    /// The wall-clock timestamp recorded when this material capture began.
    ///
    /// This value is carried in the `MaterialBeginMessage` sent to ingestd, which
    /// persists it as a `staged_at` ledger entry in `raw.temporal_ledger`. Use it
    /// as the fallback `ts_orig` for events that lack an intrinsic timestamp
    /// derived from the source material content — it is reproducible on replay
    /// because it traces back to a persisted ledger row.
    pub fn started_at(&self) -> Timestamp {
        self.started_at
    }
}

impl Drop for SourceMaterialHandle {
    fn drop(&mut self) {
        // Clean up temp file to prevent disk leaks on panic or forgotten finalize()
        drop(self.temp_file.take());
        if self.temp_path.exists()
            && let Err(e) = std::fs::remove_file(&self.temp_path)
        {
            tracing::warn!(
                path = %self.temp_path.display(),
                material_id = %self.material_id,
                error = %e,
                "Failed to clean up temp file in SourceMaterialHandle Drop"
            );
        }
    }
}

/// Message for `source_material.begin` subject
#[derive(Debug, Serialize)]
struct MaterialBeginMessage {
    material_id: String,
    material_kind: String,
    source_identifier: String,
    metadata: JsonValue,
    started_at: String,
}

/// Message for `source_material.end` subject
#[derive(Debug, Serialize)]
struct MaterialEndMessage {
    material_id: String,
    ended_at: String,
    content_hash: String,
    total_slices: usize,
    total_size_bytes: i64,
    metadata: JsonValue,
}

fn registry_source_identifier(logical_source_identifier: &str, material_id: Uuid) -> String {
    format!("{logical_source_identifier}#material={material_id}")
}

fn annotate_material_metadata(
    metadata: JsonValue,
    logical_source_identifier: &str,
    material_id: Uuid,
) -> JsonValue {
    let mut metadata = match metadata {
        JsonValue::Object(map) => JsonValue::Object(map),
        JsonValue::Null => json!({}),
        other => json!({ "value": other }),
    };

    if let Some(obj) = metadata.as_object_mut() {
        obj.entry("logical_source_identifier".to_string())
            .or_insert_with(|| JsonValue::String(logical_source_identifier.to_string()));
        obj.entry("observation_material_id".to_string())
            .or_insert_with(|| JsonValue::String(material_id.to_string()));
    }

    metadata
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
    /// let manager = AcquisitionManager::with_defaults(nats_client, "terminal");
    /// # }
    /// ```
    pub fn with_defaults(nats_client: NatsClient, source_type: impl Into<String>) -> Self {
        Self::new(nats_client, RotationPolicy::default(), source_type.into())
    }

    /// Create an acquisition manager from `NodeHandles` with default rotation.
    ///
    /// Convenience wrapper around `from_handles` that uses default rotation policy.
    pub fn from_handles_with_defaults(
        handles: &NodeHandles,
        source_type: impl Into<String>,
    ) -> NodeResult<Self> {
        Self::from_handles(handles, RotationPolicy::default(), source_type)
    }

    /// Ensure `JetStream` streams required for material capture exist.
    pub async fn bootstrap_streams(nats_client: &NatsClient) -> NodeResult<()> {
        Self::bootstrap_streams_with_namespace(nats_client, None).await
    }

    /// Ensure `JetStream` streams required for material capture exist for a namespace.
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
            max_age: std::time::Duration::from_hours(168),
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
    #[must_use]
    pub fn new(
        nats_client: NatsClient,
        rotation_policy: RotationPolicy,
        source_type: String,
    ) -> Self {
        Self::new_with_namespace(nats_client, rotation_policy, source_type, None)
    }

    /// Create new acquisition manager with an optional namespace.
    pub fn new_with_namespace(
        nats_client: NatsClient,
        rotation_policy: RotationPolicy,
        source_type: String,
        namespace: Option<String>,
    ) -> Self {
        let env = environment().clone();
        let work_dir =
            env_nonempty_string_optional("SINEX_WORK_DIR", "acquisition manager work dir")
                .map(PathBuf::from);

        let js = async_nats::jetstream::new(nats_client.clone());
        Self {
            nats_client,
            js,
            rotation_policy,
            env,
            namespace,
            source_type,
            streams_ready: Arc::new(AtomicBool::new(false)),
            streams_bootstrap_lock: Arc::new(Mutex::new(())),
            work_dir,
        }
    }

    /// Create an acquisition manager directly from node handles
    pub fn from_handles(
        handles: &NodeHandles,
        rotation_policy: RotationPolicy,
        source_type: impl Into<String>,
    ) -> NodeResult<Self> {
        let nats_client = match handles.transport() {
            crate::event_node::EventTransport::Nats(publisher) => publisher.nats_client().clone(),
        };

        Ok(Self::new(nats_client, rotation_policy, source_type.into()))
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
    /// Ported from `TemporalLedger::create_material` + `MaterialRotationManager` logic
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
        self.ensure_streams_ready_with(|| async {
            AcquisitionManager::bootstrap_streams_with_namespace(
                &self.nats_client,
                self.namespace.as_deref(),
            )
            .await
        })
        .await
    }

    async fn ensure_streams_ready_with<F, Fut>(&self, bootstrap: F) -> NodeResult<()>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = NodeResult<()>>,
    {
        if self.streams_ready.load(Ordering::SeqCst) {
            return Ok(());
        }

        let _bootstrap_guard = self.streams_bootstrap_lock.lock().await;
        if self.streams_ready.load(Ordering::SeqCst) {
            return Ok(());
        }

        bootstrap().await?;
        self.streams_ready.store(true, Ordering::SeqCst);
        Ok(())
    }

    /// Publish material begin event to NATS
    async fn publish_begin(
        &self,
        material_id: Uuid,
        source_identifier: &str,
        metadata: JsonValue,
        started_at: Timestamp,
    ) -> NodeResult<()> {
        let msg = MaterialBeginMessage {
            material_id: material_id.to_string(),
            material_kind: self.source_type.clone(),
            source_identifier: source_identifier.to_string(),
            metadata,
            started_at: started_at.format_rfc3339(),
        };

        let subject = self
            .env
            .nats_subject_with_namespace(self.namespace.as_deref(), "source_material.begin");
        let payload = serde_json::to_vec(&msg)?;

        let js = &self.js;
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

    async fn ensure_begin_published(&self, handle: &mut SourceMaterialHandle) -> NodeResult<()> {
        let Some(begin) = handle.pending_begin.take() else {
            return Ok(());
        };

        if let Err(error) = self
            .publish_begin(
                handle.material_id,
                &begin.source_identifier,
                begin.metadata.clone(),
                handle.started_at,
            )
            .await
        {
            handle.pending_begin = Some(begin);
            return Err(error);
        }

        Ok(())
    }

    async fn mirror_slice_to_local_stage(
        handle: &mut SourceMaterialHandle,
        data: &[u8],
        offset_start: i64,
    ) -> NodeResult<()> {
        if let Some(ref mut file) = handle.temp_file {
            file.write_all(data).await.map_err(|error| {
                SinexError::io("Failed to mirror published slice into the local staging file")
                    .with_context("material_id", handle.material_id.to_string())
                    .with_context("slice_index", handle.slice_count.to_string())
                    .with_context("offset", offset_start.to_string())
                    .with_context("bytes", data.len().to_string())
                    .with_std_error(&error)
            })?;
        }

        handle.hasher.update(data);
        handle.bytes_written = offset_start + data.len() as i64;
        handle.slice_count += 1;
        Ok(())
    }

    async fn ensure_pending_slice_mirrored(
        &self,
        handle: &mut SourceMaterialHandle,
    ) -> NodeResult<()> {
        let Some(pending) = handle.pending_published_slice.take() else {
            return Ok(());
        };

        if pending.offset != handle.bytes_written || pending.slice_index != handle.slice_count {
            return Err(SinexError::invalid_state(
                "pending published slice is inconsistent with local acquisition progress",
            )
            .with_context("material_id", handle.material_id.to_string())
            .with_context("pending_offset", pending.offset.to_string())
            .with_context("pending_slice_index", pending.slice_index.to_string())
            .with_context("bytes_written", handle.bytes_written.to_string())
            .with_context("slice_count", handle.slice_count.to_string()));
        }

        if let Err(error) =
            Self::mirror_slice_to_local_stage(handle, &pending.data, pending.offset).await
        {
            handle.pending_published_slice = Some(pending);
            return Err(error);
        }

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
        self.ensure_begin_published(handle).await?;
        self.ensure_pending_slice_mirrored(handle).await?;

        let offset_start = handle.bytes_written;
        let slice_index = handle.slice_count;

        self.publish_slice(handle.material_id, slice_index, data, offset_start)
            .await?;

        if let Err(error) = Self::mirror_slice_to_local_stage(handle, data, offset_start).await {
            handle.pending_published_slice = Some(PendingPublishedSlice {
                offset: offset_start,
                slice_index,
                data: data.to_vec(),
            });
            return Err(error
                .with_context("pending_local_mirror", "true")
                .with_context(
                    "recovery",
                    "retry the acquisition operation before finalizing",
                ));
        }

        debug!(
            material_id = %handle.material_id,
            slice_index,
            bytes = data.len(),
            offset_start,
            offset_end = handle.bytes_written,
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
        material_id: Uuid,
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

        let js = &self.js;
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
    /// Ported from `TemporalLedger::finalize_material`
    pub async fn finalize(&self, mut handle: SourceMaterialHandle, reason: &str) -> NodeResult<()> {
        self.finalize_with_metadata(&mut handle, reason, json!({}))
            .await
    }

    /// Cancel a material capture and finalize with cancellation metadata.
    pub async fn cancel(&self, handle: &mut SourceMaterialHandle, reason: &str) -> NodeResult<()> {
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
        handle: &mut SourceMaterialHandle,
        _reason: &str,
        metadata: JsonValue,
    ) -> NodeResult<()> {
        self.ensure_begin_published(handle).await?;
        self.ensure_pending_slice_mirrored(handle).await?;

        // Close temp file
        if let Some(mut file) = handle.temp_file.take() {
            file.flush().await?;
            file.sync_all().await?;
        }

        // Compute final hash
        let content_hash = handle.hasher.clone().finalize();
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
        material_id: Uuid,
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

        let js = &self.js;
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

    /// Check if rotation is needed (ported from `MaterialRotationManager`)
    pub fn should_rotate(&self, handle: &SourceMaterialHandle) -> bool {
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

    #[must_use]
    pub fn with_metadata(mut self, metadata: JsonValue) -> Self {
        self.metadata = metadata;
        self
    }

    #[must_use]
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

        let material_id = Uuid::now_v7();
        let logical_source_identifier = self.source_identifier;
        let registry_source_identifier =
            registry_source_identifier(&logical_source_identifier, material_id);
        let metadata =
            annotate_material_metadata(self.metadata, &logical_source_identifier, material_id);

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
            source_identifier = %logical_source_identifier,
            registry_source_identifier = %registry_source_identifier,
            temp_path = %temp_path.display(),
            "Created new source material"
        );

        let mut handle = SourceMaterialHandle {
            material_id,
            temp_file: Some(temp_file),
            temp_path,
            hasher: blake3::Hasher::new(),
            slice_count: 0,
            bytes_written: 0,
            started_at: Timestamp::now(),
            pending_begin: Some(PendingMaterialBegin {
                source_identifier: registry_source_identifier,
                metadata,
            }),
            pending_published_slice: None,
        };

        // Publish BEGIN before handing the material ID to callers. Stage-as-you-go
        // events may be emitted immediately after `register_in_flight`, so the
        // source material must already be durable rather than lazily published on
        // first slice/finalize.
        self.manager.ensure_begin_published(&mut handle).await?;

        Ok(handle)
    }
}

/// Helper: `AppendStreamAcquirer` for continuous streams (terminals, logs)
pub struct AppendStreamAcquirer {
    manager: Arc<AcquisitionManager>,
    current_handle: Option<SourceMaterialHandle>,
}

impl AppendStreamAcquirer {
    #[must_use]
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
        if self.manager.should_rotate(handle) {
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

#[cfg(test)]
mod tests {
    // Inline because these tests exercise private bootstrap coordination state;
    // extracting them would require widening the test surface of AcquisitionManager.
    use super::AcquisitionManager;
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
}
