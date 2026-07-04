//! Material Acquisition Manager for Stage-as-You-Go pattern.
//!
//! Adapted for JetStream-first architecture.
//! Handles material lifecycle: begin → append slices → finalize,
//! with rotation, hashing, and NATS publishing.

use crate::runtime::error_helpers::env_nonempty_string_optional;
use crate::runtime::nats_payload::ensure_nats_payload_fits;
use crate::runtime::stream::RuntimeHandles;
use crate::runtime::{RuntimeResult, SinexError};
use async_nats::{Client as NatsClient, jetstream};
use serde::Serialize;
use serde_json::{Value as JsonValue, json};
use sinex_primitives::{
    Uuid,
    domain::{NatsSubject, SourceIdentifier},
    environment::{SinexEnvironment, environment},
    temporal::Timestamp,
    transport,
    units::{Bytes, Seconds},
};
use std::str::FromStr;
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
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio::time::sleep;
use tracing::{debug, info, warn};

// Keep SOURCE_MATERIAL stream caps aligned with the Nix bootstrap path. The current
// nats CLI rejects --max-bytes values above signed 32-bit range.
const JETSTREAM_BOOTSTRAP_MAX_BYTES: i64 = 2_147_483_647;

/// Ordered `JetStream` stream used for all source-material lifecycle frames.
pub const SOURCE_MATERIAL_STREAM: &str = "SOURCE_MATERIAL";
/// Subject wildcard for the ordered source-material frame stream.
pub const SOURCE_MATERIAL_FRAMES_SUBJECT: NatsSubject =
    NatsSubject::from_static("source_material.frames.>");
/// Subject for material begin frames.
pub const SOURCE_MATERIAL_BEGIN_SUBJECT: NatsSubject =
    NatsSubject::from_static("source_material.frames.begin");
/// Subject prefix for material slice frames (used in `source_material_slice_subject`
/// to build a per-material subject by appending the material UUID).
pub const SOURCE_MATERIAL_SLICE_SUBJECT_PREFIX: &str = "source_material.frames.slices.";
/// Subject for material end frames.
pub const SOURCE_MATERIAL_END_SUBJECT: NatsSubject =
    NatsSubject::from_static("source_material.frames.end");

#[must_use]
#[allow(clippy::expect_used)]
pub fn source_material_slice_subject(material_id: Uuid) -> NatsSubject {
    let raw = format!("{SOURCE_MATERIAL_SLICE_SUBJECT_PREFIX}{material_id}");
    NatsSubject::from_str(&raw)
        .expect("UUIDs render to valid NATS subject segment characters by construction")
}

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

impl RotationPolicy {
    /// Build a per-source rotation policy from operator env overrides (#2184
    /// prong B — per-source granularity configuration).
    ///
    /// Resolution order for each of `max_bytes` / `max_age_seconds`:
    /// 1. source-specific `SINEX_MATERIAL_ROTATION_<KEY>_MAX_MB` /
    ///    `SINEX_MATERIAL_ROTATION_<KEY>_MAX_AGE_SECS`
    /// 2. global `SINEX_MATERIAL_ROTATION_MAX_MB` / `..._MAX_AGE_SECS`
    /// 3. the supplied `default`
    ///
    /// `<KEY>` is `source_key` uppercased with every non-alphanumeric byte mapped
    /// to `_`, so a source key such as `self_observation` reads
    /// `SINEX_MATERIAL_ROTATION_SELF_OBSERVATION_MAX_MB`.
    #[must_use]
    pub fn from_env(source_key: &str, default: Self) -> Self {
        let key = source_key
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() {
                    c.to_ascii_uppercase()
                } else {
                    '_'
                }
            })
            .collect::<String>();

        let default_mb = default.max_bytes.as_u64() / (1024 * 1024);
        let global_mb = sinex_primitives::env::parse_or(
            "SINEX_MATERIAL_ROTATION_MAX_MB",
            default_mb,
            "material rotation max mb",
        );
        let max_mb = sinex_primitives::env::parse_or(
            &format!("SINEX_MATERIAL_ROTATION_{key}_MAX_MB"),
            global_mb,
            "material rotation max mb (per-source)",
        )
        .max(1);

        let global_age = sinex_primitives::env::parse_or(
            "SINEX_MATERIAL_ROTATION_MAX_AGE_SECS",
            default.max_age_seconds.as_secs(),
            "material rotation max age secs",
        );
        let max_age = sinex_primitives::env::parse_or(
            &format!("SINEX_MATERIAL_ROTATION_{key}_MAX_AGE_SECS"),
            global_age,
            "material rotation max age secs (per-source)",
        )
        .max(1);

        Self {
            max_bytes: Bytes::from_mebibytes(max_mb),
            max_age_seconds: Seconds::from_secs(max_age),
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

/// Byte anchor returned after appending one logical source record to a stream material.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceRecordAnchor {
    pub material_id: Uuid,
    pub offset_start: i64,
    pub offset_end: i64,
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
    /// This value is carried in the `MaterialBeginMessage` sent to event_engine, which
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

/// Message for material begin frames.
#[derive(Debug, Serialize)]
struct MaterialBeginMessage {
    material_id: String,
    material_kind: String,
    source_identifier: String,
    metadata: JsonValue,
    started_at: String,
}

/// Message for material end frames.
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
    SourceIdentifier::new(
        logical_source_identifier,
        sinex_primitives::Id::<sinex_primitives::events::SourceMaterial>::from_uuid(material_id),
    )
    .to_wire()
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
    /// # use crate::runtime::acquisition_manager::AcquisitionManager;
    /// # async fn example() {
    /// let nats_client = async_nats::connect("nats://localhost").await.unwrap();
    /// let manager = AcquisitionManager::with_defaults(nats_client, "terminal");
    /// # }
    /// ```
    pub fn with_defaults(nats_client: NatsClient, source_type: impl Into<String>) -> Self {
        Self::new(nats_client, RotationPolicy::default(), source_type.into())
    }

    /// Create an acquisition manager from `RuntimeHandles` with default rotation.
    ///
    /// Convenience wrapper around `from_handles` that uses default rotation policy.
    pub fn from_handles_with_defaults(
        handles: &RuntimeHandles,
        source_type: impl Into<String>,
    ) -> RuntimeResult<Self> {
        Self::from_handles(handles, RotationPolicy::default(), source_type)
    }

    /// Ensure `JetStream` streams required for material capture exist.
    pub async fn bootstrap_streams(nats_client: &NatsClient) -> RuntimeResult<()> {
        Self::bootstrap_streams_with_namespace(nats_client, None).await
    }

    /// Ensure `JetStream` streams required for material capture exist for a namespace.
    pub async fn bootstrap_streams_with_namespace(
        nats_client: &NatsClient,
        namespace: Option<&str>,
    ) -> RuntimeResult<()> {
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
    ) -> RuntimeResult<()> {
        js.create_or_update_stream(jetstream::stream::Config {
            name: env.nats_stream_name_with_namespace(namespace, SOURCE_MATERIAL_STREAM),
            subjects: vec![
                env.nats_subject_with_namespace(namespace, SOURCE_MATERIAL_FRAMES_SUBJECT.as_str()),
            ],
            retention: jetstream::stream::RetentionPolicy::WorkQueue,
            storage: jetstream::stream::StorageType::File,
            max_age: std::time::Duration::from_hours(72),
            max_bytes: JETSTREAM_BOOTSTRAP_MAX_BYTES,
            max_message_size: 512 * 1024,
            ..Default::default()
        })
        .await
        .map_err(|e| {
            SinexError::messaging("failed to create SOURCE_MATERIAL stream").with_std_error(&e)
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

    /// Create an acquisition manager directly from runtime handles.
    ///
    /// Requires a `Nats`-backed transport: `AcquisitionManager` uses JetStream
    /// for the `SOURCE_MATERIAL` frame protocol and cannot operate over the
    /// `Direct` in-process path.
    pub fn from_handles(
        handles: &RuntimeHandles,
        rotation_policy: RotationPolicy,
        source_type: impl Into<String>,
    ) -> RuntimeResult<Self> {
        let publisher = handles.transport().nats_publisher()?;
        let nats_client = publisher.nats_client().clone();
        let namespace = publisher.namespace().map(ToOwned::to_owned);

        Ok(Self::new_with_namespace(
            nats_client,
            rotation_policy,
            source_type.into(),
            namespace,
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
    /// Ported from `TemporalLedger::create_material` + `MaterialRotationManager` logic
    pub async fn begin_material(
        &self,
        source_identifier: &str,
    ) -> RuntimeResult<SourceMaterialHandle> {
        self.build_material(source_identifier).begin().await
    }

    pub async fn begin_material_with_metadata(
        &self,
        source_identifier: &str,
        metadata: JsonValue,
    ) -> RuntimeResult<SourceMaterialHandle> {
        self.build_material(source_identifier)
            .with_metadata(metadata)
            .begin()
            .await
    }

    async fn ensure_streams_ready(&self) -> RuntimeResult<()> {
        self.ensure_streams_ready_with(|| async {
            AcquisitionManager::bootstrap_streams_with_namespace(
                &self.nats_client,
                self.namespace.as_deref(),
            )
            .await
        })
        .await
    }

    async fn ensure_streams_ready_with<F, Fut>(&self, bootstrap: F) -> RuntimeResult<()>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = RuntimeResult<()>>,
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
    ) -> RuntimeResult<()> {
        let msg = MaterialBeginMessage {
            material_id: material_id.to_string(),
            material_kind: self.source_type.clone(),
            source_identifier: source_identifier.to_string(),
            metadata,
            started_at: started_at.format_rfc3339(),
        };

        let subject = self.env.nats_subject_with_namespace(
            self.namespace.as_deref(),
            SOURCE_MATERIAL_BEGIN_SUBJECT.as_str(),
        );
        let payload = serde_json::to_vec(&msg)?;
        Self::ensure_source_material_frame_payload_fits("begin", &subject, payload.len())?;
        let mut headers = async_nats::HeaderMap::new();
        transport::insert_transport_class_headers(&mut headers, transport::Class::SourceMaterial);

        let js = &self.js;
        js.publish_with_headers(subject, headers, payload.into())
            .await
            .map_err(|e| SinexError::messaging(format!("Failed to publish material begin: {e}")))?
            .await
            .map_err(|e| {
                SinexError::messaging(format!("Failed to publish material begin (ack): {e}"))
            })?;

        debug!(material_id = %material_id, "Published material begin");
        Ok(())
    }

    async fn ensure_begin_published(&self, handle: &mut SourceMaterialHandle) -> RuntimeResult<()> {
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
    ) -> RuntimeResult<()> {
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
    ) -> RuntimeResult<()> {
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
    ) -> RuntimeResult<()> {
        self.append_record_batch(handle, &[data]).await?;
        Ok(())
    }

    /// Append multiple logical source records to the material.
    ///
    /// Each input record receives its own byte anchor. The transport sees one or
    /// more ordered slice frames, capped below the NATS payload ceiling. Hot
    /// event streams should prefer this over calling [`append_slice`](Self::append_slice)
    /// once per tiny record.
    pub async fn append_record_batch<T>(
        &self,
        handle: &mut SourceMaterialHandle,
        records: &[T],
    ) -> RuntimeResult<Vec<SourceRecordAnchor>>
    where
        T: AsRef<[u8]>,
    {
        self.ensure_begin_published(handle).await?;
        self.ensure_pending_slice_mirrored(handle).await?;

        let material_id = handle.material_id;
        let offset_start = handle.bytes_written;
        let slice_index = handle.slice_count;
        let mut next_offset = offset_start;
        let mut total_bytes = 0usize;
        let mut anchors = Vec::with_capacity(records.len());

        for record in records {
            let bytes = record.as_ref();
            let record_len = i64::try_from(bytes.len()).map_err(|error| {
                SinexError::validation("source record exceeds supported material size")
                    .with_context("record_bytes", bytes.len().to_string())
                    .with_std_error(&error)
            })?;
            let offset_end = next_offset.checked_add(record_len).ok_or_else(|| {
                SinexError::validation("source material byte offset overflow")
                    .with_context("bytes_written", next_offset.to_string())
                    .with_context("record_bytes", bytes.len().to_string())
            })?;
            total_bytes = total_bytes.checked_add(bytes.len()).ok_or_else(|| {
                SinexError::validation("material batch byte count overflow")
                    .with_context("bytes_so_far", total_bytes.to_string())
                    .with_context("record_bytes", bytes.len().to_string())
            })?;
            anchors.push(SourceRecordAnchor {
                material_id,
                offset_start: next_offset,
                offset_end,
            });
            next_offset = offset_end;
        }

        if total_bytes == 0 {
            return Ok(anchors);
        }

        let mut data = Vec::with_capacity(total_bytes.min(Self::MATERIAL_SLICE_PAYLOAD_BYTES));
        let mut publish_offset = offset_start;
        let mut publish_slice_index = slice_index;
        for record in records {
            let mut remaining = record.as_ref();
            while !remaining.is_empty() {
                let available = Self::MATERIAL_SLICE_PAYLOAD_BYTES - data.len();
                let take = remaining.len().min(available);
                data.extend_from_slice(&remaining[..take]);
                remaining = &remaining[take..];

                if data.len() == Self::MATERIAL_SLICE_PAYLOAD_BYTES {
                    self.publish_material_data_chunk(
                        handle,
                        publish_slice_index,
                        publish_offset,
                        std::mem::take(&mut data),
                    )
                    .await?;
                    publish_offset = handle.bytes_written;
                    publish_slice_index = handle.slice_count;
                    data = Vec::with_capacity(Self::MATERIAL_SLICE_PAYLOAD_BYTES);
                }
            }
        }

        if !data.is_empty() {
            self.publish_material_data_chunk(handle, publish_slice_index, publish_offset, data)
                .await?;
        }

        debug!(
            material_id = %handle.material_id,
            slice_index,
            slices = handle.slice_count.saturating_sub(slice_index),
            records = records.len(),
            bytes = total_bytes,
            offset_start,
            offset_end = handle.bytes_written,
            "Appended material record batch"
        );

        Ok(anchors)
    }

    async fn publish_material_data_chunk(
        &self,
        handle: &mut SourceMaterialHandle,
        slice_index: usize,
        offset_start: i64,
        data: Vec<u8>,
    ) -> RuntimeResult<()> {
        self.publish_slice(handle.material_id, slice_index, &data, offset_start)
            .await?;

        if let Err(error) = Self::mirror_slice_to_local_stage(handle, &data, offset_start).await {
            handle.pending_published_slice = Some(PendingPublishedSlice {
                offset: offset_start,
                slice_index,
                data,
            });
            return Err(error
                .with_context("pending_local_mirror", "true")
                .with_context(
                    "recovery",
                    "retry the acquisition operation before finalizing",
                ));
        }

        Ok(())
    }

    /// NATS maximum message payload size. Messages exceeding this will be rejected.
    /// The actual NATS default is 1MB but we use a conservative limit to account for
    /// headers and protocol overhead.
    const MAX_NATS_PAYLOAD_BYTES: usize = 512 * 1024;

    /// Physical material-slice payload target. Keep this below
    /// `MAX_NATS_PAYLOAD_BYTES` so headers and server framing cannot push a
    /// nominally-valid material frame over the live NATS ceiling.
    const MATERIAL_SLICE_PAYLOAD_BYTES: usize = 256 * 1024;

    /// Publish material slice to NATS
    async fn publish_slice(
        &self,
        material_id: Uuid,
        slice_index: usize,
        data: &[u8],
        offset: i64,
    ) -> RuntimeResult<()> {
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
            source_material_slice_subject(material_id).as_str(),
        );
        Self::ensure_source_material_frame_payload_fits("slice", &subject, data.len())?;

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
        transport::insert_transport_class_headers(&mut headers, transport::Class::SourceMaterial);

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
    pub async fn finalize(
        &self,
        mut handle: SourceMaterialHandle,
        reason: &str,
    ) -> RuntimeResult<()> {
        self.finalize_with_metadata(&mut handle, reason, json!({}))
            .await
    }

    /// Cancel a material capture and finalize with cancellation metadata.
    pub async fn cancel(
        &self,
        handle: &mut SourceMaterialHandle,
        reason: &str,
    ) -> RuntimeResult<()> {
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
    ) -> RuntimeResult<()> {
        self.ensure_begin_published(handle).await?;
        self.ensure_pending_slice_mirrored(handle).await?;

        // Close temp file
        if let Some(mut file) = handle.temp_file.take() {
            file.flush().await?;
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
    ) -> RuntimeResult<()> {
        let ended_at = Timestamp::now().format_rfc3339();

        let msg = MaterialEndMessage {
            material_id: material_id.to_string(),
            ended_at,
            content_hash: content_hash.to_string(),
            total_slices,
            total_size_bytes: total_bytes,
            metadata,
        };

        let subject = self.env.nats_subject_with_namespace(
            self.namespace.as_deref(),
            SOURCE_MATERIAL_END_SUBJECT.as_str(),
        );
        let payload = serde_json::to_vec(&msg)?;
        Self::ensure_source_material_frame_payload_fits("end", &subject, payload.len())?;
        let mut headers = async_nats::HeaderMap::new();
        transport::insert_transport_class_headers(&mut headers, transport::Class::SourceMaterial);

        let js = &self.js;
        js.publish_with_headers(subject, headers, payload.into())
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

    fn ensure_source_material_frame_payload_fits(
        frame_kind: &'static str,
        subject: &str,
        payload_bytes: usize,
    ) -> RuntimeResult<()> {
        let context = match frame_kind {
            "begin" => "source-material begin frame",
            "slice" => "source-material slice frame",
            "end" => "source-material end frame",
            _ => "source-material frame",
        };
        ensure_nats_payload_fits(context, subject, payload_bytes)
            .map_err(|error| error.with_context("frame_kind", frame_kind))
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
    publish_begin_before_return: bool,
}

impl<'a> MaterialBuilder<'a> {
    pub fn new(manager: &'a AcquisitionManager, source_identifier: impl Into<String>) -> Self {
        Self {
            manager,
            source_identifier: source_identifier.into(),
            metadata: json!({}),
            publish_begin_before_return: false,
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

    /// Publish the material BEGIN frame before returning the handle.
    ///
    /// Ordinary acquisition keeps BEGIN lazy so dropping an unused handle cannot
    /// create an orphan material row. Stage-as-You-Go needs the opposite
    /// contract because it may emit events against the material ID before the
    /// first source slice exists.
    #[must_use]
    pub fn publish_begin_before_return(mut self) -> Self {
        self.publish_begin_before_return = true;
        self
    }

    pub async fn begin(self) -> RuntimeResult<SourceMaterialHandle> {
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

        if self.publish_begin_before_return {
            self.manager.ensure_begin_published(&mut handle).await?;
        }

        Ok(handle)
    }
}

/// Helper: `AppendStreamAcquirer` for continuous streams (terminals, logs)
pub struct AppendStreamAcquirer {
    manager: Arc<AcquisitionManager>,
    current_handle: Option<SourceMaterialHandle>,
    current_source_identifier: Option<String>,
}

impl AppendStreamAcquirer {
    #[must_use]
    pub fn new(manager: Arc<AcquisitionManager>) -> Self {
        Self {
            manager,
            current_handle: None,
            current_source_identifier: None,
        }
    }

    /// Build a stream acquirer around an already-open material handle.
    ///
    /// Use this when callers need to expose the first material id before the
    /// first append, while still delegating subsequent size/age rotation to the
    /// runtime stream-acquisition path.
    #[must_use]
    pub fn from_active_handle(
        manager: Arc<AcquisitionManager>,
        handle: SourceMaterialHandle,
        source_identifier: impl Into<String>,
    ) -> Self {
        Self {
            manager,
            current_handle: Some(handle),
            current_source_identifier: Some(source_identifier.into()),
        }
    }

    /// Append data, automatically rotating if needed.
    pub async fn append(&mut self, data: &[u8], source_identifier: &str) -> RuntimeResult<()> {
        self.append_with_anchor(data, source_identifier).await?;
        Ok(())
    }

    /// Append one logical source record and return its byte anchor in the active material.
    ///
    /// This is the preferred API for row/event streams: one material grows per
    /// source, while each emitted event references the byte range for the record
    /// it interpreted.
    pub async fn append_with_anchor(
        &mut self,
        data: &[u8],
        source_identifier: &str,
    ) -> RuntimeResult<SourceRecordAnchor> {
        let mut anchors = self
            .append_many_with_anchors(&[data], source_identifier)
            .await?;
        anchors.pop().ok_or_else(|| {
            SinexError::invalid_state("single-record append returned no source material anchor")
        })
    }

    /// Append multiple logical records to the current stream material.
    ///
    /// The records are emitted as one physical source-material frame when they
    /// fit in a NATS message, while preserving one byte anchor per record.
    pub async fn append_many_with_anchors<T>(
        &mut self,
        records: &[T],
        source_identifier: &str,
    ) -> RuntimeResult<Vec<SourceRecordAnchor>>
    where
        T: AsRef<[u8]>,
    {
        if records.is_empty() {
            return Ok(Vec::new());
        }

        // Initialize if needed
        if self.current_handle.is_none() {
            self.begin_stream_material(source_identifier).await?;
        } else if self.current_source_identifier.as_deref() != Some(source_identifier) {
            info!(
                previous_source_identifier = ?self.current_source_identifier,
                source_identifier,
                "Rotating material due to source identifier change"
            );
            let old_handle = self.current_handle.take().ok_or_else(|| {
                SinexError::invalid_state(
                    "current_handle should exist for source identifier rotation",
                )
            })?;
            self.manager
                .finalize(old_handle, "source-identifier-change")
                .await?;
            self.current_source_identifier = None;
            self.begin_stream_material(source_identifier).await?;
        }

        let total_bytes = records.iter().try_fold(0usize, |total, record| {
            total.checked_add(record.as_ref().len()).ok_or_else(|| {
                SinexError::validation("material batch byte count overflow")
                    .with_context("bytes_so_far", total.to_string())
                    .with_context("record_bytes", record.as_ref().len().to_string())
            })
        })?;

        // Check rotation
        if self.should_rotate_before_append_len(total_bytes)? {
            info!("Rotating material due to size/age limits");
            let old_handle = self.current_handle.take().ok_or_else(|| {
                SinexError::invalid_state("current_handle should exist for rotation")
            })?;
            self.manager.finalize(old_handle, "rotation").await?;
            self.current_source_identifier = None;
            self.begin_stream_material(source_identifier).await?;
        }

        // Append to current material
        let handle = self.current_handle.as_mut().ok_or_else(|| {
            SinexError::invalid_state("current_handle should exist after rotation")
        })?;
        self.manager.append_record_batch(handle, records).await
    }

    /// Ensure the active stream material exists (publishing its BEGIN frame) for
    /// the given source identifier, without appending any content.
    ///
    /// This eagerly registers the source material so emitters can reference its
    /// id before the first real record — preserving the begin-before-anchor
    /// guarantee — *without* staging a placeholder byte that would otherwise mint
    /// a degenerate 1-byte material (#2184 prong E). The first real record then
    /// anchors at offset 0 of a clean, size/age-batched material.
    pub async fn ensure_open(&mut self, source_identifier: &str) -> RuntimeResult<()> {
        match self.current_handle {
            None => self.begin_stream_material(source_identifier).await,
            Some(_) if self.current_source_identifier.as_deref() != Some(source_identifier) => {
                let old_handle = self.current_handle.take().ok_or_else(|| {
                    SinexError::invalid_state(
                        "current_handle should exist for source identifier rotation",
                    )
                })?;
                self.manager
                    .finalize(old_handle, "source-identifier-change")
                    .await?;
                self.current_source_identifier = None;
                self.begin_stream_material(source_identifier).await
            }
            Some(_) => Ok(()),
        }
    }

    /// Serialize one record as JSONL, append it, and return its byte anchor.
    pub async fn append_json_line<T>(
        &mut self,
        record: &T,
        source_identifier: &str,
    ) -> RuntimeResult<SourceRecordAnchor>
    where
        T: Serialize + ?Sized,
    {
        let mut data = serde_json::to_vec(record).map_err(|error| {
            SinexError::serialization("failed to serialize source stream record")
                .with_std_error(&error)
        })?;
        data.push(b'\n');
        self.append_with_anchor(&data, source_identifier).await
    }

    fn should_rotate_before_append_len(&self, data_len: usize) -> RuntimeResult<bool> {
        let Some(handle) = self.current_handle.as_ref() else {
            return Ok(false);
        };

        if self.manager.should_rotate(handle) {
            return Ok(true);
        }

        if handle.bytes_written <= 0 {
            return Ok(false);
        }

        let incoming = i64::try_from(data_len).map_err(|error| {
            SinexError::validation("source record exceeds supported material size")
                .with_context("record_bytes", data_len.to_string())
                .with_std_error(&error)
        })?;
        let projected = handle.bytes_written.checked_add(incoming).ok_or_else(|| {
            SinexError::validation("source material byte offset overflow")
                .with_context("bytes_written", handle.bytes_written.to_string())
                .with_context("record_bytes", data_len.to_string())
        })?;

        let max_bytes =
            i64::try_from(self.manager.rotation_policy.max_bytes.as_u64()).map_err(|error| {
                SinexError::validation("source material rotation limit exceeds supported offset")
                    .with_context(
                        "max_bytes",
                        self.manager.rotation_policy.max_bytes.as_u64().to_string(),
                    )
                    .with_std_error(&error)
            })?;

        Ok(projected > max_bytes)
    }

    async fn begin_stream_material(&mut self, source_identifier: &str) -> RuntimeResult<()> {
        self.current_handle = Some(
            self.manager
                .build_material(source_identifier)
                .publish_begin_before_return()
                .begin()
                .await?,
        );
        self.current_source_identifier = Some(source_identifier.to_string());
        Ok(())
    }

    /// Return the material ID of the currently active in-flight material, if any.
    ///
    /// Used in tests and by [`AdapterBackedSource`] to verify that multiple drain
    /// cycles share the same material across drain calls.
    #[must_use]
    pub fn current_material_id(&self) -> Option<Uuid> {
        self.current_handle.as_ref().map(|h| h.material_id)
    }

    fn current_material_age_exceeds(&self, duration: std::time::Duration) -> bool {
        self.current_handle.as_ref().is_some_and(|handle| {
            (Timestamp::now() - handle.started_at).whole_seconds() >= duration.as_secs() as i64
        })
    }

    pub(crate) fn current_material_remaining_open_duration(
        &self,
        duration: std::time::Duration,
    ) -> Option<std::time::Duration> {
        let handle = self.current_handle.as_ref()?;
        let elapsed_ms = (Timestamp::now() - handle.started_at).whole_milliseconds();
        if elapsed_ms <= 0 {
            return Some(duration);
        }
        let elapsed = std::time::Duration::from_millis(elapsed_ms as u64);
        Some(duration.saturating_sub(elapsed))
    }

    /// Finalize the active stream material when it has been open for at least
    /// `duration`.
    ///
    /// This is used by source runtimes that may go idle while a stream material
    /// is open. Rotation-on-append cannot fire when no more records arrive.
    pub async fn finalize_if_age_exceeds(
        &mut self,
        duration: std::time::Duration,
        reason: &str,
    ) -> RuntimeResult<bool> {
        if !self.current_material_age_exceeds(duration) {
            return Ok(false);
        }

        self.finalize(reason).await?;
        Ok(true)
    }

    /// Finalize current material
    pub async fn finalize(&mut self, reason: &str) -> RuntimeResult<()> {
        if let Some(handle) = self.current_handle.take() {
            self.manager.finalize(handle, reason).await?;
        }
        self.current_source_identifier = None;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct BufferedAppendStreamWriterConfig {
    pub channel_capacity: usize,
    pub batch_max_records: usize,
    pub batch_max_bytes: usize,
    pub batch_coalesce_window: std::time::Duration,
    pub max_open_duration: Option<std::time::Duration>,
}

impl Default for BufferedAppendStreamWriterConfig {
    fn default() -> Self {
        Self {
            channel_capacity: 256,
            batch_max_records: 64,
            batch_max_bytes: 128 * 1024,
            batch_coalesce_window: std::time::Duration::from_millis(20),
            max_open_duration: None,
        }
    }
}

struct BufferedAppendRequest {
    payload: Option<Vec<u8>>,
    reason: Option<String>,
    continue_after_finalize: bool,
    /// Ensure the underlying source material is begun (BEGIN frame published)
    /// without appending any content. Used to eagerly register the material
    /// before the first real record so events can reference it, *without*
    /// minting a degenerate 1-byte material (#2184 prong E).
    prime: bool,
    reply: oneshot::Sender<RuntimeResult<Option<SourceRecordAnchor>>>,
}

async fn append_buffered_batch(
    stream: &mut AppendStreamAcquirer,
    source_identifier: &str,
    batch: Vec<(
        Vec<u8>,
        oneshot::Sender<RuntimeResult<Option<SourceRecordAnchor>>>,
    )>,
) {
    let records: Vec<Vec<u8>> = batch.iter().map(|(payload, _)| payload.clone()).collect();
    let result = stream
        .append_many_with_anchors(&records, source_identifier)
        .await;

    match result {
        Ok(anchors) => {
            for ((_, reply), anchor) in batch.into_iter().zip(anchors) {
                let _ = reply.send(Ok(Some(anchor)));
            }
        }
        Err(error) => {
            let message = error.to_string();
            for (_, reply) in batch {
                let _ = reply.send(Err(SinexError::processing(format!(
                    "failed to append source material batch: {message}"
                ))));
            }
        }
    }
}

async fn flush_buffered_stream_if_open_too_long(
    stream: &mut AppendStreamAcquirer,
    config: &BufferedAppendStreamWriterConfig,
) {
    let Some(max_open_duration) = config.max_open_duration else {
        return;
    };
    if !stream.current_material_age_exceeds(max_open_duration) {
        return;
    }
    if let Err(error) = stream
        .finalize("buffered append writer max open duration")
        .await
    {
        warn!(%error, "Failed to finalize buffered append stream after max open duration");
    }
}

async fn buffered_append_writer_task(
    mut stream: AppendStreamAcquirer,
    source_identifier: String,
    config: BufferedAppendStreamWriterConfig,
    mut rx: mpsc::Receiver<BufferedAppendRequest>,
) {
    let mut pending_request: Option<BufferedAppendRequest> = None;

    loop {
        let request = match pending_request.take() {
            Some(request) => request,
            None => {
                if let Some(max_open_duration) = config.max_open_duration
                    && stream.current_material_id().is_some()
                {
                    let sleep_duration = stream
                        .current_material_remaining_open_duration(max_open_duration)
                        .unwrap_or(max_open_duration);
                    match tokio::select! {
                        request = rx.recv() => request,
                        () = tokio::time::sleep(sleep_duration) => {
                            flush_buffered_stream_if_open_too_long(&mut stream, &config).await;
                            continue;
                        }
                    } {
                        Some(request) => request,
                        None => break,
                    }
                } else {
                    match rx.recv().await {
                        Some(request) => request,
                        None => break,
                    }
                }
            }
        };

        if let Some(payload) = request.payload {
            let mut batch_bytes = payload.len();
            let mut batch = vec![(payload, request.reply)];

            tokio::time::sleep(config.batch_coalesce_window).await;

            while batch.len() < config.batch_max_records {
                match rx.try_recv() {
                    Ok(next) => {
                        if let Some(next_payload) = next.payload {
                            let projected_bytes = batch_bytes.saturating_add(next_payload.len());
                            if projected_bytes > config.batch_max_bytes {
                                pending_request = Some(BufferedAppendRequest {
                                    payload: Some(next_payload),
                                    reason: next.reason,
                                    continue_after_finalize: next.continue_after_finalize,
                                    prime: next.prime,
                                    reply: next.reply,
                                });
                                break;
                            }
                            batch_bytes = projected_bytes;
                            batch.push((next_payload, next.reply));
                        } else {
                            pending_request = Some(BufferedAppendRequest {
                                payload: None,
                                reason: next.reason,
                                continue_after_finalize: next.continue_after_finalize,
                                prime: next.prime,
                                reply: next.reply,
                            });
                            break;
                        }
                    }
                    Err(mpsc::error::TryRecvError::Empty) => break,
                    Err(mpsc::error::TryRecvError::Disconnected) => break,
                }
            }

            append_buffered_batch(&mut stream, &source_identifier, batch).await;
            flush_buffered_stream_if_open_too_long(&mut stream, &config).await;
        } else if request.prime {
            // Eagerly begin the material (publish BEGIN) without staging content,
            // so the first real record anchors at offset 0 of a clean material
            // instead of inheriting a placeholder byte (#2184 prong E).
            let result = stream
                .ensure_open(&source_identifier)
                .await
                .map(|()| None)
                .map_err(|error| {
                    SinexError::lifecycle(format!(
                        "failed to prime buffered append stream: {error}"
                    ))
                });
            let _ = request.reply.send(result);
        } else {
            let reason = request
                .reason
                .as_deref()
                .unwrap_or("buffered append writer shutdown");
            let result = stream
                .finalize(reason)
                .await
                .map(|()| None)
                .map_err(|error| {
                    SinexError::lifecycle(format!(
                        "failed to finalize buffered append stream: {error}"
                    ))
                });
            let _ = request.reply.send(result);
            if !request.continue_after_finalize {
                return;
            }
        }
    }

    if let Err(error) = stream
        .finalize("buffered append writer: channel closed")
        .await
    {
        warn!(%error, "Failed to finalize buffered append stream");
    }
}

/// Background writer for rotating append-only source-material streams.
///
/// Use this when many logical observations belong to one source stream. Callers
/// get per-record byte anchors, while the runtime owns stream rotation, batching,
/// and finalization without requiring a mutex across NATS I/O.
#[derive(Clone)]
pub struct BufferedAppendStreamWriter {
    writer_tx: mpsc::Sender<BufferedAppendRequest>,
}

impl BufferedAppendStreamWriter {
    #[must_use]
    pub fn spawn(
        stream: AppendStreamAcquirer,
        source_identifier: impl Into<String>,
        config: BufferedAppendStreamWriterConfig,
    ) -> Self {
        let (writer_tx, writer_rx) = mpsc::channel(config.channel_capacity);
        tokio::spawn(buffered_append_writer_task(
            stream,
            source_identifier.into(),
            config,
            writer_rx,
        ));
        Self { writer_tx }
    }

    #[must_use]
    pub fn from_manager(
        manager: Arc<AcquisitionManager>,
        source_identifier: impl Into<String>,
        config: BufferedAppendStreamWriterConfig,
    ) -> Self {
        let source_identifier = source_identifier.into();
        let stream = AppendStreamAcquirer::new(manager);
        Self::spawn(stream, source_identifier, config)
    }

    pub async fn append(&self, payload: Vec<u8>) -> RuntimeResult<SourceRecordAnchor> {
        let (reply, response) = oneshot::channel();
        self.writer_tx
            .send(BufferedAppendRequest {
                payload: Some(payload),
                reason: None,
                continue_after_finalize: false,
                prime: false,
                reply,
            })
            .await
            .map_err(|_| {
                SinexError::processing("buffered append writer has shut down".to_string())
            })?;

        response
            .await
            .map_err(|_| {
                SinexError::processing("buffered append writer dropped reply channel".to_string())
            })?
            .and_then(|anchor| {
                anchor.ok_or_else(|| {
                    SinexError::processing(
                        "buffered append writer returned finalize response for append request"
                            .to_string(),
                    )
                })
            })
    }

    /// Eagerly begin the underlying source material (publishing its BEGIN frame)
    /// without appending content, so the first emitted event can reference the
    /// material id without minting a degenerate 1-byte material (#2184 prong E).
    pub async fn prime(&self) -> RuntimeResult<()> {
        let (reply, response) = oneshot::channel();
        let send_result = self
            .writer_tx
            .send(BufferedAppendRequest {
                payload: None,
                reason: None,
                continue_after_finalize: true,
                prime: true,
                reply,
            })
            .await;

        if send_result.is_err() {
            return Ok(());
        }

        response
            .await
            .map_err(|_| {
                SinexError::processing("buffered append writer dropped prime reply channel")
            })?
            .map(|_| ())
    }

    pub async fn finalize(&self, reason: &str) -> RuntimeResult<()> {
        let (reply, response) = oneshot::channel();
        let send_result = self
            .writer_tx
            .send(BufferedAppendRequest {
                payload: None,
                reason: Some(reason.to_string()),
                continue_after_finalize: false,
                prime: false,
                reply,
            })
            .await;

        if send_result.is_err() {
            return Ok(());
        }

        response
            .await
            .map_err(|_| {
                SinexError::processing(
                    "buffered append writer dropped finalize reply channel".to_string(),
                )
            })?
            .map(|_| ())
    }

    pub async fn flush(&self, reason: &str) -> RuntimeResult<()> {
        let (reply, response) = oneshot::channel();
        let send_result = self
            .writer_tx
            .send(BufferedAppendRequest {
                payload: None,
                reason: Some(reason.to_string()),
                continue_after_finalize: true,
                prime: false,
                reply,
            })
            .await;

        if send_result.is_err() {
            return Ok(());
        }

        response
            .await
            .map_err(|_| {
                SinexError::processing("buffered append writer dropped flush reply channel")
            })?
            .map(|_| ())
    }
}

#[cfg(test)]
#[path = "acquisition_manager_test.rs"]
mod tests;
