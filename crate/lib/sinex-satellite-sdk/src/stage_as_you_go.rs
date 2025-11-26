#![doc = include_str!("../doc/stage_as_you_go.md")]

use crate::stream_processor::{EventEmitter, ProcessorHandles, ProcessorRuntimeState};
use crate::{
    annex::{AnnexConfig, BlobManager, BlobMetadata},
    SatelliteError, SatelliteResult,
};
use camino::Utf8PathBuf;
use chrono::{DateTime, Utc};
use color_eyre::eyre::eyre;
use sinex_core::db::models::Event;
use sinex_core::db::SqlxPgPool as PgPool;
use sinex_core::types::events::LogLinePayload;
use sinex_core::types::{ulid::Ulid, Id};
use sinex_core::{db::query_helpers::ulid_to_uuid, DbPoolExt, JsonValue};
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tokio::time::timeout;
use tracing::{debug, error, info, warn};

/// Stage-as-You-Go context for managing in-flight source materials
#[derive(Clone)]
pub struct StageAsYouGoContext {
    #[allow(dead_code)]
    db_pool: PgPool,
    event_emitter: EventEmitter,
    blob_manager: Option<Arc<BlobManager>>,
    allow_offline_registration: bool,
    record_temporal_ledger: bool,
    material_registry: Arc<Mutex<HashMap<Ulid, StageMaterialInfo>>>,
}

#[derive(Debug, Clone)]
struct StageMaterialInfo {
    material_type: String,
    source_uri: Option<String>,
    started_at: DateTime<Utc>,
    backend: MaterialBackend,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MaterialBackend {
    Database,
    Offline,
}

impl StageAsYouGoContext {
    fn ledger_source_type(source_type: &str) -> &'static str {
        match source_type {
            "intrinsic_content" => "intrinsic_content",
            "inferred_mtime" => "inferred_mtime",
            "inferred_user" => "inferred_user",
            _ => "realtime_capture",
        }
    }

    /// Create a Stage-as-You-Go context from processor runtime handles
    pub fn from_runtime(runtime: &ProcessorRuntimeState) -> Self {
        Self::from_emitter(runtime.db_pool().clone(), runtime.event_emitter().clone())
    }

    /// Create a Stage-as-You-Go context directly from processor handles
    pub fn from_handles(handles: &ProcessorHandles) -> Self {
        Self::from_emitter(handles.db_pool().clone(), handles.emitter().clone())
    }

    /// Create a Stage-as-You-Go context from explicit components
    pub fn from_emitter(db_pool: PgPool, event_emitter: EventEmitter) -> Self {
        let blob_manager = Self::init_blob_manager(&db_pool, &event_emitter);
        let allow_offline_registration =
            event_emitter.dry_run() || Self::offline_registration_env_enabled();
        let record_temporal_ledger = Self::ledger_recording_enabled();

        Self {
            db_pool,
            event_emitter,
            blob_manager,
            allow_offline_registration,
            record_temporal_ledger,
            material_registry: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Convenience helper to build a context from a sender channel (tests/tooling)
    pub fn from_sender(
        db_pool: PgPool,
        event_sender: mpsc::UnboundedSender<Event<JsonValue>>,
        dry_run: bool,
    ) -> Self {
        Self::from_emitter(db_pool, EventEmitter::new(event_sender, dry_run))
    }

    /// Create a Stage-as-You-Go context with an explicitly provided blob manager
    pub fn with_blob_manager(
        db_pool: PgPool,
        event_emitter: EventEmitter,
        blob_manager: Arc<BlobManager>,
    ) -> Self {
        let allow_offline_registration =
            event_emitter.dry_run() || Self::offline_registration_env_enabled();

        Self {
            db_pool,
            event_emitter,
            blob_manager: Some(blob_manager),
            allow_offline_registration,
            record_temporal_ledger: Self::ledger_recording_enabled(),
            material_registry: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn init_blob_manager(
        db_pool: &PgPool,
        event_emitter: &EventEmitter,
    ) -> Option<Arc<BlobManager>> {
        let path = match std::env::var("SINEX_ANNEX_PATH") {
            Ok(path) => path,
            Err(_) => return None,
        };

        let repo_path = match Utf8PathBuf::from(path) {
            path => path,
        };

        let annex_config = AnnexConfig {
            repo_path,
            num_copies: None,
            large_files: None,
        };

        match BlobManager::new(
            annex_config,
            db_pool.clone(),
            Some((*event_emitter.sender()).clone()),
        ) {
            Ok(manager) => {
                info!("Stage-as-You-Go blob manager initialised");
                Some(Arc::new(manager))
            }
            Err(e) => {
                warn!("Failed to initialise blob manager: {}", e);
                None
            }
        }
    }

    /// Register in-flight source material and get its ID immediately
    ///
    /// This is the first step of Stage-as-You-Go: register the source material
    /// with minimal metadata to get an ID that can be attached to events.
    pub async fn register_in_flight(
        &self,
        material_type: &str,
        source_uri: Option<&str>,
        initial_metadata: serde_json::Value,
    ) -> SatelliteResult<Ulid> {
        let source_material_repo = self.db_pool.source_materials();
        let register_future =
            source_material_repo.register_in_flight(material_type, source_uri, initial_metadata);
        let result = match timeout(Self::db_registration_timeout(), register_future).await {
            Ok(res) => res.map_err(|err| err.to_string()),
            Err(_) => Err("register_in_flight timed out while waiting for database".to_string()),
        };

        let env_offline_enabled = Self::offline_registration_env_enabled();
        let offline_allowed = self.allow_offline_registration || env_offline_enabled;

        let (material_id, backend) = match result {
            Ok(record) => (record.id, MaterialBackend::Database),
            Err(err) if offline_allowed => {
                let fallback = Ulid::new();
                warn!(
                    material_type = material_type,
                    source_uri = ?source_uri,
                    "Stage-as-You-Go running without Postgres connectivity (reason: {err}); generating offline material id {fallback}"
                );
                (fallback, MaterialBackend::Offline)
            }
            Err(err) => {
                error!(
                    allow_offline = self.allow_offline_registration,
                    env_offline_enabled,
                    "Stage-as-You-Go failed to register in-flight material: {}",
                    err
                );
                eprintln!(
                    "Stage-as-You-Go offline registration disabled (allow_offline={}, env_offline={}): {}",
                    self.allow_offline_registration,
                    env_offline_enabled,
                    err
                );
                return Err(SatelliteError::General(eyre!(
                    "Failed to register in-flight source material: {}",
                    err
                )));
            }
        };

        info!(
            blob_id = %material_id,
            material_type = material_type,
            "Registered in-flight source material"
        );

        let info = StageMaterialInfo {
            material_type: material_type.to_string(),
            source_uri: source_uri.map(|s| s.to_string()),
            started_at: Utc::now(),
            backend,
        };

        self.material_registry
            .lock()
            .await
            .insert(material_id, info);

        Ok(material_id)
    }

    fn offline_registration_env_enabled() -> bool {
        static ENABLED: OnceLock<bool> = OnceLock::new();
        *ENABLED.get_or_init(|| {
            let mut value = std::env::var("SINEX_STAGE_ALLOW_OFFLINE")
                .or_else(|_| std::env::var("SQLX_OFFLINE"))
                .unwrap_or_default();
            value.make_ascii_lowercase();
            matches!(value.trim(), "1" | "true" | "yes")
        })
    }

    fn db_registration_timeout() -> Duration {
        Duration::from_secs(2)
    }

    /// Create and send an event with attached source material reference
    ///
    /// This is the core of Stage-as-You-Go: events are created with immediate
    /// provenance tracking via the source_material_id field.
    pub async fn emit_event_with_provenance(
        &self,
        mut event: Event<JsonValue>,
        source_material_id: Ulid,
        offset_start: Option<i64>,
        offset_end: Option<i64>,
    ) -> SatelliteResult<Ulid> {
        // Attach source material provenance to the event
        event.provenance = sinex_core::Provenance::Material {
            id: source_material_id.into(),
            anchor_byte: 0, // Default to beginning of material
            offset_start,
            offset_end,
            offset_kind: sinex_core::OffsetKind::default(),
        };

        // Add source material reference to payload metadata if not already present
        if let Some(obj) = event.payload.as_object_mut() {
            obj.insert(
                "_source_material_id".to_string(),
                serde_json::json!(source_material_id.to_string()),
            );
        }

        // Send event via event channel
        let event_id: Ulid = *event
            .id
            .as_ref()
            .ok_or_else(|| SatelliteError::Processing("Event must have an ID".to_string()))?
            .as_ulid();

        self.event_emitter.emit(event).await?;

        debug!(
            event_id = %event_id,
            source_material_id = %source_material_id,
            "Emitted event with source material provenance"
        );

        Ok(event_id)
    }

    /// Finalize in-flight source material with actual content details
    ///
    /// This is the final step of Stage-as-You-Go: once the content is fully
    /// processed, update the source material record with complete information.
    pub async fn finalize_source_material(
        &self,
        id: Ulid,
        content: &[u8],
        mime_type: Option<&str>,
        encoding: Option<&str>,
    ) -> SatelliteResult<()> {
        // Checksum is now computed when creating the blob

        let content_preview = if mime_type.map(|m| m.starts_with("text/")).unwrap_or(false) {
            Some(String::from_utf8_lossy(&content[..content.len().min(500)]).to_string())
        } else {
            None
        };

        let material_info = {
            let mut registry = self.material_registry.lock().await;
            registry.remove(&id)
        };

        let backend = material_info
            .as_ref()
            .map(|info| info.backend)
            .unwrap_or(MaterialBackend::Database);

        if backend == MaterialBackend::Offline {
            info!(
                material_id = %id,
                "Finalize skipped database updates because Stage-as-You-Go is running without Postgres connectivity"
            );
            return Ok(());
        }

        let source_material_repo = self.db_pool.source_materials();

        let mut blob_id = None;
        let mut total_bytes = content.len() as i64;

        if let Some(manager) = &self.blob_manager {
            match self
                .ingest_blob(manager.clone(), id, material_info.as_ref(), content)
                .await
            {
                Ok(blob_metadata) => {
                    blob_id = Some(blob_metadata.id.clone());
                    total_bytes = blob_metadata.size_bytes;
                }
                Err(e) => {
                    warn!(
                        material_id = %id,
                        "Failed to ingest blob into annex: {}",
                        e
                    );
                }
            }
        } else {
            warn!(
                material_id = %id,
                "Blob manager unavailable; optional_blob_id will remain unset"
            );
        }

        source_material_repo
            .finalize_in_flight(
                Id::<sinex_core::SourceMaterialRecord>::from_ulid(id),
                blob_id,
                encoding,
                content_preview,
                Some(total_bytes),
            )
            .await
            .map_err(|e| {
                SatelliteError::General(eyre!("Failed to finalize source material {}: {}", id, e))
            })?;

        if self.record_temporal_ledger {
            if let Err(e) = self
                .record_ledger_entry(id, material_info.as_ref(), total_bytes)
                .await
            {
                warn!(
                    material_id = %id,
                    "Failed to append temporal ledger entry: {}",
                    e
                );
            }
        } else {
            debug!(
                material_id = %id,
                "Temporal ledger recording disabled for Stage-as-You-Go context"
            );
        }

        info!(
            material_id = %id,
            bytes = total_bytes,
            "Finalized source material with content details"
        );

        Ok(())
    }

    fn ledger_recording_enabled() -> bool {
        static ENABLED: OnceLock<bool> = OnceLock::new();
        *ENABLED.get_or_init(|| {
            std::env::var("SINEX_STAGE_LEDGER_WRITES")
                .map(|value| {
                    matches!(
                        value.trim(),
                        "1" | "true" | "TRUE" | "True" | "yes" | "YES" | "Yes"
                    )
                })
                .unwrap_or(false)
        })
    }
}

/// Helper trait for processors that support Stage-as-You-Go
#[async_trait::async_trait]
pub trait StageAsYouGoProcessor: Send + Sync {
    /// Process content with Stage-as-You-Go pattern
    ///
    /// This method should:
    /// 1. Register in-flight source material
    /// 2. Process content and emit events with source_material_id
    /// 3. Finalize source material with complete details
    async fn process_with_staging(
        &mut self,
        content: &[u8],
        source_uri: Option<&str>,
        metadata: serde_json::Value,
    ) -> SatelliteResult<StageAsYouGoResult>;
}

/// Result of Stage-as-You-Go processing
#[derive(Debug)]
pub struct StageAsYouGoResult {
    /// ID of the source material
    pub source_material_id: Ulid,
    /// IDs of events emitted
    pub event_ids: Vec<String>,
    /// Total bytes processed
    pub bytes_processed: usize,
    /// Processing duration
    pub duration: std::time::Duration,
}

/// Example implementation for a log file processor
///
/// Usage:
/// ```ignore
/// let processor = LogFileStageProcessor::new(context, "nginx");
/// ```
pub struct LogFileStageProcessor {
    context: StageAsYouGoContext,
    log_source: String, // "nginx", "apache", "syslog", etc.
}

impl StageAsYouGoContext {
    async fn ingest_blob(
        &self,
        manager: Arc<BlobManager>,
        material_id: Ulid,
        info: Option<&StageMaterialInfo>,
        content: &[u8],
    ) -> SatelliteResult<BlobMetadata> {
        let temp_path = std::env::temp_dir().join(format!("stage-{}", material_id));
        let mut file = fs::File::create(&temp_path).await.map_err(|e| {
            SatelliteError::General(eyre!("Failed to create temp file for blob ingest: {}", e))
        })?;
        file.write_all(content)
            .await
            .map_err(|e| SatelliteError::General(eyre!("Failed to write temp blob file: {}", e)))?;
        file.flush()
            .await
            .map_err(|e| SatelliteError::General(eyre!("Failed to flush temp blob file: {}", e)))?;

        let temp_utf8 = Utf8PathBuf::from_path_buf(temp_path.clone()).map_err(|_| {
            SatelliteError::General(eyre!("Temporary path {:?} is not valid UTF-8", temp_path))
        })?;

        let original_filename = Self::infer_original_filename(info, material_id);

        let ingest_result = manager
            .ingest_file(&temp_utf8, Some(&original_filename))
            .await
            .map_err(|e| SatelliteError::General(eyre!("Blob ingestion failed: {}", e)))?;

        if let Err(e) = fs::remove_file(&temp_path).await {
            warn!(
                path = %temp_path.display(),
                "Failed to remove temporary blob file: {}",
                e
            );
        }

        Ok(ingest_result)
    }

    fn infer_original_filename(info: Option<&StageMaterialInfo>, material_id: Ulid) -> String {
        if let Some(info) = info {
            if let Some(uri) = &info.source_uri {
                if let Some(name) = uri.rsplit('/').next() {
                    if !name.is_empty() {
                        return name.to_string();
                    }
                }
            }
        }
        format!("material-{}.bin", material_id)
    }

    async fn record_ledger_entry(
        &self,
        material_id: Ulid,
        info: Option<&StageMaterialInfo>,
        total_bytes: i64,
    ) -> SatelliteResult<()> {
        let (source_type, started_at) = info
            .map(|info| (info.material_type.as_str(), info.started_at))
            .unwrap_or(("stage-as-you-go", Utc::now()));
        let ledger_source_type = Self::ledger_source_type(source_type);

        sqlx::query!(
            r#"
            INSERT INTO raw.temporal_ledger
                (source_material_id, offset_start, offset_end, offset_kind, ts_capture, precision, clock, source_type)
            VALUES
                ($1::uuid::ulid, $2, $3, $4, $5, $6, $7, $8)
            "#,
            ulid_to_uuid(material_id),
            0_i64,
            total_bytes,
            "byte",
            started_at,
            "exact",
            "wall",
            ledger_source_type
        )
        .execute(&self.db_pool)
        .await
        .map_err(|e| SatelliteError::General(eyre!("Failed to append temporal ledger entry: {}", e)))?;

        Ok(())
    }
}

impl LogFileStageProcessor {
    pub fn new(context: StageAsYouGoContext, log_source: impl Into<String>) -> Self {
        Self {
            context,
            log_source: log_source.into(),
        }
    }
}

#[async_trait::async_trait]
impl StageAsYouGoProcessor for LogFileStageProcessor {
    async fn process_with_staging(
        &mut self,
        content: &[u8],
        source_uri: Option<&str>,
        metadata: serde_json::Value,
    ) -> SatelliteResult<StageAsYouGoResult> {
        let start_time = std::time::Instant::now();

        // Step 1: Register in-flight source material
        let source_material_id = self
            .context
            .register_in_flight("log_file", source_uri, metadata)
            .await?;

        // Step 2: Process content line by line, emitting events with provenance
        let mut event_ids = Vec::new();
        let content_str = String::from_utf8_lossy(content);
        let lines: Vec<&str> = content_str.lines().collect();

        for (line_num, line) in lines.iter().enumerate() {
            if line.trim().is_empty() {
                continue;
            }

            // Calculate byte offsets for this line
            let offset_start = lines[..line_num]
                .iter()
                .map(|l| l.len() + 1) // +1 for newline
                .sum::<usize>() as i64;
            let offset_end = offset_start + line.len() as i64;

            // Create event for this log line directly with unified Event<T>
            let payload = LogLinePayload {
                line: line.to_string(),
                line_number: (line_num + 1) as u64,
                log_source: self.log_source.clone(),
                log_file: source_uri.unwrap_or("unknown").to_string(),
                offset_start,
                offset_end,
                source_material_id: source_material_id.to_string(),
            };

            // Create typed event and convert to JsonValue for emission
            let typed_event = Event::new(
                payload,
                sinex_core::Provenance::Synthesis {
                    source_event_ids: sinex_core::types::non_empty::NonEmptyVec::single(
                        sinex_core::EventId::from_ulid(Ulid::new()),
                    ),
                    operation_id: None,
                },
            );

            // Convert to JsonValue event for emission
            let mut event = typed_event.to_json_event()?;
            event.id = Some(Id::from_ulid(Ulid::new()));
            let now = Utc::now();
            if event.ts_orig.is_none() {
                event.ts_orig = Some(now);
            }

            // Emit with provenance
            let event_id = self
                .context
                .emit_event_with_provenance(
                    event,
                    source_material_id,
                    Some(offset_start),
                    Some(offset_end),
                )
                .await?;

            event_ids.push(event_id.to_string());
        }

        // Step 3: Finalize source material with complete details
        self.context
            .finalize_source_material(
                source_material_id,
                content,
                Some("text/plain"),
                Some("utf-8"),
            )
            .await?;

        Ok(StageAsYouGoResult {
            source_material_id,
            event_ids,
            bytes_processed: content.len(),
            duration: start_time.elapsed(),
        })
    }
}
