#![doc = include_str!("../docs/stage_as_you_go.md")]

use crate::acquisition_manager::{AcquisitionManager, SourceMaterialHandle};
use crate::stream_processor::{EventEmitter, ProcessorHandles, ProcessorRuntimeState};
use crate::{SatelliteError, SatelliteResult};
use chrono::Utc;
use color_eyre::eyre::eyre;
use serde_json::{json, Map as JsonMap};
use sinex_core::db::models::Event;
use sinex_core::types::events::LogLinePayload;
use sinex_core::types::{ulid::Ulid, Id};
use sinex_core::JsonValue;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tracing::{debug, info};

const MAX_SLICE_BYTES: usize = 512 * 1024;
const MATERIAL_FINALIZE_REASON: &str = "stage-as-you-go";

/// Stage-as-You-Go context for managing in-flight source materials
#[derive(Clone)]
pub struct StageAsYouGoContext {
    event_emitter: EventEmitter,
    material_registry: Arc<Mutex<HashMap<Ulid, StageMaterialInfo>>>,
    acquisition_manager: Option<Arc<AcquisitionManager>>,
    acquisition_handles: Arc<Mutex<HashMap<Ulid, SourceMaterialHandle>>>,
}

#[derive(Debug, Clone)]
struct StageMaterialInfo {
    metadata: JsonValue,
}

impl StageAsYouGoContext {
    /// Create a Stage-as-You-Go context from processor runtime handles
    pub fn from_runtime(runtime: &ProcessorRuntimeState) -> Self {
        Self::from_optional_emitter(runtime.event_emitter().clone())
    }

    /// Attach an acquisition manager so Stage-as-You-Go can publish materials via JetStream.
    pub fn with_acquisition_manager(mut self, acquisition: Arc<AcquisitionManager>) -> Self {
        self.acquisition_manager = Some(acquisition);
        self
    }

    /// Create a Stage-as-You-Go context directly from processor handles
    pub fn from_handles(handles: &ProcessorHandles) -> Self {
        Self::from_optional_emitter(handles.emitter().clone())
    }

    /// Convenience helper to build a context from a sender channel (tests/tooling)
    pub fn from_sender(
        acquisition: Arc<AcquisitionManager>,
        event_sender: mpsc::UnboundedSender<Event<JsonValue>>,
        dry_run: bool,
    ) -> Self {
        Self::from_optional_emitter(EventEmitter::new(event_sender, dry_run))
            .with_acquisition_manager(acquisition)
    }

    fn from_optional_emitter(event_emitter: EventEmitter) -> Self {
        Self {
            event_emitter,
            material_registry: Arc::new(Mutex::new(HashMap::new())),
            acquisition_manager: None,
            acquisition_handles: Arc::new(Mutex::new(HashMap::new())),
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
        let metadata = Self::prepare_initial_metadata(material_type, source_uri, initial_metadata);
        let manager = self
            .acquisition_manager
            .as_ref()
            .ok_or_else(|| {
                SatelliteError::Processing(
                    "Stage-as-You-Go context requires an acquisition manager".to_string(),
                )
            })?
            .clone();

        let identifier = source_uri.unwrap_or(material_type);
        let handle = manager
            .begin_material_with_metadata(identifier, metadata.clone())
            .await
            .map_err(|e| SatelliteError::General(eyre!("Failed to begin material: {}", e)))?;
        let material_id = handle.material_id;
        self.acquisition_handles
            .lock()
            .await
            .insert(material_id, handle);

        info!(
            blob_id = %material_id,
            material_type = material_type,
            "Registered in-flight source material via JetStream"
        );

        let info = StageMaterialInfo { metadata };

        self.material_registry
            .lock()
            .await
            .insert(material_id, info);

        Ok(material_id)
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

        let manager = self
            .acquisition_manager
            .as_ref()
            .ok_or_else(|| {
                SatelliteError::Processing(
                    "Stage-as-You-Go context requires an acquisition manager".to_string(),
                )
            })?
            .clone();

        let handle = self
            .acquisition_handles
            .lock()
            .await
            .remove(&id)
            .ok_or_else(|| {
                SatelliteError::Processing(format!(
                    "Missing acquisition handle for material {}",
                    id
                ))
            })?;

        self.finalize_via_acquisition(
            manager,
            handle,
            material_info.as_ref(),
            content,
            mime_type,
            encoding,
            content_preview.clone(),
        )
        .await?;
        info!(
            material_id = %id,
            bytes = content.len(),
            "Finalized source material via JetStream"
        );

        Ok(())
    }

    async fn finalize_via_acquisition(
        &self,
        manager: Arc<AcquisitionManager>,
        mut handle: SourceMaterialHandle,
        info: Option<&StageMaterialInfo>,
        content: &[u8],
        _mime_type: Option<&str>,
        encoding: Option<&str>,
        content_preview: Option<String>,
    ) -> SatelliteResult<()> {
        for chunk in content.chunks(MAX_SLICE_BYTES) {
            manager
                .append_slice(&mut handle, chunk)
                .await
                .map_err(|e| SatelliteError::General(eyre!("Failed to append slice: {}", e)))?;
        }

        let metadata =
            Self::build_finalize_metadata(info, content.len() as i64, content_preview, encoding);

        manager
            .finalize_with_metadata(handle, MATERIAL_FINALIZE_REASON, metadata)
            .await
            .map_err(|e| SatelliteError::General(eyre!("Failed to finalize material: {}", e)))
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
    fn build_finalize_metadata(
        info: Option<&StageMaterialInfo>,
        total_bytes: i64,
        content_preview: Option<String>,
        encoding: Option<&str>,
    ) -> JsonValue {
        let mut base = info
            .map(|i| i.metadata.clone())
            .unwrap_or_else(|| json!({}));
        if !base.is_object() {
            base = json!({});
        }
        let map = base.as_object_mut().expect("metadata normalized to object");
        map.insert("total_bytes".to_string(), JsonValue::from(total_bytes));
        if let Some(preview) = content_preview {
            map.insert("content_preview".to_string(), JsonValue::String(preview));
        }
        if let Some(enc) = encoding {
            map.insert("encoding".to_string(), JsonValue::String(enc.to_string()));
        }
        JsonValue::Object(map.clone())
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

impl StageAsYouGoContext {
    fn prepare_initial_metadata(
        material_type: &str,
        source_uri: Option<&str>,
        metadata: JsonValue,
    ) -> JsonValue {
        let mut normalized = normalize_metadata(metadata);
        let map = normalized
            .as_object_mut()
            .expect("metadata normalized to object");
        map.entry("legacy_material_type".to_string())
            .or_insert_with(|| JsonValue::String(material_type.to_string()));
        if let Some(uri) = source_uri {
            map.entry("source_uri".to_string())
                .or_insert_with(|| JsonValue::String(uri.to_string()));
        }
        normalized
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
