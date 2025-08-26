//! Stage-as-You-Go pattern implementation for real-time provenance tracking
//!
//! This module provides helpers for implementing the Stage-as-You-Go pattern where
//! source material is registered in-flight as events are being created, enabling
//! real-time provenance tracking without waiting for full ingestion completion.
//!
//! # Stage-as-You-Go Pattern
//!
//! This critical architectural pattern ensures zero provenance gaps for real-time streams.
//! It solves the fundamental problem of maintaining data lineage when events are being
//! processed and emitted before the complete source material is available.
//!
//! ## The Problem
//!
//! Traditional approaches face a dilemma:
//! - **Option 1**: Wait for complete ingestion before emitting events (high latency)
//! - **Option 2**: Emit events immediately without provenance (broken lineage)
//!
//! ## The Solution
//!
//! Stage-as-You-Go allows immediate event emission with full provenance by:
//!
//! ```rust
//! // 1. Create in-flight source material record on startup
//! let blob_id = source_material_registry.create_in_flight().await?;
//!
//! // 2. Emit events immediately with provenance
//! let event = Event {
//!     source_material_id: Some(blob_id),
//!     // ... events flow in real-time
//! };
//!
//! // 3. Periodically finalize chunks (e.g., every 5 minutes)
//! source_material_registry.finalize_chunk(blob_id).await?;
//! ```
//!
//! ## Key Benefits
//!
//! - **Real-time Processing**: No delay for event emission
//! - **Complete Provenance**: Every event linked to its source
//! - **Incremental Updates**: Source material details filled in as available
//! - **Crash Recovery**: In-flight records can be resumed or finalized
//!
//! ## Implementation Pattern
//!
//! 1. **Register In-Flight**: Create placeholder source material with initial metadata
//! 2. **Process & Emit**: Process data and emit events with source_material_id
//! 3. **Finalize**: Update source material with complete details (size, checksum, etc.)
//!
//! ## Example Use Cases
//!
//! - **Log Tailing**: Emit log events as lines arrive, finalize after rotation
//! - **Terminal Sessions**: Track commands immediately, finalize on session end
//! - **Network Streams**: Process packets in real-time, finalize on connection close

use crate::{grpc_client::IngestClient, SatelliteError, SatelliteResult};
use sinex_core_types::domain::EventSource;
use sinex_db::repositories::DbPoolExt;
use sinex_db::SqlxPgPool as PgPool;
use sinex_events::Event;
use sinex_types::ulid::Ulid;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, info};

/// Stage-as-You-Go context for managing in-flight source materials
#[derive(Clone)]
pub struct StageAsYouGoContext {
    db_pool: PgPool,
    ingest_client: Arc<Mutex<IngestClient>>,
}

impl StageAsYouGoContext {
    /// Create a new Stage-as-You-Go context
    pub fn new(db_pool: PgPool, ingest_client: IngestClient) -> Self {
        Self {
            db_pool,
            ingest_client: Arc::new(Mutex::new(ingest_client)),
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
        let result = source_material_repo
            .register_in_flight(material_type, source_uri, initial_metadata)
            .await
            .map_err(|e| {
                SatelliteError::General(anyhow::anyhow!(
                    "Failed to register in-flight source material: {}",
                    e
                ))
            })?;

        let blob_id = result.id.as_ulid();

        info!(
            blob_id = %blob_id,
            material_type = material_type,
            "Registered in-flight source material"
        );

        Ok(*blob_id)
    }

    /// Create and send an event with attached source material reference
    ///
    /// This is the core of Stage-as-You-Go: events are created with immediate
    /// provenance tracking via the source_material_id field.
    pub async fn emit_event_with_provenance(
        &self,
        mut event: Event,
        source_material_id: Ulid,
        offset_start: Option<i64>,
        offset_end: Option<i64>,
    ) -> SatelliteResult<String> {
        // Attach source material ID to the event
        event.source_material_id = Some(source_material_id);
        event.source_material_offset_start = offset_start;
        event.source_material_offset_end = offset_end;

        // Add source material reference to payload metadata if not already present
        if let Some(obj) = event.payload.as_object_mut() {
            obj.insert(
                "_source_material_id".to_string(),
                serde_json::json!(source_material_id.to_string()),
            );
        }

        // Send event via ingest client
        let mut client = self.ingest_client.lock().await;
        let event_id = client.ingest_event(&event).await?;

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
        blob_id: Ulid,
        content: &[u8],
        mime_type: Option<&str>,
        encoding: Option<&str>,
    ) -> SatelliteResult<()> {
        let checksum = blake3::hash(content).to_hex().to_string();

        let content_preview = if mime_type.map(|m| m.starts_with("text/")).unwrap_or(false) {
            Some(String::from_utf8_lossy(&content[..content.len().min(500)]).to_string())
        } else {
            None
        };

        let source_material_repo = self.db_pool.source_materials();
        source_material_repo
            .finalize_in_flight(
                blob_id,
                content.len() as i64,
                checksum,
                mime_type,
                encoding,
                content_preview,
            )
            .await
            .map_err(|e| {
                SatelliteError::General(anyhow::anyhow!(
                    "Failed to finalize source material {}: {}",
                    blob_id,
                    e
                ))
            })?;

        info!(
            blob_id = %blob_id,
            size_bytes = content.len(),
            "Finalized source material with content details"
        );

        Ok(())
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
    log_source: String,  // "nginx", "apache", "syslog", etc.
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

            // Create event for this log line
            use sinex_events::LogLinePayload;
            let event = Event::from(LogLinePayload {
                line: line.to_string(),
                line_number: (line_num + 1) as u64,
                log_source: self.log_source.clone(),
                log_file: source_uri.unwrap_or("unknown").to_string(),
                offset_start,
                offset_end,
                source_material_id: source_material_id.to_string(),
            });

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

            event_ids.push(event_id);
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

#[cfg(test)]
mod tests {
    // use super::*;

    // Tests would go here
}
