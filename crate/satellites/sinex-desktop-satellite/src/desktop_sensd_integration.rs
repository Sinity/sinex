//! Desktop sensd integration module
//!
//! This module provides integration between desktop satellite and sensd for
//! source material capture and event generation with proper provenance.
//!
//! ## Architecture
//!
//! Following the fs-watcher pattern:
//! 1. **Source Material Capture**: Desktop data → raw.source_material_registry
//! 2. **Temporal Ledger**: Precise timing → raw.temporal_ledger  
//! 3. **Event Generation**: Material processing → events with Provenance::Material

use chrono::{DateTime, Utc};
use color_eyre::eyre::{eyre, Result};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sinex_core::{
    db::models::{Provenance, RawEvent},
    types::{
        domain::{EventSource, EventType},
        Id, Ulid,
    },
};
use sqlx::PgPool;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{debug, error, info};

/// Configuration for desktop sensd integration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DesktopSensdConfig {
    /// Database URL for connecting to sensd tables
    pub database_url: String,

    /// Batch size for processing material slices
    pub batch_size: usize,

    /// Processing interval in milliseconds
    pub processing_interval_ms: u64,

    /// Enable clipboard source material capture
    pub clipboard_enabled: bool,

    /// Enable window manager source material capture
    pub window_manager_enabled: bool,
}

impl Default for DesktopSensdConfig {
    fn default() -> Self {
        Self {
            database_url: String::from("postgresql:///sinex_dev?host=/run/postgresql"),
            batch_size: 100,
            processing_interval_ms: 1000,
            clipboard_enabled: true,
            window_manager_enabled: true,
        }
    }
}

/// Material slice from desktop source data
#[derive(Debug, Clone)]
pub struct DesktopMaterialSlice {
    pub material_id: Ulid,
    pub offset_start: i64,
    pub offset_end: i64,
    pub ts_capture_start: DateTime<Utc>,
    pub ts_capture_end: DateTime<Utc>,
    pub data: Vec<u8>,
    pub metadata: serde_json::Value,
    pub source_type: String, // "clipboard" or "window_manager"
}

/// Desktop processor that uses sensd for data acquisition
pub struct DesktopSensdProcessor {
    config: DesktopSensdConfig,
    db_pool: PgPool,
    event_sender: mpsc::Sender<RawEvent>,
}

impl DesktopSensdProcessor {
    /// Create new desktop sensd processor
    pub async fn new(
        config: DesktopSensdConfig,
        event_sender: mpsc::Sender<RawEvent>,
    ) -> Result<Self> {
        let db_pool = PgPool::connect(&config.database_url).await?;

        Ok(Self {
            config,
            db_pool,
            event_sender,
        })
    }

    /// Process desktop source material to create events
    pub async fn process_material(&self, material_id: Ulid) -> Result<()> {
        info!("Processing desktop material: {}", material_id);

        // Query material from registry
        let material = sqlx::query!(
            r#"
            SELECT 
                source_material_id as "material_id: Ulid",
                source_identifier,
                created_at as acquired_at,
                data,
                total_bytes as size_bytes,
                content_type as mime_type,
                metadata
            FROM raw.source_material_registry
            WHERE source_material_id = $1::ulid
            "#,
            material_id as Ulid,
        )
        .fetch_optional(&self.db_pool)
        .await?
        .ok_or_else(|| eyre!("Desktop material {} not found", material_id))?;

        // Query temporal ledger entries
        let ledger_entries = sqlx::query!(
            r#"
            SELECT 
                offset_start,
                offset_end,
                ts_capture,
                note
            FROM raw.temporal_ledger
            WHERE material_id = $1::ulid
            ORDER BY offset_start
            "#,
            material_id as Ulid,
        )
        .fetch_all(&self.db_pool)
        .await?;

        let mut total_events = 0;

        for entry in ledger_entries {
            // Create material slice
            let slice_data = if let Some(data) = &material.data {
                let start = entry.offset_start as usize;
                let end = entry.offset_end as usize;
                if end <= data.len() {
                    data[start..end].to_vec()
                } else {
                    data.clone()
                }
            } else {
                vec![]
            };

            let slice = DesktopMaterialSlice {
                material_id,
                offset_start: entry.offset_start,
                offset_end: entry.offset_end,
                ts_capture_start: entry.ts_capture,
                ts_capture_end: entry.ts_capture,
                data: slice_data,
                metadata: serde_json::from_str(&entry.note.unwrap_or("{}".to_string()))
                    .unwrap_or_default(),
                source_type: material.source_identifier.clone(),
            };

            // Convert slice to desktop events
            let events = self.slice_to_events(slice).await?;

            for event in events {
                if let Err(e) = self.event_sender.send(event).await {
                    error!("Failed to send desktop event: {}", e);
                } else {
                    total_events += 1;
                }
            }
        }

        info!(
            "Completed processing desktop material {}, generated {} events",
            material_id, total_events
        );

        Ok(())
    }

    /// Convert desktop material slice to events
    async fn slice_to_events(&self, slice: DesktopMaterialSlice) -> Result<Vec<RawEvent>> {
        let mut events = Vec::new();

        // Parse the material data
        let material_data: serde_json::Value = if slice.data.is_empty() {
            slice.metadata.clone()
        } else {
            serde_json::from_slice(&slice.data).unwrap_or(slice.metadata.clone())
        };

        match slice.source_type.as_str() {
            "desktop_clipboard" => {
                events.extend(self.create_clipboard_events(&slice, &material_data).await?);
            }
            "desktop_window_manager" => {
                events.extend(
                    self.create_window_manager_events(&slice, &material_data)
                        .await?,
                );
            }
            "desktop_snapshot" => {
                events.extend(self.create_snapshot_events(&slice, &material_data).await?);
            }
            "desktop_monitoring" => {
                events.extend(
                    self.create_monitoring_events(&slice, &material_data)
                        .await?,
                );
            }
            _ => {
                debug!("Unknown desktop source type: {}", slice.source_type);
            }
        }

        Ok(events)
    }

    /// Create clipboard events from source material
    async fn create_clipboard_events(
        &self,
        slice: &DesktopMaterialSlice,
        data: &serde_json::Value,
    ) -> Result<Vec<RawEvent>> {
        let mut events = Vec::new();

        let selection_type = data
            .get("selection_type")
            .and_then(|v| v.as_str())
            .unwrap_or("clipboard");

        let content_type = data
            .get("content_type")
            .and_then(|v| v.as_str())
            .unwrap_or("text");

        let event_type = match selection_type {
            "primary" => "clipboard.primary_selected",
            _ => "clipboard.copied",
        };

        // Create clipboard event with material provenance
        let mut raw_event = RawEvent::from_material(
            EventSource::from("desktop_clipboard"),
            EventType::from(event_type),
            json!({
                "selection_type": selection_type,
                "content_type": content_type,
                "content_size": data.get("content_size").and_then(|v| v.as_i64()).unwrap_or(0),
                "text_preview": data.get("text_preview"),
                "source_app": data.get("source_app"),
                "window_title": data.get("window_title"),
                "content_hash": data.get("content_hash"),
                "material_id": slice.material_id.to_string(),
                "offset_start": slice.offset_start,
                "offset_end": slice.offset_end,
            }),
            slice.material_id,
            slice.offset_start,
        );
        raw_event.ts_orig = Some(slice.ts_capture_start);
        raw_event.provenance = Provenance::Material {
            id: Id::from(slice.material_id),
            anchor_byte: slice.offset_start,
            offset_kind: sinex_core::db::models::event::OffsetKind::Byte,
            offset_start: Some(slice.offset_start),
            offset_end: Some(slice.offset_end),
        };

        events.push(raw_event);
        Ok(events)
    }

    /// Create window manager events from source material
    async fn create_window_manager_events(
        &self,
        slice: &DesktopMaterialSlice,
        data: &serde_json::Value,
    ) -> Result<Vec<RawEvent>> {
        let mut events = Vec::new();

        let event_type_str = data
            .get("event_type")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");

        let event_type = match event_type_str {
            "focusedwindow" => "window_manager.window_focused",
            "openwindow" => "window_manager.window_opened",
            "closewindow" => "window_manager.window_closed",
            "movewindow" => "window_manager.window_moved",
            "workspace" => "window_manager.workspace_changed",
            "focusedmon" => "window_manager.monitor_focused",
            "state_snapshot" => "window_manager.state_captured",
            _ => "window_manager.unknown",
        };

        // Create window manager event with material provenance
        let mut raw_event = RawEvent::from_material(
            EventSource::from("desktop_window_manager"),
            EventType::from(event_type),
            json!({
                "event_type": event_type_str,
                "event_data": data.get("event_data"),
                "wm_type": data.get("wm_type"),
                "additional_metadata": data.get("additional_metadata"),
                "material_id": slice.material_id.to_string(),
                "offset_start": slice.offset_start,
                "offset_end": slice.offset_end,
            }),
            slice.material_id,
            slice.offset_start,
        );
        raw_event.ts_orig = Some(slice.ts_capture_start);
        raw_event.provenance = Provenance::Material {
            id: Id::from(slice.material_id),
            anchor_byte: slice.offset_start,
            offset_kind: sinex_core::db::models::event::OffsetKind::Byte,
            offset_start: Some(slice.offset_start),
            offset_end: Some(slice.offset_end),
        };

        events.push(raw_event);
        Ok(events)
    }

    /// Create snapshot events from source material
    async fn create_snapshot_events(
        &self,
        slice: &DesktopMaterialSlice,
        data: &serde_json::Value,
    ) -> Result<Vec<RawEvent>> {
        let mut events = Vec::new();

        // Create desktop snapshot event with material provenance
        let mut raw_event = RawEvent::from_material(
            EventSource::from("desktop"),
            EventType::from("desktop.snapshot_taken"),
            json!({
                "snapshot_type": data.get("snapshot_type"),
                "enabled_sources": data.get("enabled_sources"),
                "source_count": data.get("source_count"),
                "clipboard_enabled": data.get("clipboard_enabled"),
                "window_manager_enabled": data.get("window_manager_enabled"),
                "material_id": slice.material_id.to_string(),
                "offset_start": slice.offset_start,
                "offset_end": slice.offset_end,
            }),
            slice.material_id,
            slice.offset_start,
        );
        raw_event.ts_orig = Some(slice.ts_capture_start);
        raw_event.provenance = Provenance::Material {
            id: Id::from(slice.material_id),
            anchor_byte: slice.offset_start,
            offset_kind: sinex_core::db::models::event::OffsetKind::Byte,
            offset_start: Some(slice.offset_start),
            offset_end: Some(slice.offset_end),
        };

        events.push(raw_event);
        Ok(events)
    }

    /// Create monitoring events from source material
    async fn create_monitoring_events(
        &self,
        slice: &DesktopMaterialSlice,
        data: &serde_json::Value,
    ) -> Result<Vec<RawEvent>> {
        let mut events = Vec::new();

        let event_type_str = data
            .get("event_type")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");

        let event_type = match event_type_str {
            "monitoring_started" => "desktop.monitoring_started",
            "historical_scan_attempt" => "desktop.historical_scan_attempted",
            _ => "desktop.monitoring_event",
        };

        // Create desktop monitoring event with material provenance
        let mut raw_event = RawEvent::from_material(
            EventSource::from("desktop"),
            EventType::from(event_type),
            json!({
                "event_type": event_type_str,
                "clipboard_enabled": data.get("clipboard_enabled"),
                "window_manager_enabled": data.get("window_manager_enabled"),
                "start_time": data.get("start_time"),
                "scan_time": data.get("scan_time"),
                "note": data.get("note"),
                "material_id": slice.material_id.to_string(),
                "offset_start": slice.offset_start,
                "offset_end": slice.offset_end,
            }),
            slice.material_id,
            slice.offset_start,
        );
        raw_event.ts_orig = Some(slice.ts_capture_start);
        raw_event.provenance = Provenance::Material {
            id: Id::from(slice.material_id),
            anchor_byte: slice.offset_start,
            offset_kind: sinex_core::db::models::event::OffsetKind::Byte,
            offset_start: Some(slice.offset_start),
            offset_end: Some(slice.offset_end),
        };

        events.push(raw_event);
        Ok(events)
    }

    /// Monitor active desktop materials and process them
    pub async fn monitor_desktop_materials(&self) -> Result<()> {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(
            self.config.processing_interval_ms,
        ));

        loop {
            interval.tick().await;

            // Query for new desktop source materials that haven't been processed
            let new_materials = sqlx::query!(
                r#"
                SELECT 
                    source_material_id as "material_id: Ulid"
                FROM raw.source_material_registry
                WHERE source_identifier IN ('desktop_clipboard', 'desktop_window_manager', 'desktop_snapshot', 'desktop_monitoring')
                AND NOT EXISTS (
                    SELECT 1 FROM core.events 
                    WHERE source_material_id::ulid = source_material_registry.source_material_id
                )
                ORDER BY created_at DESC
                LIMIT 10
                "#,
            )
            .fetch_all(&self.db_pool)
            .await?;

            for material in new_materials {
                info!("Processing new desktop material: {}", material.material_id);

                if let Err(e) = self.process_material(material.material_id).await {
                    error!(
                        "Failed to process desktop material {}: {}",
                        material.material_id, e
                    );
                }
            }
        }
    }
}

/// Run desktop processor with sensd integration
pub async fn run_desktop_with_sensd(config: DesktopSensdConfig) -> Result<()> {
    info!("Starting desktop processor with sensd integration");

    // Create event channel
    let (event_sender, mut event_receiver) = mpsc::channel(1000);

    // Create processor
    let processor = Arc::new(DesktopSensdProcessor::new(config, event_sender).await?);

    // Start material monitoring task
    let monitor_processor = processor.clone();
    let monitor_task = tokio::spawn(async move {
        if let Err(e) = monitor_processor.monitor_desktop_materials().await {
            error!("Desktop material monitoring error: {}", e);
        }
    });

    // Process events (would send to ingestd in real implementation)
    let event_task = tokio::spawn(async move {
        while let Some(event) = event_receiver.recv().await {
            debug!("Received desktop event: {:?}", event.event_type);
            // Here we would send to ingestd via gRPC
        }
    });

    // Wait for tasks
    tokio::try_join!(monitor_task, event_task)?;

    Ok(())
}
