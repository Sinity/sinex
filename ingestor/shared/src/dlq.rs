use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};
use tracing::{error, info, warn};

use crate::{agent_events::*, RawEvent};

/// Failed event wrapper for DLQ storage
#[derive(Debug, Serialize, Deserialize)]
pub struct DlqEntry {
    pub failed_at: DateTime<Utc>,
    pub failure_reason: String,
    pub retry_count: u32,
    pub original_event: RawEvent,
}

/// Dead Letter Queue manager
pub struct DlqManager {
    agent_name: String,
    base_path: PathBuf,
    critical_failures_log: PathBuf,
}

impl DlqManager {
    pub fn new(agent_name: impl Into<String>) -> Result<Self> {
        let agent_name = agent_name.into();
        let base_path = PathBuf::from("/var/lib/sinex/dlq").join(&agent_name);
        let critical_failures_log = PathBuf::from("/var/log/sinex")
            .join(&agent_name)
            .join("critical_meta_failures.log");

        // Ensure directories exist
        fs::create_dir_all(&base_path)
            .with_context(|| format!("Failed to create DLQ directory: {:?}", base_path))?;
        
        if let Some(parent) = critical_failures_log.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("Failed to create log directory: {:?}", parent))?;
        }

        Ok(Self {
            agent_name,
            base_path,
            critical_failures_log,
        })
    }

    /// Write a failed event to DLQ
    pub async fn write_event(
        &self,
        event: RawEvent,
        failure_reason: String,
        retry_count: u32,
    ) -> Result<String> {
        let entry = DlqEntry {
            failed_at: Utc::now(),
            failure_reason: failure_reason.clone(),
            retry_count,
            original_event: event.clone(),
        };

        // Generate filename with timestamp and event type
        let filename = format!(
            "{}_{}_{}.json",
            entry.failed_at.format("%Y%m%d_%H%M%S"),
            event.source.replace('.', "_"),
            event.event_type.replace('.', "_")
        );
        let file_path = self.base_path.join(&filename);

        // Serialize and write to file
        let json = serde_json::to_string_pretty(&entry)
            .context("Failed to serialize DLQ entry")?;
        
        fs::write(&file_path, json)
            .with_context(|| format!("Failed to write DLQ file: {:?}", file_path))?;

        info!(
            "Written event to DLQ: {} (reason: {})",
            file_path.display(),
            failure_reason
        );

        Ok(file_path.to_string_lossy().into_owned())
    }

    /// Create a DLQ notification event
    pub fn create_dlq_notification(
        &self,
        event: &RawEvent,
        dlq_file_path: String,
        failure_reason: String,
    ) -> RawEvent {
        let dlq_event = DlqEventWritten {
            agent_name: self.agent_name.clone(),
            failed_event_source: event.source.clone(),
            failed_event_type: event.event_type.clone(),
            dlq_file_path,
            failure_reason,
        };

        create_dlq_event(dlq_event)
    }

    /// Log a critical meta-failure (when we can't even write DLQ notifications)
    pub fn log_critical_failure(&self, error: &str) -> Result<()> {
        let timestamp = Utc::now().to_rfc3339();
        let log_entry = format!("{} CRITICAL: {}\n", timestamp, error);
        
        // Append to critical failures log
        fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.critical_failures_log)
            .and_then(|mut file| {
                use std::io::Write;
                file.write_all(log_entry.as_bytes())
            })
            .with_context(|| {
                format!(
                    "Failed to write to critical failures log: {:?}",
                    self.critical_failures_log
                )
            })?;

        error!("Critical failure logged: {}", error);
        Ok(())
    }

    /// Get count of files in DLQ
    pub fn get_dlq_size(&self) -> Result<u64> {
        let count = fs::read_dir(&self.base_path)
            .context("Failed to read DLQ directory")?
            .filter_map(|entry| entry.ok())
            .filter(|entry| {
                entry
                    .file_type()
                    .map(|ft| ft.is_file())
                    .unwrap_or(false)
            })
            .count() as u64;

        Ok(count)
    }

    /// Read all DLQ entries (for potential replay)
    pub fn read_all_entries(&self) -> Result<Vec<(PathBuf, DlqEntry)>> {
        let mut entries = Vec::new();

        for entry in fs::read_dir(&self.base_path)? {
            let entry = entry?;
            let path = entry.path();
            
            if path.is_file() && path.extension().map_or(false, |ext| ext == "json") {
                match fs::read_to_string(&path) {
                    Ok(content) => match serde_json::from_str::<DlqEntry>(&content) {
                        Ok(dlq_entry) => entries.push((path, dlq_entry)),
                        Err(e) => warn!("Failed to parse DLQ file {:?}: {}", path, e),
                    },
                    Err(e) => warn!("Failed to read DLQ file {:?}: {}", path, e),
                }
            }
        }

        Ok(entries)
    }

    /// Remove a DLQ entry (after successful replay)
    pub fn remove_entry(&self, path: &Path) -> Result<()> {
        fs::remove_file(path)
            .with_context(|| format!("Failed to remove DLQ file: {:?}", path))?;
        info!("Removed DLQ entry: {:?}", path);
        Ok(())
    }
}