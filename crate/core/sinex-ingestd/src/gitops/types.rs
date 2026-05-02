//! Types for the `GitOps` schema sync service.

use serde::{Deserialize, Serialize};
use sinex_primitives::domain::{EventSource, EventType};
use sinex_primitives::temporal::Timestamp;
use uuid::Uuid;

/// A configured Git repository source for schema discovery.
#[derive(Debug, Clone)]
pub struct GitOpsSource {
    pub id: Uuid,
    pub repository_url: String,
    pub branch: String,
    pub path_pattern: String,
    pub sync_enabled: bool,
    pub last_sync_at: Option<Timestamp>,
    pub last_sync_commit: Option<String>,
    pub sync_frequency_minutes: i32,
}

impl GitOpsSource {
    /// Check whether enough time has elapsed since the last sync.
    #[must_use]
    pub fn needs_sync(&self) -> bool {
        match self.last_sync_at {
            None => true,
            Some(last) => {
                let elapsed_minutes = (Timestamp::now().inner() - last.inner()).whole_minutes();
                elapsed_minutes >= i64::from(self.sync_frequency_minutes)
            }
        }
    }
}

/// A schema file discovered inside a cloned repository.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveredSchema {
    pub source: EventSource,
    pub event_type: EventType,
    pub version: String,
    pub schema_content: serde_json::Value,
    pub file_path: String,
}

/// Statistics for a single sync cycle.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct GitOpsSyncStats {
    pub sources_checked: usize,
    pub sources_synced: usize,
    pub sources_skipped: usize,
    pub schemas_discovered: usize,
    pub schemas_created: usize,
    pub schemas_updated: usize,
    pub schemas_unchanged: usize,
    pub errors: Vec<String>,
}
