//! Filesystem event payloads

use crate::domain::SanitizedPath;
use crate::events::enums::FileModificationType;
use crate::Timestamp;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sinex_macros::EventPayload;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "fs-watcher", event_type = "file.created", version = "1.0.0")]
pub struct FileCreatedPayload {
    pub path: SanitizedPath,
    pub size: u64,
    pub created_at: Timestamp,
    pub permissions: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "fs-watcher", event_type = "file.modified")]
pub struct FileModifiedPayload {
    pub path: SanitizedPath,
    pub size: u64,
    pub modified_at: Timestamp,
    pub modification_type: FileModificationType,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "fs-watcher", event_type = "file.deleted")]
pub struct FileDeletedPayload {
    pub path: SanitizedPath,
    pub deleted_at: Timestamp,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "fs-watcher", event_type = "file.moved")]
pub struct FileMovedPayload {
    pub old_path: SanitizedPath,
    pub new_path: SanitizedPath,
    pub moved_at: Timestamp,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "fs-watcher", event_type = "dir.created")]
pub struct DirCreatedPayload {
    pub path: SanitizedPath,
    pub created_at: Timestamp,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "fs-watcher", event_type = "dir.deleted")]
pub struct DirDeletedPayload {
    pub path: SanitizedPath,
    pub deleted_at: Timestamp,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "fs-watcher", event_type = "file.discovered")]
pub struct FileDiscoveredPayload {
    pub path: SanitizedPath,
    pub size: u64,
    pub modified_at: Timestamp,
    pub permissions: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "fs-watcher", event_type = "dir.discovered")]
pub struct DirDiscoveredPayload {
    pub path: SanitizedPath,
    pub modified_at: Timestamp,
}

// Test helpers for external tests
#[cfg(any(test, feature = "testing"))]
impl FileCreatedPayload {
    pub fn test_default(path: impl Into<SanitizedPath>) -> Self {
        Self {
            path: path.into(),
            size: 0,
            created_at: crate::temporal::now().into(),
            permissions: None,
        }
    }
}

#[cfg(any(test, feature = "testing"))]
impl FileModifiedPayload {
    pub fn test_default(path: impl Into<SanitizedPath>) -> Self {
        Self {
            path: path.into(),
            size: 0,
            modified_at: crate::temporal::now().into(),
            modification_type: FileModificationType::Content,
        }
    }
}

#[cfg(any(test, feature = "testing"))]
impl FileDeletedPayload {
    pub fn test_default(path: impl Into<SanitizedPath>) -> Self {
        Self {
            path: path.into(),
            deleted_at: crate::temporal::now().into(),
        }
    }
}
