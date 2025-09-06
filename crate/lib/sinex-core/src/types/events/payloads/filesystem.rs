//! Filesystem event payloads

use crate::types::domain::SanitizedPath;
use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sinex_macros::EventPayload;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "fs-watcher", event_type = "file.created", version = "1.0.0")]
pub struct FileCreatedPayload {
    pub path: SanitizedPath,
    pub size: u64,
    pub created_at: DateTime<Utc>,
    pub permissions: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "fs-watcher", event_type = "file.modified")]
pub struct FileModifiedPayload {
    pub path: SanitizedPath,
    pub size: u64,
    pub modified_at: DateTime<Utc>,
    pub modification_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "fs-watcher", event_type = "file.deleted")]
pub struct FileDeletedPayload {
    pub path: SanitizedPath,
    pub deleted_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "fs-watcher", event_type = "file.moved")]
pub struct FileMovedPayload {
    pub old_path: SanitizedPath,
    pub new_path: SanitizedPath,
    pub moved_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "fs-watcher", event_type = "dir.created")]
pub struct DirCreatedPayload {
    pub path: SanitizedPath,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "fs-watcher", event_type = "dir.deleted")]
pub struct DirDeletedPayload {
    pub path: SanitizedPath,
    pub deleted_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "fs-watcher", event_type = "file.discovered")]
pub struct FileDiscoveredPayload {
    pub path: SanitizedPath,
    pub size: u64,
    pub modified_at: DateTime<Utc>,
    pub permissions: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "fs-watcher", event_type = "dir.discovered")]
pub struct DirDiscoveredPayload {
    pub path: SanitizedPath,
    pub modified_at: DateTime<Utc>,
}

impl FileCreatedPayload {
    /// Builder-style method for size
    pub fn with_size(mut self, size: u64) -> Self {
        self.size = size;
        self
    }

    /// Builder-style method for permissions  
    pub fn with_permissions(mut self, perms: u32) -> Self {
        self.permissions = Some(perms);
        self
    }

    /// Builder-style method for created_at timestamp
    pub fn with_created_at(mut self, timestamp: DateTime<Utc>) -> Self {
        self.created_at = timestamp;
        self
    }
}

impl FileModifiedPayload {
    /// Builder-style method for size
    pub fn with_size(mut self, size: u64) -> Self {
        self.size = size;
        self
    }

    /// Builder-style method for modification type
    pub fn with_modification_type(mut self, mod_type: impl Into<String>) -> Self {
        self.modification_type = mod_type.into();
        self
    }

    /// Builder-style method for modified_at timestamp
    pub fn with_modified_at(mut self, timestamp: DateTime<Utc>) -> Self {
        self.modified_at = timestamp;
        self
    }
}

impl FileDeletedPayload {
    /// Builder-style method for deleted_at timestamp
    pub fn with_deleted_at(mut self, timestamp: DateTime<Utc>) -> Self {
        self.deleted_at = timestamp;
        self
    }
}

impl FileMovedPayload {
    /// Builder-style method for moved_at timestamp
    pub fn with_moved_at(mut self, timestamp: DateTime<Utc>) -> Self {
        self.moved_at = timestamp;
        self
    }
}

impl DirCreatedPayload {
    /// Builder-style method for created_at timestamp
    pub fn with_created_at(mut self, timestamp: DateTime<Utc>) -> Self {
        self.created_at = timestamp;
        self
    }
}

impl DirDeletedPayload {
    /// Builder-style method for deleted_at timestamp
    pub fn with_deleted_at(mut self, timestamp: DateTime<Utc>) -> Self {
        self.deleted_at = timestamp;
        self
    }
}

impl FileDiscoveredPayload {
    /// Builder-style method for size
    pub fn with_size(mut self, size: u64) -> Self {
        self.size = size;
        self
    }

    /// Builder-style method for permissions
    pub fn with_permissions(mut self, perms: u32) -> Self {
        self.permissions = Some(perms);
        self
    }

    /// Builder-style method for modified_at timestamp
    pub fn with_modified_at(mut self, timestamp: DateTime<Utc>) -> Self {
        self.modified_at = timestamp;
        self
    }
}

impl DirDiscoveredPayload {
    /// Builder-style method for modified_at timestamp
    pub fn with_modified_at(mut self, timestamp: DateTime<Utc>) -> Self {
        self.modified_at = timestamp;
        self
    }
}

// Test helpers for external tests
#[cfg(feature = "testing")]
impl FileCreatedPayload {
    pub fn test_default(path: impl Into<SanitizedPath>) -> Self {
        Self {
            path: path.into(),
            size: 0,
            created_at: Utc::now(),
            permissions: None,
        }
    }
}

#[cfg(feature = "testing")]
impl FileModifiedPayload {
    pub fn test_default(path: impl Into<SanitizedPath>) -> Self {
        Self {
            path: path.into(),
            size: 0,
            modified_at: Utc::now(),
            modification_type: "modified".to_string(),
        }
    }
}

#[cfg(feature = "testing")]
impl FileDeletedPayload {
    pub fn test_default(path: impl Into<SanitizedPath>) -> Self {
        Self {
            path: path.into(),
            deleted_at: Utc::now(),
        }
    }
}
