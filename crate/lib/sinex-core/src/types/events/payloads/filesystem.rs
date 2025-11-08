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
    assign_setter!(
        #[doc = "Builder-style method for size"]
        with_size,
        size,
        u64
    );
    option_setter!(
        #[doc = "Builder-style method for permissions"]
        with_permissions,
        permissions,
        u32
    );
    assign_setter!(
        #[doc = "Builder-style method for created_at timestamp"]
        with_created_at,
        created_at,
        DateTime<Utc>
    );
}

impl FileModifiedPayload {
    assign_setter!(
        #[doc = "Builder-style method for size"]
        with_size,
        size,
        u64
    );
    assign_into_setter!(
        #[doc = "Builder-style method for modification type"]
        with_modification_type,
        modification_type,
        impl Into<String>
    );
    assign_setter!(
        #[doc = "Builder-style method for modified_at timestamp"]
        with_modified_at,
        modified_at,
        DateTime<Utc>
    );
}

impl FileDeletedPayload {
    assign_setter!(
        #[doc = "Builder-style method for deleted_at timestamp"]
        with_deleted_at,
        deleted_at,
        DateTime<Utc>
    );
}

impl FileMovedPayload {
    assign_setter!(
        #[doc = "Builder-style method for moved_at timestamp"]
        with_moved_at,
        moved_at,
        DateTime<Utc>
    );
}

impl DirCreatedPayload {
    assign_setter!(
        #[doc = "Builder-style method for created_at timestamp"]
        with_created_at,
        created_at,
        DateTime<Utc>
    );
}

impl DirDeletedPayload {
    assign_setter!(
        #[doc = "Builder-style method for deleted_at timestamp"]
        with_deleted_at,
        deleted_at,
        DateTime<Utc>
    );
}

impl FileDiscoveredPayload {
    assign_setter!(
        #[doc = "Builder-style method for size"]
        with_size,
        size,
        u64
    );
    option_setter!(
        #[doc = "Builder-style method for permissions"]
        with_permissions,
        permissions,
        u32
    );
    assign_setter!(
        #[doc = "Builder-style method for modified_at timestamp"]
        with_modified_at,
        modified_at,
        DateTime<Utc>
    );
}

impl DirDiscoveredPayload {
    assign_setter!(
        #[doc = "Builder-style method for modified_at timestamp"]
        with_modified_at,
        modified_at,
        DateTime<Utc>
    );
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
