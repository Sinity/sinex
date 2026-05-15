//! Bookmark-domain event payloads.
//!
//! Currently hosts the Raindrop bookmark export payload (#1091).

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sinex_macros::EventPayload;

use crate::Timestamp;

/// One bookmark observation from a Raindrop CSV export.
///
/// Raindrop's CSV columns are: `id,title,note,excerpt,url,folder,tags,created,
/// cover,highlights,favorite`. We keep the semantic fields and drop the
/// cover-image URL by default (it's a CDN reference that rots quickly and
/// has no replay role).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "raindrop", event_type = "bookmark.created")]
pub struct RaindropBookmarkPayload {
    /// Numeric Raindrop bookmark id (column `id`).
    pub raindrop_id: i64,

    /// URL of the bookmarked page (column `url`).
    pub url: String,

    /// Time the bookmark was created (column `created`).
    pub created_at: Timestamp,

    /// Folder name (column `folder`). `"Unsorted"` for the default folder.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub folder: Option<String>,

    /// Title — often the URL itself or a copy of the URL prefixed with a dash
    /// for the operator's archive (column `title`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,

    /// User note attached to the bookmark (column `note`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,

    /// Page excerpt Raindrop grabbed when the bookmark was saved (column `excerpt`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub excerpt: Option<String>,

    /// Comma-delimited tags as stored in Raindrop (column `tags`). Preserved
    /// verbatim; downstream consumers can split on `,` if they want a list.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tags: Option<String>,

    /// Whether the bookmark was marked favorite (column `favorite`).
    pub favorite: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::EventPayload;

    #[test]
    fn declares_source_and_event_type() {
        assert_eq!(
            RaindropBookmarkPayload::SOURCE.as_static_str(),
            "raindrop"
        );
        assert_eq!(
            RaindropBookmarkPayload::EVENT_TYPE.as_static_str(),
            "bookmark.created"
        );
    }
}
