//! Social platform event payloads.
//!
//! Covers Reddit GDPR exports and Wykop personal exports.
//! Reddit and Wykop go in this module rather than separate files — the
//! conceptual domain (social content created by the user) is the same.

use crate::Timestamp;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sinex_macros::EventPayload;

// ---------------------------------------------------------------------------
// Reddit
// ---------------------------------------------------------------------------

/// A comment the user posted on Reddit.
///
/// Sourced from `comments.csv` in the Reddit GDPR export.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "reddit", event_type = "social.comment.posted")]
pub struct RedditCommentPayload {
    /// Reddit's internal base-36 comment id (e.g. `ck1fsao`).
    pub reddit_id: String,
    /// Subreddit name (no `r/` prefix).
    pub subreddit: String,
    /// Comment body text.
    pub body: String,
    /// When the comment was created (from `date` column).
    pub created_at: Timestamp,
    /// Base-36 parent id (may be a comment id or link id).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_id: Option<String>,
    /// Base-36 link (submission) id the comment belongs to.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub link_id: Option<String>,
    /// Full permalink URL.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub permalink: Option<String>,
}

/// A submission (post) the user created on Reddit.
///
/// Sourced from `posts.csv` in the Reddit GDPR export.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "reddit", event_type = "social.post.created")]
pub struct RedditPostPayload {
    /// Reddit's internal base-36 post id (e.g. `38focg`).
    pub reddit_id: String,
    /// Subreddit name (no `r/` prefix).
    pub subreddit: String,
    /// Post title.
    pub title: String,
    /// When the post was submitted (from `date` column).
    pub created_at: Timestamp,
    /// Self-text body for text posts; absent for link posts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body: Option<String>,
    /// External URL for link posts; absent for text posts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    /// Full permalink URL.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub permalink: Option<String>,
}

// ---------------------------------------------------------------------------
// Wykop
// ---------------------------------------------------------------------------

/// A Wykop entry (micropost / wpis) authored by the user.
///
/// Sourced from `wykop_entries_added.jsonl`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "wykop", event_type = "social.entry.created")]
pub struct WykopEntryPayload {
    /// Numeric Wykop entry id.
    pub entry_id: u64,
    /// URL of the entry on Wykop.
    pub entry_url: String,
    /// When the entry was created.
    pub created_at: Timestamp,
    /// Text content of the entry.
    pub content: String,
    /// Hashtags extracted from the entry (without `#`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,
    /// Net vote score (up − down).
    pub votes_score: i64,
    /// URL of the attached photo, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub photo_url: Option<String>,
}

/// A comment the user posted on a Wykop entry.
///
/// Sourced from `wykop_entry_comments.jsonl`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "wykop", event_type = "social.entry_comment.posted")]
pub struct WykopEntryCommentPayload {
    /// Numeric Wykop comment id.
    pub comment_id: u64,
    /// Numeric id of the parent entry.
    pub entry_id: u64,
    /// URL of the parent entry.
    pub entry_url: String,
    /// When the comment was posted.
    pub created_at: Timestamp,
    /// Comment text content.
    pub content: String,
    /// Comment vote rating.
    pub rating: i64,
    /// URL of an attached photo, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub photo_url: Option<String>,
}

// ---------------------------------------------------------------------------
// Smoke tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::EventPayload;

    #[test]
    fn reddit_comment_declares_source_and_event_type() {
        assert_eq!(
            RedditCommentPayload::SOURCE.as_static_str(),
            "reddit"
        );
        assert_eq!(
            RedditCommentPayload::EVENT_TYPE.as_static_str(),
            "social.comment.posted"
        );
    }

    #[test]
    fn reddit_post_declares_source_and_event_type() {
        assert_eq!(RedditPostPayload::SOURCE.as_static_str(), "reddit");
        assert_eq!(
            RedditPostPayload::EVENT_TYPE.as_static_str(),
            "social.post.created"
        );
    }

    #[test]
    fn wykop_entry_declares_source_and_event_type() {
        assert_eq!(WykopEntryPayload::SOURCE.as_static_str(), "wykop");
        assert_eq!(
            WykopEntryPayload::EVENT_TYPE.as_static_str(),
            "social.entry.created"
        );
    }

    #[test]
    fn wykop_entry_comment_declares_source_and_event_type() {
        assert_eq!(
            WykopEntryCommentPayload::SOURCE.as_static_str(),
            "wykop"
        );
        assert_eq!(
            WykopEntryCommentPayload::EVENT_TYPE.as_static_str(),
            "social.entry_comment.posted"
        );
    }
}
