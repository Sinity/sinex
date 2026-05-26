//! Music-domain event payloads.
//!
//! Currently hosts the Spotify Extended Streaming History playback payload
//! (#1092). Adding more music-related event types should land in this module
//! rather than spawning a new domain per provider.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sinex_macros::EventPayload;

use crate::Timestamp;

/// One playback observation from a Spotify Extended Streaming History export.
///
/// The export records one entry per played track/episode. We mirror its
/// fields verbatim where they carry semantic load, drop the leaked IP /
/// user-agent fields by default, and surface `skipped` in two forms:
///
/// - `skipped_provider` — the raw boolean Spotify wrote into the export
/// - `skipped_inferred` — `played_ms < 30_000`, the target-vision threshold
///
/// Both are preserved so downstream consumers can pick which definition to
/// use without re-deriving it from the payload.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "spotify", event_type = "track.played")]
pub struct SpotifyTrackPlayedPayload {
    /// ISO-8601 start time, copied from the `ts` field.
    pub started_at: Timestamp,

    /// Milliseconds the track was played, from `ms_played`.
    pub played_ms: u64,

    /// Provider-supplied skipped flag (Spotify's own determination).
    pub skipped_provider: bool,

    /// Locally inferred skip: `played_ms < 30_000`.
    /// Preserved so downstream consumers can switch definitions without
    /// re-deriving from the raw payload.
    pub skipped_inferred: bool,

    /// `spotify:track:...` URI (None for podcasts and audiobooks).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub track_uri: Option<String>,

    /// Track display name. None when only podcast/audiobook metadata is set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub track_name: Option<String>,

    /// Album artist display name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artist_name: Option<String>,

    /// Album display name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub album_name: Option<String>,

    /// `spotify:episode:...` URI when the entry is a podcast episode.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub episode_uri: Option<String>,

    /// Podcast episode title.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub episode_name: Option<String>,

    /// Podcast show name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub show_name: Option<String>,

    /// Platform string (e.g. `"Windows 7 (Unknown Ed) SP0 [x86 0]"`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub platform: Option<String>,

    /// `conn_country` two-letter country code as reported by Spotify.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conn_country: Option<String>,

    /// `reason_start`: why playback began (`"uriopen"`, `"trackdone"`, etc.).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason_start: Option<String>,

    /// `reason_end`: why playback ended.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason_end: Option<String>,

    /// Whether shuffle was active.
    pub shuffle: bool,

    /// Whether the playback happened offline.
    pub offline: bool,

    /// Whether the user was in incognito (private) mode.
    pub incognito_mode: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::EventPayload;
    use xtask::sandbox::prelude::*;

    #[sinex_test]
    async fn declares_source_and_event_type() -> TestResult<()> {
        assert_eq!(SpotifyTrackPlayedPayload::SOURCE.as_static_str(), "spotify");
        assert_eq!(
            SpotifyTrackPlayedPayload::EVENT_TYPE.as_static_str(),
            "track.played"
        );
        Ok(())
    }
}
