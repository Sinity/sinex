//! Spotify Extended Streaming History parser (#1092).
//!
//! Reads `Streaming_History_Audio_*.json` files (one JSON array per file)
//! and emits one `spotify`/`track.played` event per array entry. Wired
//! through [`StaticFileAdapter`] (one-shot file read) so each export file
//! turns into one source-material registration and N parsed event intents.
//!
//! ## Skip semantics
//!
//! The parser preserves *both* the provider-reported `skipped` boolean and
//! a locally inferred `skipped_inferred = played_ms < 30_000` (target-vision
//! threshold). Downstream consumers can pick which definition to use without
//! re-deriving from the raw payload.
//!
//! ## Anchoring
//!
//! [`StaticFileAdapter`] emits one [`SourceRecord`] per file with
//! `MaterialAnchor::ByteRange { start: 0, len: <file_size> }`. The parser
//! synthesizes one per-entry `MaterialAnchor::ByteRange { start: <index>,
//! len: 1 }` per intent — `<index>` is the entry's position in the array,
//! which is stable across replays of the same export.
//!
//! Occurrence identity is expressed by `occurrence_key`, carried onto events as
//! `equivalence_key` (#1570 Prong C) so the curation duplicate workbench can
//! group equivalent occurrences. DB-backed scan-time dedup via
//! `build_occurrence_filter` is the offline #1050 import path (live source
//! units have no DB pool); automatic admission-side suppression is tracked
//! separately:
//! `(track_uri, track_name, artist_name, started_at, played_ms)` — the
//! same 5 fields in the same order on every row, regardless of whether
//! `spotify_track_uri` is populated. Rows without a URI carry an empty
//! `track_uri` and fall back to `(track_name, artist_name, started_at,
//! played_ms)` as the de-facto identity. Always emitting the same shape
//! prevents a track played twice (once URI-null, later URI-populated)
//! from producing two different keys and silently double-publishing.
//!
//! ## Privacy
//!
//! `ip_addr` and `user_agent_decrypted` are intentionally dropped — they
//! are leak-prone fields with no replay-required role. `platform` and
//! `conn_country` are kept because they carry useful temporal context.
//! `incognito_mode` is surfaced so admission policy can choose to elide
//! private listens at admission time.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::runtime::parser::{MaterialParser, ParserError, ParserResult};
use sinex_macros::SourceMeta;
use sinex_primitives::domain::{EventSource, EventType};
use sinex_primitives::parser::{
    InputShapeKind, MaterialAnchor, OccurrenceKey, ParsedEventIntent, ParserContext, ParserId,
    ParserManifest, SourceId, SourceRecord, TimingConfidence, TimingEvidence,
};
use sinex_primitives::privacy::ProcessingContext;
use sinex_primitives::source_contracts::{
    AccessScope, CheckpointFamily, Horizon, OccurrenceIdentity, PrivacyTier, ResourceProfile,
    RetentionPolicy, RunnerPack, RuntimeShape,
};
use sinex_primitives::temporal::Timestamp;

const SKIP_THRESHOLD_MS: u64 = 30_000;

// ---------------------------------------------------------------------------
// Raw export row (mirrors the JSON shape verbatim, lenient on missing fields)
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct SpotifyExportRow {
    /// ISO-8601 timestamp (e.g. `"2013-02-12T13:17:05Z"`).
    ts: String,
    #[serde(default)]
    ms_played: u64,
    #[serde(default)]
    platform: Option<String>,
    #[serde(default)]
    conn_country: Option<String>,
    #[serde(default)]
    master_metadata_track_name: Option<String>,
    #[serde(default)]
    master_metadata_album_artist_name: Option<String>,
    #[serde(default)]
    master_metadata_album_album_name: Option<String>,
    #[serde(default)]
    spotify_track_uri: Option<String>,
    #[serde(default)]
    episode_name: Option<String>,
    #[serde(default)]
    episode_show_name: Option<String>,
    #[serde(default)]
    spotify_episode_uri: Option<String>,
    #[serde(default)]
    reason_start: Option<String>,
    #[serde(default)]
    reason_end: Option<String>,
    #[serde(default)]
    shuffle: bool,
    #[serde(default)]
    skipped: bool,
    #[serde(default)]
    offline: bool,
    #[serde(default)]
    incognito_mode: bool,
}

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SpotifyHistoryConfig;

#[derive(Debug, Clone, Default, SourceMeta)]
#[source_meta(
    id = "spotify-extended-history",
    namespace = "music",
    event_source = "spotify",
    event_type = "track.played",
    adapter = "StaticFileAdapter",
    privacy_tier = PrivacyTier::Sensitive,
    horizons(Horizon::Historical),
    retention = RetentionPolicy::Forever,
    occurrence_identity = OccurrenceIdentity::Uuid5From("(track_uri, track_name, artist_name, started_at, played_ms)"),
    access_scope = AccessScope::StagedExport,
    implementation = "sinexd",
    privacy_context = ProcessingContext::Metadata,
    resource_profile = ResourceProfile::BoundedFile,
    runner_pack = RunnerPack::SinexdSource,
    checkpoint_family = CheckpointFamily::AppendStream,
    runtime_shape = RuntimeShape::OnDemand,
)]
pub struct SpotifyHistoryParser;

#[async_trait]
impl MaterialParser for SpotifyHistoryParser {
    type Config = SpotifyHistoryConfig;

    fn manifest(&self) -> ParserManifest {
        ParserManifest {
            parser_id: ParserId::from_static("spotify-extended-history"),
            parser_version: "1.0.0".into(),
            accepted_input_shapes: vec![InputShapeKind::StaticFile],
            source_id: SourceId::from_static("spotify-extended-history"),
            declared_event_types: vec![(
                EventSource::from_static("spotify"),
                EventType::from_static("track.played"),
            )],
            privacy_contexts: vec![ProcessingContext::Metadata],
            sensitivity_hints: Vec::new(),
            description: "Parses Spotify Extended Streaming History JSON exports \
                into typed track.played events. Preserves both provider and \
                inferred skip semantics; drops IP/user-agent fields."
                .into(),
        }
    }

    async fn parse_record(
        &mut self,
        record: SourceRecord,
        ctx: &ParserContext,
    ) -> ParserResult<Vec<ParsedEventIntent>> {
        let rows: Vec<SpotifyExportRow> = serde_json::from_slice(&record.bytes)
            .map_err(|e| ParserError::Parse(format!("invalid Spotify export JSON array: {e}")))?;

        let mut intents = Vec::with_capacity(rows.len());

        for (index, row) in rows.into_iter().enumerate() {
            let Some(intent) = parse_row(row, index, ctx)? else {
                continue;
            };
            intents.push(intent);
        }

        Ok(intents)
    }

    fn required_input_keys(&self) -> Vec<String> {
        vec!["/[]/ts".to_owned()]
    }
}

fn parse_row(
    row: SpotifyExportRow,
    index: usize,
    ctx: &ParserContext,
) -> ParserResult<Option<ParsedEventIntent>> {
    let started_at = parse_iso8601(&row.ts)?;
    let played_ms = row.ms_played;
    let skipped_inferred = played_ms < SKIP_THRESHOLD_MS;

    let occurrence_key = build_occurrence_key(
        row.spotify_track_uri.as_deref(),
        row.master_metadata_track_name.as_deref(),
        row.master_metadata_album_artist_name.as_deref(),
        &row.ts,
        played_ms,
    );

    let payload = serde_json::json!({
        "started_at": started_at,
        "played_ms": played_ms,
        "skipped_provider": row.skipped,
        "skipped_inferred": skipped_inferred,
        "track_uri": row.spotify_track_uri,
        "track_name": row.master_metadata_track_name,
        "artist_name": row.master_metadata_album_artist_name,
        "album_name": row.master_metadata_album_album_name,
        "episode_uri": row.spotify_episode_uri,
        "episode_name": row.episode_name,
        "show_name": row.episode_show_name,
        "platform": row.platform,
        "conn_country": row.conn_country,
        "reason_start": row.reason_start,
        "reason_end": row.reason_end,
        "shuffle": row.shuffle,
        "offline": row.offline,
        "incognito_mode": row.incognito_mode,
    });

    Ok(Some(
        ParsedEventIntent::builder()
            .source_id(ctx.source_id.clone())
            .parser_id(ParserId::from_static("spotify-extended-history"))
            .parser_version("1.0.0")
            .event_type(EventType::from_static("track.played"))
            .event_source(EventSource::from_static("spotify"))
            .payload(payload)
            .ts_orig(started_at)
            .timing(TimingEvidence::Intrinsic {
                field: "ts".into(),
                confidence: TimingConfidence::Intrinsic,
            })
            .anchor(MaterialAnchor::ByteRange {
                start: index as u64,
                len: 1,
            })
            .occurrence_key(occurrence_key)
            .privacy_context(ProcessingContext::Metadata)
            .build(),
    ))
}

fn parse_iso8601(raw: &str) -> ParserResult<Timestamp> {
    use time::OffsetDateTime;
    use time::format_description::well_known::Rfc3339;
    let dt = OffsetDateTime::parse(raw, &Rfc3339)
        .map_err(|e| ParserError::Parse(format!("invalid Spotify timestamp '{raw}': {e}")))?;
    Ok(Timestamp::new(dt))
}

/// Build a stable occurrence key for one playback row.
///
/// Always emits the same 5 fields in the same order, regardless of whether
/// `spotify_track_uri` is populated. A previous version branched on URI
/// presence — that yielded different key shapes for "same track played
/// before and after Spotify started populating URIs" and silently broke
/// dedup. The empty-string sentinel in `track_uri` is the OR-fallback
/// marker: when URI is absent, dedup falls back to `(track_name,
/// artist_name, started_at, played_ms)` because `track_uri = ""` for every
/// such row.
fn build_occurrence_key(
    uri: Option<&str>,
    track: Option<&str>,
    artist: Option<&str>,
    ts: &str,
    played_ms: u64,
) -> OccurrenceKey {
    let fields = vec![
        ("track_uri".into(), uri.unwrap_or("").to_string()),
        ("track_name".into(), track.unwrap_or("").to_string()),
        ("artist_name".into(), artist.unwrap_or("").to_string()),
        ("started_at".into(), ts.to_string()),
        ("played_ms".into(), played_ms.to_string()),
    ];
    OccurrenceKey {
        source_id: SourceId::from_static("spotify-extended-history"),
        fields,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "music_test.rs"]
mod tests;
