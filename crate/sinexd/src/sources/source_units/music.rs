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
//! Occurrence identity is expressed by `occurrence_key` (occurrence-level dedup
//! is planned, not yet wired — `build_occurrence_filter` has no production
//! callers; see #1570 Prong C / #1050):
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

use crate::node_sdk::parser::{MaterialParser, ParserError, ParserResult, StaticFileAdapter};
use sinex_primitives::domain::{EventSource, EventType};
use sinex_primitives::parser::{
    InputShapeKind, MaterialAnchor, OccurrenceKey, ParsedEventIntent, ParserContext, ParserId,
    ParserManifest, SourceRecord, SourceUnitId, TimingConfidence, TimingEvidence,
};
use sinex_primitives::privacy::ProcessingContext;
use sinex_primitives::proof::{
    CheckpointFamily, Horizon, OccurrenceIdentity, PrivacyTier, RetentionPolicy, RuntimeShape,
    SourceUnitBinding, SourceUnitBuildImpact, SourceUnitDescriptor, SubjectRef,
};
use sinex_primitives::temporal::Timestamp;
use sinex_primitives::{register_source_unit, register_source_unit_binding};

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

#[derive(Debug, Clone, Default)]
pub struct SpotifyHistoryParser;

#[async_trait]
impl MaterialParser for SpotifyHistoryParser {
    type Config = SpotifyHistoryConfig;

    fn manifest(&self) -> ParserManifest {
        ParserManifest {
            parser_id: ParserId::from_static("spotify-extended-history"),
            parser_version: "1.0.0".into(),
            accepted_input_shapes: vec![InputShapeKind::StaticFile],
            source_unit_id: SourceUnitId::from_static("spotify-extended-history"),
            declared_event_types: vec![(
                EventSource::from_static("spotify"),
                EventType::from_static("track.played"),
            )],
            privacy_contexts: vec![ProcessingContext::Metadata],
            proof_obligations: vec![
                "timestamp_intrinsic".into(),
                "skipped_provider_preserved".into(),
                "skipped_inferred_threshold_30s".into(),
                "occurrence_key_uri_or_name_tuple".into(),
                "ip_and_user_agent_dropped".into(),
            ],
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
            .source_unit_id(ctx.source_unit_id.clone())
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
        source_unit_id: SourceUnitId::from_static("spotify-extended-history"),
        fields,
    }
}

// ---------------------------------------------------------------------------
// Source unit descriptor + binding + registration
// ---------------------------------------------------------------------------

register_source_unit! {
    SourceUnitDescriptor {
        id: "spotify-extended-history",
        namespace: "music",
        event_types: &[("spotify", "track.played")],
        privacy_tier: PrivacyTier::Sensitive,
        horizons: &[Horizon::Historical],
        retention: RetentionPolicy::Forever,
        proof_obligations: &[
            "timestamp_intrinsic",
            "skipped_provider_preserved",
            "skipped_inferred_threshold_30s",
            "occurrence_key_uri_or_name_tuple",
            "ip_and_user_agent_dropped",
        ],
        occurrence_identity: OccurrenceIdentity::Uuid5From(
            "(track_uri, track_name, artist_name, started_at, played_ms)",
        ),
        access_policy: "personal_music_history",
    }
}

register_source_unit_binding! {
    SourceUnitBinding::builder(
        SubjectRef::from_static("source_unit:spotify-extended-history"),
        "spotify-extended-history",
        "music",
    )
    .implementation("sinex-source-worker")
    .adapter("StaticFileAdapter")
    .output_event_type("track.played")
    .privacy_context("Metadata")
    .material_policy("static_export_file")
    .checkpoint_policy("static_file_cursor")
    .resource_shape("file_reader")
    .source_unit_id("spotify-extended-history")
    .runner_pack("source-worker")
    .checkpoint_family(CheckpointFamily::AppendStream)
    .runtime_shape(RuntimeShape::OnDemand)
    .package_impact("spotify_extended_history_source_unit")
    .implementation_mode("rust_in_pack:source-worker")
    .build_impact(SourceUnitBuildImpact::ZERO)
    .build()
}

crate::register_adapter_ingestor!(
    source_unit_id: "spotify-extended-history",
    adapter: StaticFileAdapter,
    parser: SpotifyHistoryParser,
);

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use sinex_primitives::Uuid;
    use sinex_primitives::ids::Id;
    use sinex_primitives::parser::MaterialAnchor;
    use sinex_primitives::parser::{OccurrenceFilter, occurrence_key_string};

    use xtask::sandbox::prelude::sinex_test;

    fn test_ctx() -> ParserContext {
        ParserContext {
            source_unit_id: SourceUnitId::from_static("spotify-extended-history"),
            source_material_id: Id::new(),
            record_anchor: MaterialAnchor::ByteRange { start: 0, len: 0 },
            operation_id: Uuid::new_v4(),
            job_id: Uuid::new_v4(),
            host: "test-host".into(),
            acquisition_time: Timestamp::now(),
        }
    }

    fn record_for(bytes: &[u8]) -> SourceRecord {
        SourceRecord {
            material_id: Id::new(),
            anchor: MaterialAnchor::ByteRange {
                start: 0,
                len: bytes.len() as u64,
            },
            bytes: bytes.to_vec(),
            logical_path: None,
            source_ts_hint: None,
            metadata: serde_json::Value::Null,
        }
    }

    const SAMPLE_EXPORT: &str = r#"[
      {
        "ts": "2024-01-15T08:00:00Z",
        "ms_played": 240000,
        "platform": "Linux",
        "conn_country": "PL",
        "master_metadata_track_name": "Aqualung",
        "master_metadata_album_artist_name": "Jethro Tull",
        "master_metadata_album_album_name": "Aqualung",
        "spotify_track_uri": "spotify:track:abc123",
        "reason_start": "trackdone",
        "reason_end": "trackdone",
        "shuffle": false,
        "skipped": false,
        "offline": false,
        "incognito_mode": false
      },
      {
        "ts": "2024-01-15T08:04:00Z",
        "ms_played": 12000,
        "platform": "Linux",
        "master_metadata_track_name": "Hymn 43",
        "master_metadata_album_artist_name": "Jethro Tull",
        "spotify_track_uri": "spotify:track:xyz999",
        "reason_end": "fwdbtn",
        "shuffle": false,
        "skipped": false,
        "offline": false,
        "incognito_mode": false
      }
    ]"#;

    #[sinex_test]
    async fn parses_export_into_two_intents() -> TestResult<()> {
        let mut parser = SpotifyHistoryParser;
        let intents = parser
            .parse_record(record_for(SAMPLE_EXPORT.as_bytes()), &test_ctx())
            .await
            .unwrap();

        assert_eq!(intents.len(), 2);
        for intent in &intents {
            assert_eq!(intent.event_source.as_str(), "spotify");
            assert_eq!(intent.event_type.as_str(), "track.played");
        }
        Ok(())
    }

    #[sinex_test]
    async fn preserves_provider_and_inferred_skip() -> TestResult<()> {
        let mut parser = SpotifyHistoryParser;
        let intents = parser
            .parse_record(record_for(SAMPLE_EXPORT.as_bytes()), &test_ctx())
            .await
            .unwrap();

        // First row: 240s played, provider says not-skipped → both flags false.
        assert_eq!(intents[0].payload["skipped_provider"], false);
        assert_eq!(intents[0].payload["skipped_inferred"], false);

        // Second row: 12s played (< 30s), provider says not-skipped →
        // inferred true, provider false. Both preserved verbatim.
        assert_eq!(intents[1].payload["skipped_provider"], false);
        assert_eq!(intents[1].payload["skipped_inferred"], true);
        Ok(())
    }

    #[sinex_test]
    async fn anchor_uses_array_index() -> TestResult<()> {
        let mut parser = SpotifyHistoryParser;
        let intents = parser
            .parse_record(record_for(SAMPLE_EXPORT.as_bytes()), &test_ctx())
            .await
            .unwrap();

        assert!(matches!(
            intents[0].anchor,
            MaterialAnchor::ByteRange { start: 0, len: 1 }
        ));
        assert!(matches!(
            intents[1].anchor,
            MaterialAnchor::ByteRange { start: 1, len: 1 }
        ));
        Ok(())
    }

    #[sinex_test]
    async fn occurrence_key_always_emits_full_five_field_shape() -> TestResult<()> {
        let mut parser = SpotifyHistoryParser;
        let intents = parser
            .parse_record(record_for(SAMPLE_EXPORT.as_bytes()), &test_ctx())
            .await
            .unwrap();

        // URI present: track_uri populated; name/artist still emitted.
        let key = intents[0].occurrence_key.as_ref().unwrap();
        assert_eq!(
            key.fields,
            vec![
                ("track_uri".to_string(), "spotify:track:abc123".to_string()),
                ("track_name".to_string(), "Aqualung".to_string()),
                ("artist_name".to_string(), "Jethro Tull".to_string()),
                ("started_at".to_string(), "2024-01-15T08:00:00Z".to_string()),
                ("played_ms".to_string(), "240000".to_string()),
            ]
        );
        Ok(())
    }

    #[sinex_test]
    async fn occurrence_key_uri_absent_fills_empty_track_uri() -> TestResult<()> {
        let mut parser = SpotifyHistoryParser;
        let no_uri = r#"[{
            "ts": "2024-01-15T08:00:00Z",
            "ms_played": 100000,
            "master_metadata_track_name": "Track",
            "master_metadata_album_artist_name": "Artist",
            "shuffle": false, "skipped": false, "offline": false, "incognito_mode": false
        }]"#;
        let intents = parser
            .parse_record(record_for(no_uri.as_bytes()), &test_ctx())
            .await
            .unwrap();
        let key = intents[0].occurrence_key.as_ref().unwrap();
        // Same shape as URI-present rows: track_uri is empty sentinel.
        assert_eq!(
            key.fields,
            vec![
                ("track_uri".to_string(), String::new()),
                ("track_name".to_string(), "Track".to_string()),
                ("artist_name".to_string(), "Artist".to_string()),
                ("started_at".to_string(), "2024-01-15T08:00:00Z".to_string()),
                ("played_ms".to_string(), "100000".to_string()),
            ]
        );
        Ok(())
    }

    #[sinex_test]
    async fn occurrence_key_uri_null_and_uri_populated_produce_distinct_keys() -> TestResult<()> {
        // The wrapper-correctness regression: same logical track played
        // twice (once before Spotify populated URIs, once after) must
        // produce distinct keys so dedup doesn't drop either occurrence,
        // and identical-shape rows so an in-memory match is decidable.
        let mut parser = SpotifyHistoryParser;
        let two_plays = r#"[
            {
                "ts": "2024-01-15T08:00:00Z", "ms_played": 100000,
                "master_metadata_track_name": "Track",
                "master_metadata_album_artist_name": "Artist",
                "shuffle": false, "skipped": false, "offline": false, "incognito_mode": false
            },
            {
                "ts": "2024-01-15T08:00:00Z", "ms_played": 100000,
                "master_metadata_track_name": "Track",
                "master_metadata_album_artist_name": "Artist",
                "spotify_track_uri": "spotify:track:xyz",
                "shuffle": false, "skipped": false, "offline": false, "incognito_mode": false
            }
        ]"#;
        let intents = parser
            .parse_record(record_for(two_plays.as_bytes()), &test_ctx())
            .await
            .unwrap();
        let k0 = intents[0].occurrence_key.as_ref().unwrap();
        let k1 = intents[1].occurrence_key.as_ref().unwrap();
        // Both keys carry exactly 5 fields, in the same order.
        assert_eq!(k0.fields.len(), 5);
        assert_eq!(k1.fields.len(), 5);
        for (a, b) in k0.fields.iter().zip(k1.fields.iter()) {
            assert_eq!(a.0, b.0, "field-name positions must match");
        }
        // And they differ only in `track_uri`.
        assert_ne!(k0.fields[0].1, k1.fields[0].1);
        Ok(())
    }

    #[sinex_test]
    async fn invalid_timestamp_errors() -> TestResult<()> {
        let mut parser = SpotifyHistoryParser;
        let bad = r#"[{
            "ts": "not-a-timestamp",
            "ms_played": 100,
            "shuffle": false, "skipped": false, "offline": false, "incognito_mode": false
        }]"#;
        let result = parser
            .parse_record(record_for(bad.as_bytes()), &test_ctx())
            .await;
        let err = result.unwrap_err().to_string();
        assert!(err.contains("invalid Spotify timestamp"), "got: {err}");
        Ok(())
    }

    #[sinex_test]
    async fn empty_array_emits_no_intents() -> TestResult<()> {
        let mut parser = SpotifyHistoryParser;
        let intents = parser
            .parse_record(record_for(b"[]"), &test_ctx())
            .await
            .unwrap();
        assert!(intents.is_empty());
        Ok(())
    }

    #[sinex_test]
    async fn ip_and_user_agent_dropped() -> TestResult<()> {
        let mut parser = SpotifyHistoryParser;
        // Even when present in the export, ip_addr / user_agent_decrypted
        // do not appear in the payload (SpotifyExportRow doesn't deserialize them).
        let with_ip = r#"[{
            "ts": "2024-01-15T08:00:00Z",
            "ms_played": 100000,
            "ip_addr": "1.2.3.4",
            "user_agent_decrypted": "Mozilla/5.0",
            "spotify_track_uri": "spotify:track:abc123",
            "shuffle": false, "skipped": false, "offline": false, "incognito_mode": false
        }]"#;
        let intents = parser
            .parse_record(record_for(with_ip.as_bytes()), &test_ctx())
            .await
            .unwrap();

        let payload = &intents[0].payload;
        assert!(
            payload.get("ip_addr").is_none(),
            "ip_addr must not be carried"
        );
        assert!(
            payload.get("user_agent_decrypted").is_none(),
            "user_agent_decrypted must not be carried"
        );
        Ok(())
    }

    // -----------------------------------------------------------------------
    // OccurrenceFilter dedup (#1050)
    // -----------------------------------------------------------------------

    #[sinex_test]
    async fn occurrence_filter_first_import_all_pass() -> TestResult<()> {
        let mut parser = SpotifyHistoryParser;
        let intents = parser
            .parse_record(record_for(SAMPLE_EXPORT.as_bytes()), &test_ctx())
            .await
            .unwrap();

        // First import: empty filter, all events should pass.
        let mut filter = OccurrenceFilter::empty();
        let mut admitted = 0;
        for intent in &intents {
            let key = occurrence_key_string(
                intent
                    .occurrence_key
                    .as_ref()
                    .expect("Spotify intents must carry occurrence_key"),
            );
            if filter.contains(&key) {
                continue;
            }
            filter.insert(key);
            admitted += 1;
        }
        assert_eq!(admitted, 2, "first import: all events should pass");
        assert_eq!(filter.len(), 2, "filter should track both distinct keys");
        Ok(())
    }

    #[sinex_test]
    async fn occurrence_filter_second_import_all_filtered() -> TestResult<()> {
        let mut parser = SpotifyHistoryParser;

        // First pass: build the filter.
        let first = parser
            .parse_record(record_for(SAMPLE_EXPORT.as_bytes()), &test_ctx())
            .await
            .unwrap();
        let mut filter = OccurrenceFilter::empty();
        for intent in &first {
            if let Some(ref key) = intent.occurrence_key {
                filter.insert(occurrence_key_string(key));
            }
        }

        // Second pass: same data, all should be filtered.
        let second = parser
            .parse_record(record_for(SAMPLE_EXPORT.as_bytes()), &test_ctx())
            .await
            .unwrap();
        let mut filtered = 0;
        for intent in &second {
            if intent
                .occurrence_key
                .as_ref()
                .is_some_and(|k| filter.contains(&occurrence_key_string(k)))
            {
                filtered += 1;
            }
        }
        assert_eq!(
            filtered, 2,
            "second import: all events should be detected as duplicates"
        );
        Ok(())
    }

    #[sinex_test]
    async fn occurrence_filter_new_data_passes_old_filtered() -> TestResult<()> {
        let mut parser = SpotifyHistoryParser;

        // Seed filter with original export.
        let first = parser
            .parse_record(record_for(SAMPLE_EXPORT.as_bytes()), &test_ctx())
            .await
            .unwrap();
        let mut filter = OccurrenceFilter::empty();
        for intent in &first {
            if let Some(ref key) = intent.occurrence_key {
                filter.insert(occurrence_key_string(key));
            }
        }

        // Parse a different export: one new track, one overlap.
        // The duplicate row must match the original SAMPLE_EXPORT's first
        // entry on all 5 occurrence-key fields (track_uri, track_name,
        // artist_name, started_at, played_ms) — the always-emit-5-fields
        // shape makes this explicit rather than implicit on URI presence.
        let mixed = r#"[{
            "ts": "2024-01-15T08:00:00Z",
            "ms_played": 240000,
            "master_metadata_track_name": "Aqualung",
            "master_metadata_album_artist_name": "Jethro Tull",
            "spotify_track_uri": "spotify:track:abc123",
            "shuffle": false, "skipped": false, "offline": false, "incognito_mode": false
        },
        {
            "ts": "2024-01-16T10:30:00Z",
            "ms_played": 180000,
            "master_metadata_track_name": "New Song",
            "master_metadata_album_artist_name": "New Artist",
            "spotify_track_uri": "spotify:track:new999",
            "shuffle": false, "skipped": false, "offline": false, "incognito_mode": false
        }]"#;
        let second = parser
            .parse_record(record_for(mixed.as_bytes()), &test_ctx())
            .await
            .unwrap();

        let mut dup_count = 0;
        let mut new_count = 0;
        for intent in &second {
            let key_str = occurrence_key_string(intent.occurrence_key.as_ref().unwrap());
            if filter.contains(&key_str) {
                dup_count += 1;
            } else {
                filter.insert(key_str);
                new_count += 1;
            }
        }
        assert_eq!(dup_count, 1, "abc123 should be a duplicate");
        assert_eq!(new_count, 1, "new999 should be new");
        assert_eq!(filter.len(), 3, "filter now has 3 distinct keys");
        Ok(())
    }
}
