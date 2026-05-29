//! AC-verification tests for the Spotify Extended Streaming History parser (#1092).
//!
//! Exercises `SpotifyHistoryParser` via `MaterialParser::parse_record` with
//! small synthetic fixtures to close each acceptance criterion from #1092.

use sinex_primitives::parser::MaterialParser;
use sinex_primitives::{
    Uuid,
    ids::Id,
    parser::{MaterialAnchor, ParserContext, SourceRecord, SourceUnitId},
    temporal::Timestamp,
};
use sinexd::sources::sources::music::SpotifyHistoryParser;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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

// Minimal valid row — only required fields.
const MINIMAL_ROW: &str = r#"[{
    "ts": "2024-03-01T12:00:00Z",
    "ms_played": 200000,
    "spotify_track_uri": "spotify:track:min001",
    "master_metadata_track_name": "Some Track",
    "master_metadata_album_artist_name": "Some Artist",
    "shuffle": false,
    "skipped": false,
    "offline": false,
    "incognito_mode": false
}]"#;

// ---------------------------------------------------------------------------
// AC: JSON fixture parses into confirmed track.played events via Bus-First path
// ---------------------------------------------------------------------------

/// Verifies that a representative JSON export array parses into intents with
/// the correct source/type — the minimal contract for the Bus-First path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ac_json_export_parses_to_track_played_events() {
    let export = r#"[
      {
        "ts": "2024-06-01T10:00:00Z",
        "ms_played": 240000,
        "platform": "Linux",
        "conn_country": "PL",
        "master_metadata_track_name": "Living In The Past",
        "master_metadata_album_artist_name": "Jethro Tull",
        "master_metadata_album_album_name": "Living In The Past",
        "spotify_track_uri": "spotify:track:AAQQ1",
        "reason_start": "trackdone",
        "reason_end": "trackdone",
        "shuffle": false,
        "skipped": false,
        "offline": false,
        "incognito_mode": false
      },
      {
        "ts": "2024-06-01T10:04:10Z",
        "ms_played": 185000,
        "platform": "Linux",
        "conn_country": "PL",
        "master_metadata_track_name": "Bouree",
        "master_metadata_album_artist_name": "Jethro Tull",
        "spotify_track_uri": "spotify:track:BBQQ2",
        "shuffle": false,
        "skipped": false,
        "offline": false,
        "incognito_mode": false
      }
    ]"#;

    let mut parser = SpotifyHistoryParser;
    let intents = parser
        .parse_record(record_for(export.as_bytes()), &test_ctx())
        .await
        .unwrap();

    assert_eq!(intents.len(), 2, "expected 2 intents from 2-entry export");
    for intent in &intents {
        assert_eq!(intent.event_source.as_str(), "spotify");
        assert_eq!(intent.event_type.as_str(), "track.played");
        assert!(
            intent.occurrence_key.is_some(),
            "occurrence_key must be set"
        );
        assert!(
            intent.ts_orig.inner().year() == 2024,
            "ts_orig should reflect the export row timestamp"
        );
    }
}

// ---------------------------------------------------------------------------
// AC: Duplicate/overlapping snapshots do not double-publish the same playback
// ---------------------------------------------------------------------------

/// Parsing the same export twice (simulating overlapping snapshots) yields the
/// same `occurrence_key` for each logical playback — dedup at the DB layer relies
/// on this key being stable across separate `parse_record` calls.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ac_duplicate_snapshots_produce_identical_occurrence_keys() {
    let mut parser = SpotifyHistoryParser;

    // Parse the same bytes twice (as separate SourceRecord calls, different material_id).
    let intents_a = parser
        .parse_record(record_for(MINIMAL_ROW.as_bytes()), &test_ctx())
        .await
        .unwrap();
    let intents_b = parser
        .parse_record(record_for(MINIMAL_ROW.as_bytes()), &test_ctx())
        .await
        .unwrap();

    assert_eq!(intents_a.len(), 1);
    assert_eq!(intents_b.len(), 1);

    let key_a = intents_a[0].occurrence_key.as_ref().unwrap();
    let key_b = intents_b[0].occurrence_key.as_ref().unwrap();

    // Same logical playback → same key fields (identical content → dedup).
    assert_eq!(
        key_a.fields, key_b.fields,
        "duplicate snapshot entries must produce identical occurrence key fields"
    );
    assert_eq!(
        key_a.source_unit_id, key_b.source_unit_id,
        "source_unit_id component of occurrence key must be stable"
    );
}

/// When both URI-based and name-based rows encode the same conceptual playback
/// but only one has a URI, their occurrence keys differ — this is correct; the
/// dedup guarantee holds within a single snapshot form, not across mismatched exports.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ac_occurrence_key_uri_primary_name_fallback_differ() {
    let with_uri = r#"[{
        "ts": "2024-03-01T12:00:00Z",
        "ms_played": 200000,
        "spotify_track_uri": "spotify:track:XXXX",
        "master_metadata_track_name": "Track A",
        "master_metadata_album_artist_name": "Artist A",
        "shuffle": false, "skipped": false, "offline": false, "incognito_mode": false
    }]"#;
    let without_uri = r#"[{
        "ts": "2024-03-01T12:00:00Z",
        "ms_played": 200000,
        "master_metadata_track_name": "Track A",
        "master_metadata_album_artist_name": "Artist A",
        "shuffle": false, "skipped": false, "offline": false, "incognito_mode": false
    }]"#;

    let mut parser = SpotifyHistoryParser;

    let uri_intents = parser
        .parse_record(record_for(with_uri.as_bytes()), &test_ctx())
        .await
        .unwrap();
    let name_intents = parser
        .parse_record(record_for(without_uri.as_bytes()), &test_ctx())
        .await
        .unwrap();

    let uri_key = uri_intents[0].occurrence_key.as_ref().unwrap();
    let name_key = name_intents[0].occurrence_key.as_ref().unwrap();

    // URI path uses "track_uri" as first field.
    assert_eq!(uri_key.fields[0].0, "track_uri");
    // Name-fallback path uses "track_name" as first field.
    assert_eq!(name_key.fields[0].0, "track_name");
    assert_eq!(name_key.fields[1].0, "artist_name");
}

// ---------------------------------------------------------------------------
// AC: Skipped status records both provider boolean and local-threshold inference
// ---------------------------------------------------------------------------

/// Provider says skipped=false, but `played_ms` < 30s → inferred skipped=true.
/// Both flags must be surfaced independently.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ac_skipped_provider_and_inferred_both_preserved() {
    let rows = r#"[
      {
        "ts": "2024-03-01T09:00:00Z",
        "ms_played": 180000,
        "spotify_track_uri": "spotify:track:long001",
        "shuffle": false, "skipped": false, "offline": false, "incognito_mode": false
      },
      {
        "ts": "2024-03-01T09:03:00Z",
        "ms_played": 5000,
        "spotify_track_uri": "spotify:track:skip001",
        "shuffle": false, "skipped": false, "offline": false, "incognito_mode": false
      },
      {
        "ts": "2024-03-01T09:03:05Z",
        "ms_played": 10000,
        "spotify_track_uri": "spotify:track:skip002",
        "shuffle": false, "skipped": true, "offline": false, "incognito_mode": false
      }
    ]"#;

    let mut parser = SpotifyHistoryParser;
    let intents = parser
        .parse_record(record_for(rows.as_bytes()), &test_ctx())
        .await
        .unwrap();

    assert_eq!(intents.len(), 3);

    // Row 0: 180s played, provider not-skipped — both false.
    assert_eq!(intents[0].payload["skipped_provider"], false);
    assert_eq!(intents[0].payload["skipped_inferred"], false);

    // Row 1: 5s played (< 30s threshold), provider not-skipped — inferred true.
    assert_eq!(intents[1].payload["skipped_provider"], false);
    assert_eq!(intents[1].payload["skipped_inferred"], true);

    // Row 2: 10s played (< 30s threshold), provider true — both true.
    assert_eq!(intents[2].payload["skipped_provider"], true);
    assert_eq!(intents[2].payload["skipped_inferred"], true);
}

/// Boundary: exactly `30_000` ms is NOT inferred-skipped (threshold is < `30_000`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ac_skip_threshold_boundary_30s_not_skipped() {
    let exactly_30s = r#"[{
        "ts": "2024-03-01T12:00:00Z",
        "ms_played": 30000,
        "spotify_track_uri": "spotify:track:boundary",
        "shuffle": false, "skipped": false, "offline": false, "incognito_mode": false
    }]"#;

    let mut parser = SpotifyHistoryParser;
    let intents = parser
        .parse_record(record_for(exactly_30s.as_bytes()), &test_ctx())
        .await
        .unwrap();

    assert_eq!(
        intents[0].payload["skipped_inferred"], false,
        "exactly 30_000 ms must not be classified as inferred-skip (threshold is < 30_000)"
    );
}

// ---------------------------------------------------------------------------
// AC: Context URI + platform metadata preserved per admission/privacy policy
// ---------------------------------------------------------------------------

/// Platform, `conn_country`, `reason_start/end`, shuffle, offline, and `incognito_mode`
/// are all surfaced in the payload so admission policy can act on them.
/// `ip_addr` and `user_agent_decrypted` must NOT be carried.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ac_context_and_platform_metadata_preserved_ip_dropped() {
    let row = r#"[{
        "ts": "2024-03-01T12:00:00Z",
        "ms_played": 200000,
        "platform": "Android",
        "conn_country": "PL",
        "spotify_track_uri": "spotify:track:ctx001",
        "reason_start": "clickrow",
        "reason_end": "trackdone",
        "shuffle": true,
        "skipped": false,
        "offline": true,
        "incognito_mode": true,
        "ip_addr": "192.168.1.1",
        "user_agent_decrypted": "Spotify/8.9.0 Android/13"
    }]"#;

    let mut parser = SpotifyHistoryParser;
    let intents = parser
        .parse_record(record_for(row.as_bytes()), &test_ctx())
        .await
        .unwrap();

    let p = &intents[0].payload;

    // Context metadata that admission policy needs.
    assert_eq!(p["platform"], "Android");
    assert_eq!(p["conn_country"], "PL");
    assert_eq!(p["reason_start"], "clickrow");
    assert_eq!(p["reason_end"], "trackdone");
    assert_eq!(p["shuffle"], true);
    assert_eq!(p["offline"], true);
    assert_eq!(
        p["incognito_mode"], true,
        "incognito_mode must be preserved so admission policy can gate private listens"
    );

    // Privacy-sensitive fields must be absent.
    assert!(
        p.get("ip_addr").is_none(),
        "ip_addr is dropped at parse time, not passed to admission"
    );
    assert!(
        p.get("user_agent_decrypted").is_none(),
        "user_agent_decrypted is dropped at parse time"
    );
}

/// Episode/podcast rows are preserved with their own URI fields — `context_uri`
/// via `episode_uri` is part of the payload shape.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ac_episode_uri_preserved_in_payload() {
    let podcast_row = r#"[{
        "ts": "2024-03-01T14:00:00Z",
        "ms_played": 1800000,
        "episode_name": "Episode 42",
        "episode_show_name": "Some Podcast",
        "spotify_episode_uri": "spotify:episode:ep42",
        "platform": "Linux",
        "shuffle": false, "skipped": false, "offline": false, "incognito_mode": false
    }]"#;

    let mut parser = SpotifyHistoryParser;
    let intents = parser
        .parse_record(record_for(podcast_row.as_bytes()), &test_ctx())
        .await
        .unwrap();

    let p = &intents[0].payload;
    assert_eq!(p["episode_uri"], "spotify:episode:ep42");
    assert_eq!(p["episode_name"], "Episode 42");
    assert_eq!(p["show_name"], "Some Podcast");
}

// ---------------------------------------------------------------------------
// Edge cases that validate parser robustness
// ---------------------------------------------------------------------------

/// Empty export array produces zero intents without error.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_export_emits_no_intents() {
    let mut parser = SpotifyHistoryParser;
    let intents = parser
        .parse_record(record_for(b"[]"), &test_ctx())
        .await
        .unwrap();
    assert!(intents.is_empty());
}

/// Malformed JSON produces a parse error (not a panic).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn malformed_json_returns_error() {
    let mut parser = SpotifyHistoryParser;
    let result = parser
        .parse_record(record_for(b"{not an array}"), &test_ctx())
        .await;
    assert!(result.is_err(), "malformed JSON should return Err");
    let msg = result.unwrap_err().to_string();
    assert!(
        msg.contains("invalid Spotify export JSON array"),
        "error message should identify the parse failure: {msg}"
    );
}

/// Invalid ISO-8601 timestamp in a row propagates as a parse error.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invalid_timestamp_propagates_error() {
    let bad_ts = r#"[{
        "ts": "not-a-date",
        "ms_played": 100,
        "shuffle": false, "skipped": false, "offline": false, "incognito_mode": false
    }]"#;
    let mut parser = SpotifyHistoryParser;
    let result = parser
        .parse_record(record_for(bad_ts.as_bytes()), &test_ctx())
        .await;
    assert!(result.is_err());
    let msg = result.unwrap_err().to_string();
    assert!(msg.contains("invalid Spotify timestamp"), "got: {msg}");
}
