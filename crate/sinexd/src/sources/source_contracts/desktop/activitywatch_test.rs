use super::*;
use sinex_primitives::events::SourceMaterial;
use sinex_primitives::ids::Id;
use sinex_primitives::parser::{MaterialAnchor, SourceRecord};
use sinex_primitives::primitives::Uuid;
use xtask::sandbox::prelude::*;

fn parser_context() -> ParserContext {
    ParserContext {
        source_id: SourceId::from_static("desktop.activitywatch"),
        source_material_id: Id::<SourceMaterial>::from_uuid(Uuid::now_v7()),
        record_anchor: MaterialAnchor::SqliteRow {
            table: "events".to_string(),
            rowid: 1,
        },
        operation_id: Uuid::now_v7(),
        job_id: Uuid::now_v7(),
        host: "test-host".to_string(),
        acquisition_time: Timestamp::now(),
    }
}

fn aw_row(bucket_id: &str, started_at: serde_json::Value, data: serde_json::Value) -> SourceRecord {
    let row = serde_json::json!({
        "bucket_id": bucket_id,
        "started_at": started_at,
        "duration": 5.0,
        "data": data,
    });
    SourceRecord {
        material_id: Id::<SourceMaterial>::from_uuid(Uuid::now_v7()),
        anchor: MaterialAnchor::SqliteRow {
            table: "events".to_string(),
            rowid: 1,
        },
        bytes: serde_json::to_vec(&row).expect("row serializes"),
        logical_path: None,
        source_ts_hint: None,
        metadata: serde_json::Value::Null,
    }
}

/// sinex-dmz9 regression: a row with a valid, parseable `started_at` keeps
/// TimingEvidence::Intrinsic and a matching ts_orig -- the fix must not
/// change behavior on the well-formed path.
#[sinex_test]
async fn valid_started_at_keeps_intrinsic_timing_evidence() -> TestResult<()> {
    let mut parser = ActivityWatchParser;
    let ts_nanos = 1_700_000_000_000_000_000i64;
    let record = aw_row(
        "aw-watcher-window_host",
        serde_json::json!(ts_nanos),
        serde_json::json!({"app": "kitty", "title": "shell"}),
    );
    let ctx = parser_context();
    let intents = parser.parse_record(record, &ctx).await?;

    assert_eq!(intents.len(), 1);
    let intent = &intents[0];
    let expected_ts = Timestamp::from_unix_timestamp_nanos(i128::from(ts_nanos))
        .ok_or_else(|| color_eyre::eyre::eyre!("valid timestamp"))?;
    assert_eq!(intent.ts_orig, expected_ts);
    assert_eq!(
        intent.timing,
        TimingEvidence::Intrinsic {
            field: "started_at".into(),
            confidence: TimingConfidence::Intrinsic,
        }
    );
    Ok(())
}

/// sinex-dmz9: a row with a MISSING `started_at` must not fabricate a
/// wall-clock ts_orig that gets trusted downstream. TimingEvidence::Atemporal
/// signals intent_to_event_with_anchor (adapter_source.rs) to leave the
/// persisted event's real ts_orig unresolved for material-tier derivation.
#[sinex_test]
async fn missing_started_at_yields_atemporal_timing_evidence() -> TestResult<()> {
    let mut parser = ActivityWatchParser;
    let record = aw_row(
        "aw-watcher-window_host",
        serde_json::Value::Null,
        serde_json::json!({"app": "kitty", "title": "shell"}),
    );
    let ctx = parser_context();
    let intents = parser.parse_record(record, &ctx).await?;

    assert_eq!(intents.len(), 1);
    assert_eq!(intents[0].timing, TimingEvidence::Atemporal);
    Ok(())
}

/// sinex-dmz9: an unparseable (wrong-shape) `started_at` value must be
/// treated identically to a missing one -- Atemporal, not a fabricated
/// wall-clock timestamp trusted downstream.
#[sinex_test]
async fn unparseable_started_at_yields_atemporal_timing_evidence() -> TestResult<()> {
    let mut parser = ActivityWatchParser;
    let record = aw_row(
        "aw-watcher-afk_host",
        serde_json::json!({"not": "a timestamp"}),
        serde_json::json!({"status": "afk"}),
    );
    let ctx = parser_context();
    let intents = parser.parse_record(record, &ctx).await?;

    assert_eq!(intents.len(), 1);
    assert_eq!(intents[0].timing, TimingEvidence::Atemporal);
    Ok(())
}

/// The occurrence key must still be built deterministically even when
/// started_at is missing -- ts_orig's wall-clock placeholder still feeds
/// the key (a separate, unrelated concern from the trusted-timestamp fix
/// above); this test pins that the placeholder path doesn't panic or
/// produce an empty key.
#[sinex_test]
async fn missing_started_at_still_produces_a_nonempty_occurrence_key() -> TestResult<()> {
    let mut parser = ActivityWatchParser;
    let record = aw_row(
        "aw-watcher-web_firefox",
        serde_json::Value::Null,
        serde_json::json!({"url": "https://example.com", "title": "Example"}),
    );
    let ctx = parser_context();
    let intents = parser.parse_record(record, &ctx).await?;

    assert_eq!(intents.len(), 1);
    let key = intents[0]
        .occurrence_key
        .as_ref()
        .expect("occurrence key is always set by this parser");
    assert!(!key.fields.is_empty());
    Ok(())
}
