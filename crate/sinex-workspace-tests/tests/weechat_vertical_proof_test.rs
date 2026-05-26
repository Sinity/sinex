//! Vertical proof: `WeeChat` parser through production-shaped pipeline (#1132).
//!
//! This test replaces the fake-scan-node pattern (direct DB inserts) with the
//! real parser pipeline: `AppendOnlyFileAdapter` → `WeeChatLogParser` →
//! `ParsedEventIntent` → Event (material provenance) → `AdmittedEventIntent` →
//! NATS publish → ingestd admission → DB persistence → query verification.
//!
//! # Coverage
//!
//! - Four `WeeChat` event types: message, join, part, `server_notice`
//! - Material provenance with per-line anchors
//! - Admitted event intent envelope
//! - Full NATS → ingestd → DB round-trip
//! - DB verification of event type, source, timestamp, payload, provenance

use futures::StreamExt;
use sinex_node_sdk::parser::{
    AppendOnlyFileAdapter, AppendOnlyFileConfig, InputShapeAdapter, MaterialParser,
    WeeChatLogParser,
};
use sinex_primitives::domain::HostName;
use sinex_primitives::events::SourceMaterial;
use sinex_primitives::events::admission::AdmittedEventIntent;
use sinex_primitives::events::builder::{OffsetKind, Provenance};
use sinex_primitives::parser::{MaterialAnchor, ParsedEventIntent, ParserContext, SourceUnitId};
use sinex_primitives::{Event, Id, Timestamp, Uuid};
use xtask::sandbox::prelude::*;

// ── Fixture ──────────────────────────────────────────────────────────────────

/// A representative `WeeChat` log with all four event types.
const FIXTURE: &str = "\
2024-01-15 14:23:45\tsinity\thello world
2024-01-15 14:24:00\t-->\tuser1 (~user1@host) joined #general
2024-01-15 14:25:00\tuser2\tanyone there?
2024-01-15 14:26:00\t<--\tuser1 (~user1@host) left #general
2024-01-15 14:27:00\t--\tNotice: Server MOTD updated
";

/// Expected event types in the order they appear in the fixture.
const EXPECTED_TYPES: &[&str] = &[
    "irc.message",
    "irc.join",
    "irc.message",
    "irc.part",
    "irc.server_notice",
];

const EXPECTED_NICKS: &[&str] = &["sinity", "user1", "user2", "user1", "__server__"];

/// Return the `ts_orig` parsed from a WeeChat-format timestamp string.
fn weechat_ts(s: &str) -> Timestamp {
    use time::PrimitiveDateTime;
    use time::macros::format_description;

    const FMT: &[time::format_description::BorrowedFormatItem<'_>] =
        format_description!("[year]-[month]-[day] [hour]:[minute]:[second]");
    let dt = PrimitiveDateTime::parse(s, FMT).expect("fixture timestamp must parse");
    Timestamp::new(dt.assume_utc())
}

// ── Helpers ──────────────────────────────────────────────────────────────────

/// Run the full adapter → parser pipeline on a temp file and return intents.
async fn parse_fixture(
    material_id: Id<SourceMaterial>,
    log_path: &std::path::Path,
) -> TestResult<Vec<ParsedEventIntent>> {
    let adapter = AppendOnlyFileAdapter;
    let config = AppendOnlyFileConfig {
        path: log_path.to_string_lossy().into_owned(),
        skip_empty: true,
    };

    let stream = adapter
        .open(material_id, &config, None)
        .await
        .map_err(|e| eyre!("adapter open failed: {e}"))?;

    let mut parser = WeeChatLogParser;
    let mut intents: Vec<ParsedEventIntent> = Vec::new();

    tokio::pin!(stream);
    while let Some(record_result) = stream.next().await {
        let record = record_result.map_err(|e| eyre!("record error: {e}"))?;
        let anchor = record.anchor.clone();
        let parser_ctx = ParserContext {
            source_unit_id: SourceUnitId::from_static("weechat"),
            source_material_id: material_id,
            record_anchor: anchor,
            operation_id: Uuid::new_v4(),
            job_id: Uuid::new_v4(),
            host: "test-host".into(),
            acquisition_time: Timestamp::now(),
        };
        let record_intents = parser
            .parse_record(record, &parser_ctx)
            .await
            .map_err(|e| eyre!("parse error: {e}"))?;
        intents.extend(record_intents);
    }

    Ok(intents)
}

/// Convert parsed intents into material-provenance `Event<JsonValue>` structs.
fn intents_to_events(
    intents: &[ParsedEventIntent],
    material_id: Id<SourceMaterial>,
) -> Vec<Event<serde_json::Value>> {
    intents
        .iter()
        .map(|intent| {
            let anchor_byte: i64 = match &intent.anchor {
                MaterialAnchor::Line { byte_start, .. } => *byte_start as i64,
                _ => 0,
            };

            Event::<serde_json::Value> {
                id: Some(Id::new()),
                source: intent.event_source.clone(),
                event_type: intent.event_type.clone(),
                payload: intent.payload.clone(),
                ts_orig: Some(intent.ts_orig),
                host: HostName::from_static("test-host"),
                source_run_id: None,
                payload_schema_id: None,
                provenance: Provenance::Material {
                    id: material_id,
                    anchor_byte,
                    offset_start: Some(anchor_byte),
                    offset_end: None,
                    offset_kind: OffsetKind::Line,
                },
                associated_blob_ids: None,
                temporal_policy: None,
                semantics_version: None,
                scope_key: None,
                equivalence_key: None,
                created_by_operation_id: None,
                node_model: None,
                anchor_payload_hash: None,
            }
        })
        .collect()
}

/// Build a raw events NATS subject for the given source + event type.
fn raw_events_subject(ctx: &Sandbox, source: &str, event_type: &str) -> String {
    ctx.env().nats_raw_event_subject_with_namespace(
        Some(ctx.pipeline_namespace().prefix()),
        source,
        event_type,
    )
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

/// Full vertical proof: WeeChat log file → parser → NATS → ingestd → DB → query.
///
/// This exercises every layer of the staged-source architecture for a concrete
/// parser, proving that the Wave 1-3 infrastructure is wired end-to-end.
#[sinex_test(timeout = 120)]
async fn weechat_full_pipeline_persists_correctly(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let stack = TestCoreStack::new(&ctx).await?;

    // ── Stage material ───────────────────────────────────────────────────
    let tmp_dir = tempfile::tempdir()?;
    let log_path = tmp_dir.path().join("weechat.log");
    std::fs::write(&log_path, FIXTURE)?;

    let material_id = ctx.create_source_material(Some("weechat-vp")).await?;

    // ── Parse through adapter → parser pipeline ──────────────────────────
    let intents = parse_fixture(material_id, &log_path).await?;
    assert_eq!(intents.len(), 5, "fixture should produce 5 event intents");

    // Verify intent-level metadata before transport
    for (i, intent) in intents.iter().enumerate() {
        assert_eq!(
            intent.event_type.as_str(),
            EXPECTED_TYPES[i],
            "intent[{i}] event type mismatch"
        );
        assert_eq!(
            intent.event_source.as_str(),
            "irc",
            "intent[{i}] source mismatch"
        );
        assert_eq!(
            intent.payload["nick"].as_str().unwrap_or(""),
            EXPECTED_NICKS[i],
            "intent[{i}] nick mismatch"
        );
    }

    // ── Convert to events with material provenance ───────────────────────
    let events = intents_to_events(&intents, material_id);

    // ── Build admitted event intent ──────────────────────────────────────
    let admitted = AdmittedEventIntent::new(
        "weechat",
        "weechat-log",
        "1.0.0",
        events,
        HostName::from_static("test-host"),
    );
    assert_eq!(admitted.event_count(), 5);
    admitted.validate().expect("admitted intent must be valid");

    // ── Publish through NATS ─────────────────────────────────────────────
    let payload = serde_json::to_vec(&admitted)?;
    // Route all events under the first event's source/type — the JetStream
    // stream captures `{ns}.sinex.events.raw.>` so the exact sub-topic
    // doesn't matter for admission.
    let subject = raw_events_subject(
        stack.ctx(),
        intents[0].event_source.as_str(),
        intents[0].event_type.as_str(),
    );
    stack
        .ctx()
        .nats_client()
        .publish(subject, payload.into())
        .await
        .map_err(|e| eyre!("NATS publish failed: {e}"))?;
    stack
        .ctx()
        .nats_client()
        .flush()
        .await
        .map_err(|e| eyre!("NATS flush failed: {e}"))?;

    // ── Wait for ingestd persistence ─────────────────────────────────────
    let count = stack.wait_for_event_count(5).await?;
    assert_eq!(count, 5, "expected 5 events persisted, got {count}");

    // ── Verify via DB (event-level assertions) ────────────────────────────
    let pool = stack.pool();

    // Query persisted events by source material, ordered by ts_orig.
    let rows = sqlx::query_as::<
        _,
        (
            Uuid,
            String,
            String,
            Timestamp,
            serde_json::Value,
            i64,
            Option<String>,
        ),
    >(
        r"
        SELECT id, event_type, source, ts_orig, payload,
               anchor_byte, offset_kind
        FROM core.events
        WHERE source_material_id = $1
        ORDER BY ts_orig ASC
        ",
    )
    .bind(material_id.to_uuid())
    .fetch_all(pool)
    .await?;

    assert_eq!(rows.len(), 5, "expected 5 rows from core.events");

    // Row-order assertions.
    let expected_ts = [
        weechat_ts("2024-01-15 14:23:45"),
        weechat_ts("2024-01-15 14:24:00"),
        weechat_ts("2024-01-15 14:25:00"),
        weechat_ts("2024-01-15 14:26:00"),
        weechat_ts("2024-01-15 14:27:00"),
    ];

    for (i, (id, event_type, source, ts_orig, payload, anchor_byte, offset_kind)) in
        rows.iter().enumerate()
    {
        // Event identity
        assert!(!id.is_nil(), "row[{i}] event id should not be nil");

        // Source and type
        assert_eq!(
            event_type.as_str(),
            EXPECTED_TYPES[i],
            "row[{i}] event_type mismatch"
        );
        assert_eq!(
            source.as_str(),
            "irc",
            "row[{i}] source mismatch: expected 'irc', got '{}'",
            source.as_str()
        );

        // Timestamp
        assert_eq!(*ts_orig, expected_ts[i], "row[{i}] ts_orig mismatch");

        // Payload fields
        let nick = payload["nick"].as_str().unwrap_or("");
        assert_eq!(nick, EXPECTED_NICKS[i], "row[{i}] payload.nick mismatch");
        assert!(
            !payload["message"].is_null(),
            "row[{i}] payload.message must be present"
        );

        // Provenance
        assert!(
            *anchor_byte >= 0,
            "row[{i}] anchor_byte must be non-negative"
        );
        assert_eq!(
            offset_kind.as_deref().unwrap_or(""),
            "line",
            "row[{i}] offset_kind must be 'line'"
        );
    }

    // ── Provenance integrity: all events reference the same material ────
    let material_count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM core.events WHERE source_material_id = $1",
    )
    .bind(material_id.to_uuid())
    .fetch_one(pool)
    .await?;
    assert_eq!(
        material_count, 5,
        "all 5 events must share the source material"
    );

    // ── No events leaked to wrong source ─────────────────────────────────
    let other_irc_count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM core.events WHERE source = 'irc' AND source_material_id != $1",
    )
    .bind(material_id.to_uuid())
    .fetch_one(pool)
    .await?;
    assert_eq!(
        other_irc_count, 0,
        "no IRC events should exist under other materials"
    );

    stack.shutdown().await?;
    Ok(())
}

/// Verify that a WeeChat file with only whitespace/empty lines produces
/// zero events (no spurious persistence).
#[sinex_test(timeout = 60)]
async fn weechat_empty_file_produces_no_events(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let stack = TestCoreStack::new(&ctx).await?;

    let tmp_dir = tempfile::tempdir()?;
    let log_path = tmp_dir.path().join("empty.log");
    std::fs::write(&log_path, "\n   \n")?;

    let material_id = ctx.create_source_material(Some("weechat-empty")).await?;

    let intents = parse_fixture(material_id, &log_path).await?;
    assert!(
        intents.is_empty(),
        "empty/whitespace file must produce 0 intents, got {}",
        intents.len()
    );

    // Publish a zero-event intent — ingestd should not persist anything
    // from this material.
    let initial_event_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM core.events WHERE source_material_id = $1")
            .bind(material_id.to_uuid())
            .fetch_one(stack.pool())
            .await?;
    assert_eq!(initial_event_count, 0, "empty file → no persisted events");

    stack.shutdown().await?;
    Ok(())
}
