use super::*;

use sinex_primitives::events::SourceMaterial;
use sinex_primitives::ids::Id;
use sinex_primitives::parser::MaterialAnchor;
use xtask::sandbox::prelude::*;

fn test_ctx(mid: Id<SourceMaterial>) -> ParserContext {
    ParserContext {
        source_id: SourceId::from_static("desktop.clipboard"),
        source_material_id: mid,
        record_anchor: MaterialAnchor::ByteRange { start: 0, len: 0 },
        operation_id: sinex_primitives::Uuid::new_v4(),
        job_id: sinex_primitives::Uuid::new_v4(),
        host: "test-host".into(),
        acquisition_time: Timestamp::now(),
    }
}

/// sinex-gz75: `MaterialParser::Config` (`ClipboardParserConfig`, with a
/// real `max_preview_length` field) is never threaded to `parse_record` --
/// the trait's `parse_record(&mut self, record, ctx)` has no config
/// parameter at all, and `ClipboardParser` is a unit struct with no field to
/// hold one even if it did. The preview length is hardcoded to 100
/// (`raw_text.chars().take(100)`) regardless of what `ClipboardParserConfig`
/// says. This proves the symptom directly: a config asking for a 20-char
/// preview has zero effect on the only reachable entry point.
#[sinex_test]
#[ignore = "sinex-gz75 open: ClipboardParserConfig::max_preview_length is \
            deserializable but never threaded to parse_record -- the \
            preview is always hardcoded to 100 chars regardless of config"]
async fn preview_length_ignores_configured_max_preview_length() -> TestResult<()> {
    let configured = ClipboardParserConfig {
        max_preview_length: 20,
    };
    assert_ne!(
        configured.max_preview_length, 100,
        "sanity: this fixture must differ from the hardcoded default to be a real test"
    );

    let mid = Id::<SourceMaterial>::new();
    let mut parser = ClipboardParser;
    let record = sinex_primitives::parser::SourceRecord {
        material_id: mid,
        anchor: MaterialAnchor::ByteRange { start: 0, len: 150 },
        bytes: "x".repeat(150).into_bytes(),
        logical_path: None,
        source_ts_hint: None,
        metadata: serde_json::Value::Null,
    };

    let intents = parser.parse_record(record, &test_ctx(mid)).await?;
    assert_eq!(intents.len(), 1);
    let preview = intents[0].payload["content_preview"]
        .as_str()
        .expect("content_preview must be a string");

    assert_eq!(
        preview.chars().count(),
        configured.max_preview_length,
        "the emitted preview must honor ClipboardParserConfig::max_preview_length (20), \
         not the hardcoded 100 -- there is currently no code path that lets it"
    );
    Ok(())
}
