use super::*;
use crate::runtime::parser::records_from_journal_lines;
use sinex_primitives::events::SourceMaterial;
use sinex_primitives::ids::Id;
use sinex_primitives::parser::MaterialAnchor;
use sinex_primitives::primitives::Uuid;
use xtask::sandbox::prelude::*;

fn make_ctx(mid: Id<SourceMaterial>) -> ParserContext {
    ParserContext {
        source_id: SourceId::from_static("system.journald"),
        source_material_id: mid,
        record_anchor: MaterialAnchor::Line {
            byte_start: 0,
            line: 1,
        },
        operation_id: Uuid::new_v4(),
        job_id: Uuid::new_v4(),
        host: "test-host".into(),
        acquisition_time: Timestamp::now(),
    }
}

#[sinex_test]
async fn test_journald_parser_entry_written() -> TestResult<()> {
    let mid = Id::<SourceMaterial>::new();
    let tok = ["ghp_", "0123456789abcdef0123456789abcdef0123"].concat();
    let line = format!(
        r#"{{"__CURSOR":"s=abc;i=1","__REALTIME_TIMESTAMP":"1700000000000000","MESSAGE":"export GITHUB_TOKEN={tok}","_CMDLINE":"curl -H token={tok}","_HOSTNAME":"host1","PRIORITY":"6"}}"#
    );
    let records = records_from_journal_lines(mid, &[line.as_str()]);
    let record = records[0].as_ref().unwrap().clone();

    let mut parser = JournaldParser;
    let ctx = make_ctx(mid);
    let intents = parser.parse_record(record, &ctx).await.unwrap();

    assert_eq!(intents.len(), 1);
    assert_eq!(intents[0].event_type.as_str(), "entry.written");
    assert_eq!(intents[0].event_source.as_str(), "journald");
    assert_eq!(
        intents[0].payload["message"],
        format!("export GITHUB_TOKEN={tok}")
    );
    assert_eq!(
        intents[0].payload["cmdline"],
        format!("curl -H token={tok}")
    );
    Ok(())
}

#[sinex_test]
async fn test_journald_parser_filters_empty_lines() -> TestResult<()> {
    let mid = Id::<SourceMaterial>::new();
    let line = "";
    let records = records_from_journal_lines(mid, &[line]);

    assert!(
        records.is_empty(),
        "journal helper should mirror live stream filtering for empty lines"
    );
    Ok(())
}

#[sinex_test]
async fn test_journald_parser_suppresses_sinexd_confirmation_feedback() -> TestResult<()> {
    let mid = Id::<SourceMaterial>::new();
    let lines = [
        r#"{"__CURSOR":"s=feedback;i=1","__REALTIME_TIMESTAMP":"1700000000000000","_SYSTEMD_UNIT":"sinexd.service","SYSLOG_IDENTIFIER":"sinexd","MESSAGE":"Late confirmation arrived after provisional timeout; accepting during grace period"}"#,
        r#"{"__CURSOR":"s=feedback;i=2","__REALTIME_TIMESTAMP":"1700000000000001","_SYSTEMD_UNIT":"sinexd.service","SYSLOG_IDENTIFIER":"sinexd","MESSAGE":"Late confirmations accepted after timeout; aggregated during grace period metric=runtime.confirmation_late_total"}"#,
    ];
    let records = records_from_journal_lines(mid, &lines);
    let mut parser = JournaldParser;
    let ctx = make_ctx(mid);

    for record in records {
        let intents = parser.parse_record(record?, &ctx).await?;
        assert!(
            intents.is_empty(),
            "confirmation feedback journal entries should not create journald.entry.written events"
        );
    }

    Ok(())
}

#[sinex_test]
async fn test_journald_parser_drops_all_sinexd_self_logs() -> TestResult<()> {
    // fresh-rebuild B1: ALL of sinexd's own journald output is dropped at parse
    // (not just the old confirmation-feedback special case) — sinex no longer
    // re-ingests its own logs as activity. An ordinary sinexd log line is dropped
    // exactly like the confirmation-feedback ones.
    let mid = Id::<SourceMaterial>::new();
    let line = r#"{"__CURSOR":"s=ordinary;i=1","__REALTIME_TIMESTAMP":"1700000000000000","_SYSTEMD_UNIT":"sinexd.service","SYSLOG_IDENTIFIER":"sinexd","MESSAGE":"source catalog exported"}"#;
    let records = records_from_journal_lines(mid, &[line]);
    let record = records[0].as_ref().unwrap().clone();

    let mut parser = JournaldParser;
    let ctx = make_ctx(mid);
    let intents = parser.parse_record(record, &ctx).await?;

    assert!(
        intents.is_empty(),
        "sinexd's own journald entries must not create activity events (self-capture relic removed)"
    );
    Ok(())
}

#[sinex_test]
async fn test_journald_parser_keeps_non_sinexd_logs() -> TestResult<()> {
    // Real external host chatter (a different unit) is still captured — B1 only
    // drops sinexd's OWN entries.
    let mid = Id::<SourceMaterial>::new();
    let line = r#"{"__CURSOR":"s=ordinary;i=1","__REALTIME_TIMESTAMP":"1700000000000000","_SYSTEMD_UNIT":"nginx.service","SYSLOG_IDENTIFIER":"nginx","MESSAGE":"served request"}"#;
    let records = records_from_journal_lines(mid, &[line]);
    let record = records[0].as_ref().unwrap().clone();

    let mut parser = JournaldParser;
    let ctx = make_ctx(mid);
    let intents = parser.parse_record(record, &ctx).await?;

    assert_eq!(intents.len(), 1);
    assert_eq!(intents[0].event_type.as_str(), "entry.written");
    assert_eq!(intents[0].payload["message"], "served request");
    Ok(())
}

#[sinex_test]
async fn test_journald_parser_decodes_array_valued_message() -> TestResult<()> {
    // journalctl -o json emits MESSAGE as a JSON array of byte values whenever
    // the raw bytes aren't printable UTF-8 -- e.g. any ANSI-colored log line.
    // This exact payload is a real entry captured from this host's journal
    // (aw-server, sinnix-prime, 2026-08-10): `\x1b[33mWARN\x1b[0m` colored text.
    let mid = Id::<SourceMaterial>::new();
    let line = r#"{"__CURSOR":"s=78f85d790cdd49ff9097873af2ab963d;i=4b92dca","__REALTIME_TIMESTAMP":"1786383523889206","_SYSTEMD_UNIT":"user@1000.service","SYSLOG_IDENTIFIER":"aw-server","MESSAGE":[91,50,48,50,54,45,48,56,45,49,48,32,49,57,58,51,56,58,52,51,93,91,27,91,51,51,109,87,65,82,78,27,91,48,109,93,91,97,119,95,115,101,114,118,101,114,58,58,101,110,100,112,111,105,110,116,115,58,58,98,117,99,107,101,116,93,58,32,84,97,107,105,110,103,32,100,97,116,97,115,116,111,114,101,32,108,111,99,107,32,102,97,105,108,101,100,44,32,114,101,116,117,114,110,105,110,103,32,53,48,52,58,32,112,111,105,115,111,110,101,100,32,108,111,99,107,58,32,97,110,111,116,104,101,114,32,116,97,115,107,32,102,97,105,108,101,100,32,105,110,115,105,100,101],"PRIORITY":"6"}"#;
    let records = records_from_journal_lines(mid, &[line]);
    let record = records[0].as_ref().unwrap().clone();

    let mut parser = JournaldParser;
    let ctx = make_ctx(mid);
    let intents = parser.parse_record(record, &ctx).await?;

    assert_eq!(intents.len(), 1);
    let message = intents[0].payload["message"]
        .as_str()
        .expect("message must be a string");
    assert!(
        !message.is_empty(),
        "array-valued MESSAGE must decode to real text, not fall through to empty"
    );
    assert!(
        message.contains("Taking datastore lock failed"),
        "decoded message should contain the real log text, got: {message:?}"
    );
    assert!(
        message.contains("WARN"),
        "decoded message should preserve the surrounding ANSI-escaped text, got: {message:?}"
    );
    Ok(())
}

#[sinex_test]
async fn test_journald_parser_decodes_array_valued_generic_fields() -> TestResult<()> {
    // The same array-of-bytes encoding applies to any journald field, not just
    // MESSAGE -- the generic field map must decode it too instead of silently
    // dropping the field.
    let mid = Id::<SourceMaterial>::new();
    let line = r#"{"__CURSOR":"s=abc;i=1","__REALTIME_TIMESTAMP":"1700000000000000","MESSAGE":"plain text","SYSLOG_IDENTIFIER":"nginx","CUSTOM_FIELD":[104,101,108,108,111]}"#;
    let records = records_from_journal_lines(mid, &[line]);
    let record = records[0].as_ref().unwrap().clone();

    let mut parser = JournaldParser;
    let ctx = make_ctx(mid);
    let intents = parser.parse_record(record, &ctx).await?;

    assert_eq!(intents.len(), 1);
    let fields = intents[0].payload["fields"]
        .as_object()
        .expect("fields must be an object");
    assert_eq!(
        fields.get("CUSTOM_FIELD").and_then(|v| v.as_str()),
        Some("hello"),
        "array-valued generic field must decode, not be dropped"
    );
    Ok(())
}

#[sinex_test]
async fn test_journald_parser_preserves_microsecond_precision() -> TestResult<()> {
    // __REALTIME_TIMESTAMP carries microsecond precision and TimingConfidence::Intrinsic
    // claims full precision -- truncating to whole seconds silently discards it.
    let mid = Id::<SourceMaterial>::new();
    // 1700000000.123456 -- non-zero microsecond component.
    let line = r#"{"__CURSOR":"s=abc;i=1","__REALTIME_TIMESTAMP":"1700000000123456","MESSAGE":"precise entry"}"#;
    let records = records_from_journal_lines(mid, &[line]);
    let record = records[0].as_ref().unwrap().clone();

    let mut parser = JournaldParser;
    let ctx = make_ctx(mid);
    let intents = parser.parse_record(record, &ctx).await?;

    assert_eq!(intents.len(), 1);
    let ts_orig = intents[0].ts_orig;
    assert_eq!(
        ts_orig.inner().microsecond(),
        123_456,
        "ts_orig must preserve the microsecond component instead of truncating to whole seconds"
    );
    Ok(())
}
