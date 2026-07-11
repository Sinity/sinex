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
