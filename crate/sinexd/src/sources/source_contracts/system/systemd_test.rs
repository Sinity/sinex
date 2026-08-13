use super::*;
use crate::runtime::parser::records_from_journal_lines;
use sinex_primitives::events::SourceMaterial;
use sinex_primitives::ids::Id;
use sinex_primitives::parser::MaterialAnchor;
use sinex_primitives::primitives::Uuid;
use xtask::sandbox::prelude::*;

fn make_ctx(mid: Id<SourceMaterial>) -> ParserContext {
    ParserContext {
        source_id: SourceId::from_static("system.systemd"),
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
async fn test_systemd_parser_unit_started() -> TestResult<()> {
    let mid = Id::<SourceMaterial>::new();
    let tok = ["ghp_", "0123456789abcdef0123456789abcdef0123"].concat();
    let line = format!(
        r#"{{"__CURSOR":"s=abc;i=2","__REALTIME_TIMESTAMP":"1700000001000000","_SYSTEMD_UNIT":"systemd.service","UNIT":"nginx.service","MESSAGE":"Started nginx.service with token {tok}.","PRIORITY":"6"}}"#
    );
    let records = records_from_journal_lines(mid, &[line.as_str()]);
    let record = records[0].as_ref().unwrap().clone();

    let mut parser = SystemdParser;
    let ctx = make_ctx(mid);
    let intents = parser.parse_record(record, &ctx).await.unwrap();

    assert_eq!(intents.len(), 1);
    assert_eq!(intents[0].event_type.as_str(), "unit.started");
    assert_eq!(intents[0].event_source.as_str(), "systemd");
    // `unit.started` is a fully structured event: it captures the unit
    // identity/state, not the raw journal MESSAGE (unlike unit.failed /
    // unit.reloaded, whose payloads keep `message` for variable diagnostic
    // detail). Not persisting the raw message also keeps secrets that appear
    // in journal lines — like the token below — out of the event store.
    assert_eq!(intents[0].payload["unit_name"], "nginx.service");
    assert!(
        intents[0].payload.get("message").is_none(),
        "unit.started must not carry the raw journal message (secret-bearing): {}",
        intents[0].payload
    );
    Ok(())
}

#[sinex_test]
async fn test_systemd_parser_skips_non_unit_records() -> TestResult<()> {
    let mid = Id::<SourceMaterial>::new();
    let line = r#"{"__CURSOR":"s=abc;i=3","MESSAGE":"generic log","PRIORITY":"6"}"#;
    let records = records_from_journal_lines(mid, &[line]);
    let record = records[0].as_ref().unwrap().clone();

    let mut parser = SystemdParser;
    let ctx = make_ctx(mid);
    let intents = parser.parse_record(record, &ctx).await.unwrap();

    assert_eq!(intents.len(), 0);
    Ok(())
}

#[sinex_test]
async fn test_systemd_parser_emits_only_genuine_manager_transitions() -> TestResult<()> {
    let mid = Id::<SourceMaterial>::new();
    let lines = [
        r#"{"__CURSOR":"s=abc;i=20","__REALTIME_TIMESTAMP":"1700000000123456","_SYSTEMD_UNIT":"systemd.service","UNIT":"alpha.service","MESSAGE":"Started alpha.service."}"#,
        r#"{"__CURSOR":"s=abc;i=21","__REALTIME_TIMESTAMP":"1700000001123456","_SYSTEMD_UNIT":"systemd.service","USER_UNIT":"beta.service","MESSAGE":[83,116,111,112,112,101,100,32,98,101,116,97,46,115,101,114,118,105,99,101,46]}"#,
        r#"{"__CURSOR":"s=abc;i=22","__REALTIME_TIMESTAMP":"1700000002123456","_SYSTEMD_UNIT":"alpha.service","MESSAGE":"worker handled request"}"#,
        r#"{"__CURSOR":"s=abc;i=23","__REALTIME_TIMESTAMP":"1700000003123456","_SYSTEMD_UNIT":"beta.service","MESSAGE":[119,111,114,107,101,114,32,108,111,103,32,101,110,116,114,121]}"#,
    ];
    let records = records_from_journal_lines(mid, &lines);
    let mut parser = SystemdParser;
    let ctx = make_ctx(mid);
    let mut intents = Vec::new();
    for record in records {
        intents.extend(parser.parse_record(record?, &ctx).await?);
    }

    assert_eq!(intents.len(), 2, "ordinary unit logs must be dropped");
    assert_eq!(intents[0].event_type.as_str(), "unit.started");
    assert_eq!(intents[1].event_type.as_str(), "unit.stopped");
    assert_eq!(intents[0].payload["unit_name"], "alpha.service");
    assert_eq!(intents[1].payload["unit_name"], "beta.service");
    assert_eq!(intents[0].ts_orig.inner().microsecond(), 123_456);
    Ok(())
}

/// sinex-10ef regression: a missing/malformed `__REALTIME_TIMESTAMP` uses a
/// fabricated acquisition-time value, which must remain `Atemporal` rather
/// than being tagged as a trustworthy intrinsic timestamp.
#[sinex_test]
async fn test_systemd_missing_realtime_timestamp_is_tagged_atemporal_not_intrinsic()
-> TestResult<()> {
    let mid = Id::<SourceMaterial>::new();
    let line =
        r#"{"__CURSOR":"s=abc;i=9","UNIT":"nginx.service","MESSAGE":"Started nginx.service."}"#;
    let records = records_from_journal_lines(mid, &[line]);
    let record = records[0].as_ref().unwrap().clone();

    let mut parser = SystemdParser;
    let ctx = make_ctx(mid);
    let intents = parser.parse_record(record, &ctx).await?;

    assert_eq!(intents.len(), 1);
    assert_eq!(
        intents[0].timing,
        TimingEvidence::Atemporal,
        "an acquisition-time fallback must be tagged Atemporal (deferred \
         resolution via raw.temporal_ledger), not Intrinsic (trusted verbatim)"
    );
    Ok(())
}

#[sinex_test]
async fn test_systemd_malformed_realtime_timestamp_is_tagged_atemporal_not_intrinsic()
-> TestResult<()> {
    let mid = Id::<SourceMaterial>::new();
    let line = r#"{"__CURSOR":"s=abc;i=10","__REALTIME_TIMESTAMP":"not-a-timestamp","UNIT":"nginx.service","MESSAGE":"Started nginx.service."}"#;
    let records = records_from_journal_lines(mid, &[line]);
    let record = records[0].as_ref().unwrap().clone();

    let mut parser = SystemdParser;
    let ctx = make_ctx(mid);
    let intents = parser.parse_record(record, &ctx).await?;

    assert_eq!(intents.len(), 1);
    assert_eq!(intents[0].ts_orig, ctx.acquisition_time);
    assert_eq!(intents[0].timing, TimingEvidence::Atemporal);
    Ok(())
}

/// `i64::MAX` parses as a provider microsecond value, but is beyond the
/// representable `Timestamp` range. It must not be mislabeled Intrinsic.
#[sinex_test]
async fn test_systemd_out_of_range_realtime_timestamp_is_tagged_atemporal_not_intrinsic()
-> TestResult<()> {
    let mid = Id::<SourceMaterial>::new();
    let line = r#"{"__CURSOR":"s=abc;i=11","__REALTIME_TIMESTAMP":"9223372036854775807","UNIT":"nginx.service","MESSAGE":"Started nginx.service."}"#;
    let records = records_from_journal_lines(mid, &[line]);
    let record = records[0].as_ref().unwrap().clone();

    let mut parser = SystemdParser;
    let ctx = make_ctx(mid);
    let intents = parser.parse_record(record, &ctx).await?;

    assert_eq!(intents.len(), 1);
    assert_eq!(intents[0].ts_orig, ctx.acquisition_time);
    assert_eq!(intents[0].timing, TimingEvidence::Atemporal);
    Ok(())
}

#[sinex_test]
async fn test_infer_unit_type() -> TestResult<()> {
    assert!(matches!(
        infer_unit_type("nginx.service"),
        SystemdUnitType::Service
    ));
    assert!(matches!(
        infer_unit_type("cron.timer"),
        SystemdUnitType::Timer
    ));
    assert!(matches!(infer_unit_type("unknown"), SystemdUnitType::Other));
    Ok(())
}
