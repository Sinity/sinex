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
        r#"{{"__CURSOR":"s=abc;i=2","__REALTIME_TIMESTAMP":"1700000001000000","_SYSTEMD_UNIT":"nginx.service","MESSAGE":"Started nginx.service with token {tok}.","PRIORITY":"6"}}"#
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

/// sinex-10ef open: same bug shape as journald.rs -- a missing/malformed
/// `__REALTIME_TIMESTAMP` falls back to a fabricated `Timestamp::now()`, but
/// the intent is still unconditionally tagged `TimingEvidence::Intrinsic`
/// rather than `Atemporal`, bypassing deferred resolution via
/// `raw.temporal_ledger`.
#[sinex_test]
#[ignore = "sinex-10ef open: systemd.rs tags a fabricated Timestamp::now() fallback as \
            TimingEvidence::Intrinsic instead of Atemporal when __REALTIME_TIMESTAMP is \
            missing/malformed, bypassing temporal_ledger-based deferred resolution"]
async fn test_systemd_missing_realtime_timestamp_is_tagged_atemporal_not_intrinsic()
-> TestResult<()> {
    let mid = Id::<SourceMaterial>::new();
    let line = r#"{"__CURSOR":"s=abc;i=9","_SYSTEMD_UNIT":"nginx.service","MESSAGE":"Started nginx.service."}"#;
    let records = records_from_journal_lines(mid, &[line]);
    let record = records[0].as_ref().unwrap().clone();

    let mut parser = SystemdParser;
    let ctx = make_ctx(mid);
    let intents = parser.parse_record(record, &ctx).await?;

    assert_eq!(intents.len(), 1);
    assert_eq!(
        intents[0].timing,
        TimingEvidence::Atemporal,
        "a fabricated Timestamp::now() fallback must be tagged Atemporal (deferred \
         resolution via raw.temporal_ledger), not Intrinsic (trusted verbatim)"
    );
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
