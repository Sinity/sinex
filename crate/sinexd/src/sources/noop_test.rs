use super::*;
use xtask::sandbox::prelude::*;

fn assert_noop_report(report: &ScanReport, expected_checkpoint: Checkpoint) {
    assert_eq!(report.events_processed, 0);
    assert_eq!(report.final_checkpoint, expected_checkpoint);
    assert!(report.time_range.is_none());
    assert!(report.runtime_stats.is_empty());
    assert!(report.failed_targets.is_empty());
    assert!(report.successful_targets.is_empty());
    assert!(report.warnings.is_empty());
}

#[sinex_test]
async fn noop_source_reports_zero_work() -> TestResult<()> {
    let mut source = NoopSourceDriver;
    let mut state = NoopState;

    let snapshot = source
        .scan_snapshot(&mut state, ScanArgs::default())
        .await?;
    assert_noop_report(&snapshot, Checkpoint::None);

    let historical = source
        .scan_historical(
            &mut state,
            Checkpoint::external(serde_json::json!(42), "unused start"),
            TimeHorizon::Continuous,
            ScanArgs::default(),
        )
        .await?;
    assert_noop_report(&historical, Checkpoint::None);

    let (tx, rx) = watch::channel(false);
    tx.send(true)?;
    let continuous = source
        .run_continuous(
            &mut state,
            ContinuousStart::from_checkpoint(Checkpoint::external(
                serde_json::json!(7),
                "resume point",
            )),
            rx,
        )
        .await?;
    assert_noop_report(
        &continuous,
        Checkpoint::external(serde_json::json!(7), "resume point"),
    );

    Ok(())
}
