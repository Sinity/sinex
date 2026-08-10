use super::*;
use crate::runtime::pacing::RateBudget;
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn tracker_eta_is_none_without_horizon() -> TestResult<()> {
    let mut tracker = ScanProgressTracker::new(None);
    tracker.observe(Some(Timestamp::now()));
    assert_eq!(tracker.eta_seconds(Duration::from_secs(10)), None);
    Ok(())
}

#[sinex_test]
async fn tracker_eta_is_none_before_any_position_observed() -> TestResult<()> {
    let horizon = Timestamp::now();
    let tracker = ScanProgressTracker::new(Some(horizon));
    assert_eq!(tracker.eta_seconds(Duration::from_secs(10)), None);
    Ok(())
}

#[sinex_test]
async fn tracker_eta_is_zero_when_position_reached_horizon() -> TestResult<()> {
    let start = Timestamp::from_unix_timestamp(1_700_000_000).unwrap();
    let horizon = Timestamp::from_unix_timestamp(1_700_000_100).unwrap();
    let mut tracker = ScanProgressTracker::new(Some(horizon));
    tracker.observe(Some(start));
    tracker.observe(Some(horizon));
    assert_eq!(
        tracker.eta_seconds(Duration::from_secs(10)),
        Some(0.0),
        "position at/past horizon should report zero ETA"
    );
    Ok(())
}

#[sinex_test]
async fn tracker_eta_scales_with_observed_replay_speed() -> TestResult<()> {
    // Covered 100 "historical seconds" (start -> last) in 10 wall-seconds =
    // 10x replay speed. 200 historical seconds remain to horizon, so ETA
    // should be 200 / 10 = 20 wall-seconds.
    let start = Timestamp::from_unix_timestamp(1_700_000_000).unwrap();
    let last = Timestamp::from_unix_timestamp(1_700_000_100).unwrap();
    let horizon = Timestamp::from_unix_timestamp(1_700_000_300).unwrap();
    let mut tracker = ScanProgressTracker::new(Some(horizon));
    tracker.observe(Some(start));
    tracker.observe(Some(last));

    let eta = tracker
        .eta_seconds(Duration::from_secs(10))
        .expect("eta should be computable");
    assert!(
        (eta - 20.0).abs() < 0.001,
        "expected ETA ~20s at 10x replay speed, got {eta}"
    );
    Ok(())
}

#[sinex_test]
async fn snapshot_is_stale_after_threshold() -> TestResult<()> {
    let mut controller = PacingController::new(RateBudget::default_paced());
    controller.record_and_throttle(0, 0).await;
    let tracker = ScanProgressTracker::new(None);
    let mut snapshot = ScanProgressSnapshot::from_controller(
        "test.source",
        Timestamp::now(),
        &controller,
        &tracker,
        None,
    );

    assert!(!snapshot.is_stale(Timestamp::now()));

    // Backdate updated_at past the staleness window.
    snapshot.updated_at = snapshot.updated_at - time::Duration::seconds(120);
    assert!(snapshot.is_stale(Timestamp::now()));
    Ok(())
}

#[sinex_test]
async fn snapshot_reports_paced_flag_from_budget() -> TestResult<()> {
    let controller = PacingController::new(RateBudget::unlimited());
    let tracker = ScanProgressTracker::new(None);
    let snapshot = ScanProgressSnapshot::from_controller(
        "test.source",
        Timestamp::now(),
        &controller,
        &tracker,
        None,
    );
    assert!(!snapshot.paced);

    let paced_controller = PacingController::new(RateBudget::default_paced());
    let snapshot = ScanProgressSnapshot::from_controller(
        "test.source",
        Timestamp::now(),
        &paced_controller,
        &tracker,
        None,
    );
    assert!(snapshot.paced);
    Ok(())
}
