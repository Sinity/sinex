// RateBudget's own construction/merge/env-loading behavior is tested where
// the type lives (`sinex_primitives::pacing`, which is also its wire-shape
// authority for `ReplayGateOverrides`). This file tests the sinexd-specific
// enforcement primitives that consume a `RateBudget`: `PacingController`
// (throttle timing) and `BacklogGate` (backlog-aware pause/resume).
use super::*;
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn required_sleep_is_zero_when_unlimited() -> TestResult<()> {
    let controller = PacingController::new(RateBudget::unlimited());
    assert_eq!(
        controller.required_sleep(Duration::from_secs(0)),
        Duration::ZERO
    );
    Ok(())
}

#[sinex_test]
async fn required_sleep_enforces_events_per_sec() -> TestResult<()> {
    let mut controller = PacingController::new(RateBudget {
        events_per_sec: Some(100.0),
        bytes_per_sec: None,
        backlog_pause_threshold: None,
        backlog_resume_threshold: None,
    });
    controller.events_total = 1_000; // should take ~10s at 100/s
    let sleep = controller.required_sleep(Duration::from_secs(1));
    assert!(
        (sleep.as_secs_f64() - 9.0).abs() < 0.001,
        "expected ~9s sleep, got {sleep:?}"
    );
    Ok(())
}

#[sinex_test]
async fn required_sleep_enforces_bytes_per_sec() -> TestResult<()> {
    let mut controller = PacingController::new(RateBudget {
        events_per_sec: None,
        bytes_per_sec: Some(1_000.0),
        backlog_pause_threshold: None,
        backlog_resume_threshold: None,
    });
    controller.bytes_total = 5_000; // should take 5s at 1000 B/s
    let sleep = controller.required_sleep(Duration::from_secs(1));
    assert!(
        (sleep.as_secs_f64() - 4.0).abs() < 0.001,
        "expected ~4s sleep, got {sleep:?}"
    );
    Ok(())
}

#[sinex_test]
async fn required_sleep_takes_the_binding_constraint() -> TestResult<()> {
    // events budget requires 10s, bytes budget requires 2s — events wins.
    let mut controller = PacingController::new(RateBudget {
        events_per_sec: Some(100.0),
        bytes_per_sec: Some(1_000.0),
        backlog_pause_threshold: None,
        backlog_resume_threshold: None,
    });
    controller.events_total = 1_000;
    controller.bytes_total = 2_000;
    let sleep = controller.required_sleep(Duration::ZERO);
    assert!(
        (sleep.as_secs_f64() - 10.0).abs() < 0.001,
        "expected the tighter (events) constraint to win, got {sleep:?}"
    );
    Ok(())
}

#[sinex_test]
async fn record_and_throttle_holds_average_rate_within_budget() -> TestResult<()> {
    let mut controller = PacingController::new(RateBudget {
        events_per_sec: Some(200.0),
        bytes_per_sec: None,
        backlog_pause_threshold: None,
        backlog_resume_threshold: None,
    });

    // Emitting 50 events in one shot should force ~0.25s of sleep to hold
    // the 200 events/sec average — this proves the enforcement function
    // actually sleeps proportional to the configured budget, not just that
    // counters update.
    let start = Instant::now();
    controller.record_and_throttle(50, 0).await;
    let elapsed = start.elapsed();
    assert!(
        elapsed >= Duration::from_millis(200),
        "expected pacing to enforce ~250ms, got {elapsed:?}"
    );
    assert!(
        elapsed < Duration::from_secs(2),
        "pacing sleep took implausibly long: {elapsed:?}"
    );
    assert_eq!(controller.events_total(), 50);
    Ok(())
}

#[sinex_test]
async fn record_and_throttle_is_instant_when_unlimited() -> TestResult<()> {
    let mut controller = PacingController::new(RateBudget::unlimited());
    let start = Instant::now();
    controller
        .record_and_throttle(1_000_000, 1_000_000_000)
        .await;
    assert!(start.elapsed() < Duration::from_millis(300));
    Ok(())
}

#[sinex_test]
async fn backlog_gate_returns_immediately_when_no_signal() -> TestResult<()> {
    let gate = BacklogGate::new(100, 10);
    let result = gate.wait_for_capacity(|| async { Ok(None) }).await;
    assert_eq!(result.unwrap(), None);
    Ok(())
}

#[sinex_test]
async fn backlog_gate_returns_immediately_under_threshold() -> TestResult<()> {
    let gate = BacklogGate::new(100, 10);
    let result = gate.wait_for_capacity(|| async { Ok(Some(5)) }).await;
    assert_eq!(result.unwrap(), Some(5));
    Ok(())
}

#[sinex_test]
async fn backlog_gate_waits_out_pressure_with_hysteresis() -> TestResult<()> {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    let gate = BacklogGate::new(100, 10).with_poll_interval(Duration::from_millis(1));
    // Sequence: 500 (above pause) -> 50 (above resume, since paused) -> 5 (below resume) -> done.
    let sequence: Arc<Vec<u64>> = Arc::new(vec![500, 50, 5]);
    let idx = Arc::new(AtomicU64::new(0));

    let result = gate
        .wait_for_capacity(|| {
            let sequence = Arc::clone(&sequence);
            let idx = Arc::clone(&idx);
            async move {
                let i = idx.fetch_add(1, Ordering::SeqCst) as usize;
                let value = *sequence.get(i.min(sequence.len() - 1)).unwrap();
                Ok(Some(value))
            }
        })
        .await;

    assert_eq!(result.unwrap(), Some(5));
    assert!(idx.load(Ordering::SeqCst) >= 3);
    Ok(())
}

#[sinex_test]
async fn backlog_gate_from_budget_none_when_unlimited() -> TestResult<()> {
    assert!(BacklogGate::from_budget(&RateBudget::unlimited()).is_none());
    Ok(())
}

#[sinex_test]
async fn backlog_gate_from_budget_some_when_paced() -> TestResult<()> {
    assert!(BacklogGate::from_budget(&RateBudget::default_paced()).is_some());
    Ok(())
}
