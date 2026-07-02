use super::*;
use std::sync::atomic::AtomicU64;
use xtask::sandbox::prelude::sinex_test;

#[derive(Debug)]
struct ManualHealthClock {
    now_secs: AtomicU64,
}

impl ManualHealthClock {
    fn new(now_secs: u64) -> Self {
        Self {
            now_secs: AtomicU64::new(now_secs),
        }
    }

    fn set(&self, now_secs: u64) {
        self.now_secs.store(now_secs, Ordering::Relaxed);
    }
}

impl HealthClock for ManualHealthClock {
    fn now(&self) -> Duration {
        Duration::from_secs(self.now_secs.load(Ordering::Relaxed))
    }
}

fn reporter_with_clock(clock: Arc<ManualHealthClock>) -> HealthReporter {
    HealthReporter::new_with_clock(
        "runtime-health-test".to_string(),
        Arc::new(SelfObserver::disabled()),
        HealthThresholds {
            error_rate_degraded: 0.05,
            error_rate_failed: 0.20,
            window_seconds: 60,
            emit_stall_seconds: 0,
            refresh_seconds: 10,
        },
        clock,
    )
}

#[sinex_test]
async fn first_health_check_emits_initial_status_evidence() -> Result<()> {
    let clock = Arc::new(ManualHealthClock::new(1));
    let reporter = reporter_with_clock(clock);

    assert!(!reporter.has_emitted_status.load(Ordering::Relaxed));
    assert_eq!(reporter.check_and_emit().await?, HealthStatus::Healthy);

    assert!(reporter.has_emitted_status.load(Ordering::Relaxed));
    assert_eq!(reporter.last_status_emit_secs.load(Ordering::Relaxed), 1);
    Ok(())
}

#[sinex_test]
async fn unchanged_health_refreshes_after_configured_interval() -> Result<()> {
    let clock = Arc::new(ManualHealthClock::new(1));
    let reporter = reporter_with_clock(Arc::clone(&clock));

    reporter.check_and_emit().await?;
    clock.set(5);
    reporter.check_and_emit().await?;
    assert_eq!(
        reporter.last_status_emit_secs.load(Ordering::Relaxed),
        1,
        "unchanged health should not emit before the refresh interval"
    );

    clock.set(11);
    reporter.check_and_emit().await?;
    assert_eq!(
        reporter.last_status_emit_secs.load(Ordering::Relaxed),
        11,
        "unchanged health must refresh before event-derived liveness ages out"
    );
    Ok(())
}
