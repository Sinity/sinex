//! Pipeline coordination and synchronization.

use crate::sandbox::prelude::*;
use crate::sandbox::timing::Timeouts;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Overrides for event metadata during test publishing.
#[derive(Debug, Clone, Default)]
pub struct EventOverrides {
    /// Overide the event timestamp (RFC3339 string).
    pub ts_orig: Option<String>,
    /// Override the event ID.
    pub id: Option<Uuid>,
}

static PIPELINE_SEMAPHORE: std::sync::LazyLock<Arc<Semaphore>> = std::sync::LazyLock::new(|| {
    #[allow(
        clippy::panic,
        reason = "LazyLock init: a malformed SINEX_TEST_PIPELINE_CONCURRENCY is fatal for the whole test run"
    )]
    let permits = pipeline_concurrency_from_env().unwrap_or_else(|error| panic!("{error}"));
    Arc::new(Semaphore::new(permits))
});

fn pipeline_concurrency_from_env() -> TestResult<usize> {
    match std::env::var("SINEX_TEST_PIPELINE_CONCURRENCY") {
        Ok(raw) => parse_pipeline_concurrency(&raw),
        Err(std::env::VarError::NotPresent) => Ok(8),
        Err(error) => Err(eyre!(
            "failed to read SINEX_TEST_PIPELINE_CONCURRENCY: {error}"
        )),
    }
}

fn parse_pipeline_concurrency(raw: &str) -> TestResult<usize> {
    let permits = raw
        .parse::<usize>()
        .map_err(|error| eyre!("invalid SINEX_TEST_PIPELINE_CONCURRENCY value '{raw}': {error}"))?;
    if permits == 0 {
        return Err(eyre!(
            "invalid SINEX_TEST_PIPELINE_CONCURRENCY value '{raw}': must be greater than zero"
        ));
    }
    Ok(permits)
}

/// Acquire a permit for running a pipeline test.
///
/// This limits the number of concurrent pipeline tests to prevent
/// memory exhaustion from multiple ingestd instances.
pub async fn acquire_pipeline_permit(_namespace: &str) -> TestResult<OwnedSemaphorePermit> {
    PIPELINE_SEMAPHORE
        .clone()
        .acquire_owned()
        .await
        .map_err(|e| eyre!("Failed to acquire pipeline permit: {}", e))
}

/// Wait until a published event is persisted in the database.
pub async fn wait_for_event_persisted(
    ctx: &Sandbox,
    event_id: impl Into<EventId>,
) -> TestResult<()> {
    let event_id = event_id.into();
    // Under nextest each test is a separate process, so the in-process semaphore
    // doesn't limit real concurrency (controlled by nextest test-threads = 32).
    // With 32 concurrent ingestd processes + DB slots, events regularly need
    // 15-25s to flow through NATS → ingestd → DB.
    WaitHelpers::wait_for_event_id(&ctx.pool, event_id, Timeouts::STANDARD).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use xtask::sandbox::sinex_test;

    #[sinex_test]
    async fn test_parse_pipeline_concurrency_accepts_positive_usize() -> TestResult<()> {
        assert_eq!(parse_pipeline_concurrency("12")?, 12);
        Ok(())
    }

    #[sinex_test]
    async fn test_parse_pipeline_concurrency_rejects_invalid_number() -> TestResult<()> {
        let error = parse_pipeline_concurrency("abc").unwrap_err();
        let rendered = format!("{error:#}");
        assert!(rendered.contains("invalid SINEX_TEST_PIPELINE_CONCURRENCY value 'abc'"));
        Ok(())
    }

    #[sinex_test]
    async fn test_parse_pipeline_concurrency_rejects_zero() -> TestResult<()> {
        let error = parse_pipeline_concurrency("0").unwrap_err();
        let rendered = format!("{error:#}");
        assert!(rendered.contains("must be greater than zero"));
        Ok(())
    }
}
