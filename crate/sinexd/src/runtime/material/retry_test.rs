use super::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;
use xtask::sandbox::prelude::sinex_test;

// Note: tests use Arc<AtomicUsize> not Rc<RefCell> because the predicate
// crosses an .await point inside RetryableMaterialCapture::run, requiring
// Send.
#[derive(Clone)]
struct CountingPredicate;

impl TransientErrorPredicate for CountingPredicate {
    fn is_transient(&self, err: &SinexError) -> bool {
        err.context_map()
            .get("transient")
            .is_some_and(|v| v == "true")
    }
}

fn count() -> Arc<AtomicUsize> {
    Arc::new(AtomicUsize::new(0))
}

#[sinex_test]
async fn test_succeeds_on_first_attempt() -> xtask::sandbox::TestResult<()> {
    let retry = RetryableMaterialCapture::new();
    let attempts = count();
    let attempts_clone = attempts.clone();
    let result = retry
        .run(move || {
            let a = attempts_clone.clone();
            Box::pin(async move {
                a.fetch_add(1, Ordering::SeqCst);
                Ok::<i32, SinexError>(42)
            })
        })
        .await;

    assert!(result.is_ok());
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    Ok(())
}

#[sinex_test]
async fn test_retries_on_transient_and_succeeds() -> xtask::sandbox::TestResult<()> {
    let retry = RetryableMaterialCapture::new().with_max_attempts(3);
    let attempts = count();
    let attempts_clone = attempts.clone();
    let result = retry
        .run(move || {
            let a = attempts_clone.clone();
            Box::pin(async move {
                let n = a.fetch_add(1, Ordering::SeqCst) + 1;
                if n < 2 {
                    let err =
                        SinexError::io("test error").with_context("io_kind", "Interrupted");
                    Err(err)
                } else {
                    Ok::<i32, SinexError>(42)
                }
            })
        })
        .await;

    assert!(result.is_ok());
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    Ok(())
}

#[sinex_test]
async fn test_retries_exhaustion() -> xtask::sandbox::TestResult<()> {
    let retry = RetryableMaterialCapture::new().with_max_attempts(2);
    let attempts = count();
    let attempts_clone = attempts.clone();
    let result = retry
        .run(move || {
            let a = attempts_clone.clone();
            Box::pin(async move {
                a.fetch_add(1, Ordering::SeqCst);
                let err =
                    SinexError::io("persistent error").with_context("io_kind", "Interrupted");
                Err::<i32, _>(err)
            })
        })
        .await;

    assert!(result.is_err());
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    Ok(())
}

#[sinex_test]
async fn test_permanent_error_fails_immediately() -> xtask::sandbox::TestResult<()> {
    let retry = RetryableMaterialCapture::new().with_max_attempts(3);
    let attempts = count();
    let attempts_clone = attempts.clone();
    let result = retry
        .run(move || {
            let a = attempts_clone.clone();
            Box::pin(async move {
                a.fetch_add(1, Ordering::SeqCst);
                let err = SinexError::io("permanent error").with_context("transient", "false");
                Err::<i32, _>(err)
            })
        })
        .await;

    assert!(result.is_err());
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    Ok(())
}

#[sinex_test]
async fn test_backoff_increases_exponentially() -> xtask::sandbox::TestResult<()> {
    let retry = RetryableMaterialCapture::new()
        .with_max_attempts(3)
        .with_base_delay_ms(10);

    let attempts = count();
    let attempts_clone = attempts.clone();
    let start = Instant::now();
    let result = retry
        .run(move || {
            let a = attempts_clone.clone();
            Box::pin(async move {
                let n = a.fetch_add(1, Ordering::SeqCst) + 1;
                if n < 3 {
                    let err =
                        SinexError::io("test error").with_context("io_kind", "Interrupted");
                    Err(err)
                } else {
                    Ok::<i32, SinexError>(42)
                }
            })
        })
        .await;

    let elapsed = start.elapsed().as_millis() as u64;
    assert!(result.is_ok());
    // ~10ms + ~20ms ≥ 20ms total
    assert!(elapsed >= 20, "elapsed: {elapsed}ms");
    Ok(())
}

#[sinex_test]
async fn test_custom_predicate() -> xtask::sandbox::TestResult<()> {
    let predicate = CountingPredicate;
    let retry = RetryableMaterialCapture::new()
        .with_max_attempts(2)
        .with_predicate(predicate);

    let attempts = count();
    let attempts_clone = attempts.clone();
    let result = retry
        .run(move || {
            let a = attempts_clone.clone();
            Box::pin(async move {
                let n = a.fetch_add(1, Ordering::SeqCst) + 1;
                if n < 2 {
                    let err = SinexError::io("test error").with_context("transient", "true");
                    Err(err)
                } else {
                    Ok::<i32, SinexError>(42)
                }
            })
        })
        .await;

    assert!(result.is_ok());
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    Ok(())
}

#[sinex_test]
async fn test_max_attempts_one() -> xtask::sandbox::TestResult<()> {
    let retry = RetryableMaterialCapture::new().with_max_attempts(1);
    let attempts = count();
    let attempts_clone = attempts.clone();
    let result = retry
        .run(move || {
            let a = attempts_clone.clone();
            Box::pin(async move {
                a.fetch_add(1, Ordering::SeqCst);
                let err = SinexError::io("error").with_context("io_kind", "Interrupted");
                Err::<i32, _>(err)
            })
        })
        .await;

    assert!(result.is_err());
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    Ok(())
}

#[sinex_test]
async fn test_backoff_saturates() -> xtask::sandbox::TestResult<()> {
    let retry = RetryableMaterialCapture::new()
        .with_max_attempts(20)
        .with_base_delay_ms(1);

    let attempts = count();
    let attempts_clone = attempts.clone();
    let start = Instant::now();

    let result = retry
        .run(move || {
            let a = attempts_clone.clone();
            Box::pin(async move {
                let n = a.fetch_add(1, Ordering::SeqCst) + 1;
                if n <= 12 {
                    let err = SinexError::io("error").with_context("io_kind", "Interrupted");
                    Err(err)
                } else {
                    Ok::<i32, SinexError>(42)
                }
            })
        })
        .await;

    let elapsed = start.elapsed().as_millis() as u64;
    assert!(result.is_ok());
    // Delays cap at base_delay_ms * 2^10 = 1024ms after exponent 10.
    assert!(elapsed >= 1000, "elapsed: {elapsed}ms");
    Ok(())
}
