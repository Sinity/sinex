use sinex_primitives::error::SinexError;
use sinex_primitives::utils::wait_helpers::{wait_for_condition, wait_for_condition_adaptive};
use std::time::{Duration, Instant};
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn wait_for_condition_preserves_last_error_context() -> TestResult<()> {
    let error = wait_for_condition(
        || async {
            Err::<bool, _>(
                SinexError::validation("synthetic wait failure")
                    .with_context("phase", "wait_for_condition"),
            )
        },
        1,
        "wait helper test",
    )
    .await
    .expect_err("repeated condition failures must time out with their last error attached");

    assert_timeout_with_source(
        &error,
        "wait helper test timeout after 1 seconds",
        "synthetic wait failure",
    );
    Ok(())
}

#[sinex_test]
async fn wait_for_condition_adaptive_preserves_last_error_context() -> TestResult<()> {
    let error = wait_for_condition_adaptive(
        || async {
            Err::<bool, _>(
                SinexError::validation("synthetic adaptive wait failure")
                    .with_context("phase", "wait_for_condition_adaptive"),
            )
        },
        1,
        "adaptive wait helper test",
    )
    .await
    .expect_err("adaptive wait timeouts must retain the last condition failure");

    assert_timeout_with_source(
        &error,
        "adaptive wait helper test timeout after 1 seconds (adaptive backoff)",
        "synthetic adaptive wait failure",
    );
    Ok(())
}

#[sinex_test]
async fn wait_for_condition_adaptive_rechecks_at_timeout_boundary() -> TestResult<()> {
    let start = Instant::now();

    wait_for_condition_adaptive(
        || async move { Ok::<bool, SinexError>(start.elapsed() >= Duration::from_millis(950)) },
        1,
        "adaptive wait boundary check",
    )
    .await
    .expect("adaptive wait should perform a final check at the timeout boundary");

    Ok(())
}

fn assert_timeout_with_source(error: &SinexError, expected_message: &str, source_fragment: &str) {
    assert!(
        matches!(error, SinexError::Timeout(_)),
        "expected timeout error, got {}",
        error.variant_name()
    );
    assert_eq!(error.message(), expected_message);
    assert!(
        error
            .sources()
            .iter()
            .any(|source| source.contains(source_fragment)),
        "timeout should preserve source containing `{source_fragment}`; sources: {:?}",
        error.sources()
    );
}
