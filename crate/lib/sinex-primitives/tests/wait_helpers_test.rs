use sinex_primitives::error::SinexError;
use sinex_primitives::utils::wait_helpers::{wait_for_condition, wait_for_condition_adaptive};
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

    assert!(error.to_string().contains("wait helper test timeout"));
    assert!(format!("{error:?}").contains("synthetic wait failure"));
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

    assert!(error.to_string().contains("adaptive wait helper test timeout"));
    assert!(format!("{error:?}").contains("synthetic adaptive wait failure"));
    Ok(())
}
