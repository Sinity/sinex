use crate::runtime::content_store::ContentStoreKey;
use sinex_primitives::MaterialStatus;
use sinex_primitives::Uuid;
use xtask::sandbox::prelude::*;

use super::*;

#[sinex_test]
async fn rollback_finalization_failure_preserves_original_error_context() -> TestResult<()> {
    let error = rollback_finalization_failure(
        SinexError::validation("original finalize failure"),
        "rollback broke too",
        "record_ledger_entry",
    );

    let rendered = error.to_string();
    assert!(rendered.contains("Failed to rollback material finalization transaction"));
    assert!(rendered.contains("rollback broke too"));
    assert!(rendered.contains("original finalize failure"));
    assert!(rendered.contains("record_ledger_entry"));
    Ok(())
}

#[sinex_test]
async fn finalization_unknown_commit_error_preserves_retry_context() -> TestResult<()> {
    let content_key = ContentStoreKey::parse("SHA256E-s4--retry")?;
    let error = finalization_unknown_commit_error(
        SinexError::database("commit failed"),
        &SinexError::database("reconcile failed"),
        Uuid::now_v7(),
        &content_key,
        MaterialStatus::Completed,
    );

    assert!(finalization_commit_outcome_unknown(&error));
    assert_eq!(
        error.context_map().get("retry_state_preserved"),
        Some(&"true".to_string())
    );
    assert_eq!(
        error.context_map().get("terminal_failure_routed"),
        Some(&"false".to_string())
    );
    assert_eq!(
        error.context_map().get("final_status"),
        Some(&MaterialStatus::Completed.to_string())
    );
    assert_eq!(
        error.context_map().get("content_key"),
        Some(&content_key.key),
    );
    assert!(
        error
            .context_map()
            .get("reconcile_error")
            .is_some_and(|value| value.contains("reconcile failed"))
    );
    Ok(())
}

#[sinex_test]
async fn finalization_commit_outcome_unknown_ignores_unflagged_errors() -> TestResult<()> {
    let error = SinexError::database("ordinary failure");
    assert!(
        !finalization_commit_outcome_unknown(&error),
        "only explicitly flagged commit-reconciliation failures should preserve retry state"
    );
    Ok(())
}
