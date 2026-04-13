use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use sinex_primitives::utils::ResourceGuard;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn cleanup_now_waits_for_async_cleanup_completion() -> TestResult<()> {
    let cleaned = Arc::new(AtomicBool::new(false));
    let cleaned_clone = cleaned.clone();
    let guard = ResourceGuard::new("resource", move |_| async move {
        tokio::task::yield_now().await;
        cleaned_clone.store(true, Ordering::SeqCst);
    });

    guard.cleanup_now().await;

    assert!(
        cleaned.load(Ordering::SeqCst),
        "cleanup_now should not return before async cleanup finishes"
    );
    Ok(())
}
