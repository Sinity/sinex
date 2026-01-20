use sinex_test_utils::{sinex_test, TestResult};
use std::sync::Arc;
use std::time::Duration;

#[sinex_test]
async fn cancelled_select_branch_releases_lock() -> TestResult<()> {
    let lock = Arc::new(tokio::sync::Mutex::new(()));
    let lock_inner = lock.clone();

    // This task will acquire the lock, then get cancelled by select choosing the other branch.
    let handle = tokio::spawn(async move {
        tokio::select! {
            _ = async {
                let _guard = lock_inner.lock().await;
                // Hold long enough that cancellation has to drop the guard.
                tokio::time::sleep(Duration::from_secs(5)).await;
            } => {},
            _ = tokio::time::sleep(Duration::from_millis(20)) => {}
        }
    });

    // Wait for the task to finish (it should take the timeout branch quickly).
    let _ = handle.await;

    // The mutex should be free; lock should not hang if cancellation dropped the guard.
    let _guard = tokio::time::timeout(Duration::from_millis(100), lock.lock())
        .await
        .expect("mutex should not be held after cancellation");
    Ok(())
}
