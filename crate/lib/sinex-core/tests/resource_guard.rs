use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::time::{sleep, Duration};

use sinex_core::types::utils::resource_guard::{ResourceGuard, SimpleGuard};
use sinex_test_utils::sinex_test;

#[sinex_test]
async fn resource_guard_runs_async_cleanup_on_drop() -> color_eyre::eyre::Result<()> {
    let cleaned_up = Arc::new(AtomicBool::new(false));
    let cleaned_up_clone = cleaned_up.clone();

    {
        let _guard = ResourceGuard::new("test_resource", move |_resource| {
            let marker = cleaned_up_clone.clone();
            async move {
                marker.store(true, Ordering::Relaxed);
            }
        });

        sleep(Duration::from_millis(10)).await;
        assert!(!cleaned_up.load(Ordering::Relaxed));
    }

    sleep(Duration::from_millis(50)).await;
    assert!(cleaned_up.load(Ordering::Relaxed));
    Ok(())
}

#[sinex_test]
async fn simple_guard_runs_sync_cleanup() -> color_eyre::eyre::Result<()> {
    let cleaned_up = Arc::new(AtomicBool::new(false));
    let cleaned_up_clone = cleaned_up.clone();

    {
        let _guard = SimpleGuard::new("test_resource", move |_resource| {
            cleaned_up_clone.store(true, Ordering::Relaxed);
        });
    }

    assert!(cleaned_up.load(Ordering::Relaxed));
    Ok(())
}
