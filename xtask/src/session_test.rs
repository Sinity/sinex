use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use super::{WatchAction, WatchLoop};
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn test_watch_loop_stops_on_shutdown_signal() -> ::xtask::sandbox::TestResult<()> {
    let ticks = Arc::new(AtomicUsize::new(0));
    let loop_ = WatchLoop::new(Duration::from_millis(1));

    loop_
        .run_with_shutdown_signal(std::future::ready(Ok(())), {
            let ticks = ticks.clone();
            move |_| {
                let ticks = ticks.clone();
                async move {
                    ticks.fetch_add(1, Ordering::SeqCst);
                    Ok(WatchAction::Continue)
                }
            }
        })
        .await?;

    assert_eq!(ticks.load(Ordering::SeqCst), 1);
    Ok(())
}

#[sinex_test]
async fn test_watch_loop_surfaces_shutdown_listener_failure() -> ::xtask::sandbox::TestResult<()>
{
    let loop_ = WatchLoop::new(Duration::from_millis(1));

    let error = loop_
        .run_with_shutdown_signal(
            std::future::ready(Err(std::io::Error::other("ctrl-c unavailable"))),
            |_| async { Ok(WatchAction::Continue) },
        )
        .await
        .expect_err("shutdown listener failure should surface");

    let message = format!("{error:#}");
    assert!(message.contains("failed to wait for Ctrl+C in watch loop"));
    assert!(message.contains("ctrl-c unavailable"));
    Ok(())
}
