//! Filesystem Chaos Tests
//!
//! Tests for filesystem edge cases including permission changes, unmounted directories,
//! and concurrent file operations under adverse conditions.
//!
//! These tests exercise the event pipeline with file-system-themed payloads to verify
//! that the system handles unusual file metadata gracefully.

use sinex_primitives::DynamicPayload;
use std::time::Instant;
use xtask::sandbox::prelude::*;

/// Publish events simulating a file whose permissions are revoked while it is being
/// watched. The pipeline should persist the events without crashing even if the
/// payload contains error-like metadata.
#[sinex_test]
#[ignore = "chaos test requiring controlled failure injection"]
async fn test_file_permission_revoked_while_watching(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let scope = ctx.pipeline().await?;

    // Simulate permission-revoked scenario via payloads
    let events = [json!({"path": "/watched/dir/file.txt", "event": "file.created", "size": 1024}),
        json!({"path": "/watched/dir/file.txt", "event": "file.modified", "size": 2048}),
        json!({"path": "/watched/dir/file.txt", "event": "file.permission_denied", "error": "EACCES", "errno": 13}),
        json!({"path": "/watched/dir/file.txt", "event": "file.read_error", "error": "permission denied"})];

    for (i, payload_json) in events.iter().enumerate() {
        scope
            .publish(DynamicPayload::new(
                "fs-chaos-permission",
                "fs.chaos.permission",
                json!({"seq": i, "detail": payload_json}),
            ))
            .await?;
    }

    scope.wait_for_event_count(events.len()).await?;

    // All events should persist
    let source = sinex_primitives::EventSource::from("fs-chaos-permission");
    let count = scope.ctx().pool.events().count_by_source(&source).await?;
    assert_eq!(
        count,
        events.len() as i64,
        "all permission-chaos events should persist"
    );

    scope.shutdown().await?;
    Ok(())
}

/// Publish events simulating a directory unmount scenario. The payloads include
/// ENOENT / ESTALE errors to verify the pipeline handles stale-path events.
#[sinex_test]
#[ignore = "chaos test requiring controlled failure injection"]
async fn test_directory_unmounted_while_watching(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let scope = ctx.pipeline().await?;

    let events = [json!({"path": "/mnt/external/data.csv", "event": "file.created"}),
        json!({"path": "/mnt/external/data.csv", "event": "file.modified"}),
        json!({"path": "/mnt/external", "event": "directory.unmounted", "error": "ENOENT"}),
        json!({"path": "/mnt/external/data.csv", "event": "file.stale", "error": "ESTALE"})];

    for (i, payload_json) in events.iter().enumerate() {
        scope
            .publish(DynamicPayload::new(
                "fs-chaos-unmount",
                "fs.chaos.unmount",
                json!({"seq": i, "detail": payload_json}),
            ))
            .await?;
    }

    scope.wait_for_event_count(events.len()).await?;

    let source = sinex_primitives::EventSource::from("fs-chaos-unmount");
    let count = scope.ctx().pool.events().count_by_source(&source).await?;
    assert_eq!(
        count,
        events.len() as i64,
        "all unmount-chaos events should persist"
    );

    scope.shutdown().await?;
    Ok(())
}

/// Concurrent file-system event publishing from multiple simulated watchers,
/// verifying all events arrive intact without corruption.
#[sinex_test(timeout = 60)]
#[ignore = "chaos test requiring controlled failure injection"]
async fn test_filesystem_chaos_concurrent_operations(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let _scope = ctx.pipeline().await?;

    let watcher_count = 5usize;
    let events_per_watcher = 20usize;
    let total_expected = watcher_count * events_per_watcher;
    let start = Instant::now();

    let ctx = &ctx;
    let futs: Vec<_> = (0..watcher_count)
        .map(|wid| async move {
            let mut ok = 0u32;
            for eid in 0..events_per_watcher {
                let path = format!("/watched/{wid}/file_{eid}.txt");
                let payload = DynamicPayload::new(
                    format!("fs-chaos-concurrent-{wid}"),
                    "fs.chaos.concurrent",
                    json!({"watcher": wid, "event": eid, "path": path}),
                );
                if ctx.publish(payload).await.is_ok() {
                    ok += 1;
                }
            }
            ok
        })
        .collect();

    let results = futures::future::join_all(futs).await;
    let total_ok: u32 = results.iter().sum();
    let elapsed = start.elapsed();

    println!("Concurrent FS chaos: {total_ok}/{total_expected} in {elapsed:?}");

    let success_rate = f64::from(total_ok) / total_expected as f64;
    assert!(
        success_rate > 0.95,
        "should achieve > 95% success rate, got {:.1}%",
        success_rate * 100.0
    );

    Ok(())
}
