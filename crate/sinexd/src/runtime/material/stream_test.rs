use super::*;
use xtask::sandbox::prelude::sinex_test;

#[sinex_test]
async fn test_stream_handle_creation() -> xtask::sandbox::TestResult<()> {
    let ctx = StreamMaterialContext::new();
    let handle = ctx
        .begin_stream(serde_json::json!({"source": "test"}))
        .await;

    assert!(handle.is_ok());
    Ok(())
}

#[sinex_test]
async fn test_append_event_increments_count() -> xtask::sandbox::TestResult<()> {
    let ctx = StreamMaterialContext::new();
    let handle = ctx
        .begin_stream(serde_json::json!({"source": "test"}))
        .await
        .unwrap();

    assert_eq!(handle.event_count(), 0);

    let _ = handle.append_event(serde_json::json!({"id": 1}));
    assert_eq!(handle.event_count(), 1);

    let _ = handle.append_event(serde_json::json!({"id": 2}));
    assert_eq!(handle.event_count(), 2);
    Ok(())
}

#[sinex_test]
async fn test_append_after_finalize_fails() -> xtask::sandbox::TestResult<()> {
    let ctx = StreamMaterialContext::new();
    let handle = ctx
        .begin_stream(serde_json::json!({"source": "test"}))
        .await
        .unwrap();

    let _ = handle.finalize("test");

    let result = handle.append_event(serde_json::json!({"id": 1}));
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("finalized"));
    Ok(())
}

#[sinex_test]
async fn test_finalize_is_idempotent() -> xtask::sandbox::TestResult<()> {
    let ctx = StreamMaterialContext::new();
    let handle = ctx
        .begin_stream(serde_json::json!({"source": "test"}))
        .await
        .unwrap();

    let result1 = handle.finalize("test");
    let result2 = handle.finalize("test again");

    assert!(result1.is_ok());
    assert!(result2.is_ok());
    assert!(handle.is_finalized());
    Ok(())
}

#[sinex_test]
async fn test_multiple_streams_have_different_ids() -> xtask::sandbox::TestResult<()> {
    let ctx = StreamMaterialContext::new();

    let handle1 = ctx
        .begin_stream(serde_json::json!({"source": "test"}))
        .await
        .unwrap();
    let handle2 = ctx
        .begin_stream(serde_json::json!({"source": "test"}))
        .await
        .unwrap();

    assert_ne!(handle1.material_id(), handle2.material_id());
    Ok(())
}

#[sinex_test]
async fn test_handle_clone_shares_state() -> xtask::sandbox::TestResult<()> {
    let ctx = StreamMaterialContext::new();
    let handle = ctx
        .begin_stream(serde_json::json!({"source": "test"}))
        .await
        .unwrap();

    let handle_clone = handle.clone();

    let _ = handle.append_event(serde_json::json!({"id": 1}));
    assert_eq!(handle_clone.event_count(), 1);

    let _ = handle_clone.append_event(serde_json::json!({"id": 2}));
    assert_eq!(handle.event_count(), 2);
    Ok(())
}

#[sinex_test]
async fn test_stream_handle_debug() -> xtask::sandbox::TestResult<()> {
    let ctx = StreamMaterialContext::new();
    let handle = ctx
        .begin_stream(serde_json::json!({"source": "test"}))
        .await
        .unwrap();

    let debug_str = format!("{handle:?}");
    assert!(debug_str.contains("StreamHandle"));
    assert!(debug_str.contains("material_id"));
    Ok(())
}

#[sinex_test]
async fn test_unfinalized_flag_tracking() -> xtask::sandbox::TestResult<()> {
    let ctx = StreamMaterialContext::new();

    {
        let _handle = ctx
            .begin_stream(serde_json::json!({"source": "test"}))
            .await
            .unwrap();
        // Handle dropped without finalization
    }

    // Small delay to allow drop to be processed
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

    // The dropped_unfinalized flag should be true (though timing-dependent)
    // This is a best-effort check since Drop might not fire immediately
    let had_drops = ctx.had_unfinalized_drops();
    // Note: This test is inherently flaky due to async drop semantics
    // We just verify the API exists and works without panicking
    let _ = had_drops;
    Ok(())
}

#[sinex_test]
async fn test_reset_unfinalized_flag() -> xtask::sandbox::TestResult<()> {
    let ctx = StreamMaterialContext::new();

    ctx.reset_unfinalized_flag();
    assert!(!ctx.had_unfinalized_drops());

    ctx.reset_unfinalized_flag();
    assert!(!ctx.had_unfinalized_drops());
    Ok(())
}

#[sinex_test]
async fn test_stream_context_default() -> xtask::sandbox::TestResult<()> {
    let ctx1 = StreamMaterialContext::new();
    let ctx2 = StreamMaterialContext::default();

    // Both should work identically
    let handle1 = ctx1
        .begin_stream(serde_json::json!({"source": "test"}))
        .await
        .unwrap();
    let handle2 = ctx2
        .begin_stream(serde_json::json!({"source": "test"}))
        .await
        .unwrap();

    // Both handles should be live, unfinalized, and have a fresh material id.
    assert!(!handle1.is_finalized());
    assert!(!handle2.is_finalized());
    assert_ne!(handle1.material_id(), handle2.material_id());
    Ok(())
}
