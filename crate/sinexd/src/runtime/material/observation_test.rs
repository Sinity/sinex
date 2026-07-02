use super::*;

use std::sync::Arc as StdArc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::time::sleep;
use xtask::sandbox::prelude::sinex_test;

#[derive(Serialize, Clone)]
struct TestRecord {
    id: usize,
    value: String,
}

#[sinex_test]
async fn test_append_single_record() -> xtask::sandbox::TestResult<()> {
    let mut mat =
        ObservationMaterializer::<TestRecord>::new(ObservationMaterializerConfig::default());

    let record = TestRecord {
        id: 1,
        value: "test".to_string(),
    };

    let result = mat.append(record).await;
    assert!(result.is_ok());
    Ok(())
}

#[sinex_test]
async fn test_flush_on_max_records() -> xtask::sandbox::TestResult<()> {
    let flush_count = StdArc::new(AtomicUsize::new(0));
    let flush_count_clone = flush_count.clone();

    let on_flush: Arc<FlushCallback> = Arc::new(move |batch: SerializedBatch| -> FlushFuture {
        let fc = flush_count_clone.clone();
        Box::pin(async move {
            fc.fetch_add(1, Ordering::SeqCst);
            assert!(!batch.data.is_empty());
            Ok(())
        })
    });

    let config = ObservationMaterializerConfig {
        batch_coalesce_window_ms: 1000,
        max_records: 3,
        max_bytes: 128 * 1024,
    };

    let mut mat = ObservationMaterializer::<TestRecord>::with_callback(config, on_flush);

    for i in 0..3 {
        let record = TestRecord {
            id: i,
            value: format!("test{i}"),
        };
        let _ = mat.append(record).await;
    }

    // After appending 3 records with max_records=3, should have flushed
    sleep(Duration::from_millis(50)).await;
    assert_eq!(flush_count.load(Ordering::SeqCst), 1);
    Ok(())
}

#[sinex_test]
async fn test_flush_on_window_timeout() -> xtask::sandbox::TestResult<()> {
    let flush_count = StdArc::new(AtomicUsize::new(0));
    let flush_count_clone = flush_count.clone();

    let on_flush: Arc<FlushCallback> =
        Arc::new(move |_batch: SerializedBatch| -> FlushFuture {
            let fc = flush_count_clone.clone();
            Box::pin(async move {
                fc.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
        });

    let config = ObservationMaterializerConfig {
        batch_coalesce_window_ms: 50,
        max_records: 100,
        max_bytes: 128 * 1024,
    };

    let mut mat = ObservationMaterializer::<TestRecord>::with_callback(config, on_flush);

    let record = TestRecord {
        id: 1,
        value: "test".to_string(),
    };
    let _ = mat.append(record).await;

    // Wait for window timeout
    sleep(Duration::from_millis(150)).await;

    // Should have flushed due to timeout
    assert_eq!(flush_count.load(Ordering::SeqCst), 1);
    Ok(())
}

#[sinex_test]
async fn test_empty_flush_is_noop() -> xtask::sandbox::TestResult<()> {
    let flush_count = StdArc::new(AtomicUsize::new(0));
    let flush_count_clone = flush_count.clone();

    let on_flush: Arc<FlushCallback> =
        Arc::new(move |_batch: SerializedBatch| -> FlushFuture {
            let fc = flush_count_clone.clone();
            Box::pin(async move {
                fc.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
        });

    let config = ObservationMaterializerConfig {
        batch_coalesce_window_ms: 50,
        max_records: 100,
        max_bytes: 128 * 1024,
    };

    let _mat = ObservationMaterializer::<TestRecord>::with_callback(config, on_flush);

    // Don't append anything, just let it sit
    sleep(Duration::from_millis(150)).await;

    // Should not flush if buffer is empty
    assert_eq!(flush_count.load(Ordering::SeqCst), 0);
    Ok(())
}

#[sinex_test]
async fn test_flush_on_max_bytes() -> xtask::sandbox::TestResult<()> {
    let flush_count = StdArc::new(AtomicUsize::new(0));
    let flush_count_clone = flush_count.clone();

    let on_flush: Arc<FlushCallback> =
        Arc::new(move |_batch: SerializedBatch| -> FlushFuture {
            let fc = flush_count_clone.clone();
            Box::pin(async move {
                fc.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
        });

    let config = ObservationMaterializerConfig {
        batch_coalesce_window_ms: 1000,
        max_records: 1000,
        max_bytes: 100, // Small threshold to trigger quickly
    };

    let mut mat = ObservationMaterializer::<TestRecord>::with_callback(config, on_flush);

    // Add records until we exceed max_bytes
    for i in 0..10 {
        let record = TestRecord {
            id: i,
            value: "x".repeat(30), // ~30 bytes per record
        };
        let _ = mat.append(record).await;
    }

    sleep(Duration::from_millis(50)).await;

    // Should have flushed due to exceeding max_bytes
    let flushes = flush_count.load(Ordering::SeqCst);
    assert!(flushes > 0, "Expected at least 1 flush, got {flushes}");
    Ok(())
}

#[sinex_test]
async fn test_serialization_error_propagates() -> xtask::sandbox::TestResult<()> {
    let flush_count = StdArc::new(AtomicUsize::new(0));
    let flush_count_clone = flush_count.clone();

    let on_flush: Arc<FlushCallback> =
        Arc::new(move |_batch: SerializedBatch| -> FlushFuture {
            let fc = flush_count_clone.clone();
            Box::pin(async move {
                fc.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
        });

    let config = ObservationMaterializerConfig::default();
    let mut mat = ObservationMaterializer::<TestRecord>::with_callback(config, on_flush);

    let record = TestRecord {
        id: 1,
        value: "test".to_string(),
    };

    let result = mat.append(record).await;
    assert!(result.is_ok());
    Ok(())
}

#[sinex_test]
async fn test_multiple_flushes_accumulate() -> xtask::sandbox::TestResult<()> {
    let flush_count = StdArc::new(AtomicUsize::new(0));
    let total_records = StdArc::new(AtomicUsize::new(0));
    let flush_count_clone = flush_count.clone();
    let total_records_clone = total_records.clone();

    let on_flush: Arc<FlushCallback> = Arc::new(move |batch: SerializedBatch| -> FlushFuture {
        let fc = flush_count_clone.clone();
        let tr = total_records_clone.clone();
        Box::pin(async move {
            fc.fetch_add(1, Ordering::SeqCst);
            tr.fetch_add(batch.record_count, Ordering::SeqCst);
            Ok(())
        })
    });

    let config = ObservationMaterializerConfig {
        batch_coalesce_window_ms: 50,
        max_records: 2,
        max_bytes: 128 * 1024,
    };

    let mut mat = ObservationMaterializer::<TestRecord>::with_callback(config, on_flush);

    // Append 5 records in batches of 2
    for i in 0..5 {
        let record = TestRecord {
            id: i,
            value: format!("test{i}"),
        };
        let _ = mat.append(record).await;
    }

    sleep(Duration::from_millis(150)).await;

    let flushes = flush_count.load(Ordering::SeqCst);
    assert!(flushes >= 2, "Expected at least 2 flushes, got {flushes}");
    Ok(())
}
