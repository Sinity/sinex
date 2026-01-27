use xtask::sandbox::{
    sinex_test, BackpressureManager, BackpressureOutcome, BackpressureStrategy, ChannelHarness,
    ChannelReceiverExt, ChannelSenderExt,
};
use std::time::Duration;

#[sinex_test]
async fn monitor_tracks_send_receive() -> sinex_test_utils::TestResult<()> {
    let mut harness = ChannelHarness::new(2);
    harness
        .sender
        .send_or_log(42_u64, "monitor_send")
        .await
        .unwrap();

    let _received = harness
        .receiver
        .recv_timeout(Duration::from_millis(50))
        .await
        .unwrap();

    let stats = harness.monitor.stats();
    assert_eq!(stats.sent, 1);
    assert_eq!(stats.received, 1);
    assert_eq!(stats.errors, 0);
    Ok(())
}

#[sinex_test]
async fn recv_timeout_is_recorded() -> sinex_test_utils::TestResult<()> {
    let mut harness = ChannelHarness::<u64>::new(1);
    let result = harness
        .receiver
        .recv_timeout(Duration::from_millis(10))
        .await;
    assert!(result.is_err());
    let stats = harness.monitor.stats();
    assert_eq!(stats.timeouts, 1);
    Ok(())
}

#[sinex_test]
async fn send_timeout_is_recorded() -> sinex_test_utils::TestResult<()> {
    let harness = ChannelHarness::small_capacity();
    harness.sender.send_or_log("first", "fill").await.unwrap();

    let result = harness
        .sender
        .send_timeout("second", Duration::from_millis(10))
        .await;
    assert!(result.is_err());

    let stats = harness.monitor.stats();
    assert_eq!(stats.timeouts, 1);
    Ok(())
}

#[sinex_test]
async fn backpressure_buffer_flushes() -> sinex_test_utils::TestResult<()> {
    let mut harness = ChannelHarness::small_capacity();
    let mut manager = BackpressureManager::buffering(2);

    let first = manager
        .send_monitored(&harness.sender, "one")
        .await
        .unwrap();
    assert!(matches!(first, BackpressureOutcome::Sent));

    let second = manager
        .send_monitored(&harness.sender, "two")
        .await
        .unwrap();
    assert!(matches!(second, BackpressureOutcome::Buffered { .. }));
    assert_eq!(manager.buffer_len(), 1);

    let received = harness
        .receiver
        .recv_timeout(Duration::from_millis(50))
        .await
        .unwrap();
    assert_eq!(received, Some("one"));

    let flushed = manager.flush_monitored(&harness.sender).unwrap();
    assert_eq!(flushed, 1);

    let received = harness
        .receiver
        .recv_timeout(Duration::from_millis(50))
        .await
        .unwrap();
    assert_eq!(received, Some("two"));
    Ok(())
}

#[sinex_test]
async fn backpressure_drop_newest() -> sinex_test_utils::TestResult<()> {
    let harness = ChannelHarness::small_capacity();
    let mut manager = BackpressureManager::new(BackpressureStrategy::DropNewest);

    manager
        .send_monitored(&harness.sender, "first")
        .await
        .unwrap();

    let outcome = manager
        .send_monitored(&harness.sender, "second")
        .await
        .unwrap();
    assert!(matches!(outcome, BackpressureOutcome::Dropped(_)));

    let stats = harness.monitor.stats();
    assert_eq!(stats.dropped, 1);
    Ok(())
}

#[sinex_test]
async fn batch_receive_drains_items() -> sinex_test_utils::TestResult<()> {
    let mut harness = ChannelHarness::new(4);
    for value in ["a", "b", "c"] {
        harness.sender.send_or_log(value, "batch").await.unwrap();
    }

    let batch = harness
        .receiver
        .recv_batch(3, Duration::from_millis(50))
        .await;
    assert_eq!(batch.len(), 3);
    Ok(())
}
