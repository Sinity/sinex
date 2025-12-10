use tokio::sync::mpsc;

#[tokio::test]
async fn bounded_channel_reports_full_without_dropping_silently() {
    let (tx, mut rx) = mpsc::channel::<u64>(2);

    tx.try_send(1).expect("first send should succeed");
    tx.try_send(2).expect("second send should fill the buffer");

    let err = tx
        .try_send(3)
        .expect_err("third send should hit backpressure");
    match err {
        mpsc::error::TrySendError::Full(value) => assert_eq!(value, 3),
        other => panic!("expected full error, got {other:?}"),
    }

    // Drain to prove the first two messages are still present and ordered.
    assert_eq!(rx.recv().await, Some(1));
    assert_eq!(rx.recv().await, Some(2));
    assert!(rx.recv().await.is_none());
}
