use super::*;

#[tokio::test(flavor = "current_thread")]
async fn bulk_acquisitions_leave_capacity_for_live_requests() {
    let first = capacity(Priority::Bulk).await.unwrap();
    let second = capacity(Priority::Bulk).await.unwrap();
    let third = capacity(Priority::Bulk).await.unwrap();
    let mut fourth = Box::pin(capacity(Priority::Bulk));
    assert!(matches!(
        futures::poll!(fourth.as_mut()),
        std::task::Poll::Pending
    ));
    let live = tokio::time::timeout(std::time::Duration::from_secs(5), capacity(Priority::Live))
        .await
        .unwrap()
        .unwrap();
    drop(first);
    drop(live);
    let fourth = fourth.await.unwrap();
    drop((second, third, fourth));
}
