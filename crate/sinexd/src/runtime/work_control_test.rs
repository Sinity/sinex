use super::work_control::{WorkAdmission, WorkBudget, WorkCancellation, WorkController, WorkIdentity, WorkOutcome, WorkStopReason};

#[tokio::test]
async fn cancellation_interrupts_rate_wait() {
    let cancellation = WorkCancellation::new();
    let mut controller = WorkController::new(
        WorkIdentity::ephemeral("test", "scope"),
        WorkBudget {
            items_per_sec: Some(0.1),
            ..WorkBudget::default()
        },
        cancellation.clone(),
    );
    let task = tokio::spawn(async move {
        controller.record_batch("scan", 1, 0, Some("one".to_owned())).await
    });
    tokio::task::yield_now().await;
    cancellation.cancel();
    assert!(task.await.unwrap().is_err());
}

#[tokio::test]
async fn admission_is_bounded_and_cancellation_aware() {
    let admission = WorkAdmission::new(1);
    let first_cancel = WorkCancellation::new();
    let _first = admission.acquire(&first_cancel).await.unwrap();
    let second_cancel = WorkCancellation::new();
    let waiting = {
        let admission = admission.clone();
        let second_cancel = second_cancel.clone();
        tokio::spawn(async move { admission.acquire(&second_cancel).await })
    };
    tokio::task::yield_now().await;
    second_cancel.cancel();
    assert!(waiting.await.unwrap().is_err());
}

#[tokio::test]
async fn budget_rejects_a_batch_before_accounting_it() {
    let cancellation = WorkCancellation::new();
    let mut controller = WorkController::new(
        WorkIdentity::ephemeral("test", "scope"),
        WorkBudget {
            max_items: Some(1),
            max_runtime: None,
            ..WorkBudget::default()
        },
        cancellation,
    );
    controller
        .record_batch("scan", 1, 0, Some("one".to_owned()))
        .await
        .unwrap();
    assert!(controller
        .record_batch("scan", 1, 0, Some("two".to_owned()))
        .await
        .is_err());
    assert_eq!(controller.progress().items_done, 1);
}

#[test]
fn terminal_outcomes_are_explicit() {
    assert_eq!(WorkOutcome::Completed, WorkOutcome::Completed);
    assert_eq!(WorkOutcome::Cancelled, WorkOutcome::Cancelled);
    assert_eq!(WorkOutcome::Partial(WorkStopReason::ByteBudget), WorkOutcome::Partial(WorkStopReason::ByteBudget));
}
