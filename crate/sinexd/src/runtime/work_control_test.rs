use super::work_control::{
    WorkAdmission, WorkBudget, WorkCancellation, WorkController, WorkIdentity, WorkOutcome,
    WorkProgress, WorkStopReason,
};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

#[tokio::test]
async fn file_admission_is_cross_handle_and_cancellation_aware() {
    let directory = tempfile::tempdir().unwrap();
    let lock_path = directory.path().join("maintenance.lock");
    let first_cancel = WorkCancellation::new();
    let _first = super::work_control::WorkFileAdmission::acquire(&lock_path, &first_cancel)
        .await
        .unwrap();

    let second_cancel = WorkCancellation::new();
    let waiting = {
        let lock_path = lock_path.clone();
        let second_cancel = second_cancel.clone();
        tokio::spawn(async move {
            super::work_control::WorkFileAdmission::acquire(&lock_path, &second_cancel).await
        })
    };
    tokio::time::sleep(std::time::Duration::from_millis(125)).await;
    second_cancel.cancel();
    let result = tokio::time::timeout(std::time::Duration::from_secs(1), waiting)
        .await
        .expect("cancelled file-admission waiter must not hang")
        .unwrap();
    assert!(result.is_err());
}

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
        controller
            .record_batch("scan", 1, 0, Some("one".to_owned()))
            .await
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
async fn admission_rejects_cancellation_before_waiter_registration() {
    let admission = WorkAdmission::new(1);
    let first_cancel = WorkCancellation::new();
    let _first = admission.acquire(&first_cancel).await.unwrap();

    let already_cancelled = WorkCancellation::new();
    already_cancelled.cancel();
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        admission.acquire(&already_cancelled),
    )
    .await
    .expect("already-cancelled admission must not wait");
    assert!(result.is_err());

    let cancelled_before_wait = WorkCancellation::new();
    let acquire = admission.acquire(&cancelled_before_wait);
    cancelled_before_wait.cancel();
    let result = tokio::time::timeout(std::time::Duration::from_secs(1), acquire)
        .await
        .expect("cancellation before waiter registration must not wait");
    assert!(result.is_err());
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
    assert!(
        controller
            .record_batch("scan", 1, 0, Some("two".to_owned()))
            .await
            .is_err()
    );
    assert_eq!(controller.progress().items_done, 1);
}

#[tokio::test]
async fn resumed_progress_consumes_the_existing_budget() {
    let mut controller = WorkController::resume(
        WorkIdentity::ephemeral("test", "resume"),
        WorkBudget {
            max_items: Some(3),
            ..WorkBudget::default()
        },
        WorkCancellation::new(),
        WorkProgress::at("scan", 2, 20, Some("cursor-2".to_owned())),
    );

    controller
        .record_batch("scan", 1, 10, Some("cursor-3".to_owned()))
        .await
        .unwrap();
    assert!(controller.record_batch("scan", 1, 10, None).await.is_err());
    assert_eq!(controller.progress().items_done, 3);
    assert_eq!(
        controller.progress().checkpoint.as_deref(),
        Some("cursor-3")
    );
}

#[tokio::test]
async fn pressure_gate_pauses_then_resumes_without_losing_progress() {
    let pressured = Arc::new(AtomicBool::new(true));
    let mut controller = WorkController::new(
        WorkIdentity::ephemeral("test", "pressure"),
        WorkBudget::default(),
        WorkCancellation::new(),
    );
    let gate = Arc::clone(&pressured);
    let task = tokio::spawn(async move {
        controller
            .wait_for_pressure(
                || gate.load(Ordering::Acquire),
                std::time::Duration::from_millis(1),
            )
            .await
            .map(|()| controller)
    });
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    pressured.store(false, Ordering::Release);
    let controller = task.await.unwrap().unwrap();
    assert_eq!(controller.progress().items_done, 0);
    assert_eq!(controller.progress().blocked_on, None);
}

#[tokio::test]
async fn progress_reporting_keeps_only_the_latest_cursor() {
    let mut controller = WorkController::new(
        WorkIdentity::ephemeral("test", "bounded-memory"),
        WorkBudget::default(),
        WorkCancellation::new(),
    );
    for index in 0..10_000 {
        controller
            .record_batch("scan", 1, 1, Some(index.to_string()))
            .await
            .unwrap();
    }
    assert_eq!(controller.progress().items_done, 10_000);
    assert_eq!(controller.progress().checkpoint.as_deref(), Some("9999"));
}

#[test]
fn terminal_outcomes_are_explicit() {
    assert_eq!(WorkOutcome::Completed, WorkOutcome::Completed);
    assert_eq!(WorkOutcome::Cancelled, WorkOutcome::Cancelled);
    assert_eq!(WorkOutcome::Failed, WorkOutcome::Failed);
    assert_eq!(
        WorkOutcome::Partial(WorkStopReason::ByteBudget),
        WorkOutcome::Partial(WorkStopReason::ByteBudget)
    );
}
