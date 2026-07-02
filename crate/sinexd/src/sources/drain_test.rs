use super::*;
use std::time::Duration;
use xtask::sandbox::prelude::sinex_test;

#[sinex_test]
async fn test_drain_phases_transition_in_order() -> xtask::sandbox::TestResult<()> {
    let controller = SourceDrainController::new();
    assert_eq!(controller.current_phase().await, DrainPhase::Idle);

    controller.request_drain("test-unit").await;
    assert_eq!(controller.current_phase().await, DrainPhase::StoppingAccept);

    controller.finish_active_work("test-unit").await;
    assert_eq!(
        controller.current_phase().await,
        DrainPhase::FinishingActive
    );

    controller.flush_intents("test-unit").await;
    assert_eq!(
        controller.current_phase().await,
        DrainPhase::FlushingIntents
    );

    controller
        .wait_confirmations("test-unit", Duration::from_millis(10))
        .await;
    assert_eq!(
        controller.current_phase().await,
        DrainPhase::WaitingConfirmations
    );

    controller.finalize_materials("test-unit").await;
    assert_eq!(
        controller.current_phase().await,
        DrainPhase::FinalizingMaterials
    );

    controller.save_checkpoint("test-unit").await;
    assert_eq!(
        controller.current_phase().await,
        DrainPhase::SavingCheckpoint
    );

    controller.mark_drained("test-unit").await;
    assert_eq!(controller.current_phase().await, DrainPhase::Drained);
    Ok(())
}

#[sinex_test]
async fn test_work_guard_increments_and_decrements_counter() -> xtask::sandbox::TestResult<()> {
    let controller = SourceDrainController::new();
    assert_eq!(controller.active_work_count(), 0);

    {
        let _guard = controller.work_guard();
        assert_eq!(controller.active_work_count(), 1);
        let _guard2 = controller.work_guard();
        assert_eq!(controller.active_work_count(), 2);
    }
    // Guards dropped
    assert_eq!(controller.active_work_count(), 0);
    Ok(())
}

#[sinex_test]
async fn test_drain_waits_for_active_work() -> xtask::sandbox::TestResult<()> {
    let controller = SourceDrainController::new();
    controller.enter_work();
    assert_eq!(controller.active_work_count(), 1);

    // With active work, wait should time out
    let completed = controller
        .wait_for_active_work(Duration::from_millis(10))
        .await;
    assert!(!completed, "should not complete with active work");

    controller.exit_work();
    assert_eq!(controller.active_work_count(), 0);

    // Without active work, wait should complete
    let completed = controller
        .wait_for_active_work(Duration::from_millis(10))
        .await;
    assert!(completed, "should complete when no active work");
    Ok(())
}

#[sinex_test]
async fn test_double_drain_is_idempotent() -> xtask::sandbox::TestResult<()> {
    let controller = SourceDrainController::new();
    controller.request_drain("test-unit").await;
    assert_eq!(controller.current_phase().await, DrainPhase::StoppingAccept);

    // Calling request_drain again returns false (already draining), phase unchanged
    let already_draining = !controller.request_drain("test-unit").await;
    assert!(already_draining);
    assert_eq!(controller.current_phase().await, DrainPhase::StoppingAccept);
    Ok(())
}

#[sinex_test]
async fn test_gap_evidence_on_restart() -> xtask::sandbox::TestResult<()> {
    let controller = SourceDrainController::new();
    controller.request_drain("test-unit").await;
    // Simulate crash mid-drain
    let evidence = controller.record_gap_evidence("test-unit").await;
    assert_eq!(evidence.unit_id, "test-unit");
    assert_eq!(
        evidence.drain_phase_at_crash,
        Some(DrainPhase::StoppingAccept)
    );
    assert_eq!(evidence.in_flight_count, 0);
    Ok(())
}

#[sinex_test]
async fn test_clean_start_evidence() -> xtask::sandbox::TestResult<()> {
    let controller = SourceDrainController::new();
    let evidence = controller.clean_start_evidence("test-unit");
    assert_eq!(evidence.unit_id, "test-unit");
    assert_eq!(evidence.drain_phase_at_crash, None);
    assert_eq!(evidence.in_flight_count, 0);
    Ok(())
}
