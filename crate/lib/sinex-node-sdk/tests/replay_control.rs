use sinex_node_sdk::replay::ReplayController;
use xtask::sandbox::sinex_test;
use tokio::sync::oneshot;

#[sinex_test]
async fn pause_and_resume_toggle_state() -> color_eyre::Result<()> {
    let controller = ReplayController::new();
    assert!(!controller.is_paused());

    controller.pause();
    assert!(controller.is_paused());

    controller.resume();
    assert!(!controller.is_paused());

    Ok(())
}

#[sinex_test]
async fn cancel_sets_flag_and_errors() -> color_eyre::Result<()> {
    let controller = ReplayController::new();
    assert!(!controller.is_cancelled());

    controller.cancel();
    assert!(controller.is_cancelled());
    assert!(controller.check_cancelled().is_err());

    Ok(())
}

#[sinex_test]
async fn wait_if_paused_completes_on_resume() -> color_eyre::Result<()> {
    let controller = ReplayController::new();
    let controller_clone = controller.clone();

    controller.pause();

    let (started_tx, started_rx) = oneshot::channel();
    let waiter = tokio::spawn(async move {
        let _ = started_tx.send(());
        controller_clone.wait_if_paused().await
    });

    started_rx.await.expect("waiter should start");
    controller.resume();
    let wait_result = waiter.await?;
    assert!(wait_result.is_ok());
    assert!(!controller.is_paused());

    Ok(())
}

#[sinex_test]
async fn wait_if_paused_errors_on_cancel() -> color_eyre::Result<()> {
    let controller = ReplayController::new();
    let controller_clone = controller.clone();

    controller.pause();

    let (started_tx, started_rx) = oneshot::channel();
    let waiter = tokio::spawn(async move {
        let _ = started_tx.send(());
        controller_clone.wait_if_paused().await
    });

    started_rx.await.expect("waiter should start");
    controller.cancel();
    let wait_result = waiter.await?;
    assert!(wait_result.is_err());
    assert!(controller.is_cancelled());

    Ok(())
}
