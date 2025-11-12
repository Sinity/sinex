use sinex_satellite_sdk::replay::ReplayController;
use sinex_test_utils::{sinex_test, TestContext};

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

    tokio::spawn(async move {
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        controller_clone.resume();
    });

    assert!(controller.wait_if_paused().await.is_ok());
    assert!(!controller.is_paused());

    Ok(())
}

#[sinex_test]
async fn wait_if_paused_errors_on_cancel() -> color_eyre::Result<()> {
    let controller = ReplayController::new();
    let controller_clone = controller.clone();

    controller.pause();

    tokio::spawn(async move {
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        controller_clone.cancel();
    });

    assert!(controller.wait_if_paused().await.is_err());
    assert!(controller.is_cancelled());

    Ok(())
}
