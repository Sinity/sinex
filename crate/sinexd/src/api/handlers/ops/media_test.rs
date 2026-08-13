use super::super::package::PackageOperationSpec;
use super::*;
use sinex_primitives::privacy::{RuntimePrivateModeState, save_private_mode_state};
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn on_demand_media_actions_cancel_before_consuming_raw_output(
    ctx: TestContext,
) -> TestResult<()> {
    let state_dir = tempfile::tempdir()?;
    save_private_mode_state(
        state_dir.path(),
        &RuntimePrivateModeState::enabled_by("operator", Vec::new(), Timestamp::now()),
    )?;

    let cases = [
        PackageOperationSpec {
            operation_type: "media.screen-ocr.capture-region",
            source_id: "media.screen-ocr",
            default_mode_id: Some("source:media.screen-ocr.on-demand-region"),
            accepted_mode_ids: &["source:media.screen-ocr.on-demand-region"],
            action: "capture_region",
            surface: "media_capture",
            executor_message: "test",
        },
        PackageOperationSpec {
            operation_type: "media.screen-ocr.record-video",
            source_id: "media.screen-ocr",
            default_mode_id: Some("source:media.screen-ocr.on-demand-video"),
            accepted_mode_ids: &["source:media.screen-ocr.on-demand-video"],
            action: "record_video",
            surface: "media_capture",
            executor_message: "test",
        },
        PackageOperationSpec {
            operation_type: "media.screen-ocr.run-ocr",
            source_id: "media.screen-ocr",
            default_mode_id: Some("source:media.screen-ocr.local-model-batch"),
            accepted_mode_ids: &["source:media.screen-ocr.local-model-batch"],
            action: "run_ocr",
            surface: "media_capture",
            executor_message: "test",
        },
        PackageOperationSpec {
            operation_type: "media.audio-transcript.run-model",
            source_id: "media.audio-transcript",
            default_mode_id: Some("source:media.audio-transcript.local-model-batch"),
            accepted_mode_ids: &["source:media.audio-transcript.local-model-batch"],
            action: "run_model",
            surface: "media_capture",
            executor_message: "test",
        },
    ];

    for spec in cases {
        let mode_id = spec.default_mode_id.expect("test case has a mode");
        let mut scope = serde_json::Map::new();
        scope.insert(
            "worker_command".to_string(),
            serde_json::json!({
                "program": "sinex-private-mode-test-command-must-not-run"
            }),
        );
        let mut preview = serde_json::json!({
            "operation_type": spec.operation_type,
        });

        let result = execute_media_operation_with_state_dir(
            ctx.pool(),
            &spec,
            mode_id,
            "test-operator",
            &mut scope,
            &mut preview,
            true,
            state_dir.path(),
        )
        .await?
        .expect("worker-backed media action should have an executor result");

        assert_eq!(result.status, OperationStatus::Cancelled, "{mode_id}");
        assert_eq!(
            scope["executor_state"],
            "media_capture_blocked_private_mode"
        );
        assert_eq!(scope["capture_gate"]["reason"], "private_mode");
        assert!(scope.get("worker_output_material_id").is_none());
        assert_eq!(
            preview["executor_state"],
            "media_capture_blocked_private_mode"
        );
        assert_eq!(preview["capture_gate"]["reason"], "private_mode");
    }
    Ok(())
}
