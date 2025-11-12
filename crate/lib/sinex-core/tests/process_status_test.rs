use sinex_core::types::events::payloads::process::ProcessStatus;
use sinex_test_utils::{sinex_test, TestContext};

#[sinex_test]
async fn process_status_rejects_invalid_strings() -> color_eyre::Result<()> {
    let serialized = "critical";
    let result: Result<ProcessStatus, _> = serde_json::from_str(&format!("\"{serialized}\""));

    assert!(
        result.is_err(),
        "ProcessStatus should reject unexpected strings instead of silently accepting {serialized}"
    );

    Ok(())
}
