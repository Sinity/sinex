use sinex_core::types::events::payloads::process::ProcessHeartbeatPayload;
use sinex_core::EventPayload;
use sinex_test_utils::sinex_test;
use sinex_test_utils::TestResult;

#[sinex_test]
fn process_payload_exposes_event_metadata() -> TestResult<()> {
    assert_eq!(ProcessHeartbeatPayload::SOURCE.as_str(), "sinex");
    assert_eq!(
        ProcessHeartbeatPayload::EVENT_TYPE.as_str(),
        "process.heartbeat"
    );
    Ok(())
}
