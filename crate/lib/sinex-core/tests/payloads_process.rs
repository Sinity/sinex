use sinex_core::types::events::event_payload::EventPayload;
use sinex_core::types::events::payloads::process::ProcessHeartbeatPayload;
use sinex_test_utils::sinex_test;

#[sinex_test]
fn process_payload_exposes_event_metadata() -> color_eyre::eyre::Result<()> {
    assert_eq!(ProcessHeartbeatPayload::SOURCE.as_str(), "sinex");
    assert_eq!(
        ProcessHeartbeatPayload::EVENT_TYPE.as_str(),
        "process.heartbeat"
    );
    Ok(())
}
