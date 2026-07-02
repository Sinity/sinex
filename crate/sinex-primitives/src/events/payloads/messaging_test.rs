use super::*;
use crate::events::EventPayload;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn declares_source_and_event_type() -> TestResult<()> {
    assert_eq!(
        MessengerMessageSentPayload::SOURCE.as_static_str(),
        "messenger"
    );
    assert_eq!(
        MessengerMessageSentPayload::EVENT_TYPE.as_static_str(),
        "message.sent"
    );
    Ok(())
}
