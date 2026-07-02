use super::*;
use crate::events::EventPayload;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn declares_source_and_event_type() -> TestResult<()> {
    assert_eq!(RaindropBookmarkPayload::SOURCE.as_static_str(), "raindrop");
    assert_eq!(
        RaindropBookmarkPayload::EVENT_TYPE.as_static_str(),
        "bookmark.created"
    );
    Ok(())
}
