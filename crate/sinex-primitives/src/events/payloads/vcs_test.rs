use super::*;
use crate::events::EventPayload;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn declares_source_and_event_type() -> TestResult<()> {
    assert_eq!(GitCommitPayload::SOURCE.as_static_str(), "git");
    assert_eq!(
        GitCommitPayload::EVENT_TYPE.as_static_str(),
        "commit.created"
    );
    Ok(())
}
