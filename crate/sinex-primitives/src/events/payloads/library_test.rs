use super::*;
use crate::events::EventPayload;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn declares_source_and_event_type() -> TestResult<()> {
    assert_eq!(
        LibraryDocumentIndexedPayload::SOURCE.as_static_str(),
        "docs-library"
    );
    assert_eq!(
        LibraryDocumentIndexedPayload::EVENT_TYPE.as_static_str(),
        "document.indexed"
    );
    Ok(())
}
