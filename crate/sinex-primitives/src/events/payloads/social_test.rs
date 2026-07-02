use super::*;
use crate::events::EventPayload;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn reddit_comment_declares_source_and_event_type() -> TestResult<()> {
    assert_eq!(RedditCommentPayload::SOURCE.as_static_str(), "reddit");
    assert_eq!(
        RedditCommentPayload::EVENT_TYPE.as_static_str(),
        "social.comment.posted"
    );
    Ok(())
}

#[sinex_test]
async fn reddit_post_declares_source_and_event_type() -> TestResult<()> {
    assert_eq!(RedditPostPayload::SOURCE.as_static_str(), "reddit");
    assert_eq!(
        RedditPostPayload::EVENT_TYPE.as_static_str(),
        "social.post.created"
    );
    Ok(())
}

#[sinex_test]
async fn wykop_entry_declares_source_and_event_type() -> TestResult<()> {
    assert_eq!(WykopEntryPayload::SOURCE.as_static_str(), "wykop");
    assert_eq!(
        WykopEntryPayload::EVENT_TYPE.as_static_str(),
        "social.entry.created"
    );
    Ok(())
}

#[sinex_test]
async fn wykop_entry_comment_declares_source_and_event_type() -> TestResult<()> {
    assert_eq!(WykopEntryCommentPayload::SOURCE.as_static_str(), "wykop");
    assert_eq!(
        WykopEntryCommentPayload::EVENT_TYPE.as_static_str(),
        "social.entry_comment.posted"
    );
    Ok(())
}
