use super::*;
use sinex_primitives::Uuid;
use sinex_primitives::ids::Id;

use xtask::sandbox::prelude::sinex_test;

// -----------------------------------------------------------------------
// Test helpers
// -----------------------------------------------------------------------

fn comment_ctx() -> ParserContext {
    ParserContext {
        source_id: SourceId::from_static("reddit-gdpr-comments"),
        source_material_id: Id::new(),
        record_anchor: MaterialAnchor::ByteRange { start: 0, len: 0 },
        operation_id: Uuid::new_v4(),
        job_id: Uuid::new_v4(),
        host: "test-host".into(),
        acquisition_time: Timestamp::now(),
    }
}

fn post_ctx() -> ParserContext {
    ParserContext {
        source_id: SourceId::from_static("reddit-gdpr-posts"),
        source_material_id: Id::new(),
        record_anchor: MaterialAnchor::ByteRange { start: 0, len: 0 },
        operation_id: Uuid::new_v4(),
        job_id: Uuid::new_v4(),
        host: "test-host".into(),
        acquisition_time: Timestamp::now(),
    }
}

fn wykop_entry_ctx() -> ParserContext {
    ParserContext {
        source_id: SourceId::from_static("wykop-entries"),
        source_material_id: Id::new(),
        record_anchor: MaterialAnchor::ByteRange { start: 0, len: 0 },
        operation_id: Uuid::new_v4(),
        job_id: Uuid::new_v4(),
        host: "test-host".into(),
        acquisition_time: Timestamp::now(),
    }
}

fn wykop_entry_comment_ctx() -> ParserContext {
    ParserContext {
        source_id: SourceId::from_static("wykop-entry-comments"),
        source_material_id: Id::new(),
        record_anchor: MaterialAnchor::ByteRange { start: 0, len: 0 },
        operation_id: Uuid::new_v4(),
        job_id: Uuid::new_v4(),
        host: "test-host".into(),
        acquisition_time: Timestamp::now(),
    }
}

fn record_for(bytes: &[u8]) -> SourceRecord {
    SourceRecord {
        material_id: Id::new(),
        anchor: MaterialAnchor::ByteRange {
            start: 0,
            len: bytes.len() as u64,
        },
        bytes: bytes.to_vec(),
        logical_path: None,
        source_ts_hint: None,
        metadata: serde_json::Value::Null,
    }
}

// -----------------------------------------------------------------------
// Reddit comments
// -----------------------------------------------------------------------

const COMMENT_CSV: &str = "id,permalink,date,ip,subreddit,gildings,link,parent,body,media\n\
     ck1fsao,https://www.reddit.com/r/Futurology/comments/2em2io/elon_musk_warns_ais_could_exterminate_humanity/ck1fsao/,2014-08-27 00:59:46 UTC,,Futurology,0,https://www.reddit.com/r/Futurology/comments/2em2io/,ck1bai1,\"Great comment body.\",\n\
     ck1to2z,https://www.reddit.com/r/Futurology/comments/2em2io/elon_musk_warns_ais_could_exterminate_humanity/ck1to2z/,2014-08-27 13:36:36 UTC,,Futurology,0,https://www.reddit.com/r/Futurology/comments/2em2io/,ck1k0yu,Another comment.,\n";

#[sinex_test]
async fn reddit_comments_parses_two_rows() -> TestResult<()> {
    let mut parser = RedditCommentParser;
    let intents = parser
        .parse_record(record_for(COMMENT_CSV.as_bytes()), &comment_ctx())
        .await
        .unwrap();
    assert_eq!(intents.len(), 2);
    for intent in &intents {
        assert_eq!(intent.event_source.as_str(), "reddit");
        assert_eq!(intent.event_type.as_str(), "social.comment.posted");
    }
    Ok(())
}

#[sinex_test]
async fn reddit_comment_preserves_id_and_subreddit() -> TestResult<()> {
    let mut parser = RedditCommentParser;
    let intents = parser
        .parse_record(record_for(COMMENT_CSV.as_bytes()), &comment_ctx())
        .await
        .unwrap();
    assert_eq!(intents[0].payload["reddit_id"], "ck1fsao");
    assert_eq!(intents[0].payload["subreddit"], "Futurology");
    Ok(())
}

#[sinex_test]
async fn reddit_comment_anchor_is_one_based_line() -> TestResult<()> {
    let mut parser = RedditCommentParser;
    let intents = parser
        .parse_record(record_for(COMMENT_CSV.as_bytes()), &comment_ctx())
        .await
        .unwrap();
    assert!(matches!(
        intents[0].anchor,
        MaterialAnchor::Line { line: 1, .. }
    ));
    assert!(matches!(
        intents[1].anchor,
        MaterialAnchor::Line { line: 2, .. }
    ));
    Ok(())
}

#[sinex_test]
async fn reddit_comment_occurrence_key_uses_id_and_subreddit() -> TestResult<()> {
    let mut parser = RedditCommentParser;
    let intents = parser
        .parse_record(record_for(COMMENT_CSV.as_bytes()), &comment_ctx())
        .await
        .unwrap();
    let key = intents[0].occurrence_key.as_ref().unwrap();
    assert_eq!(
        key.fields,
        vec![
            ("reddit_id".into(), "ck1fsao".into()),
            ("subreddit".into(), "Futurology".into()),
        ]
    );
    Ok(())
}

#[sinex_test]
async fn reddit_comment_ip_gildings_media_absent_from_payload() -> TestResult<()> {
    let mut parser = RedditCommentParser;
    let intents = parser
        .parse_record(record_for(COMMENT_CSV.as_bytes()), &comment_ctx())
        .await
        .unwrap();
    let payload = &intents[0].payload;
    assert!(payload.get("ip").is_none());
    assert!(payload.get("gildings").is_none());
    assert!(payload.get("media").is_none());
    Ok(())
}

#[sinex_test]
async fn reddit_comment_timestamp_parses_utc_format() -> TestResult<()> {
    let mut parser = RedditCommentParser;
    let intents = parser
        .parse_record(record_for(COMMENT_CSV.as_bytes()), &comment_ctx())
        .await
        .unwrap();
    let ts = intents[0].ts_orig.inner();
    assert_eq!(ts.year(), 2014);
    assert_eq!(ts.month() as u8, 8);
    assert_eq!(ts.day(), 27);
    Ok(())
}

#[sinex_test]
async fn reddit_comment_invalid_timestamp_errors() -> TestResult<()> {
    let bad = "id,permalink,date,ip,subreddit,gildings,link,parent,body,media\n\
        abc,,not-a-date,,Science,0,,,body,\n";
    let mut parser = RedditCommentParser;
    let err = parser
        .parse_record(record_for(bad.as_bytes()), &comment_ctx())
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("invalid Reddit timestamp"), "got: {err}");
    Ok(())
}

// -----------------------------------------------------------------------
// Reddit posts
// -----------------------------------------------------------------------

const POST_CSV: &str = "id,permalink,date,ip,subreddit,gildings,title,url,body\n\
     38focg,https://www.reddit.com/r/kindle/comments/38focg/kindle_5621_rootjailbreak/,2015-06-03 22:18:00 UTC,,kindle,0,Kindle root/jailbreak,/r/kindle/comments/38focg/,\"Post body text.\"\n\
     3a1oqo,https://www.reddit.com/r/oculus/comments/3a1oqo/when_should_i_expect/,2015-06-16 15:17:27 UTC,,oculus,0,When should I expect CV1?,/r/oculus/comments/3a1oqo/,\n";

#[sinex_test]
async fn reddit_posts_parses_two_rows() -> TestResult<()> {
    let mut parser = RedditPostParser;
    let intents = parser
        .parse_record(record_for(POST_CSV.as_bytes()), &post_ctx())
        .await
        .unwrap();
    assert_eq!(intents.len(), 2);
    for intent in &intents {
        assert_eq!(intent.event_source.as_str(), "reddit");
        assert_eq!(intent.event_type.as_str(), "social.post.created");
    }
    Ok(())
}

#[sinex_test]
async fn reddit_post_preserves_title() -> TestResult<()> {
    let mut parser = RedditPostParser;
    let intents = parser
        .parse_record(record_for(POST_CSV.as_bytes()), &post_ctx())
        .await
        .unwrap();
    assert_eq!(intents[0].payload["title"], "Kindle root/jailbreak");
    Ok(())
}

#[sinex_test]
async fn reddit_post_empty_body_becomes_null() -> TestResult<()> {
    let mut parser = RedditPostParser;
    let intents = parser
        .parse_record(record_for(POST_CSV.as_bytes()), &post_ctx())
        .await
        .unwrap();
    // Second row has no body
    assert!(intents[1].payload["body"].is_null());
    Ok(())
}

#[sinex_test]
async fn reddit_post_occurrence_key_uses_id_and_subreddit() -> TestResult<()> {
    let mut parser = RedditPostParser;
    let intents = parser
        .parse_record(record_for(POST_CSV.as_bytes()), &post_ctx())
        .await
        .unwrap();
    let key = intents[0].occurrence_key.as_ref().unwrap();
    assert_eq!(
        key.fields,
        vec![
            ("reddit_id".into(), "38focg".into()),
            ("subreddit".into(), "kindle".into()),
        ]
    );
    Ok(())
}

// -----------------------------------------------------------------------
// Wykop entries
// -----------------------------------------------------------------------

const WYKOP_ENTRIES_JSONL: &str = "{\"platform\":\"wykop\",\"kind\":\"entry\",\"username\":\"Sinity\",\"page\":1,\"entry_id\":76315507,\"entry_url\":\"https://wykop.pl/wpis/76315507/piosenka\",\"entry_created_at\":\"2024-05-18 06:53:25\",\"entry_author\":\"Sinity\",\"entry_content\":\"Piosenka o cenzopapie\",\"entry_tags\":[\"humor\",\"sztucznainteligencja\"],\"entry_photo_url\":null,\"votes_score\":0,\"votes_up\":0,\"votes_down\":0}\n\
     {\"platform\":\"wykop\",\"kind\":\"entry\",\"username\":\"Sinity\",\"page\":1,\"entry_id\":76315508,\"entry_url\":\"https://wykop.pl/wpis/76315508/test\",\"entry_created_at\":\"2024-05-19 10:00:00\",\"entry_author\":\"Sinity\",\"entry_content\":\"Test entry\",\"entry_tags\":[],\"entry_photo_url\":\"https://example.com/photo.jpg\",\"votes_score\":5,\"votes_up\":5,\"votes_down\":0}\n";

#[sinex_test]
async fn wykop_entries_parses_two_lines() -> TestResult<()> {
    let mut parser = WykopEntryParser;
    let intents = parser
        .parse_record(
            record_for(WYKOP_ENTRIES_JSONL.as_bytes()),
            &wykop_entry_ctx(),
        )
        .await
        .unwrap();
    assert_eq!(intents.len(), 2);
    for intent in &intents {
        assert_eq!(intent.event_source.as_str(), "wykop");
        assert_eq!(intent.event_type.as_str(), "social.entry.created");
    }
    Ok(())
}

#[sinex_test]
async fn wykop_entry_preserves_id_content_tags() -> TestResult<()> {
    let mut parser = WykopEntryParser;
    let intents = parser
        .parse_record(
            record_for(WYKOP_ENTRIES_JSONL.as_bytes()),
            &wykop_entry_ctx(),
        )
        .await
        .unwrap();
    assert_eq!(intents[0].payload["entry_id"], 76315507u64);
    assert_eq!(intents[0].payload["content"], "Piosenka o cenzopapie");
    assert_eq!(
        intents[0].payload["tags"],
        serde_json::json!(["humor", "sztucznainteligencja"])
    );
    Ok(())
}

#[sinex_test]
async fn wykop_entry_null_photo_url_becomes_null() -> TestResult<()> {
    let mut parser = WykopEntryParser;
    let intents = parser
        .parse_record(
            record_for(WYKOP_ENTRIES_JSONL.as_bytes()),
            &wykop_entry_ctx(),
        )
        .await
        .unwrap();
    assert!(intents[0].payload["photo_url"].is_null());
    assert_eq!(
        intents[1].payload["photo_url"],
        "https://example.com/photo.jpg"
    );
    Ok(())
}

#[sinex_test]
async fn wykop_entry_occurrence_key_uses_entry_id() -> TestResult<()> {
    let mut parser = WykopEntryParser;
    let intents = parser
        .parse_record(
            record_for(WYKOP_ENTRIES_JSONL.as_bytes()),
            &wykop_entry_ctx(),
        )
        .await
        .unwrap();
    let key = intents[0].occurrence_key.as_ref().unwrap();
    assert_eq!(key.fields, vec![("entry_id".into(), "76315507".into())]);
    Ok(())
}

#[sinex_test]
async fn wykop_entry_timestamp_parses_datetime() -> TestResult<()> {
    let mut parser = WykopEntryParser;
    let intents = parser
        .parse_record(
            record_for(WYKOP_ENTRIES_JSONL.as_bytes()),
            &wykop_entry_ctx(),
        )
        .await
        .unwrap();
    let ts = intents[0].ts_orig.inner();
    assert_eq!(ts.year(), 2024);
    assert_eq!(ts.month() as u8, 5);
    assert_eq!(ts.day(), 18);
    Ok(())
}

#[sinex_test]
async fn wykop_entry_invalid_timestamp_errors() -> TestResult<()> {
    let bad = "{\"platform\":\"wykop\",\"kind\":\"entry\",\"username\":\"Sinity\",\"page\":1,\"entry_id\":1,\"entry_url\":\"https://wykop.pl/wpis/1/x\",\"entry_created_at\":\"not-a-time\",\"entry_author\":\"Sinity\",\"entry_content\":\"x\",\"entry_tags\":[],\"entry_photo_url\":null,\"votes_score\":0,\"votes_up\":0,\"votes_down\":0}\n";
    let mut parser = WykopEntryParser;
    let err = parser
        .parse_record(record_for(bad.as_bytes()), &wykop_entry_ctx())
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("invalid Wykop timestamp"), "got: {err}");
    Ok(())
}

// -----------------------------------------------------------------------
// Wykop entry comments
// -----------------------------------------------------------------------

const WYKOP_COMMENTS_JSONL: &str = "{\"platform\":\"wykop\",\"kind\":\"entry_comment\",\"username\":\"Sinity\",\"page\":1,\"comment_id\":279391731,\"comment_created_at\":\"2025-02-16 08:21:58\",\"comment_content\":\"Nice entry!\",\"comment_photo_url\":null,\"comment_rating\":2,\"entry_id\":80205363,\"entry_url\":\"https://wykop.pl/wpis/80205363/x\"}\n\
     {\"platform\":\"wykop\",\"kind\":\"entry_comment\",\"username\":\"Sinity\",\"page\":1,\"comment_id\":279391732,\"comment_created_at\":\"2025-02-17 09:00:00\",\"comment_content\":\"Another reply\",\"comment_photo_url\":\"https://example.com/img.png\",\"comment_rating\":0,\"entry_id\":80205364,\"entry_url\":\"https://wykop.pl/wpis/80205364/y\"}\n";

#[sinex_test]
async fn wykop_entry_comments_parses_two_lines() -> TestResult<()> {
    let mut parser = WykopEntryCommentParser;
    let intents = parser
        .parse_record(
            record_for(WYKOP_COMMENTS_JSONL.as_bytes()),
            &wykop_entry_comment_ctx(),
        )
        .await
        .unwrap();
    assert_eq!(intents.len(), 2);
    for intent in &intents {
        assert_eq!(intent.event_source.as_str(), "wykop");
        assert_eq!(intent.event_type.as_str(), "social.entry_comment.posted");
    }
    Ok(())
}

#[sinex_test]
async fn wykop_entry_comment_preserves_ids_and_content() -> TestResult<()> {
    let mut parser = WykopEntryCommentParser;
    let intents = parser
        .parse_record(
            record_for(WYKOP_COMMENTS_JSONL.as_bytes()),
            &wykop_entry_comment_ctx(),
        )
        .await
        .unwrap();
    assert_eq!(intents[0].payload["comment_id"], 279391731u64);
    assert_eq!(intents[0].payload["entry_id"], 80205363u64);
    assert_eq!(intents[0].payload["content"], "Nice entry!");
    Ok(())
}

#[sinex_test]
async fn wykop_entry_comment_occurrence_key_uses_comment_id() -> TestResult<()> {
    let mut parser = WykopEntryCommentParser;
    let intents = parser
        .parse_record(
            record_for(WYKOP_COMMENTS_JSONL.as_bytes()),
            &wykop_entry_comment_ctx(),
        )
        .await
        .unwrap();
    let key = intents[0].occurrence_key.as_ref().unwrap();
    assert_eq!(key.fields, vec![("comment_id".into(), "279391731".into())]);
    Ok(())
}

#[sinex_test]
async fn wykop_entry_comment_photo_url_present_and_absent() -> TestResult<()> {
    let mut parser = WykopEntryCommentParser;
    let intents = parser
        .parse_record(
            record_for(WYKOP_COMMENTS_JSONL.as_bytes()),
            &wykop_entry_comment_ctx(),
        )
        .await
        .unwrap();
    assert!(intents[0].payload["photo_url"].is_null());
    assert_eq!(
        intents[1].payload["photo_url"],
        "https://example.com/img.png"
    );
    Ok(())
}
