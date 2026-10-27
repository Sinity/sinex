use super::*;

fn read(line: &str) -> Result<PostPublished, RecordError> {
    let item: Map<String, Value> = serde_json::from_str(line).unwrap();
    RedditArchiveParser
        .parse(&item, &(), &crate::RecordContext::none())
        .map(|(mut out, ())| out.remove(0))
}

#[devkit::test]
fn a_comment_keeps_its_reddit_id_parent_and_text() {
    let c = read(r#"{"id":"abc123","name":"t1_abc123","created_utc":1382141010,"body":"a reply","link_id":"t3_xyz","parent_id":"t1_def","subreddit":"programming","permalink":"/r/programming/comments/xyz/t/abc123/","score":4,"retrieved_on":1429080509}"#).unwrap();
    assert_eq!(
        (c.kind.as_str(), c.native_id.as_str()),
        ("comment", "abc123")
    );
    assert_eq!(
        c.created_at,
        sinex_model::Timestamp::from_unix_secs(1_382_141_010).ok()
    );
    assert_eq!(c.body.as_deref(), Some("a reply"));
    assert_eq!(c.parent.as_deref(), Some("t1_def"));
    assert_eq!(
        c.url.as_deref(),
        Some("https://www.reddit.com/r/programming/comments/xyz/t/abc123/")
    );
    assert_eq!(c.score, Some(4));
    assert_eq!(c.extra["retrieved_on"], 1_429_080_509);
}

#[devkit::test]
fn a_submission_is_a_post_with_its_title_and_text() {
    let p = read(r#"{"id":"1xt00z","name":"t3_1xt00z","created_utc":"1392303469","title":"A question","selftext":"","url":"https://example.org/","subreddit":"Planetside","permalink":"/r/Planetside/comments/1xt00z/q/"}"#).unwrap();
    assert_eq!(p.kind, "post");
    assert_eq!(p.title.as_deref(), Some("A question"));
    assert_eq!(p.body, None, "an empty self text is no text");
    assert_eq!(p.extra["url"], "https://example.org/");
    assert!(read(r#"{"id":"x","name":"t5_x","created_utc":1}"#).is_err());
}

#[devkit::test]
fn the_plan_reads_only_the_archive_files() {
    let dir = tempfile::tempdir().unwrap();
    for name in [
        "comments.jsonl",
        "submissions.jsonl",
        "README.md",
        "GDPR-DELTA.md",
    ] {
        std::fs::write(dir.path().join(name), "").unwrap();
    }
    let plan = (FORMAT.plan.unwrap())(dir.path(), &crate::binding::Settings::default()).unwrap();
    assert_eq!(plan.len(), 2);
}
