//! Production-path obligation tests for social export parsers.
//!
//! These cases keep host-independent social export parsers on the same
//! source-unit host obligation harness as other production-path source units.

#[cfg(test)]
mod tests {
    use xtask::sandbox::prelude::*;

    const REDDIT_COMMENT_CSV: &[u8] = b"\
id,permalink,date,ip,subreddit,gildings,link,parent,body,media
ck1fsao,https://www.reddit.com/r/Futurology/comments/2em2io/ck1fsao/,2014-08-27 00:59:46 UTC,,Futurology,0,https://www.reddit.com/r/Futurology/comments/2em2io/,ck1bai1,\"Great comment body.\",
";

    const REDDIT_POST_CSV: &[u8] = b"\
id,permalink,date,ip,subreddit,gildings,title,url,body
38focg,https://www.reddit.com/r/kindle/comments/38focg/kindle_5621_rootjailbreak/,2015-06-03 22:18:00 UTC,,kindle,0,Kindle root jailbreak,/r/kindle/comments/38focg/,\"Post body text.\"
";

    const WYKOP_ENTRIES_JSONL: &[u8] = br#"{"platform":"wykop","kind":"entry","username":"Sinity","page":1,"entry_id":76315507,"entry_url":"https://wykop.pl/wpis/76315507/test","entry_created_at":"2024-05-18 06:53:25","entry_author":"Sinity","entry_content":"Test entry","entry_tags":["humor"],"entry_photo_url":null,"votes_score":0,"votes_up":0,"votes_down":0}
"#;

    const WYKOP_COMMENTS_JSONL: &[u8] = br#"{"platform":"wykop","kind":"entry_comment","username":"Sinity","page":1,"comment_id":279391731,"comment_created_at":"2025-02-16 08:21:58","comment_content":"Nice entry","comment_photo_url":null,"comment_rating":2,"entry_id":80205363,"entry_url":"https://wykop.pl/wpis/80205363/x"}
"#;

    #[sinex_test]
    async fn reddit_gdpr_comments_obligations() -> TestResult<()> {
        let failures = crate::_run_case(
            "reddit-gdpr-comments",
            crate::AdapterKind::StaticFile,
            REDDIT_COMMENT_CSV,
            &["social.comment.posted"],
            crate::ALL_OBLIGATIONS,
        )
        .await;
        assert!(
            failures.is_empty(),
            "reddit-gdpr-comments obligations failed: {failures:#?}"
        );
        Ok(())
    }

    #[sinex_test]
    async fn reddit_gdpr_posts_obligations() -> TestResult<()> {
        let failures = crate::_run_case(
            "reddit-gdpr-posts",
            crate::AdapterKind::StaticFile,
            REDDIT_POST_CSV,
            &["social.post.created"],
            crate::ALL_OBLIGATIONS,
        )
        .await;
        assert!(
            failures.is_empty(),
            "reddit-gdpr-posts obligations failed: {failures:#?}"
        );
        Ok(())
    }

    #[sinex_test]
    async fn wykop_entries_obligations() -> TestResult<()> {
        let failures = crate::_run_case(
            "wykop-entries",
            crate::AdapterKind::StaticFile,
            WYKOP_ENTRIES_JSONL,
            &["social.entry.created"],
            crate::ALL_OBLIGATIONS,
        )
        .await;
        assert!(
            failures.is_empty(),
            "wykop-entries obligations failed: {failures:#?}"
        );
        Ok(())
    }

    #[sinex_test]
    async fn wykop_entry_comments_obligations() -> TestResult<()> {
        let failures = crate::_run_case(
            "wykop-entry-comments",
            crate::AdapterKind::StaticFile,
            WYKOP_COMMENTS_JSONL,
            &["social.entry_comment.posted"],
            crate::ALL_OBLIGATIONS,
        )
        .await;
        assert!(
            failures.is_empty(),
            "wykop-entry-comments obligations failed: {failures:#?}"
        );
        Ok(())
    }
}
