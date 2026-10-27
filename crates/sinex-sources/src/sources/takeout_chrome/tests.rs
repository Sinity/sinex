use super::*;
use serde_json::json;

#[devkit::test]
fn reads_an_entry_and_keeps_new_fields() {
    let row = json!({"favicon_url": "https://a/f.ico", "page_transition": "LINK", "title": "A",
        "url": "https://a/", "client_id": "c==", "time_usec": 1_565_000_000_123_456i64,
        "page_transition_qualifier": "CLIENT_REDIRECT"});
    let (p, ()) = TakeoutChromeParser
        .parse(
            row.as_object().unwrap(),
            &(),
            &sinex_source_core::RecordContext::none(),
        )
        .unwrap();
    assert_eq!(p[0].visited_at.unix_ns(), 1_565_000_000_123_456_000);
    assert_eq!(p[0].page_transition.as_deref(), Some("LINK"));
    assert_eq!(
        p[0].extra.get("page_transition_qualifier"),
        Some(&json!("CLIENT_REDIRECT"))
    );
}

#[devkit::test]
fn selects_both_member_names() {
    assert!(select("Takeout/Chrome/BrowserHistory.json"));
    assert!(select("Takeout/Chrome/History.json"));
    assert!(!select("Takeout/Chrome/Bookmarks.html"));
}
