use serde_json::json;
use sinex_primitives::strip_postgres_jsonb_nul_chars;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn strip_postgres_jsonb_nul_chars_recurses_through_json_strings() -> TestResult<()> {
    let mut value = json!({
        "title": "a\0b",
        "nested\0object": {
            "url": "https://example.test/\0path",
            "ok": "emoji stays 🚀"
        },
        "items": ["x\0y", 42, true, null]
    });

    let stripped = strip_postgres_jsonb_nul_chars(&mut value);

    assert_eq!(stripped, 4);
    assert_eq!(
        value,
        json!({
            "title": "ab",
            "nestedobject": {
                "url": "https://example.test/path",
                "ok": "emoji stays 🚀"
            },
            "items": ["xy", 42, true, null]
        })
    );
    Ok(())
}

#[sinex_test]
async fn strip_postgres_jsonb_nul_chars_leaves_representable_json_unchanged() -> TestResult<()> {
    let mut value = json!({
        "title": "Elon's Grok 3",
        "url": "https://example.test/?q=zażółć",
        "count": 7
    });
    let expected = value.clone();

    let stripped = strip_postgres_jsonb_nul_chars(&mut value);

    assert_eq!(stripped, 0);
    assert_eq!(value, expected);
    Ok(())
}
