use super::*;
use serde_json::json;
use xtask::sandbox::sinex_test;

#[sinex_test]
#[ignore = "sinex-ufgc open: NUL-stripping on JSON object keys silently collapses two distinct keys that differ only by an embedded NUL into one, losing data"]
async fn strip_postgres_jsonb_nul_chars_does_not_collapse_distinct_keys()
-> ::xtask::sandbox::TestResult<()> {
    // "a\0" and "a" both strip down to "a" -- map::insert silently drops one entry.
    let mut value = json!({
        "a\u{0}": "first",
        "a": "second",
    });
    let map_len_before = value.as_object().unwrap().len();
    assert_eq!(map_len_before, 2, "test setup: two distinct keys expected");

    strip_postgres_jsonb_nul_chars(&mut value);

    let map = value.as_object().unwrap();
    assert_eq!(
        map.len(),
        2,
        "NUL-stripping collapsed two distinct keys into one -- data loss (map now has {} entries: {:?})",
        map.len(),
        map
    );
    Ok(())
}

#[sinex_test]
async fn validate_json_value_rejects_at_documented_depth_limit() -> ::xtask::sandbox::TestResult<()>
{
    // sinex-ufgc's "off-by-one, admits 33 levels" claim was FALSE: a 33-level
    // wrapper around a leaf value puts the leaf's own check at depth=33 (root
    // checked at depth=0, each nesting increments by 1), and `depth >
    // MAX_JSON_DEPTH` (32) correctly rejects at exactly that point. Kept as a
    // real (non-ignored) regression test pinning the documented boundary.
    let mut value = json!(1);
    for _ in 0..33 {
        value = json!({ "n": value });
    }

    let result = validate_json_value(&value);
    assert!(
        result.is_err(),
        "33-level-deep JSON should be rejected against the documented MAX_JSON_DEPTH=32 limit, but validate_json_value accepted it"
    );
    Ok(())
}
