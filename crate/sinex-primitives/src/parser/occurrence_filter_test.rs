use super::*;
use crate::parser::SourceId;
use xtask::sandbox::prelude::sinex_test;

#[sinex_test]
async fn empty_filter_contains_nothing() -> xtask::sandbox::TestResult<()> {
    let f = OccurrenceFilter::empty();
    assert!(!f.contains("anything"));
    Ok(())
}

#[sinex_test]
async fn insert_then_contains_returns_true() -> xtask::sandbox::TestResult<()> {
    let mut f = OccurrenceFilter::empty();
    f.insert("key-a".to_string());
    assert!(f.contains("key-a"));
    assert!(!f.contains("key-b"));
    assert_eq!(f.len(), 1);
    Ok(())
}

#[sinex_test]
async fn from_keys_builds_correctly() -> xtask::sandbox::TestResult<()> {
    let f = OccurrenceFilter::from_keys(["a".to_string(), "b".to_string(), "c".to_string()]);
    assert_eq!(f.len(), 3);
    assert!(f.contains("a"));
    assert!(f.contains("b"));
    assert!(f.contains("c"));
    assert!(!f.contains("d"));
    Ok(())
}

#[sinex_test]
async fn duplicate_insert_is_idempotent() -> xtask::sandbox::TestResult<()> {
    let mut f = OccurrenceFilter::empty();
    f.insert("dup".to_string());
    f.insert("dup".to_string());
    assert_eq!(f.len(), 1);
    Ok(())
}

#[sinex_test]
async fn occurrence_key_string_format() -> xtask::sandbox::TestResult<()> {
    let key = OccurrenceKey {
        source_id: SourceId::from_static("test.unit"),
        fields: vec![("a".into(), "1".into()), ("b".into(), "hello".into())],
    };
    let s = occurrence_key_string(&key);
    assert_eq!(s, "test.unit|a=1|b=hello");
    Ok(())
}

#[sinex_test]
async fn maybe_occurrence_key_string_some_and_none() -> xtask::sandbox::TestResult<()> {
    let key = OccurrenceKey {
        source_id: SourceId::from_static("test.unit"),
        fields: vec![("x".into(), "y".into())],
    };
    assert_eq!(
        maybe_occurrence_key_string(Some(&key)),
        Some("test.unit|x=y".to_string())
    );
    assert_eq!(maybe_occurrence_key_string(None), None);
    Ok(())
}

#[sinex_test]
async fn escaping_prevents_delimiter_injection_collision() -> xtask::sandbox::TestResult<()> {
    // Without escaping, `(foo, "bar|baz")` and `(foo|bar, "baz")`
    // would both encode as `test.unit|foo=bar|baz` and silently
    // dedup against each other. With escaping they are distinct.
    let k1 = OccurrenceKey {
        source_id: SourceId::from_static("test.unit"),
        fields: vec![("foo".into(), "bar|baz".into())],
    };
    let k2 = OccurrenceKey {
        source_id: SourceId::from_static("test.unit"),
        fields: vec![("foo|bar".into(), "baz".into())],
    };
    let s1 = occurrence_key_string(&k1);
    let s2 = occurrence_key_string(&k2);
    assert_ne!(s1, s2, "escaping must keep these two keys distinct");
    assert_eq!(s1, "test.unit|foo=bar\\|baz");
    assert_eq!(s2, "test.unit|foo\\|bar=baz");
    Ok(())
}

#[sinex_test]
async fn escaping_handles_equals_and_backslash() -> xtask::sandbox::TestResult<()> {
    let k = OccurrenceKey {
        source_id: SourceId::from_static("test.unit"),
        fields: vec![("name=raw".into(), "value\\with\\slash".into())],
    };
    let s = occurrence_key_string(&k);
    assert_eq!(s, "test.unit|name\\=raw=value\\\\with\\\\slash");
    Ok(())
}
