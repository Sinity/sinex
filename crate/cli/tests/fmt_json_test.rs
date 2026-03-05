use serde_json::json;
use sinexctl::fmt::{format_json, format_json_lines};
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn format_json_simple_object() -> TestResult<()> {
    let val = json!({"name": "test", "count": 42});
    let result = format_json(&val).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
    assert_eq!(parsed["name"], "test");
    assert_eq!(parsed["count"], 42);
    Ok(())
}

#[sinex_test]
async fn format_json_special_chars() -> TestResult<()> {
    let val = json!({"text": "hello \"world\"\nnewline\\backslash"});
    let result = format_json(&val).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
    assert_eq!(parsed["text"], "hello \"world\"\nnewline\\backslash");
    Ok(())
}

#[sinex_test]
async fn format_json_empty_object() -> TestResult<()> {
    let val = json!({});
    let result = format_json(&val).unwrap();
    assert_eq!(result, "{}");
    Ok(())
}

#[sinex_test]
async fn format_json_null() -> TestResult<()> {
    let val = json!(null);
    let result = format_json(&val).unwrap();
    assert_eq!(result, "null");
    Ok(())
}

#[sinex_test]
async fn format_json_nested() -> TestResult<()> {
    let val = json!({"a": {"b": {"c": [1, 2, 3]}}});
    let result = format_json(&val).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
    assert_eq!(parsed["a"]["b"]["c"][1], 2);
    Ok(())
}

#[sinex_test]
async fn format_json_lines_empty() -> TestResult<()> {
    let items: Vec<serde_json::Value> = vec![];
    let result = format_json_lines(&items).unwrap();
    assert_eq!(result, "");
    Ok(())
}

#[sinex_test]
async fn format_json_lines_multiple() -> TestResult<()> {
    let items = vec![json!({"id": 1}), json!({"id": 2}), json!({"id": 3})];
    let result = format_json_lines(&items).unwrap();
    let lines: Vec<&str> = result.trim().lines().collect();
    assert_eq!(lines.len(), 3);
    for line in lines {
        serde_json::from_str::<serde_json::Value>(line).unwrap();
    }
    Ok(())
}

#[sinex_test]
async fn format_json_lines_each_line_terminated() -> TestResult<()> {
    let items = vec![json!({"x": 1})];
    let result = format_json_lines(&items).unwrap();
    assert!(result.ends_with('\n'), "each line should end with newline");
    Ok(())
}

#[sinex_test]
async fn format_json_unicode() -> TestResult<()> {
    let val = json!({"emoji": "🎉", "cjk": "日本語", "rtl": "مرحبا"});
    let result = format_json(&val).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
    assert_eq!(parsed["emoji"], "🎉");
    assert_eq!(parsed["cjk"], "日本語");
    Ok(())
}
