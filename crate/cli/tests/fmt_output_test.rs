use serde::{Deserialize, Serialize};
use sinexctl::fmt::{format_json, format_json_lines, format_yaml};
use xtask::sandbox::prelude::*;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct TestItem {
    id: String,
    name: String,
}

// `format_list` and `format_single` write to stdout; regression coverage lives
// in the pinning tests below, which assert exact output bytes on the
// content-producing helpers.

#[sinex_test]
async fn test_format_json_pins_compact_output() -> TestResult<()> {
    let item = TestItem {
        id: "42".to_string(),
        name: "Adams".to_string(),
    };

    let rendered = format_json(&item).expect("format_json must succeed");
    assert_eq!(rendered, r#"{"id":"42","name":"Adams"}"#);
    Ok(())
}

#[sinex_test]
async fn test_format_json_lines_pins_per_item_layout() -> TestResult<()> {
    let items = vec![
        TestItem {
            id: "1".to_string(),
            name: "First".to_string(),
        },
        TestItem {
            id: "2".to_string(),
            name: "Second".to_string(),
        },
    ];

    let rendered = format_json_lines(&items).expect("format_json_lines must succeed");
    assert_eq!(
        rendered,
        "{\"id\":\"1\",\"name\":\"First\"}\n{\"id\":\"2\",\"name\":\"Second\"}\n"
    );
    Ok(())
}

#[sinex_test]
async fn test_format_json_lines_empty_input_yields_empty_string() -> TestResult<()> {
    let items: Vec<TestItem> = vec![];
    let rendered = format_json_lines(&items).expect("format_json_lines must succeed");
    assert!(
        rendered.is_empty(),
        "empty input should yield empty string, got {rendered:?}"
    );
    Ok(())
}

#[sinex_test]
async fn test_format_yaml_pins_field_layout() -> TestResult<()> {
    let item = TestItem {
        id: "1".to_string(),
        name: "Test".to_string(),
    };

    let rendered = format_yaml(&item).expect("format_yaml must succeed");
    assert!(
        rendered.contains("id: '1'") || rendered.contains("id: \"1\"") || rendered.contains("id: 1"),
        "expected `id` field in YAML output, got {rendered:?}"
    );
    assert!(
        rendered.contains("name: Test"),
        "expected `name: Test` in YAML output, got {rendered:?}"
    );
    Ok(())
}
