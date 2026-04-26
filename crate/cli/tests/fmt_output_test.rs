use serde::{Deserialize, Serialize};
use sinexctl::fmt::{format_json, format_json_lines, format_list, format_single, format_yaml};
use sinexctl::model::OutputFormat;
use xtask::sandbox::prelude::*;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct TestItem {
    id: String,
    name: String,
}

fn format_test_table(items: &[TestItem]) -> String {
    items
        .iter()
        .map(|item| format!("{}: {}", item.id, item.name))
        .collect::<Vec<_>>()
        .join("\n")
}

fn format_single_test(item: &TestItem) -> String {
    format!("{}: {}", item.id, item.name)
}

#[sinex_test]
async fn test_format_list_table() -> TestResult<()> {
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

    let result = format_list(&items, &OutputFormat::Table, "No items", format_test_table);
    assert!(result.is_ok());
    Ok(())
}

#[sinex_test]
async fn test_format_list_empty() -> TestResult<()> {
    let items: Vec<TestItem> = vec![];

    let result = format_list(&items, &OutputFormat::Table, "No items", format_test_table);
    assert!(result.is_ok());

    let result = format_list(&items, &OutputFormat::Json, "No items", format_test_table);
    assert!(result.is_ok());
    Ok(())
}

#[sinex_test]
async fn test_format_single() -> TestResult<()> {
    let item = TestItem {
        id: "1".to_string(),
        name: "Test".to_string(),
    };

    let result = format_single(&item, &OutputFormat::Table, format_single_test);
    assert!(result.is_ok());

    let result = format_single(&item, &OutputFormat::Json, format_single_test);
    assert!(result.is_ok());
    Ok(())
}

// `format_list` and `format_single` write to stdout, so the existing tests
// above can only assert that serialization did not fail. The tests below
// pin actual output bytes for the underlying content-producing helpers,
// which is where format-correctness regressions are most likely to surface.

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
