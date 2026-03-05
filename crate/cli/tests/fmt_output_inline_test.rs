use serde::{Deserialize, Serialize};
use sinexctl::fmt::{format_list, format_single};
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
