use color_eyre::Result;
use serde::Serialize;

use super::{format_json, format_yaml};
use crate::model::OutputFormat;

/// Format a list of items for output, handling empty results gracefully
pub fn format_list<T: Serialize>(
    items: &[T],
    format: &OutputFormat,
    empty_msg: &str,
    table_formatter: impl FnOnce(&[T]) -> String,
) -> Result<()> {
    if items.is_empty() {
        match format {
            OutputFormat::Table => println!("{}", empty_msg),
            OutputFormat::Json => println!("[]"),
            OutputFormat::Yaml => println!("[]"),
        }
        return Ok(());
    }

    match format {
        OutputFormat::Table => println!("{}", table_formatter(items)),
        OutputFormat::Json => {
            for item in items {
                println!("{}", format_json(item)?);
            }
        }
        OutputFormat::Yaml => {
            // Need to convert slice to Vec for yaml serialization
            let items_vec: Vec<&T> = items.iter().collect();
            println!("{}", format_yaml(&items_vec)?);
        }
    }
    Ok(())
}

/// Format a single item for output
pub fn format_single<T: Serialize>(
    item: &T,
    format: &OutputFormat,
    table_formatter: impl FnOnce(&T) -> String,
) -> Result<()> {
    match format {
        OutputFormat::Table => println!("{}", table_formatter(item)),
        OutputFormat::Json => println!("{}", format_json(item)?),
        OutputFormat::Yaml => println!("{}", format_yaml(item)?),
    }
    Ok(())
}

/// Standard empty message formatting
pub fn empty_result(format: &OutputFormat, message: &str) {
    match format {
        OutputFormat::Table => println!("{}", message),
        OutputFormat::Json => println!("null"),
        OutputFormat::Yaml => println!("null"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

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

    #[test]
    fn test_format_list_table() {
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
    }

    #[test]
    fn test_format_list_empty() {
        let items: Vec<TestItem> = vec![];

        // Table format should show message
        let result = format_list(&items, &OutputFormat::Table, "No items", format_test_table);
        assert!(result.is_ok());

        // JSON format should show empty array
        let result = format_list(&items, &OutputFormat::Json, "No items", format_test_table);
        assert!(result.is_ok());
    }

    #[test]
    fn test_format_single() {
        let item = TestItem {
            id: "1".to_string(),
            name: "Test".to_string(),
        };

        let result = format_single(&item, &OutputFormat::Table, format_single_test);
        assert!(result.is_ok());

        let result = format_single(&item, &OutputFormat::Json, format_single_test);
        assert!(result.is_ok());
    }
}
