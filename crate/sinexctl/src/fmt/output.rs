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
            OutputFormat::Table => println!("{empty_msg}"),
            OutputFormat::Json | OutputFormat::Dot => println!("[]"),
            OutputFormat::Yaml => println!("[]"),
        }
        return Ok(());
    }

    match format {
        OutputFormat::Table => println!("{}", table_formatter(items)),
        OutputFormat::Json | OutputFormat::Dot => {
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
        OutputFormat::Json | OutputFormat::Dot => println!("{}", format_json(item)?),
        OutputFormat::Yaml => println!("{}", format_yaml(item)?),
    }
    Ok(())
}

/// Standard empty message formatting
pub fn empty_result(format: &OutputFormat, message: &str) {
    match format {
        OutputFormat::Table => println!("{message}"),
        OutputFormat::Json | OutputFormat::Dot => println!("null"),
        OutputFormat::Yaml => println!("null"),
    }
}

/// Self-describing command output that handles all format types.
///
/// This enum wraps the existing `format_list` and `format_single` helpers
/// with a cleaner API that eliminates the need for manual format matching.
///
/// # Examples
///
/// ```ignore
/// // List output
/// let nodes = vec![node1, node2];
/// CommandOutput::list(nodes, "No nodes found", |nodes| format_table(nodes))
///     .display(&OutputFormat::Table)?;
///
/// // Single item output
/// CommandOutput::single(status, |s| format_status(s))
///     .display(&OutputFormat::Json)?;
///
/// // Empty result
/// CommandOutput::empty("No results")
///     .display(&OutputFormat::Table)?;
///
/// // Success message
/// CommandOutput::success("Operation completed")
///     .display(&OutputFormat::Table)?;
/// ```
type ListTableFormatter<T> = Box<dyn FnOnce(&[T]) -> String>;
type SingleTableFormatter<T> = Box<dyn FnOnce(&T) -> String>;

pub enum CommandOutput<T: Serialize> {
    /// List of items with optional empty message
    List {
        items: Vec<T>,
        empty_msg: &'static str,
        table_formatter: ListTableFormatter<T>,
    },
    /// Single item
    Single {
        item: T,
        table_formatter: SingleTableFormatter<T>,
    },
    /// Empty result with message
    Empty { message: &'static str },
    /// Success message (table shows message, JSON/YAML show {"status": "success", "message": "..."})
    Success { message: String },
}

impl<T: Serialize> CommandOutput<T> {
    /// Create a list output.
    pub fn list<F>(items: Vec<T>, empty_msg: &'static str, table_formatter: F) -> Self
    where
        F: FnOnce(&[T]) -> String + 'static,
    {
        Self::List {
            items,
            empty_msg,
            table_formatter: Box::new(table_formatter),
        }
    }

    /// Create a single item output.
    pub fn single<F>(item: T, table_formatter: F) -> Self
    where
        F: FnOnce(&T) -> String + 'static,
    {
        Self::Single {
            item,
            table_formatter: Box::new(table_formatter),
        }
    }

    /// Create an empty result output.
    #[must_use]
    pub fn empty(message: &'static str) -> Self {
        Self::Empty { message }
    }

    /// Create a success message output.
    pub fn success(message: impl Into<String>) -> Self {
        Self::Success {
            message: message.into(),
        }
    }

    /// Display the output in the given format.
    pub fn display(self, format: &OutputFormat) -> Result<()> {
        match self {
            Self::List {
                items,
                empty_msg,
                table_formatter,
            } => format_list(&items, format, empty_msg, table_formatter),
            Self::Single {
                item,
                table_formatter,
            } => format_single(&item, format, table_formatter),
            Self::Empty { message } => {
                empty_result(format, message);
                Ok(())
            }
            Self::Success { message } => match format {
                OutputFormat::Table => {
                    println!("{message}");
                    Ok(())
                }
                OutputFormat::Json | OutputFormat::Dot => {
                    let result = serde_json::json!({
                        "status": "success",
                        "message": message
                    });
                    println!("{}", format_json(&result)?);
                    Ok(())
                }
                OutputFormat::Yaml => {
                    let result = serde_json::json!({
                        "status": "success",
                        "message": message
                    });
                    println!("{}", format_yaml(&result)?);
                    Ok(())
                }
            },
        }
    }
}
