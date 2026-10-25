//! The query language: one language for the CLI, the HTTP API and MCP. Text is
//! parsed and bound here against a [`Context`] that fixes `now`, the zone and
//! the logical-day start once, then compiled to SQL by `sinex-store::query`.
//! Every query reads back as a sentence ([`read_as`]).
mod ast;
mod lex;
mod parse;
mod read_as;
pub mod text;
pub mod time;
pub mod units;

pub use ast::*;
pub use parse::{bind, parse_reference};
pub use read_as::read_as;
pub use time::Context;
pub use units::{ALIASES, Alias, Field, FieldType, Unit};

/// Why a query was refused.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct QueryError {
    pub message: String,
    /// Byte offset in the query text, when the problem has a place.
    pub at: Option<usize>,
    /// The query asks for something the language defines but Sinex does not
    /// build yet. Never answered with an empty result.
    pub not_built: bool,
}

impl QueryError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            at: None,
            not_built: false,
        }
    }

    pub fn at(at: usize, message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            at: Some(at),
            not_built: false,
        }
    }

    pub fn not_built(at: usize, message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            at: Some(at),
            not_built: true,
        }
    }

    /// The message with a caret under the place in `text`, for terminals.
    pub fn render(&self, text: &str) -> String {
        match self.at {
            Some(at) if at <= text.len() => {
                let column = text[..at].chars().count();
                format!("{}\n  {text}\n  {}^", self.message, " ".repeat(column))
            }
            _ => self.message.clone(),
        }
    }
}

#[cfg(test)]
mod tests;
