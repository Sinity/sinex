//! The query language: one language for the CLI, the HTTP API and MCP. Text is
//! parsed and bound here against a [`Context`] that fixes `now`, the zone and
//! the logical-day start once, then compiled to SQL by `sinex-store::query`.
//! Every query reads back as a sentence ([`read_as`]).
mod ast;
pub mod catalog;
pub mod cursor;
mod fill;
pub use catalog::QueryCatalog;
pub mod native;
mod parse;
mod read_as;
pub mod saved;
pub mod syntax;
pub mod tables;
pub mod text;
pub mod time;
mod typed;

pub use ast::*;
pub use parse::{bind, parse};
mod reference;
pub use read_as::read_as;
pub use reference::{citation, parse_reference};
pub use tables::{
    ALIASES, Alias, ArchiveField, EventField, FailureField, Field, FieldId, FieldType, GapField,
    MaterialField, OperationField, RunField, SourceField, Table,
};
pub use time::Context;
pub use typed::{Completion, complete};

/// What kind of refusal a query met: one of the language's kinds, or a
/// request for something the language defines but Sinex does not build yet,
/// which is never answered with an empty result.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QueryErrorKind {
    Language(query_language::ErrorKind),
    NotBuilt,
}

/// Why a query was refused.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct QueryError {
    pub kind: QueryErrorKind,
    pub message: String,
    /// Byte offset in the query text, when the problem has a place.
    pub at: Option<usize>,
    /// Byte length of the refused text; zero marks a place between tokens.
    pub length: usize,
    /// What the caller can do instead, when the language knows; boxed so a
    /// `Result` carrying the error stays small.
    pub help: Option<Box<QueryHelp>>,
}

/// A refusal's correction: a query that runs as given, the names the input
/// could have meant, and a hint.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct QueryHelp {
    pub suggestion: Option<String>,
    pub candidates: Vec<String>,
    pub hint: Option<String>,
}

impl QueryError {
    fn with(kind: QueryErrorKind, at: Option<usize>, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            at,
            length: 0,
            help: None,
        }
    }

    /// The corrected query, if the language could form one.
    pub fn suggestion(&self) -> Option<&str> {
        self.help.as_ref()?.suggestion.as_deref()
    }

    pub fn new(message: impl Into<String>) -> Self {
        Self::with(
            QueryErrorKind::Language(query_language::ErrorKind::InvalidQuery),
            None,
            message,
        )
    }

    pub fn at(at: usize, message: impl Into<String>) -> Self {
        Self::with(
            QueryErrorKind::Language(query_language::ErrorKind::InvalidQuery),
            Some(at),
            message,
        )
    }

    pub fn not_built(at: usize, message: impl Into<String>) -> Self {
        Self::with(QueryErrorKind::NotBuilt, Some(at), message)
    }

    /// The shared failure for this refusal of `text`: its kind, message,
    /// suggestion, candidates and hint, placed in `text` with a caret.
    pub fn failure(self, text: &str) -> command_surface::Failure {
        use command_surface::{Failure, FailureKind, InputSpan, Suggestion};
        use query_language::ErrorKind as Language;
        let kind = match self.kind {
            QueryErrorKind::NotBuilt => FailureKind::NotBuilt,
            QueryErrorKind::Language(Language::InvalidQuery) => FailureKind::InvalidQuery,
            QueryErrorKind::Language(Language::UnknownField) => FailureKind::UnknownField,
            QueryErrorKind::Language(Language::BadValue) => FailureKind::BadValue,
            QueryErrorKind::Language(Language::NotAQuery) => FailureKind::NotAQuery,
            QueryErrorKind::Language(Language::Ambiguous) => FailureKind::Ambiguous,
            QueryErrorKind::Language(Language::StaleCursor) => FailureKind::StaleCursor,
        };
        let help = self.help.map(|help| *help).unwrap_or_default();
        let mut failure = Failure::new(kind, self.message).with_candidates(help.candidates);
        if let Some(at) = self.at.filter(|at| *at <= text.len()) {
            failure = failure.at(
                text,
                InputSpan {
                    offset: at,
                    length: self.length,
                },
            );
        }
        if let Some(hint) = help.hint {
            failure = failure.with_hint(hint);
        }
        match help.suggestion {
            Some(corrected) => failure.with_suggestion(Suggestion::Query(corrected)),
            None => failure,
        }
    }
}

/// The language's refusal, keeping its kind, span, suggestion, candidates and hint.
impl From<query_language::Error> for QueryError {
    fn from(error: query_language::Error) -> Self {
        Self {
            kind: QueryErrorKind::Language(error.kind),
            message: error.message,
            at: Some(error.position),
            length: error.length,
            help: Some(Box::new(QueryHelp {
                suggestion: error.suggestion,
                candidates: error.candidates,
                hint: error.hint,
            })),
        }
    }
}

#[cfg(test)]
mod tests;
