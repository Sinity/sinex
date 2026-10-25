//! Parsing and binding: one pass that reads the tokens, checks every field
//! against its table's table, and resolves every time against the context. A
//! query that cannot mean what it says is refused with the reason; nothing
//! falls back silently.
use super::QueryError;
use super::ast::*;
use super::lex::{Kind, Token, tokens};
use super::tables::{Alias, COMMAND_WORDS, FieldType, Table, table_like_words, tables_accepting};
use super::text::words;
use super::time::{self, Context};
use jiff::{SignedDuration, Timestamp};
use uuid::Uuid;

mod clause;
mod filter;
mod reference;
mod suggest;
mod times;

pub use reference::parse_reference;
use suggest::typo;

/// Words that end a filter and start a clause.
const CLAUSE_WORDS: &[&str] = &[
    "where",
    "near",
    "before",
    "after",
    "during",
    "containing",
    "as",
    "group",
    "count",
    "sum",
    "min",
    "max",
    "avg",
    "count_distinct",
    "sort",
    "limit",
    "fields",
    "fill",
];

/// Words with meaning in the language; unquoted they are never search words.
const RESERVED: &[&str] = &[
    "and",
    "or",
    "not",
    "where",
    "since",
    "until",
    "in",
    "near",
    "before",
    "after",
    "during",
    "containing",
    "within",
    "as",
    "of",
    "group",
    "by",
    "count",
    "sum",
    "min",
    "max",
    "avg",
    "count_distinct",
    "sort",
    "limit",
    "fields",
    "fill",
    "exists",
    "desc",
    "asc",
    "at",
];

/// Parse and bind `text` against `ctx`.
pub fn bind(text: &str, ctx: &Context) -> Result<Query, QueryError> {
    let toks = tokens(text)?;
    if toks.is_empty() {
        return Err(QueryError::new("empty query"));
    }
    let mut p = Parser {
        toks,
        at: 0,
        ctx,
        end: text.len(),
    };
    let query = p.query(false)?;
    if let Some(t) = p.peek() {
        return Err(QueryError::at(
            t.at,
            format!("unexpected `{}`; expected a clause or the end", t.text),
        ));
    }
    Ok(query)
}

/// What field names resolve to at a point in the query.
#[derive(Clone)]
enum Scope {
    Rows {
        table: Table,
        alias: Option<&'static Alias>,
        /// Columns time joins add: `near`, `offset`, `during`, `containing`.
        columns: Vec<String>,
    },
    /// After a grouping: the group's key and aggregate columns.
    Grouped(Vec<String>),
}

impl Scope {
    fn rows(table: Table, alias: Option<&'static Alias>) -> Scope {
        Scope::Rows {
            table,
            alias,
            columns: Vec::new(),
        }
    }
}

struct Parser<'a> {
    toks: Vec<Token>,
    at: usize,
    ctx: &'a Context,
    end: usize,
}

impl Parser<'_> {
    fn peek(&self) -> Option<&Token> {
        self.toks.get(self.at)
    }

    fn peek_at(&self, offset: usize) -> Option<&Token> {
        self.toks.get(self.at + offset)
    }

    fn next(&mut self) -> Option<Token> {
        let t = self.toks.get(self.at).cloned();
        if t.is_some() {
            self.at += 1;
        }
        t
    }

    fn pos(&self) -> usize {
        self.peek().map_or(self.end, |t| t.at)
    }

    fn is_word(&self, word: &str) -> bool {
        self.peek().is_some_and(|t| t.is_word(word))
    }

    fn is_symbol(&self, symbol: &str) -> bool {
        self.peek().is_some_and(|t| t.is_symbol(symbol))
    }

    fn take_word(&mut self, word: &str) -> bool {
        if self.is_word(word) {
            self.at += 1;
            true
        } else {
            false
        }
    }

    fn take_symbol(&mut self, symbol: &str) -> bool {
        if self.is_symbol(symbol) {
            self.at += 1;
            true
        } else {
            false
        }
    }

    fn expect_word(&mut self, word: &str) -> Result<(), QueryError> {
        if self.take_word(word) {
            Ok(())
        } else {
            Err(QueryError::at(self.pos(), format!("expected `{word}`")))
        }
    }

    fn expect_symbol(&mut self, symbol: &str) -> Result<(), QueryError> {
        if self.take_symbol(symbol) {
            Ok(())
        } else {
            Err(QueryError::at(self.pos(), format!("expected `{symbol}`")))
        }
    }

    /// The next token's text, which must be a value.
    fn value_token(&mut self) -> Result<Token, QueryError> {
        match self.next() {
            Some(t) if t.kind != Kind::Symbol => Ok(t),
            Some(t) => Err(QueryError::at(
                t.at,
                format!("expected a value, found `{}`", t.text),
            )),
            None => Err(QueryError::at(self.end, "expected a value")),
        }
    }

    fn at_clause(&self) -> bool {
        self.peek()
            .is_some_and(|t| CLAUSE_WORDS.iter().any(|w| t.is_word(w)))
    }

    fn at_end_of_query(&self, nested: bool) -> bool {
        match self.peek() {
            None => true,
            Some(t) => nested && t.is_symbol(")"),
        }
    }

    fn query(&mut self, nested: bool) -> Result<Query, QueryError> {
        let first = self
            .peek()
            .cloned()
            .ok_or_else(|| QueryError::at(self.end, "expected a query"))?;
        let mut ranked = false;
        let (source, filter) = if first.kind == Kind::Word {
            let word = first.text.to_ascii_lowercase();
            if let Some(walk) = match word.as_str() {
                "trace" => Some(Walk::Trace),
                "descendants" => Some(Walk::Descendants),
                "parents" => Some(Walk::Parents),
                _ => None,
            } {
                self.at += 1;
                let reference = self.reference()?;
                (Source::Walk { walk, reference }, None)
            } else if word == "recall" {
                self.at += 1;
                (self.recall()?, None)
            } else if COMMAND_WORDS.contains(&word.as_str()) {
                return Err(QueryError::not_built(
                    first.at,
                    format!(
                        "`{word}` is a command that is not built yet; quote it to search for the word"
                    ),
                ));
            } else if let Some(table) = Table::from_name(&word) {
                self.at += 1;
                let filter = self.table_filter(&Scope::rows(table, None))?;
                (Source::Table { table, alias: None }, filter)
            } else if let Some(alias) = Alias::from_name(&word) {
                self.at += 1;
                let scope = Scope::rows(Table::Events, Some(alias));
                let filter = self.table_filter(&scope)?;
                (
                    Source::Table {
                        table: Table::Events,
                        alias: Some(alias),
                    },
                    filter,
                )
            } else {
                if let Some(suggestion) = typo(&word) {
                    return Err(QueryError::at(
                        first.at,
                        format!(
                            "`{word}` looks like a misspelled `{suggestion}`; correct it, or quote it to search for the word"
                        ),
                    ));
                }
                // Bare words: a full-text search over events.
                let scope = Scope::rows(Table::Events, None);
                let filter = self.filter(&scope)?;
                (
                    Source::Table {
                        table: Table::Events,
                        alias: None,
                    },
                    Some(filter),
                )
            }
        } else if first.kind == Kind::Quoted {
            let scope = Scope::rows(Table::Events, None);
            let filter = self.filter(&scope)?;
            (
                Source::Table {
                    table: Table::Events,
                    alias: None,
                },
                Some(filter),
            )
        } else {
            return Err(QueryError::at(
                first.at,
                format!("a query starts with a table or words, not `{}`", first.text),
            ));
        };
        if filter.as_ref().is_some_and(has_words) {
            ranked = true;
        }
        let mut scope = Scope::rows(source.table(), source.alias());
        let mut stages = Vec::new();
        while !self.at_end_of_query(nested) {
            let stage = self.clause(&mut scope)?;
            if let Stage::Where(e) = &stage
                && has_words(e)
            {
                ranked = true;
            }
            stages.push(stage);
        }
        Ok(Query {
            source,
            filter,
            stages,
            ranked,
        })
    }
}

fn has_words(e: &Expr) -> bool {
    match e {
        Expr::Words(_) => true,
        Expr::And(a, b) | Expr::Or(a, b) => has_words(a) || has_words(b),
        Expr::Not(_)
        | Expr::Compare { .. }
        | Expr::Exists(_)
        | Expr::Within { .. }
        | Expr::TimeDiff { .. } => false,
    }
}
