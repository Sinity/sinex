//! Indexed shell search: query types, text normalization and project attribution.
//! Queries are parsed and type-checked before storage is read.
use serde::{Deserialize, Serialize};
use unicode_normalization::UnicodeNormalization;
use unicode_segmentation::UnicodeSegmentation;

mod parser;
mod project;
pub use parser::{bind, explicit_raw_events};
pub use project::{
    Project, ProjectEvidence, SessionProjectAttribution, UnknownProjectMember, project,
    project_evidence,
};

/// Bump when token, text, project, filter, ranking or order meaning changes. Stored
/// search text and cursors are only reused under the same value.
pub const CONTRACT: &str = "sinex.shell-search.v5";

pub fn normalization_versions() -> String {
    format!(
        "casefold={:?};nfc={:?};words={:?}",
        caseless::UNICODE_VERSION,
        unicode_normalization::UNICODE_VERSION,
        unicode_segmentation::UNICODE_VERSION
    )
}
pub fn normalize(text: &str) -> String {
    caseless::default_case_fold_str(text).nfc().collect()
}
pub fn words(text: &str) -> Vec<String> {
    text.unicode_words().map(normalize).collect()
}

// Unicode word segmentation never emits these control characters. Encoding runs
// after tokenization, so the same characters in input stay data and cannot create
// token or field boundaries.
const TOKEN_BOUNDARY: char = '\u{1}';
const FIELD_BOUNDARY: char = '\u{2}';

/// Normalized tokens of each payload string, kept apart per string. None when the
/// result cannot be stored as PostgreSQL text.
pub fn token_text(strings: &[String]) -> Option<String> {
    fn append(out: &mut String, value: &str) -> Option<()> {
        // PostgreSQL MaxAllocSize (2^30-1) includes the four-byte varlena header.
        // Check every token and delimiter before reserving backend-sized text.
        const MAX_TEXT_BYTES: usize = (1 << 30) - 1 - 4;
        if out.len().checked_add(value.len())? > MAX_TEXT_BYTES {
            return None;
        }
        out.try_reserve(value.len()).ok()?;
        out.push_str(value);
        Some(())
    }
    let mut out = String::new();
    for field in strings {
        let mut tokens = field.unicode_words().peekable();
        if tokens.peek().is_none() {
            continue;
        }
        append(&mut out, "\u{1}")?;
        for raw_token in tokens {
            let token = normalize(raw_token);
            if token.contains([TOKEN_BOUNDARY, FIELD_BOUNDARY]) {
                return None;
            }
            append(&mut out, &token)?;
            append(&mut out, "\u{1}")?;
        }
        append(&mut out, "\u{2}")?;
    }
    Some(out)
}

/// Escapes a literal for PostgreSQL LIKE with a backslash escape.
pub fn like_literal(word: &str) -> String {
    word.replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Text {
    pub words: Vec<String>,
    pub prefix: bool,
    pub phrase: bool,
}
impl Text {
    pub fn like_patterns(&self) -> Vec<String> {
        let pattern = |tokens: &[String], prefix: bool| {
            let mut value = format!("%{TOKEN_BOUNDARY}");
            value.push_str(
                &tokens
                    .iter()
                    .map(|w| like_literal(w))
                    .collect::<Vec<_>>()
                    .join(&TOKEN_BOUNDARY.to_string()),
            );
            if !prefix {
                value.push(TOKEN_BOUNDARY);
            }
            value.push('%');
            value
        };
        if self.phrase {
            vec![pattern(&self.words, self.prefix)]
        } else {
            self.words
                .iter()
                .enumerate()
                .map(|(i, word)| {
                    pattern(
                        std::slice::from_ref(word),
                        self.prefix && i + 1 == self.words.len(),
                    )
                })
                .collect()
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Field {
    Source,
    Kind,
    Project,
    Cwd,
    When,
    ExitCode,
    Command,
    Id,
}
impl Field {
    pub const ALL: [Self; 8] = [
        Self::Source,
        Self::Kind,
        Self::Project,
        Self::Cwd,
        Self::When,
        Self::ExitCode,
        Self::Command,
        Self::Id,
    ];
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Source => "source",
            Self::Kind => "kind",
            Self::Project => "project",
            Self::Cwd => "cwd",
            Self::When => "when",
            Self::ExitCode => "exit_code",
            Self::Command => "command",
            Self::Id => "id",
        }
    }
    pub fn from_name(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|field| field.as_str() == name)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Operator {
    Eq,
    Ne,
    Lt,
    Gt,
    Le,
    Ge,
    Contains,
    Prefix,
    Exists,
}
impl Operator {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Eq => "=",
            Self::Ne => "!=",
            Self::Lt => "<",
            Self::Gt => ">",
            Self::Le => "<=",
            Self::Ge => ">=",
            Self::Contains => "~",
            Self::Prefix => "^",
            Self::Exists => "exists",
        }
    }
    /// `:` is accepted as a synonym for `=`.
    fn parse(raw: &str) -> Result<Self, &'static str> {
        match raw {
            "=" | ":" => Ok(Self::Eq),
            "!=" => Ok(Self::Ne),
            "<" => Ok(Self::Lt),
            ">" => Ok(Self::Gt),
            "<=" => Ok(Self::Le),
            ">=" => Ok(Self::Ge),
            "~" => Ok(Self::Contains),
            "^" => Ok(Self::Prefix),
            "exists" => Ok(Self::Exists),
            _ => Err("unsupported field operator"),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Expr {
    Text(Text),
    Compare {
        field: Field,
        op: Operator,
        value: String,
    },
    And(Box<Expr>, Box<Expr>),
    Or(Box<Expr>, Box<Expr>),
    Not(Box<Expr>),
}
impl Expr {
    /// Words used for ranking: positive text only, never text under `not`.
    pub fn rank_text(&self) -> Option<String> {
        match self {
            Self::Text(t) => Some(t.words.join(" ")),
            Self::And(a, b) | Self::Or(a, b) => match (a.rank_text(), b.rank_text()) {
                (Some(a), Some(b)) => Some(format!("{a} {b}")),
                (a, b) => a.or(b),
            },
            Self::Not(_) | Self::Compare { .. } => None,
        }
    }
    pub fn has_text(&self) -> bool {
        match self {
            Self::Text(_) => true,
            Self::And(a, b) | Self::Or(a, b) => a.has_text() || b.has_text(),
            Self::Not(e) => e.has_text(),
            Self::Compare { .. } => false,
        }
    }
    /// Positive text terms, for snippets.
    pub fn terms(&self, out: &mut Vec<Text>) {
        match self {
            Self::Text(t) => out.push(t.clone()),
            Self::And(a, b) | Self::Or(a, b) => {
                a.terms(out);
                b.terms(out);
            }
            Self::Not(_) | Self::Compare { .. } => {}
        }
    }
}

/// A parsed query. `sort` is None when unsorted, Some(true) for descending.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Bound {
    pub unit: String,
    pub filter: Option<Expr>,
    pub sort: Option<bool>,
    pub limit: Option<u32>,
}

#[cfg(test)]
#[path = "retrieval_text_tests.rs"]
mod text_tests;
