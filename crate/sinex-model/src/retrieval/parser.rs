//! The indexed query language: `[events|commands] [where] <filter> [since T]
//! [sort when [desc]] [limit N]`.
use super::{Bound, Expr, Field, Operator, Text, words};
use time::{OffsetDateTime, UtcOffset, format_description::well_known::Rfc3339};
use winnow::{Parser, token::take_while};

const MAX_QUERY_BYTES: usize = 16_384;
const MAX_TOKENS: usize = 256;
const OPERATOR_TOKENS: &[&str] = &["=", "!=", "<", ">", "<=", ">=", "~", "^", ":", "exists"];
/// Units and commands of the wider query language. A first word close to one of these
/// is probably a typo, so it is refused unless quoted.
const UNIT_WORDS: &[&str] = &[
    "commands",
    "events",
    "materials",
    "sources",
    "sessions",
    "recall",
    "panel",
    "compute",
    "brief",
    "context",
    "related",
    "follow",
    "trace",
    "descendants",
    "parents",
];
/// Words with meaning elsewhere in the query language; unquoted, they are not search text.
const RESERVED_WORDS: &[&str] = &[
    "recall",
    "panel",
    "compute",
    "brief",
    "context",
    "related",
    "follow",
    "trace",
    "descendants",
    "parents",
    "materials",
    "sources",
    "runs",
    "archive",
    "sessions",
    "events",
    "commands",
    "during",
    "near",
    "as",
    "fields",
    "and",
    "or",
    "where",
    "sort",
    "limit",
    "since",
    "until",
    "in",
    "within",
    "of",
    "group",
    "by",
    "exists",
];

#[derive(Clone, Debug)]
struct Token {
    value: String,
    quoted: bool,
}
impl Token {
    fn bare(value: impl Into<String>) -> Self {
        Self {
            value: value.into(),
            quoted: false,
        }
    }
}

fn tokens(mut rest: &str) -> Result<Vec<Token>, &'static str> {
    let mut out = Vec::new();
    while !rest.trim_start().is_empty() {
        rest = rest.trim_start();
        if let Some((field, _)) = rest.split_once(':') {
            if !field.is_empty() && field.chars().all(|c| c.is_ascii_lowercase() || c == '_') {
                out.push(Token::bare(field));
                out.push(Token::bare(":"));
                rest = &rest[field.len() + 1..];
                continue;
            }
        }
        if rest.starts_with('"') {
            let mut stream = serde_json::Deserializer::from_str(rest).into_iter::<String>();
            let value = stream
                .next()
                .ok_or("missing string")?
                .map_err(|_| "invalid JSON quoted string")?;
            rest = &rest[stream.byte_offset()..];
            out.push(Token {
                value,
                quoted: true,
            });
        } else if let Some(c) = rest.chars().next().filter(|c| "()=<>!~^:".contains(*c)) {
            let n = if rest.get(1..2) == Some("=") {
                2
            } else {
                c.len_utf8()
            };
            out.push(Token::bare(&rest[..n]));
            rest = &rest[n..];
        } else {
            let value: &str = take_while::<_, _, winnow::error::ContextError>(1.., |c: char| {
                !c.is_whitespace() && !"()=<>!~^\"".contains(c)
            })
            .parse_next(&mut rest)
            .map_err(|_| "invalid query token")?;
            out.push(Token::bare(value));
        }
        if out.len() > MAX_TOKENS {
            return Err("at most 256 query tokens");
        }
    }
    Ok(out)
}

struct Reader {
    tokens: Vec<Token>,
    at: usize,
}
impl Reader {
    fn is(&self, s: &str) -> bool {
        self.tokens
            .get(self.at)
            .is_some_and(|t| !t.quoted && t.value.eq_ignore_ascii_case(s))
    }
    fn take(&mut self, s: &str) -> bool {
        if self.is(s) {
            self.at += 1;
            true
        } else {
            false
        }
    }
    fn value(&mut self) -> Result<Token, &'static str> {
        let t = self
            .tokens
            .get(self.at)
            .ok_or("missing query value")?
            .clone();
        self.at += 1;
        Ok(t)
    }
    fn expr(&mut self) -> Result<Expr, &'static str> {
        let mut e = self.and()?;
        while self.take("or") {
            e = Expr::Or(Box::new(e), Box::new(self.and()?));
        }
        Ok(e)
    }
    fn and(&mut self) -> Result<Expr, &'static str> {
        let mut e = self.term()?;
        loop {
            // `and` is optional between terms: `a b` means `a and b`.
            if self.take("and")
                || self.at < self.tokens.len()
                    && !["or", ")", "sort", "limit", "since"]
                        .iter()
                        .any(|s| self.is(s))
            {
                e = Expr::And(Box::new(e), Box::new(self.term()?));
            } else {
                break;
            }
        }
        Ok(e)
    }
    fn term(&mut self) -> Result<Expr, &'static str> {
        if self.take("not") {
            return Ok(Expr::Not(Box::new(self.term()?)));
        }
        if self.take("(") {
            let e = self.expr()?;
            if !self.take(")") {
                return Err("missing closing parenthesis");
            }
            return Ok(e);
        }
        let t = self.value()?;
        if let Some(field) = Field::from_name(&t.value).filter(|_| !t.quoted) {
            return self.comparison(field);
        }
        if !t.quoted
            && RESERVED_WORDS
                .iter()
                .any(|word| word.eq_ignore_ascii_case(&t.value))
        {
            return Err("unsupported command, unit or clause; quote a literal search word");
        }
        if self
            .tokens
            .get(self.at)
            .is_some_and(|t| ["=", "~", "^", ":", "exists"].contains(&t.value.as_str()))
        {
            return Err(
                "unknown indexed field; supported: source, kind, project, cwd, when, exit_code, command, id",
            );
        }
        let (raw, prefix) = match t.value.strip_suffix('*') {
            Some(raw) => (raw, true),
            None => (t.value.as_str(), false),
        };
        if raw.contains('*') {
            return Err("only a trailing word prefix * is supported");
        }
        let words = words(raw);
        if words.is_empty() || (!t.quoted && words.len() != 1) {
            return Err("search requires words or a quoted phrase");
        }
        Ok(Expr::Text(Text {
            words,
            prefix,
            phrase: t.quoted,
        }))
    }
    /// `<field> <operator> [value]`, after the field name.
    fn comparison(&mut self, field: Field) -> Result<Expr, &'static str> {
        let operator = self.value()?;
        if operator.quoted {
            return Err("field operators must be unquoted");
        }
        let op = Operator::parse(&operator.value.to_ascii_lowercase())?;
        if op == Operator::Exists {
            return Ok(Expr::Compare {
                field,
                op,
                value: String::new(),
            });
        }
        let mut value = self.value()?.value;
        let substring = matches!(op, Operator::Contains | Operator::Prefix);
        if field == Field::Cwd && op == Operator::Prefix {
            if !value.starts_with('/') {
                return Err("cwd prefix requires an absolute component root");
            }
            if value != "/" {
                value = value.trim_end_matches('/').to_string();
            }
        }
        if value.starts_with('[')
            || (matches!(op, Operator::Eq | Operator::Ne) && value.contains('*'))
        {
            return Err("glob and list values are not supported in indexed queries");
        }
        if substring && !matches!(field, Field::Command | Field::Cwd) {
            return Err("contains/prefix supported on command and cwd only");
        }
        let value = match field {
            Field::When => OffsetDateTime::parse(&value, &Rfc3339)
                .map_err(|_| "when requires an RFC3339 time")?
                .checked_to_offset(UtcOffset::UTC)
                .and_then(|utc| utc.format(&Rfc3339).ok())
                .ok_or("when is out of range")?,
            Field::ExitCode => value
                .parse::<i64>()
                .map_err(|_| "exit_code requires integer")?
                .to_string(),
            Field::Id => {
                let id = uuid::Uuid::parse_str(&value).map_err(|_| "invalid event ID")?;
                if !matches!(op, Operator::Eq | Operator::Ne) {
                    return Err("id supports equality");
                }
                id.to_string()
            }
            _ if !matches!(op, Operator::Eq | Operator::Ne) && !substring => {
                return Err("invalid text operator");
            }
            _ => value,
        };
        Ok(Expr::Compare { field, op, value })
    }
}

/// Levenshtein distance over bytes.
fn distance(a: &str, b: &str) -> usize {
    let mut previous: Vec<_> = (0..=b.len()).collect();
    for (i, x) in a.bytes().enumerate() {
        let mut next = vec![i + 1];
        for (j, y) in b.bytes().enumerate() {
            next.push(
                (previous[j + 1] + 1)
                    .min(next[j] + 1)
                    .min(previous[j] + usize::from(x != y)),
            );
        }
        previous = next;
    }
    previous[b.len()]
}

/// Whether the text is a raw `events where` query on payload fields or `ts_orig`. Those
/// go to the raw event reader instead. Uses the same tokenizer, so quoted words and
/// values cannot select the reader.
pub fn explicit_raw_events(text: &str) -> bool {
    if text.len() > MAX_QUERY_BYTES {
        return false;
    }
    let Ok(tokens) = tokens(text) else {
        return false;
    };
    if tokens.len() < 2
        || tokens[0].quoted
        || !tokens[0].value.eq_ignore_ascii_case("events")
        || tokens[1].quoted
        || !tokens[1].value.eq_ignore_ascii_case("where")
    {
        return false;
    }
    tokens[2..].windows(2).any(|pair| {
        !pair[0].quoted
            && (pair[0].value.starts_with("payload.") || pair[0].value == "ts_orig")
            && OPERATOR_TOKENS.contains(&pair[1].value.as_str())
    })
}

/// A first word that looks like a misspelled unit or command.
fn looks_like_misspelled_unit(first: &Token) -> bool {
    let word = first.value.to_ascii_lowercase();
    !first.quoted
        && word.len() >= 4
        && Field::from_name(&word).is_none()
        && !["where", "since", "until", "sort", "limit"].contains(&word.as_str())
        && UNIT_WORDS
            .iter()
            .any(|u| *u != word && distance(&word, u) <= 2)
}

pub fn bind(text: &str) -> Result<Bound, &'static str> {
    if text.len() > MAX_QUERY_BYTES {
        return Err("query exceeds 16 KiB");
    }
    let mut r = Reader {
        tokens: tokens(text)?,
        at: 0,
    };
    if r.tokens.first().is_some_and(looks_like_misspelled_unit) {
        return Err(
            "ambiguous first word resembles a unit or command; correct it, or quote it as literal search text",
        );
    }
    let unit = if r.take("commands") {
        "commands"
    } else {
        r.take("events");
        "events"
    };
    // Without a unit the whole query is the filter; after one, `where` introduces it.
    let filter = if r.at == 0 || r.take("where") {
        Some(r.expr()?)
    } else {
        None
    };
    if unit == "events" && !filter.as_ref().is_some_and(Expr::has_text) {
        return Err(
            "indexed events require words; use commands for canonical rows or the supported raw events subset",
        );
    }
    let mut bound = Bound {
        unit: unit.to_string(),
        filter,
        sort: None,
        limit: None,
    };
    if r.take("since") {
        let value = r.value()?.value;
        OffsetDateTime::parse(&value, &Rfc3339).map_err(|_| "since requires an RFC3339 time")?;
        let since = Expr::Compare {
            field: Field::When,
            op: Operator::Ge,
            value,
        };
        bound.filter = Some(match bound.filter {
            Some(old) => Expr::And(Box::new(old), Box::new(since)),
            None => since,
        });
    }
    if r.take("sort") {
        if !r.take("when") {
            return Err("indexed sort supports when only");
        }
        bound.sort = Some(r.take("desc"));
    }
    if r.take("limit") {
        let n = r
            .value()?
            .value
            .parse::<u32>()
            .map_err(|_| "limit requires integer")?;
        if !(1..=10000).contains(&n) {
            return Err("limit must be 1..=10000");
        }
        bound.limit = Some(n);
    }
    if r.at != r.tokens.len() {
        return Err("unsupported indexed query clause");
    }
    Ok(bound)
}

#[cfg(test)]
#[path = "parser_tests.rs"]
mod tests;
