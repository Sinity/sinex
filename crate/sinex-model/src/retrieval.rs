//! Bound indexed-query subset. Parsing and type checking finish before storage is read.
use serde::{Deserialize, Serialize};
use unicode_normalization::UnicodeNormalization;
use unicode_segmentation::UnicodeSegmentation;
use winnow::{Parser, token::take_while};

pub fn normalization_basis() -> String {
    format!(
        "casefold={:?};nfc={:?};words={:?}",
        caseless::UNICODE_VERSION,
        unicode_normalization::UNICODE_VERSION,
        unicode_segmentation::UNICODE_VERSION
    )
}

// Bump for changed token/text/project/filter/ranking/order meaning; this binds
// reconstruction and cursor generation compatibility, independently of build time.
pub const CONTRACT: &str = "sinex.shell-search.v5";
pub fn normalize(text: &str) -> String {
    caseless::default_case_fold_str(text).nfc().collect()
}
pub fn words(text: &str) -> Vec<String> {
    text.unicode_words().map(normalize).collect()
}
// These control characters cannot be emitted by Unicode word segmentation.
// Encoding happens after tokenization, so literal delimiters in input are data,
// never authority to create token or field boundaries.
const TOKEN_BOUNDARY: char = '\u{1}';
const FIELD_BOUNDARY: char = '\u{2}';

/// Complete normalized token sequences, kept separate across payload strings.
/// None means the backend representation cannot be admitted, never empty text.
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
/// Escape a literal for PostgreSQL LIKE with an explicit backslash escape.
pub fn like_literal(word: &str) -> String {
    word.replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Project {
    pub name: String,
    pub roots: Vec<String>,
}
impl Project {
    pub fn validate_all(projects: &[Self]) -> Result<(), &'static str> {
        let mut names = std::collections::BTreeSet::new();
        for p in projects {
            if p.name.is_empty()
                || !names.insert(&p.name)
                || p.roots.is_empty()
                || p.roots.iter().any(|r| {
                    !r.starts_with('/')
                        || r.contains("..")
                        || r.matches('*').count() > 1
                        || r.strip_suffix('*').unwrap_or(r).contains('*')
                        || (r.ends_with('/') && r != "/")
                })
            {
                return Err(
                    "projects require unique names and absolute roots; only a final-component trailing * is supported",
                );
            }
        }
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum ProjectEvidence {
    Known { project: String },
    Unconfigured { reason: String },
    Unknown { reason: String },
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UnknownProjectMember {
    pub event: String,
    pub evidence: ProjectEvidence,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SessionProjectAttribution {
    pub basis: String,
    pub known_projects: std::collections::BTreeMap<String, usize>,
    pub unconfigured_members: usize,
    pub unknown_members: Vec<UnknownProjectMember>,
    pub total_members: usize,
    pub complete: bool,
}
pub fn project(cwd: &str, projects: &[Project]) -> Option<String> {
    match project_evidence(Some(cwd), projects) {
        ProjectEvidence::Known { project } => Some(project),
        _ => None,
    }
}
pub fn project_evidence(cwd: Option<&str>, projects: &[Project]) -> ProjectEvidence {
    let Some(cwd) = cwd else {
        return ProjectEvidence::Unknown {
            reason: "missing_cwd".into(),
        };
    };
    if cwd.is_empty() {
        return ProjectEvidence::Unknown {
            reason: "missing_cwd".into(),
        };
    }
    if !cwd.starts_with('/')
        || cwd.contains('\0')
        || cwd.split('/').any(|c| matches!(c, "." | ".."))
    {
        return ProjectEvidence::Unknown {
            reason: "invalid_cwd".into(),
        };
    }
    let mut matches = Vec::new();
    for p in projects {
        for root in &p.roots {
            let matched = if root == "/" {
                cwd.starts_with('/')
            } else if let Some(prefix) = root.strip_suffix('*') {
                cwd.starts_with(prefix)
                    && !cwd[prefix.rfind('/').unwrap_or(0) + 1..]
                        .split('/')
                        .next()
                        .unwrap_or("")
                        .is_empty()
            } else {
                cwd == root || cwd.strip_prefix(root).is_some_and(|s| s.starts_with('/'))
            };
            if matched {
                matches.push((
                    root.split('/').filter(|c| !c.is_empty()).count(),
                    p.name.clone(),
                ));
            }
        }
    }
    matches.sort();
    let Some((depth, name)) = matches.pop() else {
        return ProjectEvidence::Unconfigured {
            reason: "no_configured_root".into(),
        };
    };
    if matches.iter().any(|(d, n)| *d == depth && n != &name) {
        ProjectEvidence::Unknown {
            reason: "ambiguous_roots".into(),
        }
    } else {
        ProjectEvidence::Known { project: name }
    }
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
    fn parse(raw: &str) -> Result<Self, &'static str> {
        match raw {
            "=" => Ok(Self::Eq),
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
pub struct FieldDescriptor {
    pub field: Field,
    pub name: &'static str,
    pub reading: &'static str,
}
pub const FIELDS: &[FieldDescriptor] = &[
    FieldDescriptor {
        field: Field::Source,
        name: "source",
        reading: "source",
    },
    FieldDescriptor {
        field: Field::Kind,
        name: "kind",
        reading: "kind",
    },
    FieldDescriptor {
        field: Field::Project,
        name: "project",
        reading: "project",
    },
    FieldDescriptor {
        field: Field::Cwd,
        name: "cwd",
        reading: "cwd",
    },
    FieldDescriptor {
        field: Field::When,
        name: "when",
        reading: "when",
    },
    FieldDescriptor {
        field: Field::ExitCode,
        name: "exit_code",
        reading: "exit code",
    },
    FieldDescriptor {
        field: Field::Command,
        name: "command",
        reading: "command",
    },
    FieldDescriptor {
        field: Field::Id,
        name: "id",
        reading: "id",
    },
];
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
    /// Ranking uses positive text only, independently of exact membership.
    pub fn rank_text(&self) -> Option<String> {
        match self {
            Self::Text(t) => Some(t.words.join(" ")),
            Self::And(a, b) | Self::Or(a, b) => match (a.rank_text(), b.rank_text()) {
                (Some(a), Some(b)) => Some(format!("{a} {b}")),
                (a, b) => a.or(b),
            },
            Self::Not(_) => None,
            _ => None,
        }
    }
    pub fn has_text(&self) -> bool {
        match self {
            Self::Text(_) => true,
            Self::And(a, b) | Self::Or(a, b) => a.has_text() || b.has_text(),
            Self::Not(e) => e.has_text(),
            _ => false,
        }
    }
    pub fn terms(&self, out: &mut Vec<Text>) {
        match self {
            Self::Text(t) => out.push(t.clone()),
            Self::And(a, b) | Self::Or(a, b) => {
                a.terms(out);
                b.terms(out);
            }
            Self::Not(_) => {}
            _ => {}
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Bound {
    pub unit: String,
    pub filter: Option<Expr>,
    pub sort: Option<bool>,
    pub limit: Option<u32>,
}
#[derive(Clone, Debug)]
struct Token {
    value: String,
    quoted: bool,
}
fn tokens(mut rest: &str) -> Result<Vec<Token>, &'static str> {
    let mut out = Vec::new();
    while !rest.trim_start().is_empty() {
        rest = rest.trim_start();
        if let Some((field, _)) = rest.split_once(':') {
            if !field.is_empty() && field.chars().all(|c| c.is_ascii_lowercase() || c == '_') {
                out.push(Token {
                    value: field.into(),
                    quoted: false,
                });
                out.push(Token {
                    value: ":".into(),
                    quoted: false,
                });
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
            out.push(Token {
                value: rest[..n].into(),
                quoted: false,
            });
            rest = &rest[n..];
        } else {
            let value: &str = take_while::<_, _, winnow::error::ContextError>(1.., |c: char| {
                !c.is_whitespace() && !"()=<>!~^\"".contains(c)
            })
            .parse_next(&mut rest)
            .map_err(|_| "invalid query token")?;
            out.push(Token {
                value: value.into(),
                quoted: false,
            });
        }
        if out.len() > 256 {
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
        if let Some(descriptor) = FIELDS.iter().find(|f| !t.quoted && f.name == t.value) {
            let operator = self.value()?;
            if operator.quoted {
                return Err("field operators must be unquoted");
            }
            let op = operator.value.to_ascii_lowercase();
            if !["=", "!=", "<", ">", "<=", ">=", "~", "^", ":", "exists"].contains(&op.as_str()) {
                return Err("unsupported field operator");
            }
            let op = if op == ":" { "=".into() } else { op };
            let mut value = if op == "exists" {
                String::new()
            } else {
                self.value()?.value
            };
            if descriptor.field == Field::Cwd && op == "^" {
                if !value.starts_with('/') {
                    return Err("cwd prefix requires an absolute component root");
                }
                if value != "/" {
                    value = value.trim_end_matches('/').to_string();
                }
            }
            if value.starts_with('[') || ((op == "=" || op == "!=") && value.contains('*')) {
                return Err("glob/list filter values are unavailable in indexed retrieval");
            }
            if ["~", "^"].contains(&op.as_str()) && !["command", "cwd"].contains(&t.value.as_str())
            {
                return Err("contains/prefix supported on command and cwd only");
            }
            if op != "exists" {
                match t.value.as_str() {
                    "when" => {
                        time::OffsetDateTime::parse(
                            &value,
                            &time::format_description::well_known::Rfc3339,
                        )
                        .map_err(|_| "indexed when bounds require explicit RFC3339")?;
                        if ["~", "^"].contains(&op.as_str()) {
                            return Err("invalid time operator");
                        }
                    }
                    "exit_code" => {
                        value
                            .parse::<i64>()
                            .map_err(|_| "exit_code requires integer")?;
                        if ["~", "^"].contains(&op.as_str()) {
                            return Err("invalid integer operator");
                        }
                    }
                    "id" => {
                        uuid::Uuid::parse_str(&value).map_err(|_| "invalid event ID")?;
                        if op != "=" && op != "!=" {
                            return Err("id supports equality");
                        }
                    }
                    _ => {
                        if !["=", "!=", "~", "^"].contains(&op.as_str()) {
                            return Err("invalid text operator");
                        }
                    }
                }
            }
            if op != "exists" {
                value = match descriptor.field {
                    Field::When => time::OffsetDateTime::parse(
                        &value,
                        &time::format_description::well_known::Rfc3339,
                    )
                    .unwrap()
                    .to_offset(time::UtcOffset::UTC)
                    .format(&time::format_description::well_known::Rfc3339)
                    .unwrap(),
                    Field::ExitCode => value.parse::<i64>().unwrap().to_string(),
                    Field::Id => uuid::Uuid::parse_str(&value).unwrap().to_string(),
                    _ => value,
                };
            }
            return Ok(Expr::Compare {
                field: descriptor.field,
                op: Operator::parse(&op)?,
                value,
            });
        }
        if !t.quoted
            && [
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
            ]
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
        let prefix = t.value.ends_with('*');
        let raw = if prefix {
            &t.value[..t.value.len() - 1]
        } else {
            &t.value
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
}
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
/// Explicit legacy raw selectors keep their own binder, including its refusals.
/// Use the shared lexer so quoted words and filter values cannot choose a read path.
pub fn explicit_raw_events(text: &str) -> bool {
    if text.len() > 16_384 {
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
            && ["=", "!=", "<", ">", "<=", ">=", "~", "^", ":", "exists"]
                .contains(&pair[1].value.as_str())
    })
}
pub fn bind(text: &str) -> Result<Bound, &'static str> {
    if text.len() > 16_384 {
        return Err("query exceeds 16 KiB");
    }
    let mut r = Reader {
        tokens: tokens(text)?,
        at: 0,
    };
    if let Some(first) = r.tokens.first() {
        let word = first.value.to_ascii_lowercase();
        if !first.quoted
            && word.len() >= 4
            && !FIELDS.iter().any(|f| f.name == word)
            && !["where", "since", "until", "sort", "limit"].contains(&word.as_str())
            && [
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
            ]
            .iter()
            .any(|u| *u != word && distance(&word, u) <= 2)
        {
            return Err(
                "ambiguous first word resembles a unit or command; correct it, or quote it as literal search text",
            );
        }
    }
    let unit = if r.take("commands") {
        "commands"
    } else {
        r.take("events");
        "events"
    }
    .to_string();
    let explicit = r.at > 0;
    let filter = if explicit {
        if r.take("where") {
            Some(r.expr()?)
        } else {
            None
        }
    } else {
        Some(r.expr()?)
    };
    if unit == "events" && !filter.as_ref().is_some_and(Expr::has_text) {
        return Err(
            "indexed events require words; use commands for canonical rows or the supported raw events subset",
        );
    }
    let mut bound = Bound {
        unit,
        filter,
        sort: None,
        limit: None,
    };
    if r.take("since") {
        let value = r.value()?.value;
        time::OffsetDateTime::parse(&value, &time::format_description::well_known::Rfc3339)
            .map_err(|_| "since requires explicit RFC3339 in this subset")?;
        let e = Expr::Compare {
            field: Field::When,
            op: Operator::Ge,
            value,
        };
        bound.filter = Some(if let Some(old) = bound.filter {
            Expr::And(Box::new(old), Box::new(e))
        } else {
            e
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
mod tests {
    use super::*;
    #[test]
    fn project_roots_are_pure_component_scopes() {
        let projects = vec![
            Project {
                name: "outer".into(),
                roots: vec!["/src/tide".into()],
            },
            Project {
                name: "inner".into(),
                roots: vec!["/src/tide/tools".into()],
            },
            Project {
                name: "worktree".into(),
                roots: vec!["/realm/worktree/tide-*".into()],
            },
        ];
        assert_eq!(
            project("/src/tide/tools/bin", &projects).as_deref(),
            Some("inner")
        );
        let conflict = vec![
            Project {
                name: "a".into(),
                roots: vec!["/src/tide".into()],
            },
            Project {
                name: "b".into(),
                roots: vec!["/src/tide".into(), "/src/tide".into()],
            },
        ];
        assert_eq!(project("/src/tide", &conflict), None);
        assert_eq!(project("/src/tidepool", &projects), None);
        assert_eq!(project("/src/Tide", &projects), None);
        assert_eq!(
            project("/realm/worktree/tide-branch/crate", &projects).as_deref(),
            Some("worktree")
        );
        assert!(
            Project::validate_all(&[Project {
                name: "bad".into(),
                roots: vec!["/src/**".into()]
            }])
            .is_err()
        );
    }
    #[test]
    fn project_depth_counts_components_and_ties_are_unknown() {
        let mut projects = vec![
            Project {
                name: "short".into(),
                roots: vec!["/src/t*".into()],
            },
            Project {
                name: "long".into(),
                roots: vec!["/src/tide*".into()],
            },
        ];
        assert_eq!(project("/src/tidepool/tools", &projects), None);
        projects.push(Project {
            name: "nested".into(),
            roots: vec!["/src/tidepool/tools".into()],
        });
        assert_eq!(
            project("/src/tidepool/tools/bin", &projects).as_deref(),
            Some("nested")
        );
        projects.truncate(1);
        projects[0].roots.push("/src/tide*".into());
        projects[0].roots.push("/src/t*".into());
        assert_eq!(
            project("/src/tidepool/tools", &projects).as_deref(),
            Some("short")
        );
    }
    #[test]
    fn project_wildcard_accepts_empty_suffix_in_present_component() {
        let projects = [Project {
            name: "tide".into(),
            roots: vec!["/src/tide*".into()],
        }];
        for path in [
            "/src/tide",
            "/src/tide/tools",
            "/src/tidepool",
            "/src/tidepool/tools",
        ] {
            assert_eq!(project(path, &projects).as_deref(), Some("tide"), "{path}");
        }
        let any = [Project {
            name: "child".into(),
            roots: vec!["/src/*".into()],
        }];
        assert_eq!(project("/src", &any), None);
        assert_eq!(project("/src/", &any), None);
        assert_eq!(project("/src/tide", &any).as_deref(), Some("child"));
    }
    #[test]
    fn quoted_operator_tokens_are_data_not_field_syntax() {
        for text in [
            r#"commands where cwd "exists""#,
            r#"commands where command "=" "git status""#,
        ] {
            assert!(bind(text).is_err(), "{text}");
        }
        for text in [
            r#"commands where command = "git status""#,
            r#"commands where cwd = "exists""#,
            r#"commands where command ~ "=""#,
        ] {
            assert!(bind(text).is_ok(), "{text}");
        }
    }
    #[test]
    fn explicit_raw_selection_preserves_quoted_data() {
        assert!(explicit_raw_events(
            r#"events where payload.exit_code = "bad""#
        ));
        assert!(explicit_raw_events(
            r#"events where source = terminal.atuin-history and payload.exit_code = "bad""#
        ));
        assert!(!explicit_raw_events(
            r#"events where "payload.exit_code = bad""#
        ));
        assert!(!explicit_raw_events(
            "events where command = payload.exit_code"
        ));
    }
    #[test]
    fn unicode_and_phrase_contract() {
        assert_eq!(normalize("Straße"), "strasse");
        assert_eq!(normalize("CAFE\u{301}"), "café");
        assert_ne!(words("offset"), words("offsets"));
        let b=bind("commands where (cargo or \"git push\") and project = tide and when >= 2026-09-14T00:00:00Z sort when desc limit 20").unwrap();
        assert_eq!(b.sort, Some(true));
        assert!(bind("commands where prject = x").is_err());
        assert!(bind("commands where exit_code = nope").is_err());
    }
}

#[cfg(test)]
#[path = "project_evidence_tests.rs"]
mod project_evidence_tests;

#[cfg(test)]
#[path = "retrieval_text_tests.rs"]
mod text_representation_tests;
