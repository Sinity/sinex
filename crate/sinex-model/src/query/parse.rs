//! Parsing and binding (spec `query.md` §2): one pass that reads the tokens,
//! checks every field against its unit's table, and resolves every time
//! against the context. A query that cannot mean what it says is refused with
//! the reason; nothing falls back silently.
use super::QueryError;
use super::ast::*;
use super::lex::{Kind, Token, tokens};
use super::text::words;
use super::time::{self, Context};
use super::units::{Alias, COMMAND_WORDS, FieldType, Unit, unit_like_words, units_accepting};
use jiff::{SignedDuration, Timestamp};
use uuid::Uuid;

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
        unit: Unit,
        alias: Option<&'static Alias>,
        /// Columns time joins add: `near`, `offset`, `during`, `containing`.
        columns: Vec<String>,
    },
    /// After a grouping: the group's key and aggregate columns.
    Grouped(Vec<String>),
}

impl Scope {
    fn rows(unit: Unit, alias: Option<&'static Alias>) -> Scope {
        Scope::Rows {
            unit,
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
            } else if let Some(unit) = Unit::from_name(&word) {
                self.at += 1;
                let filter = self.unit_filter(&Scope::rows(unit, None))?;
                (Source::Unit { unit, alias: None }, filter)
            } else if let Some(alias) = Alias::from_name(&word) {
                self.at += 1;
                let scope = Scope::rows(Unit::Events, Some(alias));
                let filter = self.unit_filter(&scope)?;
                (
                    Source::Unit {
                        unit: Unit::Events,
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
                let scope = Scope::rows(Unit::Events, None);
                let filter = self.filter(&scope)?;
                (
                    Source::Unit {
                        unit: Unit::Events,
                        alias: None,
                    },
                    Some(filter),
                )
            }
        } else if first.kind == Kind::Quoted {
            let scope = Scope::rows(Unit::Events, None);
            let filter = self.filter(&scope)?;
            (
                Source::Unit {
                    unit: Unit::Events,
                    alias: None,
                },
                Some(filter),
            )
        } else {
            return Err(QueryError::at(
                first.at,
                format!("a query starts with a unit or words, not `{}`", first.text),
            ));
        };
        if filter.as_ref().is_some_and(has_words) {
            ranked = true;
        }
        let mut scope = Scope::rows(source.unit(), source.alias());
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

    fn unit_filter(&mut self, scope: &Scope) -> Result<Option<Expr>, QueryError> {
        if self.take_word("where") {
            Ok(Some(self.filter(scope)?))
        } else {
            Ok(None)
        }
    }

    fn filter(&mut self, scope: &Scope) -> Result<Expr, QueryError> {
        let mut e = self.conjunction(scope)?;
        while self.take_word("or") {
            e = Expr::Or(Box::new(e), Box::new(self.conjunction(scope)?));
        }
        Ok(e)
    }

    fn conjunction(&mut self, scope: &Scope) -> Result<Expr, QueryError> {
        let mut e = self.term(scope)?;
        loop {
            if self.take_word("and") {
                e = Expr::And(Box::new(e), Box::new(self.term(scope)?));
            } else if self.peek().is_some()
                && !self.is_word("or")
                && !self.is_symbol(")")
                && !self.at_clause()
            {
                // Terms side by side are joined with `and`.
                e = Expr::And(Box::new(e), Box::new(self.term(scope)?));
            } else {
                return Ok(e);
            }
        }
    }

    fn term(&mut self, scope: &Scope) -> Result<Expr, QueryError> {
        if self.take_word("not") {
            return Ok(Expr::Not(Box::new(self.term(scope)?)));
        }
        if self.take_symbol("(") {
            let e = self.filter(scope)?;
            self.expect_symbol(")")?;
            return Ok(e);
        }
        if self.take_word("since") {
            return self.since(scope);
        }
        if self.take_word("until") {
            return self.until(scope);
        }
        let t = self
            .peek()
            .cloned()
            .ok_or_else(|| QueryError::at(self.end, "expected a condition"))?;
        if t.kind == Kind::Quoted {
            self.at += 1;
            return self.words(&t.text, true, t.at);
        }
        if t.kind == Kind::Symbol {
            return Err(QueryError::at(t.at, format!("unexpected `{}`", t.text)));
        }
        let lower = t.text.to_ascii_lowercase();
        // `day(when) = …`
        if let Some(func) = TimeFn::from_name(&lower)
            && self.peek_at(1).is_some_and(|n| n.is_symbol("("))
        {
            self.at += 2;
            let field = self.field_ref(scope)?;
            self.expect_symbol(")")?;
            self.time_field(&field, scope, t.at)?;
            return self.comparison(Operand::TimeFn(func, field));
        }
        let follows_operator = self.peek_at(1).is_some_and(|n| {
            (n.kind == Kind::Symbol && !n.is_symbol("(") && !n.is_symbol(")") && !n.is_symbol(","))
                || n.is_word("exists")
                || n.is_word("in")
                || (n.kind == Kind::Word && n.text == "-")
        });
        if follows_operator {
            self.at += 1;
            let field = self.resolve(&t.text, scope, t.at)?;
            if self
                .peek()
                .is_some_and(|n| n.kind == Kind::Word && n.text == "-")
            {
                return self.time_diff(field, scope, t.at);
            }
            return self.comparison(Operand::Field(field));
        }
        if RESERVED.contains(&lower.as_str()) {
            return Err(QueryError::at(
                t.at,
                format!(
                    "`{}` is a keyword here; quote it to search for the word",
                    t.text
                ),
            ));
        }
        self.at += 1;
        self.words(&t.text, false, t.at)
    }

    fn words(&mut self, raw: &str, quoted: bool, at: usize) -> Result<Expr, QueryError> {
        let (raw, prefix) = match raw.strip_suffix('*') {
            Some(r) if !quoted => (r, true),
            _ => (raw, false),
        };
        if !quoted && raw.contains('*') {
            return Err(QueryError::at(
                at,
                "only a trailing `*` prefix is supported in search words",
            ));
        }
        let words = words(raw);
        if words.is_empty() {
            return Err(QueryError::at(
                at,
                format!("`{raw}` contains no words to search for"),
            ));
        }
        let phrase = quoted || words.len() > 1;
        Ok(Expr::Words(Words {
            words,
            phrase,
            prefix,
        }))
    }

    /// A field name in the current scope, or `payload.<path>` on events.
    fn resolve(&self, name: &str, scope: &Scope, at: usize) -> Result<FieldRef, QueryError> {
        match scope {
            Scope::Grouped(columns) => {
                if columns.iter().any(|c| c == name) {
                    Ok(FieldRef::Group(name.to_string()))
                } else {
                    Err(QueryError::at(
                        at,
                        format!(
                            "`{name}` is not a column of the grouped rows; they have: {}",
                            columns.join(", ")
                        ),
                    ))
                }
            }
            Scope::Rows {
                unit,
                alias,
                columns,
            } => {
                if columns.iter().any(|c| c == name) {
                    return Ok(FieldRef::Group(name.to_string()));
                }
                if let Some(path) = name.strip_prefix("payload.") {
                    if !unit.is_events() {
                        return Err(QueryError::at(
                            at,
                            format!("{} have no payload", unit.name()),
                        ));
                    }
                    let path: Vec<String> = path.split('.').map(str::to_string).collect();
                    if path.iter().any(String::is_empty) {
                        return Err(QueryError::at(
                            at,
                            format!("`{name}` is not a payload path"),
                        ));
                    }
                    return Ok(FieldRef::Payload(path));
                }
                if let Some(f) = alias
                    .and_then(|a| a.field(name))
                    .or_else(|| unit.field(name))
                {
                    return Ok(FieldRef::Field(f));
                }
                let mut names: Vec<&str> = alias
                    .map(|a| a.fields.iter().map(|f| f.name).collect())
                    .unwrap_or_default();
                names.extend(unit.fields().iter().map(|f| f.name));
                let what = alias.map_or(unit.name(), |a| a.name);
                Err(QueryError::at(
                    at,
                    format!(
                        "`{name}` is not a field of {what}; fields: {}",
                        names.join(", ")
                    ),
                ))
            }
        }
    }

    fn field_ref(&mut self, scope: &Scope) -> Result<FieldRef, QueryError> {
        let t = self.value_token()?;
        self.resolve(&t.text, scope, t.at)
    }

    fn time_field(&self, field: &FieldRef, scope: &Scope, at: usize) -> Result<(), QueryError> {
        match (field, scope) {
            (FieldRef::Field(f), _) if f.ty == FieldType::Time => Ok(()),
            (FieldRef::Group(_), Scope::Grouped(_)) => Ok(()),
            _ => Err(QueryError::at(
                at,
                format!("`{}` is not a time", field.name()),
            )),
        }
    }

    /// The unit's main time, for `since`, `until` and `in` without a field.
    fn main_time(&self, scope: &Scope, at: usize) -> Result<FieldRef, QueryError> {
        match scope {
            Scope::Rows { unit, .. } => Ok(FieldRef::Field(
                unit.field(unit.main_time())
                    .expect("every unit's main time is one of its fields"),
            )),
            Scope::Grouped(_) => Err(QueryError::at(
                at,
                "grouped rows have no main time; name a column",
            )),
        }
    }

    fn since(&mut self, scope: &Scope) -> Result<Expr, QueryError> {
        let at = self.pos();
        let lhs = Operand::Field(self.main_time(scope, at)?);
        let t = self.value_token()?;
        let from = if let Some(secs) = time::duration(&t.text) {
            self.ctx.now - SignedDuration::from_secs_f64(secs)
        } else {
            self.instant_from(&t)?
        };
        Ok(Expr::Compare {
            lhs,
            op: Op::Ge,
            rhs: Value::Time(from),
        })
    }

    fn until(&mut self, scope: &Scope) -> Result<Expr, QueryError> {
        let at = self.pos();
        let lhs = Operand::Field(self.main_time(scope, at)?);
        let t = self.value_token()?;
        // A date includes its whole logical day.
        let to = if let Some(d) = time::date(&t.text) {
            if self.peek().is_some_and(|n| time::clock(&n.text).is_some()) {
                self.at -= 1;
                self.instant_from(&self.toks[self.at].clone())?
            } else {
                self.ctx
                    .day(d)
                    .ok_or_else(|| QueryError::at(t.at, "date out of range"))?
                    .1
            }
        } else {
            self.instant_from(&t)?
        };
        Ok(Expr::Compare {
            lhs,
            op: Op::Lt,
            rhs: Value::Time(to),
        })
    }

    fn comparison(&mut self, lhs: Operand) -> Result<Expr, QueryError> {
        let at = self.pos();
        if self.take_word("exists") {
            return Ok(Expr::Exists(lhs));
        }
        if self.take_word("in") {
            let (from, to) = self.range()?;
            if let Operand::Field(FieldRef::Field(f)) = &lhs
                && f.ty != FieldType::Time
            {
                return Err(QueryError::at(
                    at,
                    format!("`{}` is not a time; `in` takes a time range", f.name),
                ));
            }
            return Ok(Expr::Within { lhs, from, to });
        }
        let op_token = self
            .next()
            .filter(|t| t.kind == Kind::Symbol)
            .ok_or_else(|| {
                QueryError::at(at, "expected an operator: = != < <= > >= ~ ^ : exists in")
            })?;
        let op = match op_token.text.as_str() {
            "=" | ":" => Op::Eq,
            "!=" => Op::Ne,
            "<" => Op::Lt,
            "<=" => Op::Le,
            ">" => Op::Gt,
            ">=" => Op::Ge,
            "~" => Op::Contains,
            "^" => Op::Prefix,
            other => {
                return Err(QueryError::at(
                    op_token.at,
                    format!("`{other}` is not an operator"),
                ));
            }
        };
        let rhs = if self.take_symbol("[") {
            if !matches!(op, Op::Eq | Op::Ne) {
                return Err(QueryError::at(at, "a list compares with `=` or `!=` only"));
            }
            let mut items = Vec::new();
            loop {
                items.push(self.value(&lhs, op)?);
                if self.take_symbol("]") {
                    break;
                }
                self.expect_symbol(",")?;
            }
            Value::List(items)
        } else {
            self.value(&lhs, op)?
        };
        Ok(Expr::Compare { lhs, op, rhs })
    }

    fn value(&mut self, lhs: &Operand, op: Op) -> Result<Value, QueryError> {
        let t = self.value_token()?;
        let at = t.at;
        let raw = t.text.clone();
        let quoted = t.kind == Kind::Quoted;
        match lhs {
            Operand::TimeFn(func, _) => {
                if matches!(op, Op::Contains | Op::Prefix) {
                    return Err(QueryError::at(
                        at,
                        format!("{}() compares with = != < <= > >=", func.name()),
                    ));
                }
                self.time_fn_value(*func, &raw, at)
            }
            Operand::Field(FieldRef::Group(_)) => Ok(infer(&raw, quoted, op)),
            Operand::Field(FieldRef::Payload(_)) => Ok(infer(&raw, quoted, op)),
            Operand::Field(FieldRef::Field(f)) => {
                let text_like = matches!(f.ty, FieldType::Text | FieldType::Path);
                if matches!(op, Op::Contains | Op::Prefix) && !text_like {
                    return Err(QueryError::at(
                        at,
                        format!(
                            "`{}` and `{}` work on text fields; `{}` is not text",
                            Op::Contains.symbol(),
                            Op::Prefix.symbol(),
                            f.name
                        ),
                    ));
                }
                if op.ordering()
                    && matches!(
                        f.ty,
                        FieldType::Id
                            | FieldType::IdList
                            | FieldType::Bool
                            | FieldType::Enum(_)
                            | FieldType::Json
                    )
                {
                    return Err(QueryError::at(
                        at,
                        format!("`{}` cannot be ordered", f.name),
                    ));
                }
                match f.ty {
                    FieldType::Time => {
                        if op.ordering() && time::duration(&raw).is_some() {
                            return Err(QueryError::at(
                                at,
                                format!(
                                    "a bare duration reads backwards here; write `since {raw}` or `{} in last {raw}`",
                                    f.name
                                ),
                            ));
                        }
                        Ok(Value::Time(self.instant_from(&t)?))
                    }
                    FieldType::Integer => raw.parse::<i64>().map(Value::Integer).map_err(|_| {
                        QueryError::at(
                            at,
                            format!(
                                "`{}` is an integer field; `{raw}` is not an integer",
                                f.name
                            ),
                        )
                    }),
                    FieldType::Number => raw.parse::<f64>().map(Value::Number).map_err(|_| {
                        QueryError::at(
                            at,
                            format!("`{}` is a number field; `{raw}` is not a number", f.name),
                        )
                    }),
                    FieldType::Bool => match raw.as_str() {
                        "true" => Ok(Value::Bool(true)),
                        "false" => Ok(Value::Bool(false)),
                        _ => Err(QueryError::at(at, format!("`{}` is true or false", f.name))),
                    },
                    FieldType::Id | FieldType::IdList => parse_id(&raw)
                        .map(Value::Id)
                        .ok_or_else(|| QueryError::at(at, format!("`{raw}` is not an ID"))),
                    FieldType::Enum(values) => {
                        if values.contains(&raw.as_str()) {
                            Ok(Value::Text(raw))
                        } else {
                            let elsewhere = units_accepting(&raw);
                            let hint = if elsewhere.is_empty() {
                                String::new()
                            } else {
                                format!("; `{raw}` is a value of {}", elsewhere.join(", "))
                            };
                            Err(QueryError::at(
                                at,
                                format!("`{}` is one of: {}{hint}", f.name, values.join(", ")),
                            ))
                        }
                    }
                    FieldType::Json => Err(QueryError::at(
                        at,
                        "compare a payload field by its path: `payload.<field>`",
                    )),
                    FieldType::Text | FieldType::Path => {
                        if !quoted && raw.contains('*') && matches!(op, Op::Eq | Op::Ne) {
                            Ok(Value::Glob(raw))
                        } else if f.ty == FieldType::Path
                            && op == Op::Prefix
                            && !raw.starts_with('/')
                        {
                            Err(QueryError::at(
                                at,
                                format!("`{} ^` takes an absolute path", f.name),
                            ))
                        } else {
                            Ok(Value::Text(raw))
                        }
                    }
                }
            }
        }
    }

    fn time_fn_value(&mut self, func: TimeFn, raw: &str, at: usize) -> Result<Value, QueryError> {
        let today = self.ctx.today();
        let date_word = |w: &str| -> Option<jiff::civil::Date> {
            match w {
                "today" => Some(today),
                "yesterday" => today.yesterday().ok(),
                _ => time::date(w),
            }
        };
        let bad = |what: &str| QueryError::at(at, format!("{}() takes {what}", func.name()));
        Ok(match func {
            TimeFn::Day => Value::Text(date_word(raw).ok_or_else(|| bad("a date"))?.to_string()),
            TimeFn::Week => {
                let d = date_word(raw).ok_or_else(|| bad("a date"))?;
                let monday = d
                    .checked_sub(jiff::Span::new().days(d.weekday().to_monday_zero_offset() as i64))
                    .map_err(|_| bad("a date in range"))?;
                Value::Text(monday.to_string())
            }
            TimeFn::Hour => {
                let h: i64 = raw.parse().map_err(|_| bad("an hour, 0 to 23"))?;
                if !(0..=23).contains(&h) {
                    return Err(bad("an hour, 0 to 23"));
                }
                Value::Integer(h)
            }
            TimeFn::Weekday => {
                const DAYS: [&str; 7] = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"];
                let short: String = raw.to_ascii_lowercase().chars().take(3).collect();
                if !DAYS.contains(&short.as_str()) {
                    return Err(bad("a weekday: mon, tue, wed, thu, fri, sat, sun"));
                }
                Value::Text(short)
            }
            TimeFn::Month => {
                if raw == "today" || raw == "now" {
                    Value::Text(format!("{:04}-{:02}", today.year(), today.month()))
                } else {
                    let (y, m) = time::month(raw).ok_or_else(|| bad("a month: 2026-08"))?;
                    Value::Text(format!("{y:04}-{m:02}"))
                }
            }
            TimeFn::MonthDay => {
                if let Some(d) = date_word(raw) {
                    Value::Text(format!("{:02}-{:02}", d.month(), d.day()))
                } else {
                    let ok = raw.len() == 5
                        && raw.as_bytes()[2] == b'-'
                        && raw[..2].parse::<u8>().is_ok_and(|m| (1..=12).contains(&m))
                        && raw[3..].parse::<u8>().is_ok_and(|d| (1..=31).contains(&d));
                    if !ok {
                        return Err(bad("a calendar day: 09-14, or a date"));
                    }
                    Value::Text(raw.to_string())
                }
            }
        })
    }

    /// An instant from a value token, consuming a following clock token when
    /// the value is a date (`2026-03-02 14:05`).
    fn instant_from(&mut self, t: &Token) -> Result<Timestamp, QueryError> {
        let raw = t.text.as_str();
        let ctx = self.ctx;
        let today = ctx.today();
        let day_start = |d: jiff::civil::Date| ctx.day(d).map(|r| r.0);
        let resolved = match raw {
            "now" => Some(ctx.now),
            "today" => day_start(today),
            "yesterday" => today.yesterday().ok().and_then(day_start),
            _ => {
                if let Some(d) = time::date(raw) {
                    if let Some(c) = self.peek().and_then(|n| time::clock(&n.text)) {
                        self.at += 1;
                        ctx.local(d.to_datetime(c))
                    } else {
                        day_start(d)
                    }
                } else if let Some(c) = time::clock(raw) {
                    ctx.local(ctx.zoned(ctx.now).date().to_datetime(c))
                } else {
                    time::instant(ctx, raw)
                }
            }
        };
        resolved.ok_or_else(|| {
            QueryError::at(
                t.at,
                format!("`{raw}` is not a time; use a date (2026-03-02), a date and clock time, RFC 3339, now, today or yesterday"),
            )
        })
    }

    /// A range after `in`: `last 7d`, `today`, `yesterday`, `this week`,
    /// `last week`, `this month`, `last month`, a date, a month or a year.
    fn range(&mut self) -> Result<(Timestamp, Timestamp), QueryError> {
        let t = self.value_token()?;
        let ctx = self.ctx;
        let today = ctx.today();
        let out_of_range = || QueryError::at(t.at, "range out of range");
        let word = t.text.to_ascii_lowercase();
        let r = match word.as_str() {
            "today" => ctx.day(today),
            "yesterday" => today.yesterday().ok().and_then(|d| ctx.day(d)),
            "this" | "last" => {
                let next = self.value_token()?;
                let what = next.text.to_ascii_lowercase();
                match (word.as_str(), what.as_str()) {
                    ("this", "week") => ctx.week_of(today),
                    ("last", "week") => today
                        .checked_sub(jiff::Span::new().days(7))
                        .ok()
                        .and_then(|d| ctx.week_of(d)),
                    ("this", "month") => ctx.month_of(today.year(), today.month()),
                    ("last", "month") => {
                        let first = today.first_of_month();
                        let prev = first.yesterday().map_err(|_| out_of_range())?;
                        ctx.month_of(prev.year(), prev.month())
                    }
                    ("last", _) => {
                        if let Some(secs) = time::duration(&what) {
                            Some((ctx.now - SignedDuration::from_secs_f64(secs), ctx.now))
                        } else if let Ok(n) = what.parse::<i64>() {
                            self.expect_word("days")?;
                            let from = today
                                .checked_sub(jiff::Span::new().days(n - 1))
                                .map_err(|_| out_of_range())?;
                            ctx.days(from, today)
                        } else {
                            return Err(QueryError::at(
                                next.at,
                                "after `last`: a duration (7d), N days, week or month",
                            ));
                        }
                    }
                    _ => return Err(QueryError::at(next.at, "after `this`: week or month")),
                }
            }
            _ => {
                if let Some(d) = time::date(&word) {
                    ctx.day(d)
                } else if let Some((y, m)) = time::month(&word) {
                    ctx.month_of(y, m)
                } else if let Some(y) = time::year(&word) {
                    ctx.year_of(y)
                } else {
                    return Err(QueryError::at(
                        t.at,
                        "a range: last 7d, today, yesterday, this week, last week, this month, last month, a date, a month (2026-08) or a year",
                    ));
                }
            }
        };
        r.ok_or_else(out_of_range)
    }

    fn time_diff(&mut self, a: FieldRef, scope: &Scope, at: usize) -> Result<Expr, QueryError> {
        self.time_field(&a, scope, at)?;
        self.at += 1; // `-`
        let b = self.field_ref(scope)?;
        self.time_field(&b, scope, at)?;
        let op_token = self
            .next()
            .ok_or_else(|| QueryError::at(self.end, "expected an operator"))?;
        let op = match op_token.text.as_str() {
            "<" => Op::Lt,
            "<=" => Op::Le,
            ">" => Op::Gt,
            ">=" => Op::Ge,
            _ => {
                return Err(QueryError::at(
                    op_token.at,
                    "a time difference compares with < <= > >=",
                ));
            }
        };
        let d = self.value_token()?;
        let seconds = time::duration(&d.text).ok_or_else(|| {
            QueryError::at(d.at, "a time difference compares with a duration: 1d")
        })?;
        Ok(Expr::TimeDiff { a, b, op, seconds })
    }

    fn reference(&mut self) -> Result<Reference, QueryError> {
        let t = self.value_token()?;
        parse_reference(&t.text).map_err(|e| QueryError {
            at: Some(t.at),
            ..e
        })
    }

    fn recall(&mut self) -> Result<Source, QueryError> {
        let ctx = self.ctx;
        let default_window = 30.0 * 60.0;
        let Some(t) = self.peek().cloned() else {
            let w = SignedDuration::from_secs_f64(default_window);
            return Ok(Source::Recall {
                from: ctx.now - w,
                to: ctx.now + w,
            });
        };
        let word = t.text.to_ascii_lowercase();
        if matches!(word.as_str(), "last" | "mark") {
            return Err(QueryError::not_built(
                t.at,
                format!("`recall {word}` needs sessions and moment marks, which are not built yet"),
            ));
        }
        if CLAUSE_WORDS.contains(&word.as_str()) {
            let w = SignedDuration::from_secs_f64(default_window);
            return Ok(Source::Recall {
                from: ctx.now - w,
                to: ctx.now + w,
            });
        }
        // A whole day.
        let day = match word.as_str() {
            "today" => Some(ctx.today()),
            "yesterday" => ctx.today().yesterday().ok(),
            _ => time::date(&word).filter(|_| {
                !self
                    .peek_at(1)
                    .is_some_and(|n| time::clock(&n.text).is_some())
            }),
        };
        if let Some(d) = day {
            self.at += 1;
            let (from, to) = ctx
                .day(d)
                .ok_or_else(|| QueryError::at(t.at, "date out of range"))?;
            return Ok(Source::Recall { from, to });
        }
        self.at += 1;
        let centre = self.instant_from(&t)?;
        let mut window = default_window;
        if let Some(w) = self.peek().cloned()
            && let Some(rest) = w
                .text
                .strip_prefix('±')
                .or_else(|| w.text.strip_prefix("+-"))
        {
            self.at += 1;
            window = time::duration(rest).ok_or_else(|| QueryError::at(w.at, "a window: ±20m"))?;
        }
        let w = SignedDuration::from_secs_f64(window);
        Ok(Source::Recall {
            from: centre - w,
            to: centre + w,
        })
    }

    fn clause(&mut self, scope: &mut Scope) -> Result<Stage, QueryError> {
        let t = self.peek().cloned().expect("caller checked for the end");
        let word = t.text.to_ascii_lowercase();
        if t.kind != Kind::Word {
            return Err(QueryError::at(t.at, format!("unexpected `{}`", t.text)));
        }
        self.at += 1;
        match word.as_str() {
            "where" => Ok(Stage::Where(self.filter(scope)?)),
            "since" => Ok(Stage::Where(self.since(scope)?)),
            "until" => Ok(Stage::Where(self.until(scope)?)),
            "near" | "before" | "after" | "during" | "containing" => {
                if matches!(scope, Scope::Grouped(_)) {
                    return Err(QueryError::at(
                        t.at,
                        "time joins apply to rows, not to groups; join before grouping",
                    ));
                }
                let kind = match word.as_str() {
                    "near" => JoinKind::Near,
                    "before" => JoinKind::Before,
                    "after" => JoinKind::After,
                    "during" => JoinKind::During,
                    _ => JoinKind::Containing,
                };
                self.expect_symbol("(")?;
                let inner_at = self.pos();
                let inner = self.query(true)?;
                self.expect_symbol(")")?;
                if !matches!(inner.source, Source::Unit { .. })
                    || inner.stages.iter().any(|s| matches!(s, Stage::Group(_)))
                {
                    return Err(QueryError::at(
                        inner_at,
                        "the inner query of a time join selects rows of a unit, without grouping",
                    ));
                }
                let mut at = TimePoint::When;
                if self.take_word("at") {
                    let p = self.value_token()?;
                    at = match p.text.as_str() {
                        "when" => TimePoint::When,
                        "start" => TimePoint::Start,
                        "end" => TimePoint::End,
                        _ => return Err(QueryError::at(p.at, "`at` takes when, start or end")),
                    };
                }
                let within_secs =
                    if matches!(kind, JoinKind::Near | JoinKind::Before | JoinKind::After) {
                        self.expect_word("within")?;
                        let d = self.value_token()?;
                        Some(
                            time::duration(&d.text).ok_or_else(|| {
                                QueryError::at(d.at, "`within` takes a duration: 5m")
                            })?,
                        )
                    } else {
                        None
                    };
                if let Scope::Rows { columns, .. } = scope {
                    // Mirrors the compiler: the first join's columns are
                    // `near`, `offset`; later ones get `_2`, `_3`.
                    let n = columns
                        .iter()
                        .filter(|c| {
                            ["near", "during", "containing"].iter().any(|base| {
                                c.as_str() == *base || c.starts_with(&format!("{base}_"))
                            })
                        })
                        .count();
                    let suffix = if n == 0 {
                        String::new()
                    } else {
                        format!("_{}", n + 1)
                    };
                    let names: &[&str] = match kind {
                        JoinKind::Near | JoinKind::Before | JoinKind::After => &["near", "offset"],
                        JoinKind::During => &["during"],
                        JoinKind::Containing => &["containing"],
                    };
                    columns.extend(names.iter().map(|b| format!("{b}{suffix}")));
                }
                Ok(Stage::Join(Join {
                    kind,
                    inner: Box::new(inner),
                    at,
                    within_secs,
                }))
            }
            "as" => {
                self.expect_word("of")?;
                if self.take_word("seq") {
                    let n = self.value_token()?;
                    let seq = n
                        .text
                        .parse::<i64>()
                        .map_err(|_| QueryError::at(n.at, "`as of seq` takes a commit position"))?;
                    return Ok(Stage::AsOf(AsOf::Seq(seq)));
                }
                let v = self.value_token()?;
                Ok(Stage::AsOf(AsOf::Time(self.instant_from(&v)?)))
            }
            "group" => {
                self.expect_word("by")?;
                let mut keys = Vec::new();
                loop {
                    keys.push(self.group_key(scope)?);
                    if !self.take_symbol(",") {
                        break;
                    }
                }
                let mut aggregates = self.aggregates(scope)?;
                if aggregates.is_empty() {
                    aggregates.push(Aggregate {
                        function: AggFn::Count,
                        field: None,
                    });
                }
                if self.is_word("fill") {
                    return Err(QueryError::not_built(
                        self.pos(),
                        "`fill` needs coverage from the analysis layer, which is not built yet",
                    ));
                }
                let group = Group { keys, aggregates };
                *scope = Scope::Grouped(group_columns(&group));
                Ok(Stage::Group(group))
            }
            "count" | "sum" | "min" | "max" | "avg" | "count_distinct" => {
                self.at -= 1;
                let aggregates = self.aggregates(scope)?;
                let group = Group {
                    keys: Vec::new(),
                    aggregates,
                };
                *scope = Scope::Grouped(group_columns(&group));
                Ok(Stage::Group(group))
            }
            "fill" => Err(QueryError::not_built(
                t.at,
                "`fill` needs coverage from the analysis layer, which is not built yet",
            )),
            "sort" => {
                let mut keys = Vec::new();
                loop {
                    let k = self
                        .peek()
                        .cloned()
                        .ok_or_else(|| QueryError::at(self.end, "expected a sort key"))?;
                    let key = if k.is_word("rank") {
                        self.at += 1;
                        SortBy::Rank
                    } else {
                        SortBy::Operand(self.group_key(scope)?)
                    };
                    let descending = if self.take_word("desc") {
                        true
                    } else {
                        self.take_word("asc");
                        false
                    };
                    keys.push(SortKey { key, descending });
                    if !self.take_symbol(",") {
                        break;
                    }
                }
                Ok(Stage::Sort(keys))
            }
            "limit" => {
                let n = self.value_token()?;
                let limit = n
                    .text
                    .parse::<u64>()
                    .ok()
                    .filter(|n| *n > 0)
                    .ok_or_else(|| QueryError::at(n.at, "`limit` takes a positive integer"))?;
                Ok(Stage::Limit(limit))
            }
            "fields" => {
                let mut fields = Vec::new();
                loop {
                    fields.push(self.field_ref(scope)?);
                    if !self.take_symbol(",") {
                        break;
                    }
                }
                Ok(Stage::Fields(fields))
            }
            _ => Err(QueryError::at(
                t.at,
                format!(
                    "`{}` is not a clause; clauses: {}",
                    t.text,
                    CLAUSE_WORDS.join(", ")
                ),
            )),
        }
    }

    fn group_key(&mut self, scope: &Scope) -> Result<Operand, QueryError> {
        let t = self.value_token()?;
        if let Some(func) = TimeFn::from_name(&t.text.to_ascii_lowercase())
            && self.take_symbol("(")
        {
            let field = self.field_ref(scope)?;
            self.expect_symbol(")")?;
            self.time_field(&field, scope, t.at)?;
            return Ok(Operand::TimeFn(func, field));
        }
        Ok(Operand::Field(self.resolve(&t.text, scope, t.at)?))
    }

    fn aggregates(&mut self, scope: &Scope) -> Result<Vec<Aggregate>, QueryError> {
        let mut out = Vec::new();
        while let Some(function) = self
            .peek()
            .filter(|t| t.kind == Kind::Word)
            .and_then(|t| AggFn::from_name(&t.text.to_ascii_lowercase()))
        {
            self.at += 1;
            let field = if function == AggFn::Count && !self.is_symbol("(") {
                None
            } else {
                self.expect_symbol("(")?;
                let f = self.field_ref(scope)?;
                self.expect_symbol(")")?;
                if let FieldRef::Field(field) = &f
                    && matches!(function, AggFn::Sum | AggFn::Avg)
                    && !matches!(field.ty, FieldType::Integer | FieldType::Number)
                {
                    return Err(QueryError::new(format!(
                        "{}() needs a number; `{}` is not one",
                        function.name(),
                        field.name
                    )));
                }
                Some(f)
            };
            out.push(Aggregate { function, field });
            self.take_symbol(",");
        }
        Ok(out)
    }
}

fn group_columns(group: &Group) -> Vec<String> {
    group
        .keys
        .iter()
        .map(Operand::column)
        .chain(group.aggregates.iter().map(Aggregate::column))
        .collect()
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

/// A value whose type comes from the literal: payload paths and grouped
/// columns have no declared type.
fn infer(raw: &str, quoted: bool, op: Op) -> Value {
    if quoted {
        return Value::Text(raw.to_string());
    }
    if let Ok(i) = raw.parse::<i64>() {
        return Value::Integer(i);
    }
    if let Ok(n) = raw.parse::<f64>()
        && n.is_finite()
    {
        return Value::Number(n);
    }
    match raw {
        "true" => Value::Bool(true),
        "false" => Value::Bool(false),
        _ if raw.contains('*') && matches!(op, Op::Eq | Op::Ne) => Value::Glob(raw.to_string()),
        _ => Value::Text(raw.to_string()),
    }
}

fn parse_id(raw: &str) -> Option<Uuid> {
    let raw = raw.strip_prefix("sinex:ev/").unwrap_or(raw);
    Uuid::parse_str(raw).ok()
}

/// A citation: `sinex:ev/<id>`, a bare event ID, `sinex:occ/...` or
/// `sinex:eq/...` (spec `query.md` §6.4).
pub fn parse_reference(raw: &str) -> Result<Reference, QueryError> {
    if let Some(id) = parse_id(raw) {
        return Ok(Reference::Event(id));
    }
    if let Some(rest) = raw.strip_prefix("sinex:occ/") {
        let parts: Vec<&str> = rest.split('/').collect();
        if !(2..=4).contains(&parts.len()) {
            return Err(QueryError::new(
                "sinex:occ/<material>/<anchor>[/<event type>[/<ordinal>]]",
            ));
        }
        let material = Uuid::parse_str(parts[0])
            .map_err(|_| QueryError::new("the material in sinex:occ is not an ID"))?;
        let anchor = parts[1]
            .parse()
            .map_err(|_| QueryError::new("the anchor in sinex:occ is not a byte offset"))?;
        let ordinal = parts
            .get(3)
            .map(|o| {
                o.parse()
                    .map_err(|_| QueryError::new("the ordinal in sinex:occ is not a number"))
            })
            .transpose()?;
        return Ok(Reference::Occurrence {
            material,
            anchor,
            event_type: parts.get(2).map(|s| s.to_string()),
            ordinal,
        });
    }
    if let Some(rest) = raw.strip_prefix("sinex:eq/") {
        let mut parts = rest.splitn(3, '/');
        let (Some(source), Some(event_type), Some(key)) =
            (parts.next(), parts.next(), parts.next())
        else {
            return Err(QueryError::new(
                "sinex:eq/<source>/<event type>/<equivalence key>",
            ));
        };
        return Ok(Reference::Equivalence {
            source: source.to_string(),
            event_type: event_type.to_string(),
            key: key.to_string(),
        });
    }
    if raw.starts_with("sinex:at/") {
        return Err(QueryError {
            not_built: true,
            ..QueryError::new("sinex:at citations need spanning outputs, which are not built yet")
        });
    }
    Err(QueryError::new(format!(
        "`{raw}` is not a reference; use an event ID, sinex:ev/<id>, sinex:occ/... or sinex:eq/..."
    )))
}

/// A first word within two typing mistakes of a unit, alias or command.
fn typo(word: &str) -> Option<&'static str> {
    if word.len() < 4 || RESERVED.contains(&word) || Unit::Events.field(word).is_some() {
        return None;
    }
    unit_like_words().find(|candidate| *candidate != word && distance(word, candidate) <= 2)
}

/// Levenshtein distance over bytes.
fn distance(a: &str, b: &str) -> usize {
    let mut previous: Vec<usize> = (0..=b.len()).collect();
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
