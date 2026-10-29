//! Lower shared syntax into Sinex's checked SQL execution plan.
//! All tokenization, grammar and time expressions belong to query-language.
use super::*;
use jiff::{SignedDuration, Timestamp};
use query_language::ast as syntax;

pub fn bind(text: &str, ctx: &Context) -> Result<Query, QueryError> {
    bind_syntax(super::syntax::parse(text, ctx)?, ctx)
}
pub fn parse(text: &str, ctx: &Context) -> Result<Query, QueryError> {
    lower(super::syntax::parse(text, ctx)?, ctx)
}
pub(super) fn bind_syntax(input: syntax::Query, ctx: &Context) -> Result<Query, QueryError> {
    let input = query_language::bind::bind(&input, &super::syntax::catalog(ctx))
        .map_err(super::syntax::error)?
        .into_query();
    let mut q = lower(input, ctx)?;
    super::typed::check(&mut q, ctx)?;
    super::fill::bind(&mut q, ctx)?;
    Ok(q)
}
pub(super) fn validate_syntax(input: syntax::Query, ctx: &Context) -> Result<Query, QueryError> {
    ctx.clock()?;
    let mut q = Binder {
        ctx,
        validating: true,
    }
    .query(input)?;
    super::typed::check(&mut q, ctx)?;
    super::fill::bind(&mut q, ctx)?;
    Ok(q)
}
pub(super) fn lower(input: syntax::Query, ctx: &Context) -> Result<Query, QueryError> {
    ctx.clock()?;
    Binder {
        ctx,
        validating: false,
    }
    .query(input)
}
#[derive(Clone)]
enum Scope {
    Rows {
        table: Table,
        alias: Option<&'static Alias>,
        columns: Vec<String>,
    },
    Grouped(Vec<String>),
}
impl Scope {
    fn rows(table: Table, alias: Option<&'static Alias>) -> Self {
        Self::Rows {
            table,
            alias,
            columns: vec![],
        }
    }
}
struct Binder<'a> {
    ctx: &'a Context,
    validating: bool,
}
impl Binder<'_> {
    pub(super) fn resolve(
        &self,
        name: &str,
        scope: &Scope,
        at: usize,
    ) -> Result<FieldRef, QueryError> {
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
                table,
                alias,
                columns,
            } => {
                if columns.iter().any(|c| c == name) {
                    return Ok(FieldRef::Group(name.to_string()));
                }
                if let Some(path) = name.strip_prefix("payload.") {
                    if !table.is_events() {
                        return Err(QueryError::at(
                            at,
                            format!("{} have no payload", table.name()),
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
                    .or_else(|| table.field(name))
                {
                    return Ok(FieldRef::Field(f));
                }
                let mut names: Vec<&str> = alias
                    .map(|a| a.fields.iter().map(|f| f.name).collect())
                    .unwrap_or_default();
                names.extend(table.fields().iter().map(|f| f.name));
                let what = alias.map_or(table.name(), |a| a.name);
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

    pub(super) fn time_field(
        &self,
        field: &FieldRef,
        scope: &Scope,
        at: usize,
    ) -> Result<(), QueryError> {
        match (field, scope) {
            (FieldRef::Field(f), _) if f.ty == FieldType::Time => Ok(()),
            (FieldRef::Group(_), Scope::Grouped(_)) => Ok(()),
            (FieldRef::Payload(path), _) if self.payload_time(path) => Ok(()),
            _ => Err(QueryError::at(
                at,
                format!("`{}` is not a time", field.name()),
            )),
        }
    }

    fn payload_time(&self, path: &[String]) -> bool {
        self.ctx
            .catalog
            .as_ref()
            .and_then(|catalog| catalog.field(path))
            .is_none_or(|field| {
                matches!(
                    field.ty,
                    super::catalog::DataType::Time | super::catalog::DataType::Json
                )
            })
    }

    pub(super) fn time_fn_value(
        &mut self,
        func: TimeFn,
        raw: &str,
        at: usize,
    ) -> Result<Value, QueryError> {
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

    fn instant(&self, value: &syntax::Value, at: usize) -> Result<Timestamp, QueryError> {
        Ok(
            query_language::time::resolve(self.scalar(value, at)?, &self.ctx.clock()?)
                .map_err(super::syntax::error)?
                .value
                .start(),
        )
    }
    fn scalar<'a>(&self, value: &'a syntax::Value, at: usize) -> Result<&'a str, QueryError> {
        value
            .text()
            .ok_or_else(|| QueryError::at(at, "expected one scalar value"))
    }
    fn seconds(&self, value: &syntax::Value, at: usize) -> Result<f64, QueryError> {
        time::duration(self.scalar(value, at)?)
            .filter(|s| *s > 0.0)
            .ok_or_else(|| QueryError::at(at, "expected a positive duration"))
    }
    fn operand(
        &self,
        value: &syntax::Operand,
        scope: &Scope,
        at: usize,
    ) -> Result<Operand, QueryError> {
        match value {
            syntax::Operand::Field { name } => Ok(Operand::Field(self.resolve(name, scope, at)?)),
            syntax::Operand::Function { name, arguments } => {
                let func = TimeFn::from_name(name)
                    .ok_or_else(|| QueryError::not_built(at, format!("{name}() is not built")))?;
                let [syntax::Operand::Field { name }] = arguments.as_slice() else {
                    return Err(QueryError::at(at, "time functions take one time field"));
                };
                let field = self.resolve(name, scope, at)?;
                self.time_field(&field, scope, at)?;
                Ok(Operand::TimeFn(func, field))
            }
        }
    }
    fn value(
        &mut self,
        lhs: &Operand,
        op: Op,
        value: &syntax::Value,
        at: usize,
    ) -> Result<Value, QueryError> {
        if matches!(value, syntax::Value::Unknown) {
            return Ok(Value::Unknown);
        }
        if matches!(value, syntax::Value::Any) {
            return Err(QueryError::not_built(
                at,
                "wildcard comparison requires the field:* match form",
            ));
        }
        if let syntax::Value::Set { values } = value {
            if !matches!(op, Op::Eq | Op::Ne) {
                return Err(QueryError::at(at, "a list compares with = or !="));
            }
            return values
                .iter()
                .map(|v| self.value(lhs, op, v, at))
                .collect::<Result<Vec<_>, _>>()
                .map(Value::List);
        }
        if self.validating && matches!(value, syntax::Value::Parameter { .. }) {
            return match lhs {
                Operand::TimeFn(func, _) => self.time_fn_value(
                    *func,
                    match func {
                        TimeFn::Day | TimeFn::Week => "2000-01-01",
                        TimeFn::Hour => "1",
                        TimeFn::Weekday => "mon",
                        TimeFn::Month => "2000-01",
                        TimeFn::MonthDay => "01-01",
                    },
                    at,
                ),
                Operand::Field(FieldRef::Field(f)) => Ok(match f.ty {
                    FieldType::Id | FieldType::IdList => Value::Id(uuid::Uuid::nil()),
                    FieldType::Enum(values) => Value::Text(values[0].into()),
                    FieldType::Integer | FieldType::Number | FieldType::Bool | FieldType::Time => {
                        return Err(QueryError::at(
                            at,
                            "a text parameter does not have this field type",
                        ));
                    }
                    _ if f.name == "event_type" => Value::PlaceholderText("/synthetic".into()),
                    _ => Value::Text("/synthetic".into()),
                }),
                _ => Ok(Value::PlaceholderText("/synthetic".into())),
            };
        }
        let raw = self.scalar(value, at)?;
        let quoted = matches!(value, syntax::Value::Scalar { quoted: true, .. });
        if matches!(value, syntax::Value::Parameter { .. }) {
            return Err(QueryError::at(at, "unbound saved query parameter"));
        }
        match lhs {
            Operand::TimeFn(func, _) => {
                if matches!(op, Op::Contains | Op::Prefix) {
                    return Err(QueryError::at(
                        at,
                        "time functions compare with = != < <= > >=",
                    ));
                }
                self.time_fn_value(*func, raw, at)
            }
            Operand::Field(FieldRef::Group(_)) => Ok(infer(raw, quoted, op)),
            Operand::Field(FieldRef::Payload(path) | FieldRef::TypedPayload { path, .. }) => {
                if self
                    .ctx
                    .catalog
                    .as_ref()
                    .and_then(|c| c.field(path))
                    .is_some_and(|f| f.ty == catalog::DataType::Time)
                {
                    Ok(Value::Time(self.instant(value, at)?))
                } else {
                    Ok(infer(raw, quoted, op))
                }
            }
            Operand::Field(FieldRef::Field(f)) => {
                let text_like = matches!(
                    f.ty,
                    FieldType::Text | FieldType::TextList | FieldType::Path | FieldType::PathList
                );
                if matches!(op, Op::Contains | Op::Prefix) && !text_like {
                    return Err(QueryError::at(at, format!("{} is not text", f.name)));
                }
                if op.ordering()
                    && matches!(
                        f.ty,
                        FieldType::Id
                            | FieldType::IdList
                            | FieldType::PathList
                            | FieldType::TextList
                            | FieldType::Bool
                            | FieldType::Enum(_)
                            | FieldType::Json
                    )
                {
                    return Err(QueryError::at(at, format!("{} cannot be ordered", f.name)));
                }
                Ok(match f.ty {
                    FieldType::Time => Value::Time(self.instant(value, at)?),
                    FieldType::Integer => Value::Integer(
                        raw.parse()
                            .map_err(|_| QueryError::at(at, "expected an integer"))?,
                    ),
                    FieldType::Number => Value::Number(
                        raw.parse::<f64>()
                            .ok()
                            .filter(|n| n.is_finite())
                            .ok_or_else(|| QueryError::at(at, "expected a finite number"))?,
                    ),
                    FieldType::Bool => Value::Bool(
                        raw.parse()
                            .map_err(|_| QueryError::at(at, "expected true or false"))?,
                    ),
                    FieldType::Id | FieldType::IdList => Value::Id(
                        super::reference::parse_id(raw)
                            .ok_or_else(|| QueryError::at(at, "expected an event ID"))?,
                    ),
                    FieldType::Enum(values) => {
                        if !values.contains(&raw) {
                            return Err(QueryError::at(
                                at,
                                format!("{} is one of {}", f.name, values.join(", ")),
                            ));
                        }
                        Value::Text(raw.into())
                    }
                    FieldType::Json => {
                        return Err(QueryError::at(at, "compare payload by its field path"));
                    }
                    _ => {
                        if f.ty == FieldType::PathList
                            && !matches!(op, Op::Eq | Op::Ne | Op::Prefix)
                        {
                            return Err(QueryError::at(at, "path lists compare with = != or ^"));
                        }
                        if matches!(f.ty, FieldType::Path | FieldType::PathList)
                            && op == Op::Prefix
                            && !raw.starts_with('/')
                        {
                            return Err(QueryError::at(at, "path prefix must be absolute"));
                        }
                        if !quoted && raw.contains('*') && matches!(op, Op::Eq | Op::Ne) {
                            Value::Glob(raw.into())
                        } else {
                            Value::Text(raw.into())
                        }
                    }
                })
            }
        }
    }
    fn comparison(
        &mut self,
        lhs: Operand,
        op: Op,
        value: &syntax::Value,
        at: usize,
    ) -> Result<Expr, QueryError> {
        let rhs = self.value(&lhs, op, value, at)?;
        Ok(Expr::Compare { lhs, op, rhs })
    }
    fn condition(&mut self, c: &syntax::Condition, scope: &Scope) -> Result<Expr, QueryError> {
        use syntax::ConditionKind as C;
        let at = c.span.start;
        Ok(match &c.kind {
            C::And { conditions } | C::Or { conditions } => {
                let mut iter = conditions.iter();
                let mut out = self.condition(
                    iter.next()
                        .ok_or_else(|| QueryError::at(at, "empty condition"))?,
                    scope,
                )?;
                for c2 in iter {
                    let rhs = self.condition(c2, scope)?;
                    out = if matches!(c.kind, C::And { .. }) {
                        Expr::And(Box::new(out), Box::new(rhs))
                    } else {
                        Expr::Or(Box::new(out), Box::new(rhs))
                    };
                }
                out
            }
            C::Not { condition } => Expr::Not(Box::new(self.condition(condition, scope)?)),
            C::Match { field, value } => {
                let lhs = Operand::Field(self.resolve(field, scope, at)?);
                match value {
                    syntax::Value::Any => Expr::Truth {
                        value: query_language::semantics::Truth::True,
                        operand: lhs,
                    },
                    syntax::Value::Unknown => Expr::Not(Box::new(Expr::Exists(lhs))),
                    _ => self.comparison(lhs, Op::Eq, value, at)?,
                }
            }
            C::Compare { operand, op, value } => {
                let lhs = self.operand(operand, scope, at)?;
                self.comparison(lhs, *op, value, at)?
            }
            C::Since { operand, value } | C::Until { operand, value } => {
                let lhs = self.operand(operand, scope, at)?;
                let resolved =
                    query_language::time::resolve(self.scalar(value, at)?, &self.ctx.clock()?)
                        .map_err(super::syntax::error)?;
                let until = matches!(c.kind, C::Until { .. });
                let bound = if until {
                    resolved.value.until_bound()
                } else {
                    resolved.value.start()
                };
                Expr::Compare {
                    lhs,
                    op: if until { Op::Lt } else { Op::Ge },
                    rhs: Value::Time(bound),
                }
            }
            C::Similar { value } => {
                let raw = self.scalar(value, at)?;
                let reference=super::reference::parse_id(raw).ok_or_else(||QueryError::not_built(at,"similar requires a supplied text.embedding event reference; no embedding provider is configured for text"))?;
                Expr::Similar { reference }
            }
            C::FieldExists { operand } => Expr::Exists(self.operand(operand, scope, at)?),
            C::Text { text, mode } => {
                if *mode == syntax::TextMode::Approximate {
                    return Err(QueryError::not_built(at, "approximate search is not built"));
                }
                let words = text::words(text);
                if words.is_empty() {
                    return Err(QueryError::at(at, "search contains no words"));
                }
                Expr::Words(Words {
                    phrase: *mode == syntax::TextMode::Phrase || words.len() > 1,
                    prefix: *mode == syntax::TextMode::Prefix,
                    words,
                })
            }
            C::TimeRange { operand, value } => {
                let lhs = self.operand(operand, scope, at)?;
                let field = match &lhs {
                    Operand::Field(f) | Operand::TimeFn(_, f) => f,
                };
                self.time_field(field, scope, at)?;
                let r = query_language::time::resolve(self.scalar(value, at)?, &self.ctx.clock()?)
                    .map_err(super::syntax::error)?
                    .value
                    .range()
                    .ok_or_else(|| QueryError::at(at, "expected a time range"))?;
                Expr::Within {
                    lhs,
                    from: r.start,
                    to: r.end,
                }
            }
            C::Between { operand, from, to } => {
                let lhs = self.operand(operand, scope, at)?;
                let a = self.comparison(lhs.clone(), Op::Ge, from, at)?;
                let b = self.comparison(lhs, Op::Le, to, at)?;
                Expr::And(Box::new(a), Box::new(b))
            }
            C::Overlaps { from, to } => {
                let from = self.instant(from, at)?;
                let to = to.as_ref().map(|v| self.instant(v, at)).transpose()?;
                if to.is_some_and(|to| to <= from) {
                    return Err(QueryError::at(at, "overlap end must follow its start"));
                }
                Expr::Overlaps { from, to }
            }
            C::TimeDifference {
                left,
                right,
                op,
                value,
            } => {
                let Operand::Field(a) = self.operand(left, scope, at)? else {
                    return Err(QueryError::at(at, "time difference needs fields"));
                };
                let Operand::Field(b) = self.operand(right, scope, at)? else {
                    return Err(QueryError::at(at, "time difference needs fields"));
                };
                self.time_field(&a, scope, at)?;
                self.time_field(&b, scope, at)?;
                Expr::TimeDiff {
                    a,
                    b,
                    op: *op,
                    seconds: self.seconds(value, at)?,
                }
            }
            _ => {
                return Err(QueryError::not_built(
                    at,
                    "this query operator has no Sinex execution adapter",
                ));
            }
        })
    }

    fn query(&mut self, input: syntax::Query) -> Result<Query, QueryError> {
        if !input.attach.is_empty() {
            return Err(QueryError::not_built(0, "attached relations are not built"));
        }
        let (source, filter) = match input.source {
            syntax::Source::Unit {
                unit, condition, ..
            } => {
                let alias = Alias::from_name(&unit);
                let table = Table::from_name(&unit)
                    .or(alias.map(|_| Table::Events))
                    .ok_or_else(|| QueryError::new(format!("unknown table {unit}")))?;
                let filter = condition
                    .as_ref()
                    .map(|c| self.condition(c, &Scope::rows(table, alias)))
                    .transpose()?;
                (Source::Table { table, alias }, filter)
            }
            syntax::Source::Walk {
                direction,
                reference,
            } => (
                Source::Walk {
                    walk: match direction {
                        syntax::WalkDirection::Trace => Walk::Trace,
                        syntax::WalkDirection::Parents => Walk::Parents,
                        syntax::WalkDirection::Descendants => Walk::Descendants,
                    },
                    reference: super::reference::parse_reference(&reference)?,
                },
                None,
            ),
            syntax::Source::RecallLast { unit, condition } => {
                let alias = Alias::from_name(&unit)
                    .ok_or_else(|| QueryError::new("missing recorded session relation"))?;
                let filter = condition
                    .as_ref()
                    .map(|c| self.condition(c, &Scope::rows(Table::Events, Some(alias))))
                    .transpose()?;
                (Source::RecallLast { filter }, None)
            }
            syntax::Source::RecallMark { number, window } => (
                Source::RecallMark {
                    number,
                    window: window
                        .as_ref()
                        .map(|v| self.seconds(v, 0))
                        .transpose()?
                        .unwrap_or(1800.0),
                },
                None,
            ),
            syntax::Source::Recall { time, window } => {
                let r = if let Some(syntax::Value::Scalar { text, .. }) = &time
                    && text.starts_with("sinex:window/")
                {
                    match super::reference::parse_reference(text)? {
                        Reference::Window { from, to } => (from, to),
                        _ => unreachable!(),
                    }
                } else {
                    let resolved = time
                        .as_ref()
                        .map(|v| {
                            query_language::time::resolve(self.scalar(v, 0)?, &self.ctx.clock()?)
                                .map_err(super::syntax::error)
                        })
                        .transpose()?;
                    if window.is_none()
                        && let Some(r) = resolved.as_ref().and_then(|r| r.value.range())
                    {
                        (r.start, r.end)
                    } else {
                        let centre = resolved.map_or(self.ctx.now, |r| r.value.start());
                        let w = SignedDuration::from_secs_f64(
                            window
                                .as_ref()
                                .map(|v| self.seconds(v, 0))
                                .transpose()?
                                .unwrap_or(1800.0),
                        );
                        (
                            centre
                                .checked_sub(w)
                                .map_err(|e| QueryError::new(e.to_string()))?,
                            centre
                                .checked_add(w)
                                .map_err(|e| QueryError::new(e.to_string()))?,
                        )
                    }
                };
                (Source::Recall { from: r.0, to: r.1 }, None)
            }
            _ => {
                return Err(QueryError::not_built(
                    0,
                    "saved queries must be resolved before binding",
                ));
            }
        };
        let mut scope = Scope::rows(source.table(), source.alias());
        let mut stages = Vec::new();
        let mut group_input: Option<Scope> = None;
        for stage in input.stages {
            use syntax::StageKind as S;
            let at = stage.span.start;
            match stage.kind {
                S::Where { condition } => {
                    stages.push(Stage::Where(self.condition(&condition, &scope)?))
                }
                S::AsOf { value, sequence } => stages.push(Stage::AsOf(if sequence {
                    AsOf::Seq(
                        self.scalar(&value, at)?
                            .parse()
                            .map_err(|_| QueryError::at(at, "invalid commit position"))?,
                    )
                } else {
                    AsOf::Time(self.instant(&value, at)?)
                })),
                S::Group { keys } => {
                    group_input = Some(scope.clone());
                    let keys = keys
                        .iter()
                        .map(|k| self.operand(k, &scope, at))
                        .collect::<Result<Vec<_>, _>>()?;
                    let group = Group {
                        keys,
                        aggregates: vec![],
                        fill: None,
                    };
                    scope = Scope::Grouped(group_columns(&group));
                    stages.push(Stage::Group(group));
                }
                S::Aggregate { aggregates } => {
                    // Aggregate fields refer to the relation entering a group.
                    let rows = group_input.take().unwrap_or_else(|| scope.clone());
                    let aggregates = aggregates
                        .into_iter()
                        .map(|a| {
                            let function = match a.function {
                                syntax::AggregateFunction::Count => AggFn::Count,
                                syntax::AggregateFunction::Sum => AggFn::Sum,
                                syntax::AggregateFunction::Avg => AggFn::Avg,
                                syntax::AggregateFunction::Min => AggFn::Min,
                                syntax::AggregateFunction::Max => AggFn::Max,
                                syntax::AggregateFunction::CountDistinct => AggFn::CountDistinct,
                                _ => {
                                    return Err(QueryError::not_built(at, "aggregate is not built"));
                                }
                            };
                            if a.fields.len() > 1 {
                                return Err(QueryError::at(at, "aggregate takes one field"));
                            }
                            let field = a
                                .fields
                                .first()
                                .map(|f| self.resolve(f, &rows, at))
                                .transpose()?;
                            if matches!(function, AggFn::Sum | AggFn::Avg)
                                && let Some(FieldRef::Field(f)) = &field
                                && !matches!(f.ty, FieldType::Integer | FieldType::Number)
                            {
                                return Err(QueryError::at(at, "aggregate needs a number"));
                            }
                            Ok(Aggregate { function, field })
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    if let Some(Stage::Group(g)) = stages.last_mut()
                        && g.aggregates.is_empty()
                    {
                        g.aggregates = aggregates;
                        scope = Scope::Grouped(group_columns(g));
                    } else {
                        let g = Group {
                            keys: vec![],
                            aggregates,
                            fill: None,
                        };
                        scope = Scope::Grouped(group_columns(&g));
                        stages.push(Stage::Group(g));
                    }
                }
                S::Fill => {
                    let Some(Stage::Group(g)) = stages.last_mut() else {
                        return Err(QueryError::at(at, "fill follows a grouped relation"));
                    };
                    g.fill = Some(Fill {
                        from: self.ctx.now,
                        to: self.ctx.now,
                    });
                    scope = Scope::Grouped(group_columns(g));
                }
                S::Sort { keys } => {
                    let keys=keys.into_iter().map(|k| {
                    let key=if matches!(&k.operand,syntax::Operand::Field{name} if name=="rank") {SortBy::Rank} else {SortBy::Operand(self.operand(&k.operand,&scope,at)?)};
                    Ok(SortKey{key,descending:k.descending})
                }).collect::<Result<Vec<_>,QueryError>>()?;
                    stages.push(Stage::Sort(keys));
                }
                S::Limit { count } => {
                    if count == 0 {
                        return Err(QueryError::at(at, "limit takes a positive integer"));
                    }
                    stages.push(Stage::Limit(count));
                }
                S::Fields { fields } => stages.push(Stage::Fields(
                    fields
                        .iter()
                        .map(|f| self.resolve(f, &scope, at))
                        .collect::<Result<Vec<_>, _>>()?,
                )),
                S::Temporal {
                    relation,
                    query,
                    at: point,
                    within,
                    all,
                } => {
                    if all {
                        return Err(QueryError::not_built(
                            at,
                            "all temporal matches are not built",
                        ));
                    }
                    let Scope::Rows { columns, .. } = &mut scope else {
                        return Err(QueryError::at(at, "time joins apply before grouping"));
                    };
                    let inner = self.query(*query)?;
                    if !matches!(inner.source, Source::Table { .. })
                        || inner.stages.iter().any(|s| matches!(s, Stage::Group(_)))
                    {
                        return Err(QueryError::at(at, "time join must select rows of a table"));
                    }
                    let kind = match relation {
                        syntax::TemporalRelation::Near => JoinKind::Near,
                        syntax::TemporalRelation::Before => JoinKind::Before,
                        syntax::TemporalRelation::After => JoinKind::After,
                        syntax::TemporalRelation::During => JoinKind::During,
                        syntax::TemporalRelation::Containing => JoinKind::Containing,
                    };
                    let within_secs = within.as_ref().map(|v| self.seconds(v, at)).transpose()?;
                    if matches!(kind, JoinKind::Near | JoinKind::Before | JoinKind::After)
                        && within_secs.is_none()
                    {
                        return Err(QueryError::at(at, "time join needs within duration"));
                    }
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
                    columns.extend(names.iter().map(|name| format!("{name}{suffix}")));
                    stages.push(Stage::Join(Join {
                        kind,
                        inner: Box::new(inner),
                        at: match point {
                            syntax::TimePoint::When => TimePoint::When,
                            syntax::TimePoint::Start => TimePoint::Start,
                            syntax::TimePoint::End => TimePoint::End,
                        },
                        within_secs,
                    }));
                }
                _ => {
                    return Err(QueryError::not_built(
                        at,
                        "this stage has no Sinex execution adapter",
                    ));
                }
            }
        }
        for stage in &mut stages {
            if let Stage::Group(g) = stage
                && g.aggregates.is_empty()
            {
                g.aggregates.push(Aggregate {
                    function: AggFn::Count,
                    field: None,
                });
            }
        }
        let ranked = filter.as_ref().is_some_and(has_words)
            || stages
                .iter()
                .any(|s| matches!(s,Stage::Where(e) if has_words(e)));
        let mut q = Query {
            source,
            filter,
            stages,
            ranked,
        };
        q.ranked |= q.similar_reference()?.is_some();
        Ok(q)
    }
}
fn group_columns(group: &Group) -> Vec<String> {
    let aggregates = if group.aggregates.is_empty() {
        vec!["count".to_owned()]
    } else {
        group.aggregates.iter().map(Aggregate::column).collect()
    };
    group
        .keys
        .iter()
        .map(Operand::column)
        .chain(aggregates)
        .chain(group.fill.iter().map(|_| "coverage".into()))
        .collect()
}
fn has_words(e: &Expr) -> bool {
    match e {
        Expr::Words(_) => true,
        Expr::And(a, b) | Expr::Or(a, b) => has_words(a) || has_words(b),
        _ => false,
    }
}
pub(super) fn infer(raw: &str, quoted: bool, op: Op) -> Value {
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
