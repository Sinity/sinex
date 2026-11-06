//! Lower shared syntax into Sinex's checked SQL execution plan.
//! All tokenization, grammar and time expressions belong to query-language.
use super::*;
use jiff::Timestamp;
use query_language::ast as syntax;
use query_language::scope::{FieldInfo, NodePath, Origin, Scope, Typing};

pub fn bind(text: &str, ctx: &Context) -> Result<Query, QueryError> {
    bind_syntax(super::syntax::parse(text, ctx)?, ctx)
}
pub fn parse(text: &str, ctx: &Context) -> Result<Query, QueryError> {
    lower(
        shared_bound(super::syntax::parse(text, ctx)?, ctx)?,
        ctx,
        false,
    )
}
fn shared_bound(
    input: syntax::Query,
    ctx: &Context,
) -> Result<query_language::bind::BoundQuery, QueryError> {
    query_language::bind::bind(&input, &super::syntax::catalog(ctx)).map_err(super::syntax::error)
}
pub(super) fn bind_syntax(input: syntax::Query, ctx: &Context) -> Result<Query, QueryError> {
    let mut q = lower(shared_bound(input, ctx)?, ctx, false)?;
    super::typed::check(&mut q, ctx)?;
    super::fill::bind(&mut q, ctx)?;
    Ok(q)
}
pub(super) fn validate_syntax(input: syntax::Query, ctx: &Context) -> Result<Query, QueryError> {
    let mut q = lower(shared_bound(input, ctx)?, ctx, true)?;
    super::typed::check(&mut q, ctx)?;
    super::fill::bind(&mut q, ctx)?;
    Ok(q)
}
pub(super) fn bind_native_syntax(
    input: syntax::Query,
    ctx: &Context,
    source: Source,
    validating: bool,
) -> Result<Query, QueryError> {
    let mut query = lower(shared_bound(input, ctx)?, ctx, validating)?;
    query.source = source;
    super::typed::check(&mut query, ctx)?;
    super::fill::bind(&mut query, ctx)?;
    Ok(query)
}
fn lower(
    bound: query_language::bind::BoundQuery,
    ctx: &Context,
    validating: bool,
) -> Result<Query, QueryError> {
    ctx.clock()?;
    let (input, typing) = bound.into_parts();
    Binder {
        ctx,
        validating,
        typing: &typing,
    }
    .query(input, &NodePath::default())
}
struct Binder<'a> {
    ctx: &'a Context,
    validating: bool,
    typing: &'a Typing,
}
impl Binder<'_> {
    fn resolve_info(&self, info: &FieldInfo, at: usize) -> Result<FieldRef, QueryError> {
        if let Origin::Catalog { unit, field } = &info.origin {
            let alias = Alias::from_name(unit);
            let table = Table::from_name(unit)
                .or(alias.map(|_| Table::Events))
                .ok_or_else(|| {
                    QueryError::at(at, format!("unsupported bound row domain {unit}"))
                })?;
            if let Some(path) = field.strip_prefix("payload.") {
                if !table.is_events() {
                    return Err(QueryError::at(at, "bound relation has no payload"));
                }
                return Ok(FieldRef::Payload(
                    path.split('.').map(str::to_owned).collect(),
                ));
            }
            return alias
                .and_then(|alias| alias.field(field))
                .or_else(|| table.field(field))
                .map(FieldRef::Field)
                .or_else(|| NativeField::from_name(field).map(FieldRef::Native))
                .ok_or_else(|| {
                    QueryError::at(
                        at,
                        format!("bound catalog field {unit}.{field} has no execution adapter"),
                    )
                });
        }
        Ok(FieldRef::TypedGroup(BoundColumn::new(info.clone())))
    }
    pub(super) fn resolve(
        &self,
        name: &str,
        scope: &Scope,
        at: usize,
    ) -> Result<FieldRef, QueryError> {
        let info = scope
            .field(name)
            .ok_or_else(|| QueryError::at(at, format!("{name} is absent from its bound scope")))?;
        self.resolve_info(info, at)
    }

    pub(super) fn time_field(
        &self,
        field: &FieldRef,
        scope: &Scope,
        at: usize,
    ) -> Result<(), QueryError> {
        match field {
            FieldRef::Field(f) if f.ty == FieldType::Time => Ok(()),
            FieldRef::Native(field) if field.field_type() == FieldType::Time => Ok(()),
            FieldRef::TypedGroup(column)
                if scope.field(column.name()).is_some_and(|field| {
                    matches!(
                        field.field.kind,
                        query_language::catalog::FieldKind::Time
                            | query_language::catalog::FieldKind::Json
                    )
                }) =>
            {
                Ok(())
            }
            FieldRef::Payload(path) if self.payload_time(path) => Ok(()),
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
        time_fn_value(self.ctx, func, raw, at)
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
    fn seconds(&self, value: &syntax::Value, at: usize) -> Result<String, QueryError> {
        time::duration_decimal(self.scalar(value, at)?)
            .filter(|seconds| !seconds.starts_with('-'))
            .ok_or_else(|| QueryError::at(at, "expected a non-negative duration"))
    }
    fn operand(
        &self,
        value: &syntax::Expression,
        scope: &Scope,
        at: usize,
    ) -> Result<Operand, QueryError> {
        self.operand_at(value, scope, at, None)
    }
    fn operand_at(
        &self,
        value: &syntax::Expression,
        scope: &Scope,
        at: usize,
        path: Option<&NodePath>,
    ) -> Result<Operand, QueryError> {
        use query_language::catalog::FieldKind as K;
        Ok(match value {
            syntax::Expression::Field { name } => {
                Operand::Field(match path.and_then(|path| self.typing.operand(path)) {
                    Some(info) => self.resolve_info(&info.result, at)?,
                    None => self.resolve(name, scope, at)?,
                })
            }
            syntax::Expression::Value { value } => {
                let (value, kind) = match value {
                    syntax::Value::Unknown => (Value::Unknown, K::Json),
                    syntax::Value::Scalar {
                        text,
                        quoted: false,
                    } if query_language::number::is_decimal(text) => {
                        (Value::Text(text.clone()), K::Number)
                    }
                    syntax::Value::Scalar {
                        text,
                        quoted: false,
                    } if matches!(text.as_str(), "true" | "false") => {
                        (Value::Bool(text == "true"), K::Bool)
                    }
                    syntax::Value::Scalar {
                        text,
                        quoted: false,
                    } if time::duration_decimal(text).is_some() => (
                        Value::Text(time::duration_decimal(text).unwrap()),
                        K::Duration,
                    ),
                    syntax::Value::Scalar { text, .. } => (Value::Text(text.clone()), K::Text),
                    syntax::Value::Parameter { name } if self.validating => {
                        (Value::PlaceholderText(name.clone()), K::Text)
                    }
                    _ => return Err(QueryError::at(at, "an expression needs one scalar value")),
                };
                Operand::Literal { value, kind }
            }
            syntax::Expression::Arithmetic {
                operator,
                left,
                right,
            } => {
                let left_path = path.map(|path| path.field("left"));
                let right_path = path.map(|path| path.field("right"));
                let left = self.operand_at(left, scope, at, left_path.as_ref())?;
                let right = self.operand_at(right, scope, at, right_path.as_ref())?;
                let kind = path
                    .and_then(|path| self.typing.operand(path))
                    .map(|info| info.result.field.kind.clone())
                    .or_else(|| {
                        query_language::scope::arithmetic_kind(
                            *operator,
                            &left.kind(),
                            &right.kind(),
                        )
                    })
                    .ok_or_else(|| {
                        QueryError::at(at, "arithmetic operands have incompatible dimensions")
                    })?;
                Operand::Arithmetic {
                    operator: *operator,
                    left: Box::new(left),
                    right: Box::new(right),
                    kind,
                }
            }
            syntax::Expression::Function { name, arguments } => {
                let arguments = arguments
                    .iter()
                    .enumerate()
                    .map(|(index, argument)| {
                        let argument_path = path.map(|path| path.field("arguments").index(index));
                        self.operand_at(argument, scope, at, argument_path.as_ref())
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                if name == "text" {
                    let [argument] = arguments.as_slice() else {
                        return Err(QueryError::at(at, "text takes one value"));
                    };
                    Operand::Text(Box::new(argument.clone()))
                } else if name == "abs" {
                    let [argument] = arguments.as_slice() else {
                        return Err(QueryError::at(at, "abs takes one numeric value"));
                    };
                    Operand::Absolute(Box::new(argument.clone()))
                } else if name == "len" {
                    let [argument] = arguments.as_slice() else {
                        return Err(QueryError::at(at, "len takes one value"));
                    };
                    Operand::Length(Box::new(argument.clone()))
                } else if name == "glob" {
                    let [left, right] = arguments.as_slice() else {
                        return Err(QueryError::at(at, "glob takes two text values"));
                    };
                    Operand::Glob {
                        left: Box::new(left.clone()),
                        right: Box::new(right.clone()),
                    }
                } else if name == "case_eq" {
                    let [left, right, value] = arguments.as_slice() else {
                        return Err(QueryError::at(at, "case_eq takes three values"));
                    };
                    Operand::CaseEqual {
                        left: Box::new(left.clone()),
                        right: Box::new(right.clone()),
                        value: Box::new(value.clone()),
                    }
                } else {
                    let func = TimeFn::from_name(name).ok_or_else(|| {
                        QueryError::not_built(at, format!("{name}() is not built"))
                    })?;
                    let [Operand::Field(field)] = arguments.as_slice() else {
                        return Err(QueryError::at(at, "time functions take one time field"));
                    };
                    self.time_field(field, scope, at)?;
                    Operand::TimeFn(func, field.clone())
                }
            }
        })
    }
    fn column(
        &self,
        column: &syntax::Column,
        scope: &Scope,
        at: usize,
    ) -> Result<Operand, QueryError> {
        let operand = self.operand(&column.expression, scope, at)?;
        let name = column.column();
        if operand.column() == name {
            Ok(operand)
        } else {
            Ok(Operand::Named {
                operand: Box::new(operand),
                name,
            })
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
                        TimeFn::Hour | TimeFn::Year => "1",
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
            // The typed pass knows the selected payload kinds and output columns.
            // Keep the scalar spelling until that declaration chooses its type.
            Operand::Field(
                FieldRef::Native(_)
                | FieldRef::Group(_)
                | FieldRef::TypedGroup(_)
                | FieldRef::Payload(_)
                | FieldRef::TypedPayload { .. },
            ) => Ok(Value::Text(raw.to_owned())),
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
                    FieldType::Integer | FieldType::Number => Value::Text(raw.to_owned()),
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
                        Value::Text(raw.into())
                    }
                })
            }
            Operand::Named { operand, .. } => self.value(operand, op, value, at),
            other => {
                use query_language::catalog::FieldKind as K;
                Ok(match other.kind() {
                    K::Integer | K::Number | K::Money => Value::Text(raw.to_owned()),
                    K::Duration => Value::Text(
                        time::duration_decimal(raw)
                            .ok_or_else(|| QueryError::at(at, "expected a duration"))?,
                    ),
                    K::Time => Value::Time(self.instant(value, at)?),
                    K::Bool => Value::Bool(
                        raw.parse()
                            .map_err(|_| QueryError::at(at, "expected true or false"))?,
                    ),
                    _ => Value::Text(raw.to_owned()),
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
    fn natural_match(
        &mut self,
        field: &str,
        value: &syntax::Value,
        scope: &Scope,
        at: usize,
    ) -> Result<Expr, QueryError> {
        if let syntax::Value::Set { values } = value {
            let mut values = values.iter();
            let first = values
                .next()
                .ok_or_else(|| QueryError::at(at, "empty match set"))?;
            let mut result = self.natural_match(field, first, scope, at)?;
            for value in values {
                result = Expr::Or(
                    Box::new(result),
                    Box::new(self.natural_match(field, value, scope, at)?),
                );
            }
            return Ok(result);
        }
        Ok({
            let lhs = Operand::Field(self.resolve(field, scope, at)?);
            match value {
                syntax::Value::Any => Expr::Truth {
                    value: query_language::semantics::Truth::True,
                    operand: lhs,
                },
                syntax::Value::Unknown => Expr::Not(Box::new(Expr::Exists(lhs))),
                syntax::Value::Known => Expr::Exists(lhs),
                _ => {
                    use query_language::catalog::NaturalMatch as M;
                    match scope
                        .field(field)
                        .expect("resolved field")
                        .field
                        .natural_match()
                    {
                        M::Contains => self.comparison(lhs, Op::Contains, value, at)?,
                        M::Subtree => self.comparison(lhs, Op::Prefix, value, at)?,
                        M::Words => {
                            let raw = self.scalar(value, at)?;
                            Expr::FieldWords {
                                operand: lhs,
                                words: Words {
                                    words: vec![raw.to_lowercase()],
                                    phrase: false,
                                    prefix: false,
                                },
                            }
                        }
                        M::Period => {
                            let resolved = query_language::time::resolve(
                                self.scalar(value, at)?,
                                &self.ctx.clock()?,
                            )
                            .map_err(super::syntax::error)?
                            .value;
                            if let Some(range) = resolved.range() {
                                Expr::Within {
                                    lhs,
                                    from: range.start,
                                    to: range.end,
                                }
                            } else {
                                self.comparison(lhs, Op::Eq, value, at)?
                            }
                        }
                        M::Equal | M::Same => self.comparison(lhs, Op::Eq, value, at)?,
                    }
                }
            }
        })
    }
    fn condition(
        &mut self,
        c: &syntax::Condition,
        scope: &Scope,
        path: &NodePath,
    ) -> Result<Expr, QueryError> {
        use syntax::ConditionKind as C;
        let at = c.span.start;
        Ok(match &c.kind {
            C::And { conditions } | C::Or { conditions } => {
                let mut iter = conditions.iter().enumerate();
                let mut out = self.condition(
                    iter.next()
                        .ok_or_else(|| QueryError::at(at, "empty condition"))?
                        .1,
                    scope,
                    &path.field("kind").field("conditions").index(0),
                )?;
                for (index, c2) in iter {
                    let rhs = self.condition(
                        c2,
                        scope,
                        &path.field("kind").field("conditions").index(index),
                    )?;
                    out = if matches!(c.kind, C::And { .. }) {
                        Expr::And(Box::new(out), Box::new(rhs))
                    } else {
                        Expr::Or(Box::new(out), Box::new(rhs))
                    };
                }
                out
            }
            C::Not { condition } => Expr::Not(Box::new(self.condition(
                condition,
                scope,
                &path.field("kind").field("condition"),
            )?)),
            C::Match { field, value } => self.natural_match(field, value, scope, at)?,
            C::Compare { left, op, right } => {
                let lhs = self.operand(left, scope, at)?;
                match right {
                    syntax::Expression::Value { value } => self.comparison(lhs, *op, value, at)?,
                    expression => Expr::Compare {
                        lhs,
                        op: *op,
                        rhs: Value::Operand(Box::new(self.operand(expression, scope, at)?)),
                    },
                }
            }
            C::Since { field, value } | C::Until { field, value } => {
                let lhs = Operand::Field(self.resolve(field, scope, at)?);
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
            C::Rank {
                list: syntax::RankList::Similar,
                value,
            } => {
                let raw = self.scalar(value, at)?;
                let reference=super::reference::parse_id(raw).ok_or_else(||QueryError::not_built(at,"similar requires a supplied text.embedding event reference; no embedding provider is configured for text"))?;
                Expr::Similar { reference }
            }
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
            C::TimeRange { left, value } => {
                let lhs = self.operand(left, scope, at)?;
                let field = match &lhs {
                    Operand::Field(f) | Operand::TimeFn(_, f) => f,
                    _ => return Err(QueryError::at(at, "time range requires a time field")),
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
            C::Between { left, from, to } => {
                let lhs = self.operand(left, scope, at)?;
                let a = self.comparison(lhs.clone(), Op::Ge, from, at)?;
                let b = self.comparison(lhs, Op::Le, to, at)?;
                Expr::And(Box::new(a), Box::new(b))
            }
            C::Extension { name, arguments } if name == "holding" || name == "overlaps" => {
                use syntax::ExtensionArgument as A;
                let (from, to) = if name == "holding" {
                    let [A::Name(mode), A::Value(value)] = arguments.as_slice() else {
                        return Err(QueryError::at(
                            at,
                            "holding takes at INSTANT or during PERIOD",
                        ));
                    };
                    let resolved =
                        query_language::time::resolve(self.scalar(value, at)?, &self.ctx.clock()?)
                            .map_err(super::syntax::error)?
                            .value;
                    match mode.as_str() {
                        "at" => (resolved.start(), None),
                        "during" => {
                            let range = resolved.range().ok_or_else(|| {
                                QueryError::at(at, "holding during takes a period")
                            })?;
                            (range.start, Some(range.end))
                        }
                        _ => {
                            return Err(QueryError::at(
                                at,
                                "holding takes at INSTANT or during PERIOD",
                            ));
                        }
                    }
                } else {
                    let [A::Value(from), A::Optional(rest)] = arguments.as_slice() else {
                        return Err(QueryError::at(at, "invalid interval arguments"));
                    };
                    let end = match rest.as_slice() {
                        [] => None,
                        [A::Keyword(_), A::Value(to)] => Some(self.instant(to, at)?),
                        _ => return Err(QueryError::at(at, "invalid interval end")),
                    };
                    (self.instant(from, at)?, end)
                };
                if to.is_some_and(|to| to <= from) {
                    return Err(QueryError::at(at, "interval end must follow its start"));
                }
                Expr::Overlaps { from, to }
            }
            C::Order {
                relation,
                query,
                within,
                at: point,
                scope: order_scope,
                pick,
            } => {
                let on = self.order_on(
                    *relation,
                    within.as_ref(),
                    *point,
                    order_scope.as_deref(),
                    at,
                )?;
                let inner_path = path.field("kind").field("query");
                let inner_scope = self.output_scope(&inner_path)?;
                let pick = match pick {
                    syntax::WitnessChoice::Nearest => Vec::new(),
                    syntax::WitnessChoice::FirstBy { keys } => keys
                        .iter()
                        .enumerate()
                        .map(|(index, key)| {
                            Ok(SortKey {
                                key: SortBy::Operand(
                                    self.operand_at(
                                        &key.expression,
                                        &inner_scope,
                                        at,
                                        Some(
                                            &path
                                                .field("kind")
                                                .field("pick")
                                                .field("keys")
                                                .index(index)
                                                .field("expression"),
                                        ),
                                    )?,
                                ),
                                descending: key.descending,
                            })
                        })
                        .collect::<Result<Vec<_>, QueryError>>()?,
                };
                Expr::Order(Box::new(Join {
                    on,
                    pick,
                    columns: self.inner_columns(&inner_scope, at)?,
                    required: vec![],
                    inner: Box::new(self.query((**query).clone(), &inner_path)?),
                }))
            }
            C::Reference { reference } => {
                let Reference::Event(id) = super::reference::parse_reference(reference)? else {
                    return Err(QueryError::at(
                        at,
                        "use read or trace for a source-coordinate reference",
                    ));
                };
                Expr::Compare {
                    lhs: Operand::Field(self.resolve("id", scope, at)?),
                    op: Op::Eq,
                    rhs: Value::Id(id),
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

    fn inner_columns(&self, scope: &Scope, at: usize) -> Result<Vec<Operand>, QueryError> {
        scope
            .fields
            .iter()
            .map(|info| {
                Ok(Operand::Named {
                    operand: Box::new(Operand::Field(self.resolve_info(info, at)?)),
                    name: info.field.name.clone(),
                })
            })
            .collect()
    }
    fn output_scope(&self, path: &NodePath) -> Result<Scope, QueryError> {
        let query = self
            .typing
            .query(path)
            .ok_or_else(|| QueryError::new("bound inner query scope is missing"))?;
        Ok(query
            .transforms
            .last()
            .map(|stage| &stage.after)
            .unwrap_or(&query.source)
            .clone())
    }
    fn order_on(
        &self,
        relation: syntax::OrderRelation,
        within: Option<&syntax::Distance>,
        point: syntax::TimePoint,
        scope: Option<&str>,
        at: usize,
    ) -> Result<JoinOn, QueryError> {
        if scope.is_some() {
            return Err(QueryError::not_built(
                at,
                "Sinex has no declared step-order scope",
            ));
        }
        let within_secs = within
            .map(|distance| match distance {
                syntax::Distance::Time { duration } => self.seconds(duration, at),
                syntax::Distance::Steps { .. } => Err(QueryError::not_built(
                    at,
                    "Sinex has no declared step-order scope",
                )),
            })
            .transpose()?;
        Ok(JoinOn::Order {
            relation: match relation {
                syntax::OrderRelation::Near => JoinKind::Near,
                syntax::OrderRelation::Before => JoinKind::Before,
                syntax::OrderRelation::After => JoinKind::After,
                syntax::OrderRelation::During => JoinKind::During,
                syntax::OrderRelation::Containing => JoinKind::Containing,
            },
            within_secs,
            at: match point {
                syntax::TimePoint::When => TimePoint::When,
                syntax::TimePoint::Start => TimePoint::Start,
                syntax::TimePoint::End => TimePoint::End,
                syntax::TimePoint::MatchWhen => TimePoint::MatchWhen,
                syntax::TimePoint::MatchStart => TimePoint::MatchStart,
                syntax::TimePoint::MatchEnd => TimePoint::MatchEnd,
            },
        })
    }

    fn query(&mut self, input: syntax::Query, path: &NodePath) -> Result<Query, QueryError> {
        let typing = self
            .typing
            .query(path)
            .cloned()
            .ok_or_else(|| QueryError::new("bound query scope is missing"))?;
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
                    .map(|c| {
                        self.condition(c, &typing.source, &path.field("source").field("condition"))
                    })
                    .transpose()?;
                (Source::Table { table, alias }, filter)
            }
            _ => {
                return Err(QueryError::not_built(
                    0,
                    "saved queries must be resolved before binding",
                ));
            }
        };
        let mut stages = Vec::new();
        for (index, stage) in input.transforms.into_iter().enumerate() {
            let stage_path = path.field("transforms").index(index).field("kind");
            let scope = typing
                .transforms
                .get(index)
                .map(|stage| &stage.before)
                .ok_or_else(|| QueryError::new("bound stage scope is missing"))?;
            use syntax::TransformKind as S;
            let at = stage.span.start;
            match stage.kind {
                S::Where { condition } => stages.push(Stage::Where(self.condition(
                    &condition,
                    scope,
                    &stage_path.field("condition"),
                )?)),
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
                    let keys = keys
                        .iter()
                        .map(|k| self.column(k, scope, at))
                        .collect::<Result<Vec<_>, _>>()?;
                    let group = Group {
                        keys,
                        aggregates: vec![],
                        fill: None,
                    };
                    stages.push(Stage::Group(group));
                }
                S::Aggregate { aggregates } => {
                    // Aggregate fields refer to the relation entering a group.
                    let aggregates = aggregates
                        .into_iter()
                        .enumerate()
                        .map(|(aggregate_index, a)| {
                            let function = match &a.function {
                                syntax::AggregateFunction::Count => AggFn::Count,
                                syntax::AggregateFunction::Sum => AggFn::Sum,
                                syntax::AggregateFunction::Avg => AggFn::Avg,
                                syntax::AggregateFunction::Min => AggFn::Min,
                                syntax::AggregateFunction::Max => AggFn::Max,
                                syntax::AggregateFunction::CountDistinct => AggFn::CountDistinct,
                                _ => {
                                    return Err(QueryError::not_built(
                                        at,
                                        "aggregate is not built",
                                    ));
                                }
                            };
                            if a.arguments.len() > 1 {
                                return Err(QueryError::at(at, "aggregate takes one argument"));
                            }
                            let argument = a
                                .arguments
                                .first()
                                .map(|expression| {
                                    let path = stage_path
                                        .field("aggregates")
                                        .index(aggregate_index)
                                        .field("arguments")
                                        .index(0);
                                    self.operand_at(expression, scope, at, Some(&path))
                                })
                                .transpose()?;
                            if matches!(function, AggFn::Sum | AggFn::Avg)
                                && let Some(Operand::Field(FieldRef::Field(f))) = &argument
                                && !matches!(f.ty, FieldType::Integer | FieldType::Number)
                            {
                                return Err(QueryError::at(at, "aggregate needs a number"));
                            }
                            Ok(Aggregate {
                                function,
                                argument,
                                name: Some(a.column()),
                            })
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    if let Some(Stage::Group(g)) = stages.last_mut()
                        && g.aggregates.is_empty()
                    {
                        g.aggregates = aggregates;
                    } else {
                        let g = Group {
                            keys: vec![],
                            aggregates,
                            fill: None,
                        };
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
                }
                S::Sort { keys } => {
                    let keys=keys.into_iter().map(|k| {
                    let key=if matches!(&k.expression,syntax::Expression::Field{name} if name=="rank") {SortBy::Rank} else {SortBy::Operand(self.operand(&k.expression,scope,at)?)};
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
                S::First { count } => {
                    if matches!(stages.last(), Some(Stage::Group(group)) if group.aggregates.is_empty() && group.fill.is_none())
                    {
                        let Some(Stage::Group(group)) = stages.pop() else {
                            unreachable!()
                        };
                        stages.push(Stage::GroupFirst {
                            keys: group.keys,
                            count,
                        });
                    } else {
                        stages.push(Stage::Limit(count));
                    }
                }
                S::Fields { columns } => stages.push(Stage::Fields(
                    columns
                        .iter()
                        .map(|column| self.column(column, scope, at))
                        .collect::<Result<Vec<_>, _>>()?,
                )),
                S::Join { query, on } => {
                    let inner_path = stage_path.field("query");
                    let inner_scope = self.output_scope(&inner_path)?;
                    let on = match on {
                        syntax::JoinOn::Order {
                            relation,
                            within,
                            at: point,
                            scope: order_scope,
                        } => self.order_on(
                            relation,
                            within.as_ref(),
                            point,
                            order_scope.as_deref(),
                            at,
                        )?,
                        syntax::JoinOn::Key { left, right } => JoinOn::Key {
                            left: self.resolve(&left, scope, at)?,
                            right: self.resolve(&right, &inner_scope, at)?,
                        },
                    };
                    stages.push(Stage::Join(Join {
                        on,
                        pick: vec![],
                        columns: self.inner_columns(&inner_scope, at)?,
                        required: vec![],
                        inner: Box::new(self.query(*query, &inner_path)?),
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
                    argument: None,
                    name: None,
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
fn has_words(e: &Expr) -> bool {
    match e {
        Expr::Words(_) => true,
        Expr::And(a, b) | Expr::Or(a, b) => has_words(a) || has_words(b),
        _ => false,
    }
}

pub(super) fn time_fn_value(
    ctx: &Context,
    func: TimeFn,
    raw: &str,
    at: usize,
) -> Result<Value, QueryError> {
    let today = ctx.today();
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
        TimeFn::Hour | TimeFn::Year => {
            let h: i64 = raw.parse().map_err(|_| bad("an hour, 0 to 23"))?;
            if func == TimeFn::Hour && !(0..=23).contains(&h) {
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
