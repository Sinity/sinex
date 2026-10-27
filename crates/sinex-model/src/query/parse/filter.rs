//! Filters: boolean structure, terms, search words, field names and
//! comparisons with typed values.
use super::reference::parse_id;
use super::*;

impl Parser<'_> {
    pub(super) fn table_filter(&mut self, scope: &Scope) -> Result<Option<Expr>, QueryError> {
        if self.take_word("where") {
            Ok(Some(self.filter(scope)?))
        } else {
            Ok(None)
        }
    }

    pub(super) fn filter(&mut self, scope: &Scope) -> Result<Expr, QueryError> {
        let mut e = self.conjunction(scope)?;
        while self.take_word("or") {
            e = Expr::Or(Box::new(e), Box::new(self.conjunction(scope)?));
        }
        Ok(e)
    }

    pub(super) fn conjunction(&mut self, scope: &Scope) -> Result<Expr, QueryError> {
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

    pub(super) fn term(&mut self, scope: &Scope) -> Result<Expr, QueryError> {
        if self.take_word("not") {
            return Ok(Expr::Not(Box::new(self.term(scope)?)));
        }
        if self.take_symbol("(") {
            let e = self.filter(scope)?;
            self.expect_symbol(")")?;
            return Ok(e);
        }
        if self.take_word("overlaps") {
            let t = self.value_token()?;
            let from = self.instant_from(&t)?;
            let to = if self.take_word("..") {
                let t = self.value_token()?;
                let to = self.instant_from(&t)?;
                if from >= to {
                    return Err(QueryError::at(t.at, "overlap end must follow its start"));
                }
                Some(to)
            } else {
                None
            };
            return Ok(Expr::Overlaps { from, to });
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

    pub(super) fn words(&mut self, raw: &str, quoted: bool, at: usize) -> Result<Expr, QueryError> {
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

    pub(super) fn field_ref(&mut self, scope: &Scope) -> Result<FieldRef, QueryError> {
        let t = self.value_token()?;
        self.resolve(&t.text, scope, t.at)
    }

    pub(super) fn time_field(
        &self,
        field: &FieldRef,
        scope: &Scope,
        at: usize,
    ) -> Result<(), QueryError> {
        match (field, scope) {
            (FieldRef::Field(f), _) if f.ty == FieldType::Time => Ok(()),
            (FieldRef::Payload(_), _) => Ok(()),
            (FieldRef::Group(_), Scope::Grouped(_)) => Ok(()),
            _ => Err(QueryError::at(
                at,
                format!("`{}` is not a time", field.name()),
            )),
        }
    }

    /// The table's main time, for `since`, `until` and `in` without a field.
    pub(super) fn main_time(&self, scope: &Scope, at: usize) -> Result<FieldRef, QueryError> {
        match scope {
            Scope::Rows { table, .. } => Ok(FieldRef::Field(
                table
                    .field(table.main_time())
                    .expect("every table's main time is one of its fields"),
            )),
            Scope::Grouped(_) => Err(QueryError::at(
                at,
                "grouped rows have no main time; name a column",
            )),
        }
    }

    pub(super) fn comparison(&mut self, lhs: Operand) -> Result<Expr, QueryError> {
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

    pub(super) fn value(&mut self, lhs: &Operand, op: Op) -> Result<Value, QueryError> {
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
            Operand::Field(FieldRef::Payload(_) | FieldRef::TypedPayload { .. }) => {
                Ok(infer(&raw, quoted, op))
            }
            Operand::Field(FieldRef::Field(f)) => {
                let text_like = matches!(
                    f.ty,
                    FieldType::Text | FieldType::Path | FieldType::PathList
                );
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
                            | FieldType::PathList
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
                            let elsewhere = tables_accepting(&raw);
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
                    FieldType::Text | FieldType::Path | FieldType::PathList => {
                        if f.ty == FieldType::PathList
                            && !matches!(op, Op::Eq | Op::Ne | Op::Prefix)
                        {
                            return Err(QueryError::at(at, "path lists compare with = != or ^"));
                        }
                        if !quoted && raw.contains('*') && matches!(op, Op::Eq | Op::Ne) {
                            Ok(Value::Glob(raw))
                        } else if matches!(f.ty, FieldType::Path | FieldType::PathList)
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
}

/// A value whose type comes from the literal: payload paths and grouped
/// columns have no declared type.
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
