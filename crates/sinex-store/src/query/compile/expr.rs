//! Fields, operands, comparisons, search words and the conditions every base
//! relation carries, as SQL fragments.
use super::*;

impl Compiler<'_> {
    /// The SQL of a field, and what kind of value it is.
    pub(super) fn field(&self, cols: &Cols, f: &FieldRef) -> Result<(SqlFragment, Kind)> {
        match (cols, f) {
            (Cols::Base { .. }, FieldRef::Native(NativeField::Identity)) => {
                let sql = self.positioned(cols, fields::tiebreak(Self::table_of(cols)));
                Ok((sql, Kind::Typed(FieldType::Text)))
            }
            (Cols::Base { .. }, FieldRef::Native(field)) => {
                let (start, end, _, known) = self.span_metadata(cols)?;
                let value = match field {
                    NativeField::SpanStart => start,
                    NativeField::SpanEnd => end,
                    NativeField::Snippet | NativeField::Identity => {
                        return Err(invalid("snippet is generated with its search relation"));
                    }
                };
                let mut sql = SqlFragment::text("CASE WHEN ");
                sql.append(known).push(" THEN ").append(value).push(" END");
                Ok((sql, Kind::Typed(field.field_type())))
            }
            (Cols::Named { .. }, FieldRef::Native(field)) => Ok((
                SqlFragment::text(Self::named(cols, field.name())),
                Kind::Typed(field.field_type()),
            )),
            (Cols::Base { alias, .. }, FieldRef::Field(field)) => {
                let sql = alias
                    .filter(|a| a.field(field.name).is_some())
                    .and_then(|a| fields::alias_field(a.name, field.name))
                    .or_else(|| fields::field(field.id))
                    .ok_or_else(|| invalid(format!("no SQL for field {}", field.name)))?;
                Ok((self.positioned(cols, &sql), Kind::Typed(field.ty)))
            }
            (_, FieldRef::Payload(_)) => {
                Err(invalid("payload path must be bound to its event schema"))
            }
            (Cols::Base { .. }, FieldRef::TypedPayload { path, ty }) => {
                let mut raw = SqlFragment::text(Self::template(cols, "({t}.payload #> "));
                raw.param(text_array(path)).push("::text[])");
                Ok((Self::typed_json(raw, *ty), Kind::Typed(*ty)))
            }
            (Cols::Base { .. }, FieldRef::Group(name)) => Err(invalid(format!(
                "`{name}` names a group column outside a grouping"
            ))),
            (Cols::Base { .. }, FieldRef::TypedGroup(column)) => Err(invalid(format!(
                "`{}` names a group column outside a grouping",
                column.name()
            ))),
            (Cols::Named { .. }, FieldRef::Group(name)) => {
                Err(invalid(format!("`{name}` has no bound output column type")))
            }
            // Shared binding supplies column lineage; the product resolves its
            // physical scalar type against the current declaration snapshot.
            (Cols::Named { .. }, FieldRef::TypedGroup(column)) => Ok((
                SqlFragment::text(Self::named(cols, column.name())),
                Kind::Typed(
                    column
                        .field_type()
                        .ok_or_else(|| invalid("bound output column type is unavailable"))?,
                ),
            )),
            (Cols::Named { .. }, FieldRef::Field(field)) => Ok((
                SqlFragment::text(Self::named(cols, field.name)),
                Kind::Typed(field.ty),
            )),
            (Cols::Named { .. }, FieldRef::TypedPayload { ty, .. }) => Ok((
                SqlFragment::text(Self::named(cols, &column_name(f))),
                Kind::Typed(*ty),
            )),
        }
    }

    fn typed_json(raw: SqlFragment, ty: FieldType) -> SqlFragment {
        let mut out = SqlFragment::new();
        if ty == FieldType::Time {
            out.push("core.payload_time(")
                .append(raw)
                .push(" #>> '{}')");
            return out;
        }
        if ty == FieldType::TextList {
            out.push("(CASE WHEN jsonb_typeof(")
                .append(raw.clone())
                .push(") = 'array' THEN ARRAY(SELECT jsonb_array_elements_text(")
                .append(raw)
                .push(")) END)");
            return out;
        }
        let (kind, cast) = match ty {
            FieldType::Integer => ("number", "numeric"),
            FieldType::Number => ("number", "numeric"),
            FieldType::Bool => ("boolean", "boolean"),
            FieldType::Time => ("string", "timestamptz"),
            FieldType::Text => ("string", "text"),
            _ => return raw,
        };
        out.push("(CASE WHEN jsonb_typeof(")
            .append(raw.clone())
            .push(") = '")
            .push(kind)
            .push("' THEN (")
            .append(raw)
            .push(" #>> '{}')::")
            .push(cast)
            .push(" END)");
        out
    }

    pub(super) fn time_fn(&self, func: TimeFn, t: SqlFragment) -> Result<SqlFragment> {
        if func == TimeFn::Hour {
            let mut out = SqlFragment::text("extract(hour from (");
            out.append(t)
                .push(" AT TIME ZONE ")
                .param(self.ctx.zone_name())
                .push("))::bigint");
            return Ok(out);
        }
        let setting = self
            .calendar
            .as_ref()
            .ok_or_else(|| invalid("logical-day SQL requires shared boundary preparation"))?;
        let column = match func {
            TimeFn::Day => "day",
            TimeFn::Week => "week",
            TimeFn::Weekday => "weekday",
            TimeFn::Month => "month",
            TimeFn::MonthDay => "month_day",
            TimeFn::Year => "year",
            TimeFn::Hour => unreachable!(),
        };
        let mut ns = SqlFragment::text("(extract(epoch from ");
        ns.append(t).push(")*1000000000)");
        let mut out = SqlFragment::text(format!(
            "(SELECT d.{column} FROM pg_temp.sinex_query_days d WHERE d.setting="
        ));
        out.param(setting.clone())
            .push(" AND d.from_ns<=")
            .append(ns.clone())
            .push(" AND ")
            .append(ns)
            .push("<d.until_ns ORDER BY d.from_ns DESC LIMIT 1)");
        Ok(out)
    }

    pub(super) fn operand(&self, cols: &Cols, o: &Operand) -> Result<(SqlFragment, Kind)> {
        query_language::depth::deeper(|| match o {
            Operand::Field(f) => self.field(cols, f),
            Operand::Named { operand, .. } => self.operand(cols, operand),
            Operand::Literal { value, kind } => {
                use query_language::catalog::FieldKind as K;
                let ty = o.field_type().map_err(|e| invalid(e.to_string()))?;
                let cast = match kind {
                    K::Integer | K::Number | K::Money | K::Duration => "numeric",
                    K::Bool => "boolean",
                    K::Time => "timestamptz",
                    K::Reference => "uuid",
                    K::Json => "jsonb",
                    _ => "text",
                };
                let mut sql = SqlFragment::new();
                if matches!(value, Value::Unknown) {
                    sql.push(format!("NULL::{cast}"));
                } else {
                    sql.param(value_param(value)?).push(format!("::{cast}"));
                }
                Ok((sql, Kind::Typed(ty)))
            }
            Operand::Glob { left, right } => {
                let (mut text, text_kind) = self.operand(cols, left)?;
                let (mut pattern, pattern_kind) = self.operand(cols, right)?;
                if text_kind == Kind::Typed(FieldType::Json) {
                    text = Self::typed_json(text, FieldType::Text);
                }
                if pattern_kind == Kind::Typed(FieldType::Json) {
                    pattern = Self::typed_json(pattern, FieldType::Text);
                }
                let mut out = SqlFragment::text("(");
                out.append(text)
                    .push("::text LIKE replace(replace(replace(replace(")
                    .append(pattern)
                    .push("::text, ");
                for (index, (from, to)) in [("\\", "\\\\"), ("%", "\\%"), ("_", "\\_"), ("*", "%")]
                    .into_iter()
                    .enumerate()
                {
                    if index > 0 {
                        out.push(", ");
                    }
                    out.param(from).push(", ").param(to).push(")");
                }
                out.push(" ESCAPE ").param("\\").push(")");
                Ok((out, Kind::Typed(FieldType::Bool)))
            }
            Operand::Text(value) => {
                let (value, kind) = self.operand(cols, value)?;
                let mut out = SqlFragment::text(if kind == Kind::Typed(FieldType::Json) {
                    "core.query_text("
                } else {
                    "("
                });
                out.append(value)
                    .push(if kind == Kind::Typed(FieldType::Json) {
                        ")"
                    } else {
                        ")::text"
                    });
                Ok((out, Kind::Typed(FieldType::Text)))
            }
            Operand::Absolute(value) => {
                if matches!(
                    &**value,
                    Operand::Literal {
                        value: Value::Unknown,
                        ..
                    }
                ) {
                    return Ok((
                        SqlFragment::text("NULL::numeric"),
                        Kind::Typed(FieldType::Number),
                    ));
                }
                let (sql, kind) = self.operand(cols, value)?;
                let mut out = SqlFragment::text("abs((");
                out.append(sql).push(")::numeric)");
                Ok((out, kind))
            }
            Operand::Length(value) => {
                let (sql, kind) = self.operand(cols, value)?;
                let mut out = SqlFragment::text(match kind {
                    Kind::Typed(FieldType::TextList | FieldType::PathList | FieldType::IdList) => {
                        "cardinality("
                    }
                    Kind::Typed(FieldType::Json) => "(CASE jsonb_typeof(",
                    _ => "char_length(",
                });
                if kind == Kind::Typed(FieldType::Json) {
                    out.append(sql.clone())
                        .push(") WHEN 'array' THEN jsonb_array_length(")
                        .append(sql.clone())
                        .push(") WHEN 'string' THEN char_length(")
                        .append(sql)
                        .push(" #>> '{}') END)");
                } else {
                    out.append(sql).push(")");
                }
                Ok((out, Kind::Typed(FieldType::Integer)))
            }
            Operand::Arithmetic {
                operator,
                left,
                right,
                ..
            } => {
                use query_language::{ast::Arithmetic as A, catalog::FieldKind as K};
                let left_time = left.kind() == K::Time;
                let right_time = right.kind() == K::Time;
                let (mut l, lk) = self.semantic_operand(cols, left)?;
                let (mut r, rk) = self.semantic_operand(cols, right)?;
                if lk == Kind::Typed(FieldType::Json) {
                    l = Self::typed_json(l, FieldType::Number);
                }
                if rk == Kind::Typed(FieldType::Json) {
                    r = Self::typed_json(r, FieldType::Number);
                }
                let mut out = SqlFragment::new();
                if *operator == A::Subtract && left_time && right_time {
                    out.push("extract(epoch from (")
                        .append(l)
                        .push(" - ")
                        .append(r)
                        .push("))");
                } else if left_time || right_time {
                    if left_time {
                        out.push("(")
                            .append(l)
                            .push(" ")
                            .push(operator.symbol())
                            .push(" (")
                            .append(r)
                            .push(" * interval '1 second'))");
                    } else {
                        out.push("(")
                            .append(r)
                            .push(" + (")
                            .append(l)
                            .push(" * interval '1 second'))");
                    }
                } else {
                    out.push("((")
                        .append(l)
                        .push(")::numeric ")
                        .push(operator.symbol());
                    if *operator == A::Divide {
                        out.push(" NULLIF((").append(r).push(")::numeric, 0))");
                    } else {
                        out.push(" (").append(r).push(")::numeric)");
                    }
                }
                Ok((
                    out,
                    Kind::Typed(o.field_type().map_err(|e| invalid(e.to_string()))?),
                ))
            }
            Operand::CaseEqual { left, right, value } => {
                let (left, left_kind) = self.semantic_operand(cols, left)?;
                let (right, right_kind) = self.semantic_operand(cols, right)?;
                let condition =
                    self.compare_operands(left, left_kind, Op::Eq, right, right_kind)?;
                let (value, kind) = self.operand(cols, value)?;
                let mut out = SqlFragment::text("(CASE WHEN ");
                out.append(condition)
                    .push(" THEN ")
                    .append(value)
                    .push(" ELSE NULL END)");
                Ok((out, kind))
            }
            Operand::TimeFn(func, f) => {
                let (t, kind) = self.semantic_operand(cols, &Operand::Field(f.clone()))?;
                let t = if kind == Kind::Typed(FieldType::Json) {
                    Self::typed_json(t, FieldType::Time)
                } else {
                    t
                };
                Ok((
                    self.time_fn(*func, t)?,
                    if matches!(func, TimeFn::Hour | TimeFn::Year) {
                        Kind::Typed(FieldType::Integer)
                    } else {
                        Kind::Derived
                    },
                ))
            }
        })
    }

    fn semantic_operand(&self, cols: &Cols, operand: &Operand) -> Result<(SqlFragment, Kind)> {
        match operand.unnamed() {
            Operand::TimeFn(func @ (TimeFn::Day | TimeFn::Week | TimeFn::Month), field) => {
                let (value, kind) = self.field(cols, field)?;
                let value = if kind == Kind::Typed(FieldType::Json) {
                    Self::typed_json(value, FieldType::Time)
                } else {
                    value
                };
                Ok((
                    self.time_bucket(*func, value)?,
                    Kind::Typed(FieldType::Time),
                ))
            }
            Operand::Field(FieldRef::TypedGroup(column))
                if matches!(
                    column.time_function(),
                    Some(TimeFn::Day | TimeFn::Week | TimeFn::Month)
                ) =>
            {
                let (value, _) = self.operand(cols, operand)?;
                Ok((
                    self.time_bucket_label(
                        column.time_function().expect("calendar lineage"),
                        value,
                    )?,
                    Kind::Typed(FieldType::Time),
                ))
            }
            _ => self.operand(cols, operand),
        }
    }

    pub(super) fn compare_operands(
        &self,
        mut left: SqlFragment,
        left_kind: Kind,
        op: Op,
        mut right: SqlFragment,
        right_kind: Kind,
    ) -> Result<SqlFragment> {
        if left_kind == Kind::Typed(FieldType::Json) && right_kind != Kind::Typed(FieldType::Json) {
            let Kind::Typed(ty) = right_kind else {
                return Err(invalid(
                    "dynamic JSON cannot compare to an untyped calendar label",
                ));
            };
            left = Self::typed_json(left, ty);
        }
        if right_kind == Kind::Typed(FieldType::Json) && left_kind != Kind::Typed(FieldType::Json) {
            let Kind::Typed(ty) = left_kind else {
                return Err(invalid(
                    "dynamic JSON cannot compare to an untyped calendar label",
                ));
            };
            right = Self::typed_json(right, ty);
        }
        if left_kind == Kind::Typed(FieldType::Json) && right_kind == left_kind {
            let null_if = |value| {
                let mut out = SqlFragment::text("NULLIF(");
                out.append(value).push(", 'null'::jsonb)");
                out
            };
            left = null_if(left);
            right = null_if(right);
        }
        let mut out = SqlFragment::text("(");
        match op {
            Op::Contains => {
                out.push("position(lower(")
                    .append(right)
                    .push(") in lower(")
                    .append(left)
                    .push(")) > 0");
            }
            Op::Prefix if left_kind == Kind::Typed(FieldType::Path) => {
                out.append(left.clone())
                    .push(" = rtrim(")
                    .append(right.clone())
                    .push(", '/') OR starts_with(")
                    .append(left)
                    .push(", rtrim(")
                    .append(right)
                    .push(", '/') || '/')");
            }
            Op::Prefix => {
                out.push("starts_with(lower(")
                    .append(left)
                    .push("), lower(")
                    .append(right)
                    .push("))");
            }
            _ => {
                out.append(left)
                    .push(" ")
                    .push(op_sql(op))
                    .push(" ")
                    .append(right);
            }
        }
        out.push(")");
        Ok(out)
    }

    pub(super) fn json_as(raw: SqlFragment, value: &Value) -> SqlFragment {
        let ty = match value {
            Value::Integer(_) | Value::Number(_) => FieldType::Number,
            Value::Bool(_) => FieldType::Bool,
            Value::Time(_) => FieldType::Time,
            _ => FieldType::Text,
        };
        Self::typed_json(raw, ty)
    }

    /// A schemaless comparison gets its scalar type from the current projected
    /// JSON value. The query spelling remains exact until that row is known.
    fn compare_json_scalar(&self, lhs: SqlFragment, op: Op, raw: &str) -> Result<SqlFragment> {
        let string = self.compare(
            Self::typed_json(lhs.clone(), FieldType::Text),
            Kind::Typed(FieldType::Text),
            op,
            &Value::Text(raw.into()),
        )?;
        let mut number = Self::typed_json(lhs.clone(), FieldType::Number);
        number.push(" ").push(op_sql(op)).push(" (CASE WHEN ")
            .param(raw)
            .push(" ~ '^[+-]?([0-9]+([.][0-9]*)?|[.][0-9]+)([eE][+-]?[0-9]+)?$' AND pg_input_is_valid(")
            .param(raw).push(", 'numeric') THEN ")
            .param(raw).push("::numeric END)");
        let boolean = if matches!(op, Op::Eq | Op::Ne) {
            match raw {
                "true" => self.compare(
                    Self::typed_json(lhs.clone(), FieldType::Bool),
                    Kind::Typed(FieldType::Bool),
                    op,
                    &Value::Bool(true),
                )?,
                "false" => self.compare(
                    Self::typed_json(lhs.clone(), FieldType::Bool),
                    Kind::Typed(FieldType::Bool),
                    op,
                    &Value::Bool(false),
                )?,
                _ => SqlFragment::text("NULL::boolean"),
            }
        } else {
            SqlFragment::text("NULL::boolean")
        };
        let mut out = SqlFragment::text("(CASE jsonb_typeof(");
        out.append(lhs)
            .push(") WHEN 'string' THEN ")
            .append(string)
            .push(" WHEN 'number' THEN ")
            .append(number)
            .push(" WHEN 'boolean' THEN ")
            .append(boolean)
            .push(" ELSE NULL::boolean END)");
        Ok(out)
    }

    pub(super) fn compare(
        &self,
        lhs: SqlFragment,
        kind: Kind,
        op: Op,
        v: &Value,
    ) -> Result<SqlFragment> {
        if matches!(v, Value::Unknown) {
            return Ok(SqlFragment::text("NULL::boolean"));
        }
        if let Value::List(items) = v {
            let parts = items
                .iter()
                .map(|item| self.compare(lhs.clone(), kind, op, item))
                .collect::<Result<Vec<_>>>()?;
            // `= [a, b]` is any of; `!= [a, b]` is none of.
            let sep = if op == Op::Ne { " AND " } else { " OR " };
            return Ok(SqlFragment::join(parts, sep).parenthesized());
        }
        if matches!(kind, Kind::Typed(FieldType::PathList | FieldType::TextList)) {
            let paths = kind == Kind::Typed(FieldType::PathList);
            if op.ordering() || (paths && op == Op::Contains) {
                return Err(invalid(
                    "lists compare their members with = != ^ or ~ (text only)",
                ));
            }
            let comparison = self.compare(
                SqlFragment::text("member.value"),
                Kind::Typed(if paths {
                    FieldType::Path
                } else {
                    FieldType::Text
                }),
                if op == Op::Ne { Op::Eq } else { op },
                v,
            )?;
            let mut out = SqlFragment::text("(CASE WHEN ");
            out.append(lhs.clone()).push(" IS NULL THEN NULL ELSE ");
            if op == Op::Ne {
                out.push("NOT ");
            }
            out.push("EXISTS (SELECT 1 FROM unnest(")
                .append(lhs)
                .push(") AS member(value) WHERE ")
                .append(comparison)
                .push(") END)");
            return Ok(out);
        }
        if kind == Kind::Typed(FieldType::Json)
            && !matches!(op, Op::Contains | Op::Prefix)
            && let Value::Text(raw) = v
        {
            return self.compare_json_scalar(lhs, op, raw);
        }
        if matches!(kind, Kind::Typed(FieldType::Integer | FieldType::Number))
            && let Value::Text(raw) = v
        {
            if matches!(op, Op::Contains | Op::Prefix) {
                return Err(invalid("~ and ^ require text"));
            }
            let mut out = SqlFragment::text("(");
            out.append(lhs)
                .push(" ")
                .push(op_sql(op))
                .push(" ")
                .param(raw.clone())
                .push("::numeric)");
            return Ok(out);
        }
        let l = if kind == Kind::Typed(FieldType::Json) {
            Self::json_as(lhs, v)
        } else {
            lhs
        };
        let mut out = SqlFragment::text("(");
        match (op, v, kind) {
            (Op::Contains, Value::Text(s), _) => {
                out.append(l)
                    .push(" ILIKE ")
                    .param(format!("%{}%", like_escape(s)))
                    .push(" ESCAPE '\\'");
            }
            (Op::Prefix, Value::Text(s), Kind::Typed(FieldType::Path)) => {
                let root = if s == "/" {
                    s.clone()
                } else {
                    s.trim_end_matches('/').to_string()
                };
                if root == "/" {
                    out.append(l).push(" LIKE '/%'");
                } else {
                    // Whole components, case-sensitive, never LIKE: `_` and
                    // `%` in a path are not wildcards.
                    out.append(l.clone())
                        .push(" = ")
                        .param(root.clone())
                        .push(" OR left(")
                        .append(l)
                        .push(", ")
                        .param(root.chars().count() as i32 + 1)
                        .push(") = ")
                        .param(format!("{root}/"));
                }
            }
            (Op::Prefix, Value::Text(s), _) => {
                out.push("lower(")
                    .append(l)
                    .push(") LIKE lower(")
                    .param(format!("{}%", like_escape(s)))
                    .push(") ESCAPE '\\'");
            }
            (Op::Contains | Op::Prefix, _, _) => {
                return Err(invalid("`~` and `^` compare with text"));
            }
            (Op::Eq | Op::Ne, Value::Id(id), Kind::Typed(FieldType::IdList)) => {
                if op == Op::Ne {
                    out.push("NOT ");
                }
                out.push("(")
                    .param(*id)
                    .push(" = ANY(")
                    .append(l)
                    .push("))");
            }
            (_, v, _) => {
                out.append(l)
                    .push(" ")
                    .push(op_sql(op))
                    .push(" ")
                    .param(value_param(v)?);
            }
        }
        out.push(")");
        Ok(out)
    }

    pub(super) fn id_of(cols: &Cols) -> Result<SqlFragment> {
        match cols {
            Cols::Base { table, .. } if table.is_events() => {
                Ok(SqlFragment::text(Self::template(cols, "{t}.id")))
            }
            Cols::Named {
                r,
                table,
                grouped: false,
                ..
            } if table.is_events() => Ok(SqlFragment::text(format!("{r}.\"_event_id\""))),
            _ => Err(invalid(
                "full-text words search events; this relation has no event rows",
            )),
        }
    }

    pub(super) fn words(&self, cols: &Cols, w: &Words) -> Result<SqlFragment> {
        let id = Self::id_of(cols)?;
        let mut out = SqlFragment::new();
        if w.prefix {
            let mut pattern = String::from("(^| )");
            let escaped: Vec<String> = w.words.iter().map(|x| regex_escape(x)).collect();
            pattern.push_str(&escaped.join(" "));
            // Eligible kinds read their own trigram table; words-only kinds
            // retain prefix answers through the protected full-text view.
            out.push("(")
                .append(id)
                .push(" IN (SELECT s.event_id FROM core.visible_search_trigram s WHERE s.text ~ ")
                .param(pattern.clone())
                .push(" UNION ALL SELECT s.event_id FROM core.visible_search_text s WHERE NOT EXISTS (SELECT 1 FROM core.visible_search_trigram t WHERE t.event_id=s.event_id) AND s.text ~ ")
                .param(pattern)
                .push("))");
            return Ok(out);
        }
        out.push("EXISTS (SELECT 1 FROM core.visible_search_text s WHERE s.event_id = ")
            .append(id)
            .push(" AND ");
        if w.phrase {
            out.push("s.tsv @@ phraseto_tsquery('simple', ")
                .param(w.words.join(" "))
                .push(")");
        } else {
            out.push("s.tsv @@ plainto_tsquery('simple', ")
                .param(w.words.join(" "))
                .push(")");
        }
        out.push(")");
        Ok(out)
    }

    pub(super) fn expr(&self, cols: &Cols, e: &Expr) -> Result<SqlFragment> {
        query_language::depth::deeper(|| {
            Ok(match e {
                Expr::Order(join) => self.order_predicate(cols, join)?,
                Expr::And(a, b) => {
                    let mut f = SqlFragment::text("(");
                    f.append(self.expr(cols, a)?)
                        .push(" AND ")
                        .append(self.expr(cols, b)?)
                        .push(")");
                    f
                }
                Expr::Or(a, b) => {
                    let mut f = SqlFragment::text("(");
                    f.append(self.expr(cols, a)?)
                        .push(" OR ")
                        .append(self.expr(cols, b)?)
                        .push(")");
                    f
                }
                Expr::Not(a) => {
                    let mut f = SqlFragment::text("(NOT ");
                    f.append(self.expr(cols, a)?).push(")");
                    f
                }
                Expr::Truth { value, operand } => {
                    self.operand(cols, operand)?;
                    SqlFragment::text(match value {
                        query_language::semantics::Truth::True => "true",
                        query_language::semantics::Truth::False => "false",
                        query_language::semantics::Truth::Unknown => "NULL::boolean",
                    })
                }
                Expr::Compare { lhs, op, rhs } => {
                    if let Value::Operand(right) = rhs {
                        let (left, left_kind) = self.semantic_operand(cols, lhs)?;
                        let (right, right_kind) = self.semantic_operand(cols, right)?;
                        self.compare_operands(left, left_kind, *op, right, right_kind)?
                    } else {
                        let (left, kind) = self.operand(cols, lhs)?;
                        self.compare(left, kind, *op, rhs)?
                    }
                }
                Expr::Exists(o) => {
                    let (l, kind) = self.operand(cols, o)?;
                    let mut f = SqlFragment::text("(");
                    if matches!(kind, Kind::Typed(FieldType::Json)) {
                        f.append(l.clone())
                            .push(" IS NOT NULL AND jsonb_typeof(")
                            .append(l)
                            .push(") <> 'null'");
                    } else {
                        f.append(l).push(" IS NOT NULL");
                    }
                    f.push(")");
                    f
                }
                Expr::Words(w) => self.words(cols, w)?,
                Expr::FieldWords { operand, words } => {
                    let (value, _) = self.operand(cols, operand)?;
                    let mut out = SqlFragment::text("(CASE WHEN ");
                    out.append(value.clone()).push(" IS NULL THEN NULL ELSE EXISTS (SELECT 1 FROM regexp_split_to_table(lower(")
                    .append(value).push("), '[^[:alnum:]]+') AS matched_word(word) WHERE word = ")
                    .param(words.words.join(" ")).push(") END)");
                    out
                }
                Expr::Within { lhs, from, to } => {
                    let (l, kind) = self.semantic_operand(cols, lhs)?;
                    let l = if kind == Kind::Typed(FieldType::Json) {
                        Self::typed_json(l, FieldType::Time)
                    } else {
                        l
                    };
                    let mut f = SqlFragment::text("(");
                    f.append(l.clone())
                        .push(" >= ")
                        .param(time_value(*from)?)
                        .push(" AND ")
                        .append(l)
                        .push(" < ")
                        .param(time_value(*to)?)
                        .push(")");
                    f
                }
                Expr::Similar { .. } => {
                    return Err(invalid("rank directive reached predicate compiler"));
                }
                Expr::Overlaps { from, to } => self.overlaps(cols, *from, *to)?,
            })
        })
    }

    fn overlaps(
        &self,
        cols: &Cols,
        from: jiff::Timestamp,
        to: Option<jiff::Timestamp>,
    ) -> Result<SqlFragment> {
        if let Cols::Named {
            r, grouped: false, ..
        } = cols
        {
            let mut out = SqlFragment::text(format!(
                "({r}.\"_has_span\" AND {r}.\"_s\" {} ",
                if to.is_some() { "<" } else { "<=" }
            ));
            out.param(time_value(to.unwrap_or(from))?)
                .push(format!(" AND ({r}.\"_e\" IS NULL OR {r}.\"_e\" > "))
                .param(time_value(from)?)
                .push(format!(" OR ({r}.\"_inclusive\" AND {r}.\"_e\" = "))
                .param(time_value(from)?)
                .push(")))");
            return Ok(out);
        }
        let Cols::Base { table, alias, .. } = cols else {
            return Err(invalid("overlaps applies before grouping or limiting"));
        };
        if !table.is_events() {
            return Err(invalid("overlaps requires event spans"));
        }
        let mut parts = Vec::new();
        for a in sinex_model::query::ALIASES
            .iter()
            .filter(|a| a.spans && alias.is_none_or(|selected| selected.name == a.name))
        {
            let Some((start, end, inclusive)) = fields::span(*table, Some(a.name)) else {
                continue;
            };
            let start = Self::template(cols, &start);
            let end = Self::template(cols, &end);
            let mut f = SqlFragment::text(Self::template(cols, "({t}.event_type = ANY("));
            f.param(text_array(
                &a.kinds.iter().map(|s| (*s).into()).collect::<Vec<_>>(),
            ))
            .push(format!(
                "::text[]) AND {start} {} ",
                if to.is_some() { "<" } else { "<=" }
            ))
            .param(time_value(to.unwrap_or(from))?)
            .push(format!(
                " AND ({end} IS NULL OR {end} {} ",
                if inclusive { ">=" } else { ">" }
            ))
            .param(time_value(from)?)
            .push("))");
            parts.push(f);
        }
        if parts.is_empty() {
            Ok(SqlFragment::text("false"))
        } else {
            Ok(SqlFragment::join(parts, " OR ").parenthesized())
        }
    }

    /// The conditions every base relation carries: live at the commit
    /// position, and the alias's kinds.
    pub(super) fn base_conditions(&self, cols: &Cols) -> Vec<SqlFragment> {
        let Cols::Base { table, alias, .. } = cols else {
            return Vec::new();
        };
        let mut out = Vec::new();
        match table {
            Table::Events | Table::Drift => {
                let mut f = SqlFragment::text(Self::template(cols, "{t}.commit_seq <= "));
                f.param(self.seq).push(Self::template(
                    cols,
                    " AND NOT audit.is_archived_at({t}.id, ",
                ));
                f.param(self.seq).push(")");
                out.push(f);
            }
            Table::Archive => {
                let mut f = SqlFragment::text(Self::template(cols, "{t}.commit_seq <= "));
                f.param(self.seq)
                    .push(Self::template(cols, " AND {a}.archived AND {a}.commit_seq <= "))
                    .param(self.seq)
                    .push(Self::template(cols, " AND NOT EXISTS (SELECT 1 FROM audit.archive_changes later WHERE later.event_id={t}.id AND later.commit_seq>{a}.commit_seq AND later.commit_seq <= "))
                    .param(self.seq).push(")");
                out.push(f);
            }
            _ => {}
        }
        if *table == Table::Drift {
            out.push(self.positioned(cols, super::super::source_views::DRIFT));
        }
        if let Some(a) = alias {
            let mut f = SqlFragment::text(Self::template(cols, "{t}.event_type = ANY("));
            let kinds: Vec<String> = a.kinds.iter().map(|k| k.to_string()).collect();
            f.param(text_array(&kinds)).push("::text[])");
            out.push(f);
            if let Some((field, value)) = a.only
                && let Some(sql) = fields::alias_field(a.name, field)
            {
                let mut f =
                    SqlFragment::text(format!("(({})::text = ", Self::template(cols, &sql)));
                f.param(value.to_owned()).push(")");
                out.push(f);
            }
            for rows in a.kind_rows {
                let mut f = SqlFragment::text(Self::template(cols, "({t}.event_type <> "));
                f.param(rows.kind.to_owned()).push(" OR (true");
                for (field, value) in rows.equal {
                    f.push(Self::template(cols, " AND {t}.payload ->> "))
                        .param((*field).to_owned())
                        .push(" = ")
                        .param((*value).to_owned());
                }
                for field in rows.present {
                    f.push(Self::template(cols, " AND {t}.payload ->> "))
                        .param((*field).to_owned())
                        .push(" IS NOT NULL");
                }
                for (field, values) in rows.not_in {
                    let values: Vec<String> = values.iter().map(|v| v.to_string()).collect();
                    f.push(Self::template(cols, " AND coalesce({t}.payload ->> "))
                        .param((*field).to_owned())
                        .push(", '') <> ALL(")
                        .param(text_array(&values))
                        .push("::text[])");
                }
                f.push("))");
                out.push(f);
            }
            if let Some(fused) = a.fused {
                // A source record a live fused record joins is that record's member.
                let mut f = SqlFragment::text(
                    "NOT EXISTS (SELECT 1 FROM core.visible_events fused WHERE fused.event_type = ",
                );
                f.param(fused.to_owned())
                    .push(Self::template(
                        cols,
                        " AND fused.parents @> ARRAY[{t}.id] AND fused.commit_seq <= ",
                    ))
                    .param(self.seq)
                    .push(" AND NOT audit.is_archived_at(fused.id, ")
                    .param(self.seq)
                    .push("))");
                out.push(f);
            }
        }
        out
    }

    pub(super) fn main_time(&self, cols: &Cols) -> Result<SqlFragment> {
        if let Cols::Named { r, .. } = cols {
            return Ok(SqlFragment::text(format!("{r}.\"_t\"")));
        }
        let table = Self::table_of(cols);
        let field = table
            .field(table.main_time())
            .expect("every table's main time is a field");
        Ok(self.field(cols, &FieldRef::Field(field))?.0)
    }

    /// Private native time metadata survives projection independently of aliases.
    /// Ordinary events choose an interval schema by kind; other rows are points.
    pub(super) fn span_metadata(
        &self,
        cols: &Cols,
    ) -> Result<(SqlFragment, SqlFragment, SqlFragment, SqlFragment)> {
        if let Cols::Named { r, .. } = cols {
            return Ok((
                SqlFragment::text(format!("{r}.\"_s\"")),
                SqlFragment::text(format!("{r}.\"_e\"")),
                SqlFragment::text(format!("{r}.\"_inclusive\"")),
                SqlFragment::text(format!("{r}.\"_has_span\"")),
            ));
        }
        let when = self.main_time(cols)?;
        let Cols::Base { table, alias, .. } = cols else {
            unreachable!()
        };
        if let Some((start, end, inclusive)) = self.span(cols) {
            return Ok((
                start,
                end,
                SqlFragment::text(inclusive.to_string()),
                SqlFragment::text("true"),
            ));
        }
        if !table.is_events() || alias.is_some() {
            return Ok((
                when.clone(),
                when,
                SqlFragment::text("true"),
                SqlFragment::text("false"),
            ));
        }
        let mut start = SqlFragment::text("CASE ");
        let mut end = SqlFragment::text("CASE ");
        let mut inclusive = SqlFragment::text("CASE ");
        let mut spans = vec![];
        for alias in sinex_model::query::ALIASES.iter().filter(|a| a.spans) {
            let Some((s, e, inc)) = fields::span(*table, Some(alias.name)) else {
                continue;
            };
            let mut selected = SqlFragment::text(Self::template(cols, "{t}.event_type = ANY("));
            selected
                .param(text_array(
                    &alias
                        .kinds
                        .iter()
                        .map(|s| (*s).to_owned())
                        .collect::<Vec<_>>(),
                ))
                .push("::text[])");
            start
                .push("WHEN ")
                .append(selected.clone())
                .push(" THEN ")
                .push(Self::template(cols, &s))
                .push(" ");
            end.push("WHEN ")
                .append(selected.clone())
                .push(" THEN ")
                .push(Self::template(cols, &e))
                .push(" ");
            inclusive
                .push("WHEN ")
                .append(selected.clone())
                .push(format!(" THEN {inc} "));
            spans.push(selected);
        }
        if spans.is_empty() {
            return Ok((
                when.clone(),
                when,
                SqlFragment::text("true"),
                SqlFragment::text("false"),
            ));
        }
        start.push("ELSE ").append(when.clone()).push(" END");
        end.push("ELSE ").append(when).push(" END");
        inclusive.push("ELSE true END");
        Ok((
            start,
            end,
            inclusive,
            SqlFragment::join(spans, " OR ").parenthesized(),
        ))
    }

    pub(super) fn span(&self, cols: &Cols) -> Option<(SqlFragment, SqlFragment, bool)> {
        if let Cols::Named {
            r, grouped: false, ..
        } = cols
        {
            return Some((
                SqlFragment::text(format!("{r}.\"_s\"")),
                SqlFragment::text(format!("{r}.\"_e\"")),
                false,
            ));
        }
        let Cols::Base { table, alias, .. } = cols else {
            return None;
        };
        let (s, e, inclusive) = fields::span(*table, alias.map(|a| a.name))?;
        Some((
            SqlFragment::text(Self::template(cols, &s)),
            SqlFragment::text(Self::template(cols, &e)),
            inclusive,
        ))
    }

    pub(super) fn rank(&self, cols: &Cols, words: &[String]) -> SqlFragment {
        let mut f = SqlFragment::text(Self::template(
            cols,
            "(SELECT ts_rank(s.tsv, plainto_tsquery('simple', ",
        ));
        f.param(words.join(" ")).push(Self::template(
            cols,
            ")) FROM core.visible_search_text s WHERE s.event_id = {t}.id)",
        ));
        f
    }

    pub(super) fn snippet(&self, cols: &Cols, words: &[String]) -> SqlFragment {
        let mut f =
            SqlFragment::text("(SELECT ts_headline('simple', s.text, plainto_tsquery('simple', ");
        f.param(words.join(" ")).push(Self::template(
            cols,
            "), 'MaxFragments=2, MaxWords=24, MinWords=6, StartSel=[, StopSel=]') FROM core.visible_search_text s WHERE s.event_id = {t}.id)",
        ));
        f
    }
}
