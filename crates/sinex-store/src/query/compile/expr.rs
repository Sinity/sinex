//! Fields, operands, comparisons, search words and the conditions every base
//! relation carries, as SQL fragments.
use super::*;

impl Compiler<'_> {
    /// The SQL of a field, and what kind of value it is.
    pub(super) fn field(&self, cols: &Cols, f: &FieldRef) -> Result<(SqlFragment, Kind)> {
        match (cols, f) {
            (Cols::Base { table, alias, .. }, FieldRef::Field(field)) => {
                let sql = alias
                    .filter(|a| a.field(field.name).is_some())
                    .and_then(|a| fields::alias_field(a.name, field.name))
                    .or_else(|| fields::field(*table, field.name))
                    .ok_or_else(|| invalid(format!("no SQL for field {}", field.name)))?;
                Ok((self.positioned(cols, &sql), Kind::Typed(field.ty)))
            }
            (_, FieldRef::Payload(_)) => {
                Err(invalid("payload path must be bound to its event schema"))
            }
            (Cols::Base { .. }, FieldRef::TypedPayload { path, ty }) => {
                let mut raw = SqlFragment::text(Self::template(cols, "({t}.payload #> "));
                raw.param(V::String(Some(text_array(path))))
                    .push("::text[])");
                Ok((Self::typed_json(raw, *ty), Kind::Typed(*ty)))
            }
            (Cols::Base { .. }, FieldRef::Group(name)) => Err(invalid(format!(
                "`{name}` names a group column outside a grouping"
            ))),
            // A group column, or a column a time join added.
            (Cols::Named { r, .. }, FieldRef::Group(name)) => Ok((
                SqlFragment::text(format!("{r}.{}", quote(name))),
                Kind::Untyped,
            )),
            (Cols::Named { r, .. }, FieldRef::Field(field)) => Ok((
                SqlFragment::text(format!("{r}.{}", quote(field.name))),
                Kind::Typed(field.ty),
            )),
            (Cols::Named { r, .. }, FieldRef::TypedPayload { ty, .. }) => Ok((
                SqlFragment::text(format!("{r}.{}", quote(&column_name(f)))),
                Kind::Typed(*ty),
            )),
        }
    }

    fn typed_json(raw: SqlFragment, ty: FieldType) -> SqlFragment {
        let mut out = SqlFragment::new();
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

    pub(super) fn time_fn(&self, func: TimeFn, t: SqlFragment) -> SqlFragment {
        let mut local = SqlFragment::text("(");
        local
            .append(t)
            .push(" AT TIME ZONE ")
            .param(V::String(Some(self.ctx.zone_name())))
            .push(")");
        let mut shifted = SqlFragment::text("(");
        shifted
            .append(local.clone())
            .push(" - ")
            .param(V::String(Some(format!(
                "{} hours {} minutes",
                self.ctx.day_start.hour(),
                self.ctx.day_start.minute()
            ))))
            .push("::interval)");
        let mut out = SqlFragment::new();
        match func {
            TimeFn::Hour => {
                out.push("extract(hour from ")
                    .append(local)
                    .push(")::bigint");
            }
            TimeFn::Day => {
                out.push("to_char(").append(shifted).push(", 'YYYY-MM-DD')");
            }
            TimeFn::Week => {
                out.push("to_char(date_trunc('week', ")
                    .append(shifted)
                    .push("), 'YYYY-MM-DD')");
            }
            TimeFn::Weekday => {
                out.push("lower(to_char(").append(shifted).push(", 'Dy'))");
            }
            TimeFn::Month => {
                out.push("to_char(").append(shifted).push(", 'YYYY-MM')");
            }
            TimeFn::MonthDay => {
                out.push("to_char(").append(shifted).push(", 'MM-DD')");
            }
        }
        out
    }

    pub(super) fn operand(&self, cols: &Cols, o: &Operand) -> Result<(SqlFragment, Kind)> {
        match o {
            Operand::Field(f) => self.field(cols, f),
            Operand::TimeFn(func, f) => {
                let (t, _) = self.field(cols, f)?;
                Ok((self.time_fn(*func, t), Kind::Derived))
            }
        }
    }

    pub(super) fn compare(
        &self,
        lhs: SqlFragment,
        kind: Kind,
        op: Op,
        v: &Value,
    ) -> Result<SqlFragment> {
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
        let l = lhs;
        let mut out = SqlFragment::text("(");
        match (op, v, kind) {
            (Op::Eq | Op::Ne, Value::Glob(g), _) => {
                let pattern = like_escape(g).replace('*', "%");
                out.append(l)
                    .push(if op == Op::Eq { " LIKE " } else { " NOT LIKE " })
                    .param(V::String(Some(pattern)))
                    .push(" ESCAPE '\\'");
            }
            (Op::Contains, Value::Text(s), _) => {
                out.append(l)
                    .push(" ILIKE ")
                    .param(V::String(Some(format!("%{}%", like_escape(s)))))
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
                        .param(V::String(Some(root.clone())))
                        .push(" OR left(")
                        .append(l)
                        .push(", ")
                        .param(V::Int(Some(root.chars().count() as i32 + 1)))
                        .push(") = ")
                        .param(V::String(Some(format!("{root}/"))));
                }
            }
            (Op::Prefix, Value::Text(s), _) => {
                out.push("lower(")
                    .append(l)
                    .push(") LIKE lower(")
                    .param(V::String(Some(format!("{}%", like_escape(s)))))
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
                    .param(V::Uuid(Some(*id)))
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
            } if table.is_events() => Ok(SqlFragment::text(format!("{r}.\"_id\""))),
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
            // Separate branches expose the partial index's literal predicate.
            // Both read through the protected view, including excluded kinds.
            out.push("(")
                .append(id)
                .push(" IN (SELECT s.event_id FROM core.visible_search_text s WHERE s.trigram_indexed AND s.text ~ ")
                .param(V::String(Some(pattern.clone())))
                .push(" UNION ALL SELECT s.event_id FROM core.visible_search_text s WHERE NOT s.trigram_indexed AND s.text ~ ")
                .param(V::String(Some(pattern)))
                .push("))");
            return Ok(out);
        }
        out.push("EXISTS (SELECT 1 FROM core.visible_search_text s WHERE s.event_id = ")
            .append(id)
            .push(" AND ");
        if w.phrase {
            out.push("s.tsv @@ phraseto_tsquery('simple', ")
                .param(V::String(Some(w.words.join(" "))))
                .push(")");
        } else {
            out.push("s.tsv @@ plainto_tsquery('simple', ")
                .param(V::String(Some(w.words.join(" "))))
                .push(")");
        }
        out.push(")");
        Ok(out)
    }

    pub(super) fn expr(&self, cols: &Cols, e: &Expr) -> Result<SqlFragment> {
        Ok(match e {
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
            Expr::Compare { lhs, op, rhs } => {
                let (l, kind) = self.operand(cols, lhs)?;
                self.compare(l, kind, *op, rhs)?
            }
            Expr::Exists(o) => {
                let (l, kind) = self.operand(cols, o)?;
                let mut f = SqlFragment::text("(");
                if kind == Kind::Typed(FieldType::Json) {
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
            Expr::Within { lhs, from, to } => {
                let (l, _) = self.operand(cols, lhs)?;
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
            Expr::Overlaps { from, to } => self.overlaps(cols, *from, *to)?,
            Expr::TimeDiff { a, b, op, seconds } => {
                let (a, _) = self.field(cols, a)?;
                let (b, _) = self.field(cols, b)?;
                let mut f = SqlFragment::text("(extract(epoch from (");
                f.append(a)
                    .push(" - ")
                    .append(b)
                    .push(")) ")
                    .push(op_sql(*op))
                    .push(" ")
                    .param(V::Double(Some(*seconds)))
                    .push(")");
                f
            }
        })
    }

    fn overlaps(
        &self,
        cols: &Cols,
        from: jiff::Timestamp,
        to: Option<jiff::Timestamp>,
    ) -> Result<SqlFragment> {
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
            f.param(V::String(Some(text_array(
                &a.kinds.iter().map(|s| (*s).into()).collect::<Vec<_>>(),
            ))))
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
                f.param(V::BigInt(Some(self.seq))).push(Self::template(
                    cols,
                    " AND NOT audit.is_archived_at({t}.id, ",
                ));
                f.param(V::BigInt(Some(self.seq))).push(")");
                out.push(f);
            }
            Table::Archive => {
                let mut f = SqlFragment::text(Self::template(cols, "{t}.commit_seq <= "));
                f.param(V::BigInt(Some(self.seq)))
                    .push(Self::template(cols, " AND {a}.archived AND {a}.commit_seq <= "))
                    .param(V::BigInt(Some(self.seq)))
                    .push(Self::template(cols, " AND NOT EXISTS (SELECT 1 FROM audit.archive_changes later WHERE later.event_id={t}.id AND later.commit_seq>{a}.commit_seq AND later.commit_seq <= "))
                    .param(V::BigInt(Some(self.seq))).push(")");
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
            f.param(V::String(Some(text_array(&kinds))))
                .push("::text[])");
            out.push(f);
            if let Some((field, value)) = a.only
                && let Some(sql) = fields::alias_field(a.name, field)
            {
                let mut f =
                    SqlFragment::text(format!("(({})::text = ", Self::template(cols, &sql)));
                f.param(V::String(Some(value.to_owned()))).push(")");
                out.push(f);
            }
        }
        out
    }

    pub(super) fn main_time(&self, cols: &Cols) -> Result<SqlFragment> {
        let table = Self::table_of(cols);
        let field = table
            .field(table.main_time())
            .expect("every table's main time is a field");
        Ok(self.field(cols, &FieldRef::Field(field))?.0)
    }

    pub(super) fn span(&self, cols: &Cols) -> Option<(SqlFragment, SqlFragment, bool)> {
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
        f.param(V::String(Some(words.join(" "))))
            .push(Self::template(
                cols,
                ")) FROM core.visible_search_text s WHERE s.event_id = {t}.id)",
            ));
        f
    }

    pub(super) fn snippet(&self, cols: &Cols, words: &[String]) -> SqlFragment {
        let mut f =
            SqlFragment::text("(SELECT ts_headline('simple', s.text, plainto_tsquery('simple', ");
        f.param(V::String(Some(words.join(" ")))).push(Self::template(
            cols,
            "), 'MaxFragments=2, MaxWords=24, MinWords=6, StartSel=[, StopSel=]') FROM core.visible_search_text s WHERE s.event_id = {t}.id)",
        ));
        f
    }
}
