//! Fields, operands, comparisons, search words and the conditions every base
//! relation carries, as SQL fragments.
use super::*;

impl Compiler<'_> {
    /// The SQL of a field, and what kind of value it is.
    pub(super) fn field(&self, cols: &Cols, f: &FieldRef) -> Result<(Frag, Kind)> {
        match (cols, f) {
            (Cols::Base { unit, alias, .. }, FieldRef::Field(field)) => {
                let sql = alias
                    .filter(|a| a.field(field.name).is_some())
                    .and_then(|a| fields::alias_field(a.name, field.name))
                    .or_else(|| fields::field(*unit, field.name))
                    .ok_or_else(|| invalid(format!("no SQL for field {}", field.name)))?;
                Ok((
                    Frag::text(Self::template(cols, &sql)),
                    Kind::Typed(field.ty),
                ))
            }
            (Cols::Base { .. }, FieldRef::Payload(path)) => {
                let mut f = Frag::text(Self::template(cols, "({t}.payload #> "));
                f.param(V::String(Some(text_array(path)))).push("::text[])");
                Ok((f, Kind::Json))
            }
            (Cols::Base { .. }, FieldRef::Group(name)) => Err(invalid(format!(
                "`{name}` names a group column outside a grouping"
            ))),
            // A group column, or a column a time join added.
            (Cols::Named { r, .. }, FieldRef::Group(name)) => {
                Ok((Frag::text(format!("{r}.{}", quote(name))), Kind::Untyped))
            }
            (Cols::Named { r, .. }, FieldRef::Field(field)) => Ok((
                Frag::text(format!("{r}.{}", quote(field.name))),
                Kind::Typed(field.ty),
            )),
            (Cols::Named { r, .. }, FieldRef::Payload(_)) => Ok((
                Frag::text(format!("{r}.{}", quote(&column_name(f)))),
                Kind::Json,
            )),
        }
    }

    pub(super) fn time_fn(&self, func: TimeFn, t: Frag) -> Frag {
        let mut local = Frag::text("(");
        local
            .append(t)
            .push(" AT TIME ZONE ")
            .param(V::String(Some(self.ctx.zone_name())))
            .push(")");
        let mut shifted = Frag::text("(");
        shifted
            .append(local.clone())
            .push(" - ")
            .param(V::String(Some(format!(
                "{} hours {} minutes",
                self.ctx.day_start.hour(),
                self.ctx.day_start.minute()
            ))))
            .push("::interval)");
        let mut out = Frag::new();
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

    pub(super) fn operand(&self, cols: &Cols, o: &Operand) -> Result<(Frag, Kind)> {
        match o {
            Operand::Field(f) => self.field(cols, f),
            Operand::TimeFn(func, f) => {
                let (t, _) = self.field(cols, f)?;
                Ok((self.time_fn(*func, t), Kind::Derived))
            }
        }
    }

    /// A payload JSON value read as the type of `v`.
    pub(super) fn json_as(j: Frag, v: &Value) -> Frag {
        let mut out = Frag::new();
        match v {
            Value::Integer(_) | Value::Number(_) => {
                out.push("(CASE WHEN jsonb_typeof(")
                    .append(j.clone())
                    .push(") = 'number' THEN (")
                    .append(j)
                    .push(" #>> '{}')::numeric END)");
            }
            Value::Bool(_) => {
                out.push("(CASE WHEN jsonb_typeof(")
                    .append(j.clone())
                    .push(") = 'boolean' THEN (")
                    .append(j)
                    .push(" #>> '{}')::boolean END)");
            }
            _ => {
                out.push("(").append(j).push(" #>> '{}')");
            }
        }
        out
    }

    pub(super) fn compare(&self, lhs: Frag, kind: Kind, op: Op, v: &Value) -> Result<Frag> {
        if let Value::List(items) = v {
            let parts = items
                .iter()
                .map(|item| self.compare(lhs.clone(), kind, op, item))
                .collect::<Result<Vec<_>>>()?;
            // `= [a, b]` is any of; `!= [a, b]` is none of.
            let sep = if op == Op::Ne { " AND " } else { " OR " };
            return Ok(Frag::join(parts, sep).parenthesized());
        }
        if kind == Kind::Typed(FieldType::PathList) {
            if !matches!(op, Op::Eq | Op::Ne | Op::Prefix) {
                return Err(invalid("path lists compare with = != or ^"));
            }
            let comparison = self.compare(
                Frag::text("member.path"),
                Kind::Typed(FieldType::Path),
                if op == Op::Ne { Op::Eq } else { op },
                v,
            )?;
            let mut out = Frag::text("(CASE WHEN ");
            out.append(lhs.clone()).push(" IS NULL THEN NULL ELSE ");
            if op == Op::Ne {
                out.push("NOT ");
            }
            out.push("EXISTS (SELECT 1 FROM unnest(")
                .append(lhs)
                .push(") AS member(path) WHERE ")
                .append(comparison)
                .push(") END)");
            return Ok(out);
        }
        let l = if kind == Kind::Json {
            Self::json_as(lhs, v)
        } else {
            lhs
        };
        let mut out = Frag::text("(");
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

    pub(super) fn id_of(cols: &Cols) -> Result<Frag> {
        match cols {
            Cols::Base { unit, .. } if unit.is_events() => {
                Ok(Frag::text(Self::template(cols, "{t}.id")))
            }
            Cols::Named {
                r,
                unit,
                grouped: false,
            } if unit.is_events() => Ok(Frag::text(format!("{r}.\"_id\""))),
            _ => Err(invalid(
                "full-text words search events; this relation has no event rows",
            )),
        }
    }

    pub(super) fn words(&self, cols: &Cols, w: &Words) -> Result<Frag> {
        let id = Self::id_of(cols)?;
        let mut out =
            Frag::text("EXISTS (SELECT 1 FROM core.visible_search_text s WHERE s.event_id = ");
        out.append(id).push(" AND ");
        if w.prefix {
            let mut pattern = String::from("(^| )");
            let escaped: Vec<String> = w.words.iter().map(|x| regex_escape(x)).collect();
            pattern.push_str(&escaped.join(" "));
            out.push("s.text ~ ").param(V::String(Some(pattern)));
        } else if w.phrase {
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

    pub(super) fn expr(&self, cols: &Cols, e: &Expr) -> Result<Frag> {
        Ok(match e {
            Expr::And(a, b) => {
                let mut f = Frag::text("(");
                f.append(self.expr(cols, a)?)
                    .push(" AND ")
                    .append(self.expr(cols, b)?)
                    .push(")");
                f
            }
            Expr::Or(a, b) => {
                let mut f = Frag::text("(");
                f.append(self.expr(cols, a)?)
                    .push(" OR ")
                    .append(self.expr(cols, b)?)
                    .push(")");
                f
            }
            Expr::Not(a) => {
                let mut f = Frag::text("(NOT ");
                f.append(self.expr(cols, a)?).push(")");
                f
            }
            Expr::Compare { lhs, op, rhs } => {
                let (l, kind) = self.operand(cols, lhs)?;
                self.compare(l, kind, *op, rhs)?
            }
            Expr::Exists(o) => {
                let (l, kind) = self.operand(cols, o)?;
                let mut f = Frag::text("(");
                if kind == Kind::Json {
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
                let mut f = Frag::text("(");
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
            Expr::TimeDiff { a, b, op, seconds } => {
                let (a, _) = self.field(cols, a)?;
                let (b, _) = self.field(cols, b)?;
                let mut f = Frag::text("(extract(epoch from (");
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

    /// The conditions every base relation carries: live at the commit
    /// position, and the alias's kinds.
    pub(super) fn base_conditions(&self, cols: &Cols) -> Vec<Frag> {
        let Cols::Base { unit, alias, .. } = cols else {
            return Vec::new();
        };
        let mut out = Vec::new();
        match unit {
            Unit::Events => {
                let mut f = Frag::text(Self::template(cols, "{t}.commit_seq <= "));
                f.param(V::BigInt(Some(self.seq))).push(Self::template(
                    cols,
                    " AND NOT EXISTS (SELECT 1 FROM audit.archived x WHERE x.event_id = {t}.id AND x.archived_seq <= ",
                ));
                f.param(V::BigInt(Some(self.seq))).push(")");
                out.push(f);
            }
            Unit::Archive => {
                let mut f = Frag::text(Self::template(cols, "{t}.commit_seq <= "));
                f.param(V::BigInt(Some(self.seq)))
                    .push(Self::template(cols, " AND {a}.archived_seq <= "))
                    .param(V::BigInt(Some(self.seq)));
                out.push(f);
            }
            _ => {}
        }
        if let Some(a) = alias {
            let mut f = Frag::text(Self::template(cols, "{t}.event_type = ANY("));
            let kinds: Vec<String> = a.kinds.iter().map(|k| k.to_string()).collect();
            f.param(V::String(Some(text_array(&kinds))))
                .push("::text[])");
            out.push(f);
            if a.open_only
                && let Some(open) = fields::alias_field(a.name, "open")
            {
                out.push(Frag::text(format!(
                    "({} IS TRUE)",
                    Self::template(cols, &open)
                )));
            }
        }
        out
    }

    pub(super) fn main_time(&self, cols: &Cols) -> Result<Frag> {
        let unit = Self::unit_of(cols);
        let field = unit
            .field(unit.main_time())
            .expect("every unit's main time is a field");
        Ok(self.field(cols, &FieldRef::Field(field))?.0)
    }

    pub(super) fn span(&self, cols: &Cols) -> Option<(Frag, Frag, bool)> {
        let Cols::Base { unit, alias, .. } = cols else {
            return None;
        };
        let (s, e, inclusive) = fields::span(*unit, alias.map(|a| a.name))?;
        Some((
            Frag::text(Self::template(cols, &s)),
            Frag::text(Self::template(cols, &e)),
            inclusive,
        ))
    }

    pub(super) fn rank(&self, cols: &Cols, words: &[String]) -> Frag {
        let mut f = Frag::text(Self::template(
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

    pub(super) fn snippet(&self, cols: &Cols, words: &[String]) -> Frag {
        let mut f = Frag::text("(SELECT ts_headline('simple', s.text, plainto_tsquery('simple', ");
        f.param(V::String(Some(words.join(" ")))).push(Self::template(
            cols,
            "), 'MaxFragments=2, MaxWords=24, MinWords=6, StartSel=[, StopSel=]') FROM core.visible_search_text s WHERE s.event_id = {t}.id)",
        ));
        f
    }
}
