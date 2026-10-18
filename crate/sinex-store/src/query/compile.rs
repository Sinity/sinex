//! Compiling a bound query to SQL (spec `query.md` §4-§11). The boolean
//! structure is kept exactly: two conditions joined by `and` are both
//! required, and a comparison with an unknown value is unknown, never true.
//! Every query reads events live at one commit position, so the pages of one
//! answer agree with each other and `as of` is the same rule at an earlier
//! position.
use super::fields;
use super::sql::Frag;
use crate::{Error, Result};
use sea_query::Value as V;
use sinex_model::query::text::{like_escape, regex_escape};
use sinex_model::query::{
    AggFn, Alias, Context, Expr, FieldRef, FieldType, Join, JoinKind, Op, Operand, Query, SortBy,
    Source, Stage, TimeFn, TimePoint, Unit, Value, Words,
};

/// A compiled answer: one SELECT whose rows are numbered `_n` in answer order
/// and carry the output columns.
pub(crate) struct Compiled {
    pub answer: Frag,
    pub columns: Vec<String>,
}

pub(crate) struct Compiler<'c> {
    ctx: &'c Context,
    seq: i64,
    next: usize,
}

/// How field names resolve in a relation.
#[derive(Clone)]
enum Cols {
    /// The unit's own tables, under aliases `t` (and `a` for the archive).
    Base {
        unit: Unit,
        alias: Option<&'static Alias>,
        t: String,
        a: String,
    },
    /// The named columns of a subquery `r`.
    Named {
        r: String,
        unit: Unit,
        grouped: bool,
    },
}

/// What kind of value an operand yields, for choosing comparisons.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Typed(FieldType),
    /// A payload path: JSON, read according to the literal compared with.
    Json,
    /// A grouped column: the database knows its type.
    Untyped,
    /// A time function's text or number.
    Derived,
}

fn invalid(msg: impl Into<String>) -> Error {
    Error::Invalid(msg.into())
}

fn quote(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// A Postgres text array literal: `{"a","b"}`.
pub(crate) fn text_array(items: &[String]) -> String {
    let quoted: Vec<String> = items
        .iter()
        .map(|s| format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\"")))
        .collect();
    format!("{{{}}}", quoted.join(","))
}

fn time_value(t: jiff::Timestamp) -> Result<V> {
    let odt = time::OffsetDateTime::from_unix_timestamp_nanos(t.as_nanosecond())
        .map_err(|e| invalid(format!("time out of range: {e}")))?;
    Ok(V::TimeDateTimeWithTimeZone(Some(odt)))
}

fn value_param(v: &Value) -> Result<V> {
    Ok(match v {
        Value::Text(s) | Value::Glob(s) => V::String(Some(s.clone())),
        Value::Integer(i) => V::BigInt(Some(*i)),
        Value::Number(n) => V::Double(Some(*n)),
        Value::Bool(b) => V::Bool(Some(*b)),
        Value::Time(t) => time_value(*t)?,
        Value::Id(u) => V::Uuid(Some(*u)),
        Value::List(_) => return Err(invalid("a list inside a list")),
    })
}

fn op_sql(op: Op) -> &'static str {
    match op {
        Op::Eq => "=",
        Op::Ne => "<>",
        Op::Lt => "<",
        Op::Le => "<=",
        Op::Gt => ">",
        Op::Ge => ">=",
        Op::Contains | Op::Prefix => unreachable!("handled before"),
    }
}

/// Positive full-text words: never words under `not`.
fn positive_words(e: &Expr, out: &mut Vec<String>) {
    match e {
        Expr::Words(w) => out.extend(w.words.iter().cloned()),
        Expr::And(a, b) | Expr::Or(a, b) => {
            positive_words(a, out);
            positive_words(b, out);
        }
        _ => {}
    }
}

/// Every row field an expression names (not inside joins).
fn expr_fields(e: &Expr, out: &mut Vec<FieldRef>) {
    match e {
        Expr::And(a, b) | Expr::Or(a, b) => {
            expr_fields(a, out);
            expr_fields(b, out);
        }
        Expr::Not(a) => expr_fields(a, out),
        Expr::Compare { lhs, .. } | Expr::Exists(lhs) | Expr::Within { lhs, .. } => {
            operand_fields(lhs, out)
        }
        Expr::TimeDiff { a, b, .. } => {
            out.push(a.clone());
            out.push(b.clone());
        }
        Expr::Words(_) => {}
    }
}

fn operand_fields(o: &Operand, out: &mut Vec<FieldRef>) {
    match o {
        Operand::Field(f) | Operand::TimeFn(_, f) => out.push(f.clone()),
    }
}

fn column_name(f: &FieldRef) -> String {
    f.name()
}

impl<'c> Compiler<'c> {
    pub fn new(ctx: &'c Context, seq: i64) -> Self {
        Self { ctx, seq, next: 0 }
    }

    fn alias(&mut self, prefix: &str) -> String {
        self.next += 1;
        format!("{prefix}{}", self.next)
    }

    fn template(cols: &Cols, s: &str) -> String {
        match cols {
            Cols::Base { t, a, .. } => s.replace("{t}", t).replace("{a}", a),
            Cols::Named { r, .. } => s.replace("{t}", r),
        }
    }

    fn unit_of(cols: &Cols) -> Unit {
        match cols {
            Cols::Base { unit, .. } | Cols::Named { unit, .. } => *unit,
        }
    }

    /// The SQL of a field, and what kind of value it is.
    fn field(&self, cols: &Cols, f: &FieldRef) -> Result<(Frag, Kind)> {
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
            (Cols::Named { r, grouped, .. }, FieldRef::Group(name)) if *grouped => {
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
            (Cols::Named { .. }, FieldRef::Group(name)) => {
                Err(invalid(format!("`{name}` is not a column here")))
            }
        }
    }

    fn time_fn(&self, func: TimeFn, t: Frag) -> Frag {
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

    fn operand(&self, cols: &Cols, o: &Operand) -> Result<(Frag, Kind)> {
        match o {
            Operand::Field(f) => self.field(cols, f),
            Operand::TimeFn(func, f) => {
                let (t, _) = self.field(cols, f)?;
                Ok((self.time_fn(*func, t), Kind::Derived))
            }
        }
    }

    /// A payload JSON value read as the type of `v`.
    fn json_as(j: Frag, v: &Value) -> Frag {
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

    fn compare(&self, lhs: Frag, kind: Kind, op: Op, v: &Value) -> Result<Frag> {
        if let Value::List(items) = v {
            let parts = items
                .iter()
                .map(|item| self.compare(lhs.clone(), kind, op, item))
                .collect::<Result<Vec<_>>>()?;
            // `= [a, b]` is any of; `!= [a, b]` is none of.
            let sep = if op == Op::Ne { " AND " } else { " OR " };
            return Ok(Frag::join(parts, sep).parenthesized());
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

    fn id_of(cols: &Cols) -> Result<Frag> {
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

    fn words(&self, cols: &Cols, w: &Words) -> Result<Frag> {
        let id = Self::id_of(cols)?;
        let mut out = Frag::text("EXISTS (SELECT 1 FROM core.search_text s WHERE s.event_id = ");
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

    fn expr(&self, cols: &Cols, e: &Expr) -> Result<Frag> {
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
    fn base_conditions(&self, cols: &Cols) -> Vec<Frag> {
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

    fn main_time(&self, cols: &Cols) -> Result<Frag> {
        let unit = Self::unit_of(cols);
        let field = unit
            .field(unit.main_time())
            .expect("every unit's main time is a field");
        Ok(self.field(cols, &FieldRef::Field(field))?.0)
    }

    fn span(&self, cols: &Cols) -> Option<(Frag, Frag)> {
        let Cols::Base { unit, alias, .. } = cols else {
            return None;
        };
        let (s, e) = fields::span(*unit, alias.map(|a| a.name))?;
        Some((
            Frag::text(Self::template(cols, &s)),
            Frag::text(Self::template(cols, &e)),
        ))
    }

    fn rank(&self, cols: &Cols, words: &[String]) -> Frag {
        let mut f = Frag::text(Self::template(
            cols,
            "(SELECT ts_rank(s.tsv, plainto_tsquery('simple', ",
        ));
        f.param(V::String(Some(words.join(" "))))
            .push(Self::template(
                cols,
                ")) FROM core.search_text s WHERE s.event_id = {t}.id)",
            ));
        f
    }

    fn snippet(&self, cols: &Cols, words: &[String]) -> Frag {
        let mut f = Frag::text("(SELECT ts_headline('simple', s.text, plainto_tsquery('simple', ");
        f.param(V::String(Some(words.join(" ")))).push(Self::template(
            cols,
            "), 'MaxFragments=2, MaxWords=24, MinWords=6, StartSel=[, StopSel=]') FROM core.search_text s WHERE s.event_id = {t}.id)",
        ));
        f
    }

    /// The rows of an inner query of a time join, as `_id`, `_t` (the time
    /// the join measures), `_s` and `_e` (the span, when rows have one).
    fn inner(&mut self, q: &Query, at: TimePoint) -> Result<Frag> {
        let Source::Unit { unit, alias } = &q.source else {
            return Err(invalid(
                "the inner query of a time join selects rows of a unit",
            ));
        };
        let t = self.alias("i");
        let a = self.alias("ia");
        let cols = Cols::Base {
            unit: *unit,
            alias: *alias,
            t,
            a,
        };
        let mut conds = self.base_conditions(&cols);
        if let Some(f) = &q.filter {
            conds.push(self.expr(&cols, f)?);
        }
        let mut laterals = Vec::new();
        for stage in &q.stages {
            match stage {
                Stage::Where(e) => conds.push(self.expr(&cols, e)?),
                Stage::Join(j) => laterals.push(self.join(&cols, j, laterals.len())?.0),
                Stage::AsOf(_) => {}
                _ => {
                    return Err(invalid(
                        "the inner query of a time join takes filters and joins only",
                    ));
                }
            }
        }
        let when = self.main_time(&cols)?;
        let span = self.span(&cols);
        let point = match (at, &span) {
            (TimePoint::When, _) => when.clone(),
            (TimePoint::Start, Some((s, _))) => s.clone(),
            (TimePoint::End, Some((_, e))) => e.clone(),
            (_, None) => {
                return Err(invalid(
                    "`at start` and `at end` need inner rows with a span",
                ));
            }
        };
        let mut f = Frag::text("SELECT ");
        f.push(Self::template(&cols, fields::tiebreak(*unit)))
            .push(" AS _id, ")
            .append(point)
            .push(" AS _t, ");
        match span {
            Some((s, e)) => {
                f.append(s).push(" AS _s, ").append(e).push(" AS _e");
            }
            None => {
                f.append(when).push(" AS _s, NULL::timestamptz AS _e");
            }
        }
        f.push(" FROM ")
            .push(Self::template(&cols, fields::from(*unit)));
        for l in laterals {
            f.push(" ").append(l);
        }
        if !conds.is_empty() {
            f.push(" WHERE ").append(Frag::join(conds, " AND "));
        }
        Ok(f)
    }

    /// A time join as a lateral subquery picking the nearest match; rows
    /// without one drop out (a semijoin: several matches never multiply a
    /// row). Returns the join and the columns it adds.
    fn join(&mut self, outer: &Cols, j: &Join, n: usize) -> Result<(Frag, Vec<(String, String)>)> {
        let inner = self.inner(&j.inner, j.at)?;
        let jl = self.alias("j");
        let suffix = if n == 0 {
            String::new()
        } else {
            format!("_{}", n + 1)
        };
        let ot = self.main_time(outer)?;
        let mut f = Frag::text("JOIN LATERAL (SELECT ");
        let mut columns = Vec::new();
        let secs = || V::Double(j.within_secs);
        match j.kind {
            JoinKind::Near | JoinKind::Before | JoinKind::After => {
                f.push(format!("{jl}._id AS near, extract(epoch from ("))
                    .append(ot.clone());
                f.push(format!(" - {jl}._t)) AS \"offset\" FROM ("))
                    .append(inner)
                    .push(format!(") {jl} WHERE "));
                match j.kind {
                    JoinKind::Near => {
                        f.push(format!("{jl}._t >= "))
                            .append(ot.clone())
                            .push(" - make_interval(secs => ")
                            .param(secs())
                            .push(format!(") AND {jl}._t <= "))
                            .append(ot.clone())
                            .push(" + make_interval(secs => ")
                            .param(secs())
                            .push(format!(") ORDER BY abs(extract(epoch from ({jl}._t - "))
                            .append(ot)
                            .push(format!("))), {jl}._t, {jl}._id LIMIT 1"));
                    }
                    JoinKind::Before => {
                        // The outer row happened before the inner one.
                        f.push(format!("{jl}._t > "))
                            .append(ot.clone())
                            .push(format!(" AND {jl}._t <= "))
                            .append(ot)
                            .push(" + make_interval(secs => ")
                            .param(secs())
                            .push(format!(") ORDER BY {jl}._t, {jl}._id LIMIT 1"));
                    }
                    _ => {
                        f.push(format!("{jl}._t < "))
                            .append(ot.clone())
                            .push(format!(" AND {jl}._t >= "))
                            .append(ot)
                            .push(" - make_interval(secs => ")
                            .param(secs())
                            .push(format!(") ORDER BY {jl}._t DESC, {jl}._id LIMIT 1"));
                    }
                }
                columns.push((format!("near{suffix}"), format!("{jl}.near")));
                columns.push((format!("offset{suffix}"), format!("{jl}.\"offset\"")));
            }
            JoinKind::During => {
                f.push(format!("{jl}._id AS m FROM ("))
                    .append(inner)
                    .push(format!(") {jl} WHERE {jl}._s IS NOT NULL AND "))
                    .append(ot.clone())
                    .push(format!(" >= {jl}._s AND ({jl}._e IS NULL OR "))
                    .append(ot)
                    .push(format!(" < {jl}._e) ORDER BY {jl}._s, {jl}._id LIMIT 1"));
                columns.push((format!("during{suffix}"), format!("{jl}.m")));
            }
            JoinKind::Containing => {
                let (os, oe) = self
                    .span(outer)
                    .ok_or_else(|| invalid("`containing` needs outer rows with a span: sessions, intervals, commands or runs"))?;
                f.push(format!("{jl}._id AS m FROM ("))
                    .append(inner)
                    .push(format!(") {jl} WHERE {jl}._t >= "))
                    .append(os)
                    .push(" AND (")
                    .append(oe.clone())
                    .push(format!(" IS NULL OR {jl}._t < "))
                    .append(oe)
                    .push(format!(") ORDER BY {jl}._t, {jl}._id LIMIT 1"));
                columns.push((format!("containing{suffix}"), format!("{jl}.m")));
            }
        }
        f.push(format!(") {jl} ON true"));
        Ok((f, columns))
    }

    /// Compile a whole query into its numbered answer.
    pub fn compile(&mut self, q: &Query) -> Result<Compiled> {
        let Source::Unit { unit, alias } = &q.source else {
            return Err(invalid(
                "walks and recall are answered by their own readers",
            ));
        };
        let unit = *unit;
        if q.stages.iter().any(|s| matches!(s, Stage::AsOf(_))) && !unit.is_events() {
            return Err(invalid(format!(
                "`as of` reads events; {} have no commit history",
                unit.name()
            )));
        }
        let t = self.alias("t");
        let a = self.alias("a");
        let base = Cols::Base {
            unit,
            alias: *alias,
            t,
            a,
        };

        // Words, for ranking and snippets.
        let mut words = Vec::new();
        if let Some(f) = &q.filter {
            positive_words(f, &mut words);
        }
        for s in &q.stages {
            if let Stage::Where(e) = s {
                positive_words(e, &mut words);
            }
        }

        // Which row fields later stages and the output need as columns.
        let explicit_fields = q.stages.iter().find_map(|s| match s {
            Stage::Fields(f) => Some(f.clone()),
            _ => None,
        });
        let grouped = q.stages.iter().any(|s| matches!(s, Stage::Group(_)));
        let mut output: Vec<FieldRef> = Vec::new();
        if !grouped {
            match &explicit_fields {
                Some(f) => output.extend(f.iter().cloned()),
                None => {
                    for name in unit.default_fields() {
                        if alias.is_some() && *name == "payload" {
                            continue;
                        }
                        output.push(FieldRef::Field(
                            unit.field(name).expect("default fields exist"),
                        ));
                    }
                    if let Some(a) = alias {
                        output.extend(a.fields.iter().map(FieldRef::Field));
                    }
                }
            }
        }
        let mut needed = output.clone();
        for s in &q.stages {
            match s {
                Stage::Where(e) => expr_fields(e, &mut needed),
                Stage::Sort(keys) => {
                    for k in keys {
                        if let SortBy::Operand(o) = &k.key {
                            operand_fields(o, &mut needed);
                        }
                    }
                }
                Stage::Group(g) => {
                    for k in &g.keys {
                        operand_fields(k, &mut needed);
                    }
                    for agg in &g.aggregates {
                        if let Some(f) = &agg.field {
                            needed.push(f.clone());
                        }
                    }
                }
                _ => {}
            }
        }
        needed.retain(|f| !matches!(f, FieldRef::Group(_)));
        let mut seen = std::collections::HashSet::new();
        needed.retain(|f| seen.insert(column_name(f)));

        // The base relation: filter, rows-phase conditions and joins.
        let mut conds = self.base_conditions(&base);
        if let Some(f) = &q.filter {
            conds.push(self.expr(&base, f)?);
        }
        let mut laterals = Vec::new();
        let mut join_columns = Vec::new();
        let mut stages = q.stages.iter().peekable();
        while let Some(stage) = stages.peek() {
            match stage {
                Stage::Where(e) => conds.push(self.expr(&base, e)?),
                Stage::Join(j) => {
                    let (l, cols) = self.join(&base, j, laterals.len())?;
                    laterals.push(l);
                    join_columns.extend(cols);
                }
                Stage::AsOf(_) | Stage::Fields(_) => {}
                _ => break,
            }
            stages.next();
        }
        let mut select = Vec::new();
        for f in &needed {
            let (sql, _) = self.field(&base, f)?;
            let mut c = sql;
            c.push(format!(" AS {}", quote(&column_name(f))));
            select.push(c);
        }
        let mut id = Frag::text(Self::template(&base, fields::tiebreak(unit)));
        id.push(" AS \"_id\"");
        select.push(id);
        let mut when = self.main_time(&base)?;
        when.push(" AS \"_t\"");
        select.push(when);
        let ranked = q.ranked && unit.is_events();
        if ranked {
            let mut r = self.rank(&base, &words);
            r.push(" AS \"_rank\"");
            select.push(r);
            let mut s = self.snippet(&base, &words);
            s.push(" AS \"snippet\"");
            select.push(s);
        }
        for (name, sql) in &join_columns {
            select.push(Frag::text(format!("{sql} AS {}", quote(name))));
        }
        let mut rel = Frag::text("SELECT ");
        rel.append(Frag::join(select, ", "))
            .push(" FROM ")
            .push(Self::template(&base, fields::from(unit)));
        for l in laterals {
            rel.push(" ").append(l);
        }
        if !conds.is_empty() {
            rel.push(" WHERE ").append(Frag::join(conds, " AND "));
        }

        let mut columns: Vec<String> = output.iter().map(column_name).collect();
        if ranked {
            columns.push("snippet".into());
        }
        columns.extend(join_columns.iter().map(|(n, _)| n.clone()));
        let mut is_grouped = false;
        let mut sort: Option<Vec<sinex_model::query::SortKey>> = None;
        let mut limit: Option<u64> = None;
        let mut internal = vec!["_id".to_string(), "_t".to_string()];
        if ranked {
            internal.push("_rank".into());
        }

        // Later stages wrap the relation.
        for stage in stages {
            let r = self.alias("r");
            let cols = Cols::Named {
                r: r.clone(),
                unit,
                grouped: is_grouped,
            };
            match stage {
                Stage::Where(e) => {
                    let cond = self.expr(&cols, e)?;
                    rel = self.wrap(
                        rel,
                        &r,
                        &cols,
                        sort.take(),
                        limit.take(),
                        is_grouped,
                        ranked,
                    )?;
                    let mut f = Frag::text("SELECT * FROM (");
                    f.append(rel).push(format!(") {r} WHERE ")).append(cond);
                    rel = f;
                }
                Stage::Sort(keys) => sort = Some(keys.clone()),
                Stage::Limit(n) => {
                    if limit.is_some() {
                        rel = self.wrap(
                            rel,
                            &r,
                            &cols,
                            sort.take(),
                            limit.take(),
                            is_grouped,
                            ranked,
                        )?;
                    }
                    limit = Some(*n);
                }
                Stage::Group(g) => {
                    rel = self.wrap(
                        rel,
                        &r,
                        &cols,
                        sort.take(),
                        limit.take(),
                        is_grouped,
                        ranked,
                    )?;
                    let gr = self.alias("g");
                    let gcols = Cols::Named {
                        r: gr.clone(),
                        unit,
                        grouped: is_grouped,
                    };
                    let mut keys = Vec::new();
                    let mut select = Vec::new();
                    let mut names = Vec::new();
                    for k in &g.keys {
                        let (sql, _) = self.operand(&gcols, k)?;
                        keys.push(sql.clone());
                        let mut c = sql;
                        c.push(format!(" AS {}", quote(&k.column())));
                        select.push(c);
                        names.push(k.column());
                    }
                    for agg in &g.aggregates {
                        let mut c = match (&agg.function, &agg.field) {
                            (AggFn::Count, None) => Frag::text("count(*)"),
                            (func, Some(f)) => {
                                let (sql, kind) = self.field(&gcols, f)?;
                                let arg = if kind == Kind::Json {
                                    Self::json_as(sql, &Value::Number(0.0))
                                } else {
                                    sql
                                };
                                let mut c = Frag::text(match func {
                                    AggFn::Count => "count(",
                                    AggFn::Sum => "sum(",
                                    AggFn::Min => "min(",
                                    AggFn::Max => "max(",
                                    AggFn::Avg => "avg(",
                                    AggFn::CountDistinct => "count(DISTINCT ",
                                });
                                c.append(arg).push(")");
                                c
                            }
                            (_, None) => return Err(invalid("this aggregate needs a field")),
                        };
                        c.push(format!(" AS {}", quote(&agg.column())));
                        select.push(c);
                        names.push(agg.column());
                    }
                    let mut f = Frag::text("SELECT ");
                    f.append(Frag::join(select, ", "))
                        .push(" FROM (")
                        .append(rel)
                        .push(format!(") {gr}"));
                    // By position: a key's parameters (zone, day start) are
                    // separate placeholders each time they are written.
                    if !keys.is_empty() {
                        let positions: Vec<String> =
                            (1..=keys.len()).map(|i| i.to_string()).collect();
                        f.push(" GROUP BY ").push(positions.join(", "));
                    }
                    rel = f;
                    columns = names;
                    internal.clear();
                    is_grouped = true;
                }
                Stage::Join(_) => {
                    return Err(invalid(
                        "time joins apply to rows before sorting, limiting or grouping",
                    ));
                }
                Stage::Fields(_) | Stage::AsOf(_) => {}
            }
        }

        // The answer: numbered in order, limited by the query's own limit.
        if let Some(fields) = &explicit_fields
            && is_grouped
        {
            columns = fields.iter().map(column_name).collect();
        }
        let r = self.alias("z");
        let cols = Cols::Named {
            r: r.clone(),
            unit,
            grouped: is_grouped,
        };
        let order = self.order(&cols, sort.as_deref(), is_grouped, ranked, &columns)?;
        let mut out_cols: Vec<String> = columns
            .iter()
            .map(|c| format!("{r}.{}", quote(c)))
            .collect();
        out_cols.extend(internal.iter().map(|c| format!("{r}.{}", quote(c))));
        let mut answer = Frag::text("SELECT row_number() OVER (ORDER BY ");
        answer
            .append(order.clone())
            .push(") AS \"_n\", ")
            .push(out_cols.join(", "))
            .push(" FROM (")
            .append(rel)
            .push(format!(") {r} ORDER BY "))
            .append(order);
        if let Some(n) = limit {
            answer.push(" LIMIT ").param(V::BigInt(Some(n as i64)));
        }
        Ok(Compiled { answer, columns })
    }

    /// Apply a pending sort and limit by wrapping the relation.
    #[allow(clippy::too_many_arguments)]
    fn wrap(
        &mut self,
        rel: Frag,
        r: &str,
        cols: &Cols,
        sort: Option<Vec<sinex_model::query::SortKey>>,
        limit: Option<u64>,
        grouped: bool,
        ranked: bool,
    ) -> Result<Frag> {
        let Some(n) = limit else {
            return Ok(rel);
        };
        let order = self.order(cols, sort.as_deref(), grouped, ranked, &[])?;
        let mut f = Frag::text("SELECT * FROM (");
        f.append(rel)
            .push(format!(") {r} ORDER BY "))
            .append(order)
            .push(" LIMIT ")
            .param(V::BigInt(Some(n as i64)));
        Ok(f)
    }

    /// The order of rows: the explicit sort with unknowns last, then the
    /// default (relevance when ranked, else time), then the row's own key.
    fn order(
        &self,
        cols: &Cols,
        sort: Option<&[sinex_model::query::SortKey]>,
        grouped: bool,
        ranked: bool,
        group_columns: &[String],
    ) -> Result<Frag> {
        let Cols::Named { r, .. } = cols else {
            unreachable!("ordering applies to named relations")
        };
        let mut parts = Vec::new();
        if let Some(keys) = sort {
            for k in keys {
                let mut f = match &k.key {
                    SortBy::Rank if ranked && !grouped => Frag::text(format!("{r}.\"_rank\"")),
                    SortBy::Rank => {
                        return Err(invalid("`sort rank` needs a full-text search over rows"));
                    }
                    SortBy::Operand(o) => self.operand(cols, o)?.0,
                };
                f.push(if k.descending {
                    " DESC NULLS LAST"
                } else {
                    " ASC NULLS LAST"
                });
                parts.push(f);
            }
        }
        if grouped {
            for c in group_columns {
                parts.push(Frag::text(format!("{r}.{} ASC NULLS LAST", quote(c))));
            }
            if parts.is_empty() {
                parts.push(Frag::text("1"));
            }
        } else {
            if sort.is_none() && ranked {
                parts.push(Frag::text(format!("{r}.\"_rank\" DESC NULLS LAST")));
            }
            parts.push(Frag::text(format!("{r}.\"_t\" ASC NULLS LAST")));
            parts.push(Frag::text(format!("{r}.\"_id\" ASC")));
        }
        Ok(Frag::join(parts, ", "))
    }
}
