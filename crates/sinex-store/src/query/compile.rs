//! Compiling a bound query to SQL. The boolean structure is kept exactly: two
//! conditions joined by `and` are both required, and a comparison with an
//! unknown value is unknown, never true. Every query reads events live at one
//! commit position, so the pages of one answer agree with each other and
//! `as of` is the same rule at an earlier position.
use super::fields;
use super::sql::SqlFragment;
use crate::{Error, Result};
use sea_query::Value as V;
use sinex_model::query::text::{like_escape, regex_escape};
use sinex_model::query::{
    AggFn, Alias, Context, Expr, FieldRef, FieldType, Join, JoinKind, Op, Operand, Query, SortBy,
    Source, Stage, Table, TimeFn, TimePoint, Value, Words,
};

mod expr;
mod fill;
mod join;

/// A compiled answer: one SELECT whose rows are numbered `_n` in answer order
/// and carry the output columns.
pub(crate) struct Compiled {
    /// The rows, numbered `_n` in order.
    pub answer: SqlFragment,
    /// The number of rows, without ordering or numbering them.
    pub count: SqlFragment,
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
    /// The table's own tables, under aliases `t` (and `a` for the archive).
    Base {
        table: Table,
        alias: Option<&'static Alias>,
        t: String,
        a: String,
    },
    /// The named columns of a subquery `r`.
    Named {
        r: String,
        table: Table,
        grouped: bool,
    },
}

/// What kind of value an operand yields, for choosing comparisons.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Typed(FieldType),
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
        Expr::Words(_) | Expr::Overlaps { .. } => {}
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
    pub(crate) fn new(ctx: &'c Context, seq: i64) -> Self {
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

    fn positioned(&self, cols: &Cols, sql: &str) -> SqlFragment {
        let sql = Self::template(cols, sql);
        let mut parts = sql.split("{seq}");
        let mut out = SqlFragment::text(parts.next().expect("one part"));
        for part in parts {
            out.param(V::BigInt(Some(self.seq))).push(part);
        }
        out
    }

    fn table_of(cols: &Cols) -> Table {
        match cols {
            Cols::Base { table, .. } | Cols::Named { table, .. } => *table,
        }
    }

    /// Compile a whole query into its numbered answer.
    pub(crate) fn compile(&mut self, q: &Query) -> Result<Compiled> {
        let Source::Table { table, alias } = &q.source else {
            return Err(invalid(
                "walks and recall are answered by their own readers",
            ));
        };
        let table = *table;
        if q.stages.iter().any(|s| matches!(s, Stage::AsOf(_))) && !table.is_events() {
            return Err(invalid(format!(
                "`as of` reads events; {} have no commit history",
                table.name()
            )));
        }
        let t = self.alias("t");
        let a = self.alias("a");
        let base = Cols::Base {
            table,
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
                    for name in table.default_fields() {
                        if alias.is_some() && *name == "payload" {
                            continue;
                        }
                        output.push(FieldRef::Field(
                            table.field(name).expect("default fields exist"),
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
        let mut id = SqlFragment::text(Self::template(&base, fields::tiebreak(table)));
        id.push(" AS \"_id\"");
        select.push(id);
        let mut when = self.main_time(&base)?;
        when.push(" AS \"_t\"");
        select.push(when);
        let ranked = q.ranked && table.is_events();
        if ranked {
            let mut r = self.rank(&base, &words);
            r.push(" AS \"_rank\"");
            select.push(r);
            let mut s = self.snippet(&base, &words);
            s.push(" AS \"snippet\"");
            select.push(s);
        }
        for (name, sql) in &join_columns {
            select.push(SqlFragment::text(format!("{sql} AS {}", quote(name))));
        }
        let mut rel = SqlFragment::text("SELECT ");
        rel.append(SqlFragment::join(select, ", "))
            .push(" FROM ")
            .append(self.positioned(&base, fields::from(table)));
        if table == Table::Gaps {
            rel.push(Self::template(&base, " {t}"));
        }
        for l in laterals {
            rel.push(" ").append(l);
        }
        if !conds.is_empty() {
            rel.push(" WHERE ")
                .append(SqlFragment::join(conds, " AND "));
        }

        let mut columns: Vec<String> = output.iter().map(column_name).collect();
        if ranked {
            columns.push("snippet".into());
        }
        if explicit_fields.is_none() {
            columns.extend(join_columns.iter().map(|(n, _)| n.clone()));
        }
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
                table,
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
                    let mut f = SqlFragment::text("SELECT * FROM (");
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
                        table,
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
                            (AggFn::Count, None) => SqlFragment::text("count(*)"),
                            (func, Some(f)) => {
                                let (arg, _) = self.field(&gcols, f)?;
                                let mut c = SqlFragment::text(match func {
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
                    let mut f = SqlFragment::text("SELECT ");
                    f.append(SqlFragment::join(select, ", "))
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
                    if let Some(fill) = &g.fill {
                        f = self.fill(f, g, fill)?;
                        names.push("coverage".into());
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
            table,
            grouped: is_grouped,
        };
        let order = self.order(&cols, sort.as_deref(), is_grouped, ranked, &columns)?;
        let mut out_cols: Vec<String> = columns
            .iter()
            .map(|c| format!("{r}.{}", quote(c)))
            .collect();
        out_cols.extend(internal.iter().map(|c| format!("{r}.{}", quote(c))));
        let mut count = SqlFragment::text("SELECT count(*) FROM (");
        match limit {
            Some(n) => count
                .push("SELECT 1 FROM (")
                .append(rel.clone())
                .push(format!(") {r} ORDER BY "))
                .append(order.clone())
                .push(" LIMIT ")
                .param(V::BigInt(Some(n as i64))),
            None => count.append(rel.clone()),
        };
        count.push(") c");
        let mut answer = SqlFragment::text("SELECT row_number() OVER (ORDER BY ");
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
        Ok(Compiled {
            answer,
            count,
            columns,
        })
    }

    /// Apply a pending sort and limit by wrapping the relation.
    #[allow(clippy::too_many_arguments)]
    fn wrap(
        &mut self,
        rel: SqlFragment,
        r: &str,
        cols: &Cols,
        sort: Option<Vec<sinex_model::query::SortKey>>,
        limit: Option<u64>,
        grouped: bool,
        ranked: bool,
    ) -> Result<SqlFragment> {
        let Some(n) = limit else {
            return Ok(rel);
        };
        let order = self.order(cols, sort.as_deref(), grouped, ranked, &[])?;
        let mut f = SqlFragment::text("SELECT * FROM (");
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
    ) -> Result<SqlFragment> {
        let Cols::Named { r, .. } = cols else {
            unreachable!("ordering applies to named relations")
        };
        let mut parts = Vec::new();
        if let Some(keys) = sort {
            for k in keys {
                let mut f = match &k.key {
                    SortBy::Rank if ranked && !grouped => {
                        SqlFragment::text(format!("{r}.\"_rank\""))
                    }
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
                parts.push(SqlFragment::text(format!(
                    "{r}.{} ASC NULLS LAST",
                    quote(c)
                )));
            }
            if parts.is_empty() {
                parts.push(SqlFragment::text("1"));
            }
        } else {
            if sort.is_none() && ranked {
                parts.push(SqlFragment::text(format!("{r}.\"_rank\" DESC NULLS LAST")));
            }
            parts.push(SqlFragment::text(format!("{r}.\"_t\" ASC NULLS LAST")));
            parts.push(SqlFragment::text(format!("{r}.\"_id\" ASC")));
        }
        Ok(SqlFragment::join(parts, ", "))
    }
}
