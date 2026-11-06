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
    AggFn, Alias, Context, Expr, FieldRef, FieldType, Join, JoinKind, JoinOn, Op, Operand,
    OrderingOp, Query, SortBy, Source, Stage, Table, TimeFn, TimePoint, Value, Words,
};
use std::collections::BTreeMap;

pub(super) mod calendar;
mod expr;
mod fill;
mod join;

/// A compiled answer with hidden typed sort keys and a stable row identity.
pub(crate) struct Compiled {
    /// The complete ordered relation, including its hidden keyset columns.
    pub answer: SqlFragment,
    /// The number of rows, without ordering or numbering them.
    pub count: SqlFragment,
    pub columns: Vec<String>,
    pub keys: Vec<(String, bool)>,
}

impl Compiled {
    /// SQL identifier of an output value in the private answer relation.
    pub(crate) fn output_column(&self, logical: &str) -> Result<String> {
        self.columns
            .iter()
            .position(|name| name == logical)
            .map(|index| quote(&format!("_value{index}")))
            .ok_or_else(|| invalid(format!("no output column `{logical}`")))
    }

    /// Public row object, independent of the private pagination columns.
    pub(crate) fn json_object(&self) -> String {
        let parts: Vec<_> = self
            .columns
            .chunks(50)
            .enumerate()
            .map(|(chunk_index, chunk)| {
                let pairs: Vec<_> = chunk
                    .iter()
                    .enumerate()
                    .map(|(index, name)| {
                        format!(
                            "'{}', q.{}",
                            name.replace('\'', "''"),
                            quote(&format!("_value{}", chunk_index * 50 + index))
                        )
                    })
                    .collect();
                format!("jsonb_build_object({})", pairs.join(", "))
            })
            .collect();
        if parts.is_empty() {
            "'{}'::jsonb".into()
        } else {
            parts.join(" || ")
        }
    }
}

pub(crate) struct Compiler<'c> {
    ctx: &'c Context,
    seq: i64,
    next: usize,
    calendar: Option<String>,
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
        renamed: BTreeMap<String, String>,
        ordinal: Option<String>,
    },
}

/// What kind of value an operand yields, for choosing comparisons.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Typed(FieldType),
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
        Value::List(_) | Value::Unknown | Value::Operand(_) => {
            return Err(invalid("non-scalar SQL parameter"));
        }
        Value::PlaceholderText(_) => return Err(invalid("unbound saved-query parameter")),
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
        Expr::Truth { operand, .. } | Expr::FieldWords { operand, .. } => {
            operand_fields(operand, out)
        }
        Expr::And(a, b) | Expr::Or(a, b) => {
            expr_fields(a, out);
            expr_fields(b, out);
        }
        Expr::Not(a) => expr_fields(a, out),
        Expr::Order(join) => join_fields(join, out),
        Expr::Compare { lhs, rhs, .. } => {
            operand_fields(lhs, out);
            if let Value::Operand(rhs) = rhs {
                operand_fields(rhs, out);
            }
        }
        Expr::Exists(lhs) | Expr::Within { lhs, .. } => operand_fields(lhs, out),
        Expr::Words(_) | Expr::Overlaps { .. } | Expr::Similar { .. } => {}
    }
}

fn operand_fields(o: &Operand, out: &mut Vec<FieldRef>) {
    match o {
        Operand::Field(f) | Operand::TimeFn(_, f) => out.push(f.clone()),
        Operand::Arithmetic { left, right, .. } | Operand::Glob { left, right } => {
            operand_fields(left, out);
            operand_fields(right, out);
        }
        Operand::CaseEqual { left, right, value } => {
            operand_fields(left, out);
            operand_fields(right, out);
            operand_fields(value, out);
        }
        Operand::Length(value) | Operand::Named { operand: value, .. } => {
            operand_fields(value, out)
        }
        Operand::Literal {
            value: Value::Operand(value),
            ..
        } => operand_fields(value, out),
        Operand::Literal { .. } => {}
    }
}

fn has_order_witness(expression: &Expr) -> bool {
    match expression {
        Expr::Order(_) => true,
        Expr::And(left, right) | Expr::Or(left, right) => {
            has_order_witness(left) || has_order_witness(right)
        }
        _ => false,
    }
}

fn join_fields(join: &Join, out: &mut Vec<FieldRef>) {
    if let JoinOn::Key { left, .. } = &join.on {
        out.push(left.clone());
    }
}

fn column_name(f: &FieldRef) -> String {
    f.name()
}

impl<'c> Compiler<'c> {
    pub(crate) fn new(ctx: &'c Context, seq: i64) -> Self {
        Self {
            ctx,
            seq,
            next: 0,
            calendar: None,
        }
    }

    fn with_calendar(ctx: &'c Context, seq: i64) -> Self {
        Self {
            ctx,
            seq,
            next: 0,
            calendar: Some(calendar::setting(ctx)),
        }
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

    fn named(cols: &Cols, logical: &str) -> String {
        let Cols::Named { r, renamed, .. } = cols else {
            unreachable!("named columns require a relation")
        };
        format!(
            "{r}.{}",
            quote(renamed.get(logical).map(String::as_str).unwrap_or(logical))
        )
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
        let similar = q.similar_reference()?;
        if similar.is_some() && !table.is_events() {
            return Err(invalid("similar ranks event rows"));
        }
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
        let mut output: Vec<FieldRef> = Vec::new();
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
        let mut needed = output.clone();
        if let Some(filter) = &q.filter {
            expr_fields(filter, &mut needed);
        }
        for s in &q.stages {
            match s {
                Stage::Where(e) => expr_fields(e, &mut needed),
                Stage::Join(join) => join_fields(join, &mut needed),
                Stage::Fields(operands) => {
                    for operand in operands {
                        operand_fields(operand, &mut needed);
                    }
                }
                Stage::Sort(keys) => {
                    for k in keys {
                        if let SortBy::Operand(o) = &k.key {
                            operand_fields(o, &mut needed);
                        }
                    }
                }
                Stage::GroupFirst { keys, .. } => {
                    for key in keys {
                        operand_fields(key, &mut needed);
                    }
                }
                Stage::Group(g) => {
                    for k in &g.keys {
                        operand_fields(k, &mut needed);
                    }
                    for agg in &g.aggregates {
                        if let Some(argument) = &agg.argument {
                            operand_fields(argument, &mut needed);
                        }
                    }
                }
                _ => {}
            }
        }
        needed.retain(|f| !matches!(f, FieldRef::Group(_) | FieldRef::TypedGroup(_)));
        let mut seen = std::collections::HashSet::new();
        needed.retain(|f| seen.insert(column_name(f)));

        // Source conditions use the same relation and matched-column semantics
        // as later filters. PostgreSQL can push scalar filters into this base.
        let conds = self.base_conditions(&base);
        let source_filter = q
            .filter
            .as_ref()
            .map(|expression| Stage::Where(expression.clone()));
        let stages = source_filter.iter().chain(q.stages.iter());
        let mut renamed = BTreeMap::new();
        let mut ordinal = None;
        let mut select = Vec::new();
        for f in &needed {
            let (sql, _) = self.field(&base, f)?;
            let mut c = sql;
            let physical = self.alias("c");
            renamed.insert(column_name(f), physical.clone());
            c.push(format!(" AS {}", quote(&physical)));
            select.push(c);
        }
        let mut id = SqlFragment::text(Self::template(&base, fields::tiebreak(table)));
        id.push(" AS \"_id\"");
        select.push(id);
        let event_id = if table.is_events() {
            Self::template(&base, fields::tiebreak(table))
        } else {
            "NULL::uuid".into()
        };
        select.push(SqlFragment::text(format!("{event_id} AS \"_event_id\"")));
        let when = self.main_time(&base)?;
        let mut timestamp = when.clone();
        timestamp.push(" AS \"_t\"");
        select.push(timestamp);
        let (mut start, mut end, mut inclusive, mut has_span) = self.span_metadata(&base)?;
        start.push(" AS \"_s\"");
        end.push(" AS \"_e\"");
        inclusive.push(" AS \"_inclusive\"");
        has_span.push(" AS \"_has_span\"");
        select.extend([start, end, inclusive, has_span]);
        let parents = if table.is_events() {
            format!(
                "ARRAY[{}]::uuid[]",
                Self::template(&base, fields::tiebreak(table))
            )
        } else {
            "ARRAY[]::uuid[]".into()
        };
        let mut parents = SqlFragment::text(parents);
        parents.push(" AS \"_parents\"");
        select.push(parents);
        let ranked = q.ranked && table.is_events();
        if ranked {
            let mut r = if let Some(reference) = similar {
                crate::vectors::rank_sql(&Self::template(&base, "{t}.id"), reference, self.seq)
            } else {
                self.rank(&base, &words)
            };
            r.push(" AS \"_rank\"");
            select.push(r);
            let mut s = self.snippet(&base, &words);
            let physical = self.alias("c");
            renamed.insert("snippet".into(), physical.clone());
            s.push(format!(" AS {}", quote(&physical)));
            select.push(s);
        }
        let mut rel = SqlFragment::text("SELECT ");
        rel.append(SqlFragment::join(select, ", "))
            .push(" FROM ")
            .append(self.positioned(&base, fields::from(table)));
        if table == Table::Gaps {
            rel.push(Self::template(&base, " {t}"));
        }
        if !conds.is_empty() {
            rel.push(" WHERE ")
                .append(SqlFragment::join(conds, " AND "));
        }

        let mut columns: Vec<String> = output.iter().map(column_name).collect();
        if ranked {
            columns.push("snippet".into());
        }
        let mut group_order = Vec::new();
        let mut is_grouped = false;
        let mut sort: Option<Vec<sinex_model::query::SortKey>> = None;
        let mut limit: Option<u64> = None;
        let mut internal: Vec<String> = vec![
            "_id".into(),
            "_event_id".into(),
            "_t".into(),
            "_s".into(),
            "_e".into(),
            "_inclusive".into(),
            "_has_span".into(),
            "_parents".into(),
        ];
        if ranked {
            internal.push("_rank".into());
        }

        // Later stages wrap the relation.
        let mut join_count = 0;
        for stage in stages {
            let r = self.alias("r");
            let cols = Cols::Named {
                r: r.clone(),
                table,
                grouped: is_grouped,
                renamed: renamed.clone(),
                ordinal: ordinal.clone(),
            };
            match stage {
                Stage::Where(e) => {
                    let Some(e) = e.predicate() else {
                        continue;
                    };
                    rel = self.wrap(
                        rel,
                        &r,
                        &cols,
                        sort.clone(),
                        limit.take(),
                        is_grouped,
                        ranked,
                        &group_order,
                    )?;
                    let witnesses = self.order_witnesses(&cols, &e)?;
                    let mut condition_cols = cols.clone();
                    if !witnesses.is_empty() {
                        let order =
                            self.order(&cols, sort.as_deref(), is_grouped, ranked, &group_order)?;
                        let next_ordinal = self.alias("o");
                        let mut ordered = SqlFragment::text(format!(
                            "SELECT {r}.*, row_number() OVER (ORDER BY "
                        ));
                        ordered
                            .append(order)
                            .push(format!(") AS {} FROM (", quote(&next_ordinal)))
                            .append(rel)
                            .push(format!(") {r}"));
                        rel = ordered;
                        ordinal = Some(next_ordinal);
                        sort = None;
                        condition_cols = Cols::Named {
                            r: r.clone(),
                            table,
                            grouped: is_grouped,
                            renamed: renamed.clone(),
                            ordinal: ordinal.clone(),
                        };
                    }
                    for witness in witnesses {
                        let mut select = vec![SqlFragment::text(format!("{r}.*"))];
                        for (visible, (name, value)) in witness
                            .columns
                            .into_iter()
                            .map(|column| (true, column))
                            .chain(
                                witness
                                    .hidden_columns
                                    .into_iter()
                                    .map(|column| (false, column)),
                            )
                        {
                            let physical = self.alias("c");
                            select.push(SqlFragment::text(format!(
                                "{value} AS {}",
                                quote(&physical)
                            )));
                            if visible && !columns.contains(&name) {
                                columns.push(name.clone());
                            }
                            renamed.insert(name, physical);
                        }
                        let mut witnessed = SqlFragment::text("SELECT ");
                        witnessed
                            .append(SqlFragment::join(select, ", "))
                            .push(" FROM (")
                            .append(rel)
                            .push(format!(") {r} "))
                            .append(witness.lateral);
                        rel = witnessed;
                        condition_cols = Cols::Named {
                            r: r.clone(),
                            table,
                            grouped: is_grouped,
                            renamed: renamed.clone(),
                            ordinal: ordinal.clone(),
                        };
                    }
                    let cond = self.expr(&condition_cols, &e)?;
                    let mut f = if has_order_witness(&e) {
                        let proof = self.predicate_parents(&condition_cols, &e)?;
                        let mut retained: Vec<_> = renamed
                            .values()
                            .cloned()
                            .chain(
                                internal
                                    .iter()
                                    .filter(|name| name.as_str() != "_parents")
                                    .cloned(),
                            )
                            .chain(ordinal.iter().cloned())
                            .collect();
                        retained.sort();
                        retained.dedup();
                        let mut out = SqlFragment::text("SELECT ");
                        out.push(
                            retained
                                .iter()
                                .map(|name| format!("{r}.{}", quote(name)))
                                .collect::<Vec<_>>()
                                .join(", "),
                        )
                        .push(format!(
                            ", ARRAY(SELECT DISTINCT unnest({r}.\"_parents\" || "
                        ))
                        .append(proof)
                        .push(")) AS \"_parents\" FROM (");
                        out
                    } else {
                        SqlFragment::text("SELECT * FROM (")
                    };
                    f.append(rel).push(format!(") {r} WHERE ")).append(cond);
                    rel = f;
                }
                Stage::Sort(keys) => {
                    if limit.is_some() {
                        rel = self.wrap(
                            rel,
                            &r,
                            &cols,
                            sort.clone(),
                            limit.take(),
                            is_grouped,
                            ranked,
                            &group_order,
                        )?;
                    }
                    sort = Some(keys.clone());
                }
                Stage::Limit(n) => {
                    if limit.is_some() {
                        rel = self.wrap(
                            rel,
                            &r,
                            &cols,
                            sort.clone(),
                            limit.take(),
                            is_grouped,
                            ranked,
                            &group_order,
                        )?;
                    }
                    limit = Some(*n);
                }
                Stage::GroupFirst { keys, count } => {
                    rel = self.wrap(
                        rel,
                        &r,
                        &cols,
                        sort.clone(),
                        limit.take(),
                        is_grouped,
                        ranked,
                        &group_order,
                    )?;
                    let order =
                        self.order(&cols, sort.as_deref(), is_grouped, ranked, &group_order)?;
                    let partition = keys
                        .iter()
                        .map(|key| self.operand(&cols, key).map(|(sql, _)| sql))
                        .collect::<Result<Vec<_>>>()?;
                    let rank = self.alias("group_rank");
                    let next_ordinal = self.alias("o");
                    let selected = self.alias("selected");
                    let mut ranked_rows =
                        SqlFragment::text(format!("SELECT {r}.*, row_number() OVER ("));
                    if !partition.is_empty() {
                        ranked_rows
                            .push("PARTITION BY ")
                            .append(SqlFragment::join(partition, ", "))
                            .push(" ");
                    }
                    ranked_rows
                        .push("ORDER BY ")
                        .append(order.clone())
                        .push(format!(
                            ") AS {}, row_number() OVER (ORDER BY ",
                            quote(&rank)
                        ))
                        .append(order)
                        .push(format!(") AS {} FROM (", quote(&next_ordinal)))
                        .append(rel)
                        .push(format!(") {r}"));
                    let mut survivors = SqlFragment::text("SELECT * FROM (");
                    survivors
                        .append(ranked_rows)
                        .push(format!(
                            ") {selected} WHERE {selected}.{} <= ",
                            quote(&rank)
                        ))
                        .param(V::BigInt(Some(i64::try_from(*count).unwrap_or(i64::MAX))));
                    rel = survivors;
                    ordinal = Some(next_ordinal);
                    sort = None;
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
                        &group_order,
                    )?;
                    let gr = self.alias("g");
                    let gcols = Cols::Named {
                        r: gr.clone(),
                        table,
                        grouped: is_grouped,
                        renamed: renamed.clone(),
                        ordinal: ordinal.clone(),
                    };
                    let mut keys = Vec::new();
                    let mut select = Vec::new();
                    let mut names = Vec::new();
                    let mut group_names = BTreeMap::new();
                    for k in &g.keys {
                        let (sql, _) = self.operand(&gcols, k)?;
                        keys.push(sql.clone());
                        let mut c = sql;
                        let physical = self.alias("c");
                        group_names.insert(k.column(), physical.clone());
                        c.push(format!(" AS {}", quote(&physical)));
                        select.push(c);
                        names.push(k.column());
                    }
                    for agg in &g.aggregates {
                        let mut c = match (&agg.function, &agg.argument) {
                            (AggFn::Count, None) => SqlFragment::text("count(*)"),
                            (func, Some(f)) => {
                                let (arg, kind) = self.operand(&gcols, f)?;
                                let arg = if kind == Kind::Typed(FieldType::Json)
                                    && matches!(func, AggFn::Sum | AggFn::Avg)
                                {
                                    Self::json_as(arg, &Value::Number(0.0))
                                } else {
                                    arg
                                };
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
                        let physical = self.alias("c");
                        group_names.insert(agg.column(), physical.clone());
                        c.push(format!(" AS {}", quote(&physical)));
                        select.push(c);
                        names.push(agg.column());
                    }
                    select.push(SqlFragment::text(format!(
                        "ARRAY(SELECT DISTINCT parent::uuid FROM jsonb_array_elements(jsonb_agg(to_jsonb({gr}.\"_parents\"))) a(parents) CROSS JOIN LATERAL jsonb_array_elements_text(a.parents) p(parent) ORDER BY parent::uuid) AS \"_parents\""
                    )));
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
                        f = self.fill(f, g, fill, &group_names)?;
                        group_names.insert("coverage".into(), "coverage".into());
                        names.push("coverage".into());
                    }
                    let identity_keys = g
                        .keys
                        .iter()
                        .map(|k| format!("g_identity.{}", quote(&group_names[&k.column()])))
                        .collect::<Vec<_>>()
                        .join(", ");
                    let mut identified = SqlFragment::text(format!(
                        "SELECT g_identity.*, jsonb_build_array({identity_keys})::text AS \"_id\", NULL::uuid AS \"_event_id\", NULL::timestamptz AS \"_t\", NULL::timestamptz AS \"_s\", NULL::timestamptz AS \"_e\", true AS \"_inclusive\", false AS \"_has_span\" FROM ("
                    ));
                    identified.append(f).push(") g_identity");
                    rel = identified;
                    columns = names;
                    renamed = group_names;
                    ordinal = None;
                    group_order = columns.clone();
                    internal = vec![
                        "_id".into(),
                        "_event_id".into(),
                        "_t".into(),
                        "_s".into(),
                        "_e".into(),
                        "_inclusive".into(),
                        "_has_span".into(),
                        "_parents".into(),
                    ];
                    is_grouped = true;
                }
                Stage::Join(join) => {
                    // Materialize the outer order before a match can replace
                    // names used by a preceding sort or projection.
                    let order =
                        self.order(&cols, sort.as_deref(), is_grouped, ranked, &group_order)?;
                    let next_ordinal = self.alias("o");
                    let mut ordered =
                        SqlFragment::text(format!("SELECT {r}.*, row_number() OVER (ORDER BY "));
                    ordered
                        .append(order.clone())
                        .push(format!(") AS {} FROM (", quote(&next_ordinal)))
                        .append(rel)
                        .push(format!(") {r}"));
                    if let Some(n) = limit.take() {
                        ordered
                            .push(" ORDER BY ")
                            .append(order)
                            .push(" LIMIT ")
                            .param(V::BigInt(Some(n as i64)));
                    }
                    ordinal = Some(next_ordinal.clone());
                    sort = None;
                    let joined = self.join(&cols, join, join_count)?;
                    join_count += 1;
                    let mut retained: Vec<String> = renamed.values().cloned().collect();
                    retained.extend(
                        internal
                            .iter()
                            .filter(|name| !matches!(name.as_str(), "_id" | "_parents"))
                            .cloned(),
                    );
                    retained.push(next_ordinal.clone());
                    retained.sort();
                    retained.dedup();
                    let mut select: Vec<_> = retained
                        .iter()
                        .map(|name| SqlFragment::text(format!("{r}.{}", quote(name))))
                        .collect();
                    let identity = joined.identity.unwrap_or_else(|| format!("{r}.\"_id\""));
                    select.push(SqlFragment::text(format!("{identity} AS \"_id\"")));
                    let parents = joined
                        .parents
                        .unwrap_or_else(|| format!("{r}.\"_parents\""));
                    select.push(SqlFragment::text(format!("{parents} AS \"_parents\"")));
                    if let Some(child_ordinal) = joined.ordinal {
                        let pair_ordinal = self.alias("o");
                        select.push(SqlFragment::text(format!(
                            "row_number() OVER (ORDER BY {r}.{}, {child_ordinal}) AS {}",
                            quote(&next_ordinal),
                            quote(&pair_ordinal)
                        )));
                        ordinal = Some(pair_ordinal);
                    }
                    for (visible, (name, value)) in joined
                        .columns
                        .into_iter()
                        .map(|column| (true, column))
                        .chain(
                            joined
                                .hidden_columns
                                .into_iter()
                                .map(|column| (false, column)),
                        )
                    {
                        let physical = self.alias("c");
                        select.push(SqlFragment::text(format!(
                            "{value} AS {}",
                            quote(&physical)
                        )));
                        if visible && !columns.contains(&name) {
                            columns.push(name.clone());
                        }
                        renamed.insert(name, physical);
                    }
                    let mut combined = SqlFragment::text("SELECT ");
                    combined
                        .append(SqlFragment::join(select, ", "))
                        .push(" FROM (")
                        .append(ordered)
                        .push(format!(") {r} "))
                        .append(joined.lateral);
                    rel = combined;
                }
                Stage::Fields(operands) => {
                    let order =
                        self.order(&cols, sort.as_deref(), is_grouped, ranked, &group_order)?;
                    let mut projected = SqlFragment::text(format!("SELECT {r}.*"));
                    let next_ordinal = self.alias("o");
                    projected
                        .push(", row_number() OVER (ORDER BY ")
                        .append(order)
                        .push(format!(") AS {}", quote(&next_ordinal)));
                    let mut projected_names = BTreeMap::new();
                    for operand in operands {
                        let (value, _) = self.operand(&cols, operand)?;
                        let physical = self.alias("c");
                        projected
                            .push(", ")
                            .append(value)
                            .push(format!(" AS {}", quote(&physical)));
                        projected_names.insert(operand.column(), physical);
                    }
                    projected.push(" FROM (").append(rel).push(format!(") {r}"));
                    if let Some(n) = limit.take() {
                        let order =
                            self.order(&cols, sort.as_deref(), is_grouped, ranked, &group_order)?;
                        projected
                            .push(" ORDER BY ")
                            .append(order)
                            .push(" LIMIT ")
                            .param(V::BigInt(Some(n as i64)));
                    }
                    rel = projected;
                    columns = operands.iter().map(Operand::column).collect();
                    renamed = projected_names;
                    ordinal = Some(next_ordinal);
                    sort = None;
                }
                Stage::AsOf(_) => {}
            }
        }

        // The answer: numbered in order, limited by the query's own limit.
        let r = self.alias("z");
        let cols = Cols::Named {
            r: r.clone(),
            table,
            grouped: is_grouped,
            renamed,
            ordinal,
        };
        let order = self.order(&cols, sort.as_deref(), is_grouped, ranked, &group_order)?;
        let mut out_cols: Vec<String> = columns
            .iter()
            .enumerate()
            .map(|(index, c)| {
                format!(
                    "{} AS {}",
                    Self::named(&cols, c),
                    quote(&format!("_value{index}"))
                )
            })
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
        let terms = self.order_terms(&cols, sort.as_deref(), is_grouped, ranked, &group_order)?;
        let mut keys = Vec::new();
        let mut projections = Vec::new();
        let mut types = Vec::new();
        for (index, (term, descending)) in terms.into_iter().enumerate() {
            let name = format!("_k{index}");
            let mut ty = SqlFragment::text("pg_typeof(");
            ty.append(term.clone()).push(")::text");
            types.push(ty);
            let mut projection = term;
            projection.push(format!(" AS {}", quote(&name)));
            projections.push(projection);
            keys.push((name, descending));
        }
        let mut type_array = SqlFragment::text("jsonb_build_array(");
        type_array
            .append(SqlFragment::join(types, ", "))
            .push(") AS \"_key_types\"");
        projections.push(type_array);
        projections.push(SqlFragment::text(format!(
            "{r}.\"_id\"::text AS \"_row_identity\""
        )));
        let mut answer = SqlFragment::text("SELECT ");
        answer
            .push(out_cols.join(", "))
            .push(", ")
            .append(SqlFragment::join(projections, ", "))
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
            keys,
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
        group_columns: &[String],
    ) -> Result<SqlFragment> {
        let Some(n) = limit else {
            return Ok(rel);
        };
        let order = self.order(cols, sort.as_deref(), grouped, ranked, group_columns)?;
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
        let terms = self.order_terms(cols, sort, grouped, ranked, group_columns)?;
        Ok(SqlFragment::join(
            terms
                .into_iter()
                .map(|(mut f, descending)| {
                    f.push(if descending {
                        " DESC NULLS LAST"
                    } else {
                        " ASC NULLS LAST"
                    });
                    f
                })
                .collect(),
            ", ",
        ))
    }
    fn order_terms(
        &self,
        cols: &Cols,
        sort: Option<&[sinex_model::query::SortKey]>,
        grouped: bool,
        ranked: bool,
        group_columns: &[String],
    ) -> Result<Vec<(SqlFragment, bool)>> {
        let Cols::Named { r, .. } = cols else {
            unreachable!("order requires a named relation")
        };
        let mut parts = Vec::new();
        if let Some(keys) = sort {
            for k in keys {
                let f = match &k.key {
                    SortBy::Rank if ranked && !grouped => {
                        SqlFragment::text(format!("{r}.\"_rank\""))
                    }
                    SortBy::Rank => {
                        return Err(invalid("sort rank needs a ranked query over rows"));
                    }
                    SortBy::Operand(o) => self.operand(cols, o)?.0,
                };
                parts.push((f, k.descending));
            }
        }
        if let Cols::Named {
            ordinal: Some(ordinal),
            ..
        } = cols
        {
            parts.push((SqlFragment::text(format!("{r}.{}", quote(ordinal))), false));
        } else if grouped {
            for c in group_columns {
                parts.push((SqlFragment::text(Self::named(cols, c)), false));
            }
        } else {
            if sort.is_none() && ranked {
                parts.push((SqlFragment::text(format!("{r}.\"_rank\"")), true));
            }
            parts.push((SqlFragment::text(format!("{r}.\"_t\"")), false));
        }
        parts.push((
            SqlFragment::text(format!("{r}.\"_id\"::text COLLATE \"C\"")),
            false,
        ));
        Ok(parts)
    }
}
