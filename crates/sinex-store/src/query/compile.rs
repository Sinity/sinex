//! Compiling a bound query to SQL. The boolean structure is kept exactly: two
//! conditions joined by `and` are both required, and a comparison with an
//! unknown value is unknown, never true. Every query reads events live at one
//! commit position, so the pages of one answer agree with each other and
//! `as of` is the same rule at an earlier position.
use super::fields;
use super::sql::{Param, SqlFragment};
use crate::{Error, Result};
use sinex_query::text::{like_escape, regex_escape};
use sinex_query::{
    AggFn, Alias, BuiltInField, Context, Expr, FieldRef, FieldType, Join, JoinKind, JoinOn, Op,
    Operand, OrderingOp, Query, SortBy, Source, Stage, Table, TimeFn, TimePoint, Value, Words,
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
    withheld_columns: Vec<String>,
}

// Only conjunctions make a word predicate necessary for every selected row.
fn required_words(filter: &Expr) -> Option<&Words> {
    let mut remaining = vec![filter];
    while let Some(filter) = remaining.pop() {
        match filter {
            Expr::Words(words) if !words.prefix => return Some(words),
            Expr::And(left, right) => remaining.extend([right.as_ref(), left.as_ref()]),
            _ => {}
        }
    }
    None
}

impl Compiled {
    pub(crate) fn order_by(&self) -> String {
        self.keys
            .iter()
            .map(|(name, desc)| {
                format!(
                    "q.\"{name}\" {} NULLS LAST",
                    if *desc { "DESC" } else { "ASC" }
                )
            })
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// SQL identifier of an output value in the private answer relation.
    pub(crate) fn output_column(&self, output: &str) -> Result<String> {
        self.columns
            .iter()
            .position(|name| name == output)
            .map(|index| quote(&format!("_value{index}")))
            .ok_or_else(|| invalid(format!("no output column `{output}`")))
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
                        let value = format!("q.{}", quote(&format!("_value{}", chunk_index * 50 + index)));
                        let value = if self.withheld_columns.contains(name) {
                            format!("CASE WHEN q.\"_withheld\" IS NOT NULL THEN q.\"_withheld\" ELSE to_jsonb({value}) END")
                        } else {
                            format!("to_jsonb({value})")
                        };
                        format!("'{}', {value}", name.replace('\'', "''"))
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
    indexed_words: bool,
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

struct NeededColumns {
    output: Vec<FieldRef>,
    withheld_columns: Vec<String>,
    needed: Vec<FieldRef>,
    snippet_requested: bool,
}

struct SearchColumns<'a> {
    indexed: Option<&'a Words>,
    ranked: bool,
    words: &'a [String],
    similar: Option<uuid::Uuid>,
    snippet_requested: bool,
}

struct CompileState {
    table: Table,
    ranked: bool,
    rel: SqlFragment,
    columns: Vec<String>,
    withheld_columns: Vec<String>,
    renamed: BTreeMap<String, String>,
    ordinal: Option<String>,
    group_order: Vec<String>,
    is_grouped: bool,
    sort: Option<Vec<sinex_query::SortKey>>,
    limit: Option<u64>,
    internal: Vec<String>,
    join_count: usize,
}

impl CompileState {
    fn cols(&self, r: &str) -> Cols {
        Cols::Named {
            r: r.into(),
            table: self.table,
            grouped: self.is_grouped,
            renamed: self.renamed.clone(),
            ordinal: self.ordinal.clone(),
        }
    }
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

fn time_value(t: jiff::Timestamp) -> Result<Param> {
    let odt = time::OffsetDateTime::from_unix_timestamp_nanos(t.as_nanosecond())
        .map_err(|e| invalid(format!("time out of range: {e}")))?;
    Ok(Param::from(odt))
}

fn value_param(v: &Value) -> Result<Param> {
    Ok(match v {
        Value::Text(s) => Param::from(s.clone()),
        Value::Integer(i) => Param::from(*i),
        Value::Number(n) => Param::from(*n),
        Value::Bool(b) => Param::from(*b),
        Value::Time(t) => time_value(*t)?,
        Value::Id(u) => Param::from(*u),
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

/// Every row field an expression names (not inside joins).
fn expr_fields(e: &Expr, out: &mut Vec<FieldRef>) {
    query_language::depth::deeper(|| match e {
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
    })
}

fn operand_fields(o: &Operand, out: &mut Vec<FieldRef>) {
    query_language::depth::deeper(|| match o {
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
        Operand::Text(value)
        | Operand::Absolute(value)
        | Operand::Length(value)
        | Operand::Named { operand: value, .. } => operand_fields(value, out),
        Operand::Literal {
            value: Value::Operand(value),
            ..
        } => operand_fields(value, out),
        Operand::Literal { .. } => {}
    })
}

fn exposes_matched_row(expression: &Expr) -> bool {
    query_language::depth::deeper(|| match expression {
        Expr::Order(_) => true,
        Expr::And(left, right) | Expr::Or(left, right) => {
            exposes_matched_row(left) || exposes_matched_row(right)
        }
        _ => false,
    })
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
            indexed_words: false,
        }
    }

    fn with_calendar(ctx: &'c Context, seq: i64) -> Self {
        Self {
            ctx,
            seq,
            next: 0,
            calendar: Some(calendar::setting(ctx)),
            indexed_words: false,
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

    fn named(cols: &Cols, name: &str) -> String {
        let Cols::Named { r, renamed, .. } = cols else {
            unreachable!("named columns require a relation")
        };
        format!(
            "{r}.{}",
            quote(renamed.get(name).map(String::as_str).unwrap_or(name))
        )
    }

    fn positioned(&self, cols: &Cols, sql: &str) -> SqlFragment {
        let sql = Self::template(cols, sql);
        let mut parts = sql.split("{seq}");
        let mut out = SqlFragment::text(parts.next().expect("one part"));
        for part in parts {
            out.param(self.seq).push(part);
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
        let (table, alias, similar) = self.check_source(q)?;
        let t = self.alias("t");
        let a = self.alias("a");
        let base = Cols::Base { table, alias, t, a };
        let (search, words) = self.word_search(q, table);
        let needed = self.needed_columns(q, table, alias);
        let search = SearchColumns {
            indexed: search,
            ranked: q.ranked && table.is_events(),
            words: &words,
            similar,
            snippet_requested: needed.snippet_requested,
        };
        let mut state = self.base_relation(&base, needed, &search)?;
        let source_filter = q
            .filter
            .as_ref()
            .map(|expression| Stage::Where(expression.clone()));
        for stage in source_filter.iter().chain(q.stages.iter()) {
            match stage {
                Stage::Where(e) => self.where_stage(&mut state, e)?,
                Stage::Sort(keys) => self.sort_stage(&mut state, keys)?,
                Stage::Limit(n) => self.limit_stage(&mut state, n)?,
                Stage::GroupFirst { keys, count } => {
                    self.group_first_stage(&mut state, keys, count)?
                }
                Stage::Group(g) => self.group_stage(&mut state, g)?,
                Stage::Join(join) => self.join_stage(&mut state, join)?,
                Stage::Fields(operands) => self.fields_stage(&mut state, operands)?,
                Stage::AsOf(_) => {
                    self.alias("r");
                }
            }
        }
        self.numbered_answer(state)
    }

    fn check_source(
        &self,
        q: &Query,
    ) -> Result<(Table, Option<&'static Alias>, Option<uuid::Uuid>)> {
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
        Ok((table, *alias, similar))
    }

    fn word_search<'a>(&mut self, q: &'a Query, table: Table) -> (Option<&'a Words>, Vec<String>) {
        let search = (table == Table::Events)
            .then(|| {
                q.filter.as_ref().and_then(required_words).or_else(|| {
                    q.stages
                        .iter()
                        .take_while(|stage| matches!(stage, Stage::Where(_)))
                        .find_map(|stage| match stage {
                            Stage::Where(filter) => required_words(filter),
                            _ => None,
                        })
                })
            })
            .flatten();
        self.indexed_words = search.is_some();

        // Words, for ranking and snippets.
        let mut words = Vec::new();
        if let Some(f) = &q.filter {
            words.extend(
                f.positive_words()
                    .into_iter()
                    .flat_map(|w| w.words.iter().cloned()),
            );
        }
        for s in &q.stages {
            if let Stage::Where(e) = s {
                words.extend(
                    e.positive_words()
                        .into_iter()
                        .flat_map(|w| w.words.iter().cloned()),
                );
            }
        }

        (search, words)
    }

    fn needed_columns(
        &self,
        q: &Query,
        table: Table,
        alias: Option<&'static Alias>,
    ) -> NeededColumns {
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
        let withheld_columns: Vec<String> = alias
            .into_iter()
            .flat_map(|a| a.fields.iter().map(|f| f.name.to_owned()))
            .collect();
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
        let snippet_requested = needed
            .iter()
            .any(|field| matches!(field, FieldRef::BuiltIn(BuiltInField::Snippet)));
        needed.retain(|f| {
            !matches!(
                f,
                FieldRef::Group(_)
                    | FieldRef::TypedGroup(_)
                    | FieldRef::BuiltIn(BuiltInField::Snippet)
            )
        });
        let mut seen = std::collections::HashSet::new();
        needed.retain(|f| seen.insert(column_name(f)));

        NeededColumns {
            output,
            withheld_columns,
            needed,
            snippet_requested,
        }
    }

    fn base_relation(
        &mut self,
        base: &Cols,
        needed: NeededColumns,
        search: &SearchColumns<'_>,
    ) -> Result<CompileState> {
        let NeededColumns {
            output,
            withheld_columns,
            needed,
            ..
        } = needed;
        let table = Self::table_of(base);
        let ranked = search.ranked;
        let conds = self.base_conditions(base);
        let (select, renamed) = self.base_select(base, table, &needed, search)?;
        let from = self.base_from(base, table, search.indexed, search.words);
        let mut rel = SqlFragment::text("SELECT ");
        rel.append(SqlFragment::join(select, ", "))
            .push(" FROM ")
            .append(from);
        if table == Table::Gaps {
            rel.push(Self::template(base, " {t}"));
        }
        if !conds.is_empty() {
            rel.push(" WHERE ")
                .append(SqlFragment::join(conds, " AND "));
        }

        let mut columns: Vec<String> = output.iter().map(column_name).collect();
        if ranked {
            columns.push("snippet".into());
        }
        let group_order = Vec::new();
        let is_grouped = false;
        let sort: Option<Vec<sinex_query::SortKey>> = None;
        let limit: Option<u64> = None;
        let mut internal: Vec<String> = vec![
            "_withheld".into(),
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

        if self.indexed_words {
            internal.push("_search_tsv".into());
        }

        Ok(CompileState {
            table,
            ranked,
            rel,
            columns,
            withheld_columns,
            renamed,
            ordinal: None,
            group_order,
            is_grouped,
            sort,
            limit,
            internal,
            join_count: 0,
        })
    }

    fn base_select(
        &mut self,
        base: &Cols,
        table: Table,
        needed: &[FieldRef],
        search: &SearchColumns<'_>,
    ) -> Result<(Vec<SqlFragment>, BTreeMap<String, String>)> {
        let mut renamed = BTreeMap::new();
        let mut select = Vec::new();
        for f in needed {
            let (sql, _) = self.field(base, f)?;
            let mut c = sql;
            let alias = self.alias("c");
            renamed.insert(column_name(f), alias.clone());
            c.push(format!(" AS {}", quote(&alias)));
            select.push(c);
        }
        self.base_metadata(base, table, &mut select)?;
        if search.ranked {
            let mut r = if let Some(reference) = search.similar {
                crate::vectors::rank_sql(&Self::template(base, "{t}.id"), reference, self.seq)
            } else {
                self.rank(base, search.words)
            };
            r.push(" AS \"_rank\"");
            select.push(r);
        }
        if search.ranked || search.snippet_requested {
            let mut snippet = if search.ranked {
                self.snippet(base, search.words)
            } else {
                SqlFragment::text("NULL::text")
            };
            let alias = self.alias("c");
            renamed.insert("snippet".into(), alias.clone());
            snippet.push(format!(" AS {}", quote(&alias)));
            select.push(snippet);
        }
        if self.indexed_words {
            select.push(SqlFragment::text(Self::template(
                base,
                "{t}._search_tsv AS \"_search_tsv\"",
            )));
        }
        Ok((select, renamed))
    }

    fn base_metadata(
        &self,
        base: &Cols,
        table: Table,
        select: &mut Vec<SqlFragment>,
    ) -> Result<()> {
        let withheld = if table.is_events() {
            Self::template(
                base,
                "CASE WHEN {t}.payload ? 'withheld' THEN {t}.payload END",
            )
        } else {
            "NULL::jsonb".into()
        };
        select.push(SqlFragment::text(format!("{withheld} AS \"_withheld\"")));
        let mut id = SqlFragment::text(Self::template(base, fields::tiebreak(table)));
        id.push(" AS \"_id\"");
        select.push(id);
        let event_id = if table.is_events() {
            Self::template(base, fields::tiebreak(table))
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
                Self::template(base, fields::tiebreak(table))
            )
        } else {
            "ARRAY[]::uuid[]".into()
        };
        let mut parents = SqlFragment::text(parents);
        parents.push(" AS \"_parents\"");
        select.push(parents);
        Ok(())
    }

    fn base_from(
        &mut self,
        base: &Cols,
        table: Table,
        search: Option<&Words>,
        words: &[String],
    ) -> SqlFragment {
        let words_for_rank = &words;
        let mut from = self.positioned(base, fields::from(table));
        if let Some(words) = search {
            let hits = self.alias("words");
            let event = self.alias("event");
            // This lateral boundary reads the event and authorizes its search
            // tuple once. OFFSET keeps rank references in later page wrappers
            // from expanding into repeated history lookups.
            from = SqlFragment::text(
                "(SELECT event_id, text, tsv FROM core.search_text WHERE tsv @@ ",
            );
            from.push(if words.phrase {
                    "phraseto_tsquery('simple', "
                } else {
                    "plainto_tsquery('simple', "
                })
                .param(words.words.join(" "))
                .push(format!(")) {hits} CROSS JOIN LATERAL (SELECT {event}.*, {hits}.tsv AS _search_tsv, ts_rank({hits}.tsv, plainto_tsquery('simple', "))
                .param(words_for_rank.join(" "))
                .push(format!(")) AS _search_rank, ts_headline('simple', {hits}.text, plainto_tsquery('simple', "))
                .param(words_for_rank.join(" "))
                .push(format!("), 'MaxFragments=2, MaxWords=24, MinWords=6, StartSel=[, StopSel=]') AS _search_snippet FROM core.visible_events {event} WHERE {event}.id={hits}.event_id AND core.class_visible({event}.class) OFFSET 0) "))
                .push(Self::template(base, "{t}"));
        }
        from
    }

    fn where_stage(&mut self, state: &mut CompileState, e: &Expr) -> Result<()> {
        let r = self.alias("r");
        let cols = state.cols(&r);

        let Some(e) = e.predicate() else {
            return Ok(());
        };
        state.rel = self.wrap(state, &r, &cols, false)?;
        let matched = self.matched_rows(&cols, &e)?;
        let condition_cols = cols.clone();
        if !matched.is_empty() {
            let order = self.order(
                &cols,
                state.sort.as_deref(),
                state.is_grouped,
                state.ranked,
                &state.group_order,
            )?;
            let next_ordinal = self.alias("o");
            let mut ordered =
                SqlFragment::text(format!("SELECT {r}.*, row_number() OVER (ORDER BY "));
            ordered
                .append(order)
                .push(format!(") AS {} FROM (", quote(&next_ordinal)))
                .append(std::mem::take(&mut state.rel))
                .push(format!(") {r}"));
            state.rel = ordered;
            state.ordinal = Some(next_ordinal);
            state.sort = None;
        }
        for row in matched {
            let mut select = vec![SqlFragment::text(format!("{r}.*"))];
            for (visible, (name, value)) in row
                .columns
                .into_iter()
                .map(|column| (true, column))
                .chain(row.hidden_columns.into_iter().map(|column| (false, column)))
            {
                let alias = self.alias("c");
                select.push(SqlFragment::text(format!("{value} AS {}", quote(&alias))));
                if visible && !state.columns.contains(&name) {
                    state.columns.push(name.clone());
                }
                state.renamed.insert(name, alias);
            }
            let mut with_matched = SqlFragment::text("SELECT ");
            with_matched
                .append(SqlFragment::join(select, ", "))
                .push(" FROM (")
                .append(std::mem::take(&mut state.rel))
                .push(format!(") {r} "))
                .append(row.lateral);
            state.rel = with_matched;
        }
        let cond = self.expr(&condition_cols, &e)?;
        let mut f = if exposes_matched_row(&e) {
            let match_parents = self.predicate_parents(&condition_cols, &e)?;
            let mut kept: Vec<_> = state
                .renamed
                .values()
                .cloned()
                .chain(
                    state
                        .internal
                        .iter()
                        .filter(|name| name.as_str() != "_parents")
                        .cloned(),
                )
                .chain(state.ordinal.iter().cloned())
                .collect();
            kept.sort();
            kept.dedup();
            let mut out = SqlFragment::text("SELECT ");
            out.push(
                kept.iter()
                    .map(|name| format!("{r}.{}", quote(name)))
                    .collect::<Vec<_>>()
                    .join(", "),
            )
            .push(format!(
                ", ARRAY(SELECT DISTINCT unnest({r}.\"_parents\" || "
            ))
            .append(match_parents)
            .push(")) AS \"_parents\" FROM (");
            out
        } else {
            SqlFragment::text("SELECT * FROM (")
        };
        f.append(std::mem::take(&mut state.rel))
            .push(format!(") {r} WHERE "))
            .append(cond);
        state.rel = f;
        Ok(())
    }

    fn sort_stage(
        &mut self,
        state: &mut CompileState,
        keys: &[sinex_query::SortKey],
    ) -> Result<()> {
        let r = self.alias("r");
        let cols = state.cols(&r);

        if state.limit.is_some() {
            state.rel = self.wrap(state, &r, &cols, false)?;
        }
        state.sort = Some(keys.to_vec());
        Ok(())
    }

    fn limit_stage(&mut self, state: &mut CompileState, n: &u64) -> Result<()> {
        let r = self.alias("r");
        let cols = state.cols(&r);

        if state.limit.is_some() {
            state.rel = self.wrap(state, &r, &cols, false)?;
        }
        state.limit = Some(*n);
        Ok(())
    }

    fn group_first_stage(
        &mut self,
        state: &mut CompileState,
        keys: &[Operand],
        count: &u64,
    ) -> Result<()> {
        let r = self.alias("r");
        let cols = state.cols(&r);

        state.rel = self.wrap(state, &r, &cols, false)?;
        let order = self.order(
            &cols,
            state.sort.as_deref(),
            state.is_grouped,
            state.ranked,
            &state.group_order,
        )?;
        let partition = keys
            .iter()
            .map(|key| self.operand(&cols, key).map(|(sql, _)| sql))
            .collect::<Result<Vec<_>>>()?;
        let rank = self.alias("group_rank");
        let next_ordinal = self.alias("o");
        let selected = self.alias("selected");
        let mut ranked_rows = SqlFragment::text(format!("SELECT {r}.*, row_number() OVER ("));
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
            .append(std::mem::take(&mut state.rel))
            .push(format!(") {r}"));
        let mut survivors = SqlFragment::text("SELECT * FROM (");
        survivors
            .append(ranked_rows)
            .push(format!(
                ") {selected} WHERE {selected}.{} <= ",
                quote(&rank)
            ))
            .param(i64::try_from(*count).unwrap_or(i64::MAX));
        state.rel = survivors;
        state.ordinal = Some(next_ordinal);
        state.sort = None;
        Ok(())
    }

    fn group_stage(&mut self, state: &mut CompileState, g: &sinex_query::Group) -> Result<()> {
        let r = self.alias("r");
        let cols = state.cols(&r);

        state.rel = self.wrap(state, &r, &cols, true)?;
        let gr = self.alias("g");
        let gcols = Cols::Named {
            r: gr.clone(),
            table: state.table,
            grouped: state.is_grouped,
            renamed: state.renamed.clone(),
            ordinal: state.ordinal.clone(),
        };
        let mut keys = Vec::new();
        let mut select = Vec::new();
        let mut names = Vec::new();
        let mut group_names = BTreeMap::new();
        for k in &g.keys {
            let (sql, _) = self.operand(&gcols, k)?;
            keys.push(sql.clone());
            let mut c = sql;
            let alias = self.alias("c");
            group_names.insert(k.column(), alias.clone());
            c.push(format!(" AS {}", quote(&alias)));
            select.push(c);
            names.push(k.column());
        }
        for agg in &g.aggregates {
            let mut c = self.aggregate(&gcols, agg)?;
            let alias = self.alias("c");
            group_names.insert(agg.column(), alias.clone());
            c.push(format!(" AS {}", quote(&alias)));
            select.push(c);
            names.push(agg.column());
        }
        select.push(SqlFragment::text(format!(
    "ARRAY(SELECT DISTINCT parent::uuid FROM jsonb_array_elements(jsonb_agg(to_jsonb({gr}.\"_parents\"))) a(parents) CROSS JOIN LATERAL jsonb_array_elements_text(a.parents) p(parent) ORDER BY parent::uuid) AS \"_parents\""
)));
        let mut f = SqlFragment::text("SELECT ");
        f.append(SqlFragment::join(select, ", "))
            .push(" FROM (")
            .append(std::mem::take(&mut state.rel))
            .push(format!(") {gr}"));
        // By position: a key's parameters (zone, day start) are
        // separate placeholders each time they are written.
        if !keys.is_empty() {
            let positions: Vec<String> = (1..=keys.len()).map(|i| i.to_string()).collect();
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
        state.rel = identified;
        state.withheld_columns.clear();
        state.columns = names;
        state.renamed = group_names;
        state.ordinal = None;
        state.group_order = state.columns.clone();
        state.internal = vec![
            "_id".into(),
            "_event_id".into(),
            "_t".into(),
            "_s".into(),
            "_e".into(),
            "_inclusive".into(),
            "_has_span".into(),
            "_parents".into(),
        ];
        state.is_grouped = true;
        Ok(())
    }

    fn join_stage(&mut self, state: &mut CompileState, join: &Join) -> Result<()> {
        let r = self.alias("r");
        let cols = state.cols(&r);

        // Materialize the outer order before a match can replace
        // names used by a preceding sort or fields stage.
        let order = self.order(
            &cols,
            state.sort.as_deref(),
            state.is_grouped,
            state.ranked,
            &state.group_order,
        )?;
        let next_ordinal = self.alias("o");
        let mut ordered = SqlFragment::text(format!("SELECT {r}.*, row_number() OVER (ORDER BY "));
        ordered
            .append(order.clone())
            .push(format!(") AS {} FROM (", quote(&next_ordinal)))
            .append(std::mem::take(&mut state.rel))
            .push(format!(") {r}"));
        if let Some(n) = state.limit.take() {
            ordered
                .push(" ORDER BY ")
                .append(order)
                .push(" LIMIT ")
                .param(n as i64);
        }
        state.ordinal = Some(next_ordinal.clone());
        state.sort = None;
        let joined = self.join(&cols, join, state.join_count)?;
        state.join_count += 1;
        let mut kept: Vec<String> = state.renamed.values().cloned().collect();
        kept.extend(
            state
                .internal
                .iter()
                .filter(|name| !matches!(name.as_str(), "_id" | "_parents"))
                .cloned(),
        );
        kept.push(next_ordinal.clone());
        kept.sort();
        kept.dedup();
        let mut select: Vec<_> = kept
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
            state.ordinal = Some(pair_ordinal);
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
            let alias = self.alias("c");
            select.push(SqlFragment::text(format!("{value} AS {}", quote(&alias))));
            if visible && !state.columns.contains(&name) {
                state.columns.push(name.clone());
            }
            state.renamed.insert(name, alias);
        }
        let mut combined = SqlFragment::text("SELECT ");
        combined
            .append(SqlFragment::join(select, ", "))
            .push(" FROM (")
            .append(ordered)
            .push(format!(") {r} "))
            .append(joined.lateral);
        state.rel = combined;
        Ok(())
    }

    fn fields_stage(&mut self, state: &mut CompileState, operands: &[Operand]) -> Result<()> {
        let r = self.alias("r");
        let cols = state.cols(&r);

        let order = self.order(
            &cols,
            state.sort.as_deref(),
            state.is_grouped,
            state.ranked,
            &state.group_order,
        )?;
        let mut selected_fields = SqlFragment::text(format!("SELECT {r}.*"));
        let next_ordinal = self.alias("o");
        selected_fields
            .push(", row_number() OVER (ORDER BY ")
            .append(order)
            .push(format!(") AS {}", quote(&next_ordinal)));
        let mut selected_names = BTreeMap::new();
        for operand in operands {
            let (value, _) = self.operand(&cols, operand)?;
            let alias = self.alias("c");
            selected_fields
                .push(", ")
                .append(value)
                .push(format!(" AS {}", quote(&alias)));
            selected_names.insert(operand.column(), alias);
        }
        selected_fields
            .push(" FROM (")
            .append(std::mem::take(&mut state.rel))
            .push(format!(") {r}"));
        if let Some(n) = state.limit.take() {
            let order = self.order(
                &cols,
                state.sort.as_deref(),
                state.is_grouped,
                state.ranked,
                &state.group_order,
            )?;
            selected_fields
                .push(" ORDER BY ")
                .append(order)
                .push(" LIMIT ")
                .param(n as i64);
        }
        state.rel = selected_fields;
        state.withheld_columns = operands
            .iter()
            .filter_map(|operand| {
                let mut fields = Vec::new();
                operand_fields(operand, &mut fields);
                fields
                    .iter()
                    .any(|field| state.withheld_columns.contains(&column_name(field)))
                    .then(|| operand.column())
            })
            .collect();
        state.columns = operands.iter().map(Operand::column).collect();
        state.renamed = selected_names;
        state.ordinal = Some(next_ordinal);
        state.sort = None;
        Ok(())
    }

    fn aggregate(&self, gcols: &Cols, agg: &sinex_query::Aggregate) -> Result<SqlFragment> {
        let c = match (&agg.function, &agg.argument) {
            (AggFn::Count, None) => SqlFragment::text("count(*)"),
            (func, Some(f)) => {
                let (arg, kind) = self.operand(gcols, f)?;
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
        Ok(c)
    }

    fn numbered_answer(&mut self, mut state: CompileState) -> Result<Compiled> {
        let r = self.alias("z");
        let cols = Cols::Named {
            r: r.clone(),
            table: state.table,
            grouped: state.is_grouped,
            renamed: state.renamed,
            ordinal: state.ordinal,
        };
        let order = self.order(
            &cols,
            state.sort.as_deref(),
            state.is_grouped,
            state.ranked,
            &state.group_order,
        )?;
        let mut out_cols: Vec<String> = state
            .columns
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
        out_cols.extend(state.internal.iter().map(|c| format!("{r}.{}", quote(c))));
        let mut count = SqlFragment::text("SELECT count(*) FROM (");
        match state.limit {
            Some(n) => count
                .push("SELECT 1 FROM (")
                .append(state.rel.clone())
                .push(format!(") {r} ORDER BY "))
                .append(order.clone())
                .push(" LIMIT ")
                .param(n as i64),
            None => count.append(state.rel.clone()),
        };
        count.push(") c");
        let terms = self.order_terms(
            &cols,
            state.sort.as_deref(),
            state.is_grouped,
            state.ranked,
            &state.group_order,
        )?;
        let mut keys = Vec::new();
        let mut key_columns = Vec::new();
        let mut types = Vec::new();
        for (index, (term, descending)) in terms.into_iter().enumerate() {
            let name = format!("_k{index}");
            let mut ty = SqlFragment::text("pg_typeof(");
            ty.append(term.clone()).push(")::text");
            types.push(ty);
            let mut key_column = term;
            key_column.push(format!(" AS {}", quote(&name)));
            key_columns.push(key_column);
            keys.push((name, descending));
        }
        let mut type_array = SqlFragment::text("jsonb_build_array(");
        type_array
            .append(SqlFragment::join(types, ", "))
            .push(") AS \"_key_types\"");
        key_columns.push(type_array);
        key_columns.push(SqlFragment::text(format!(
            "{r}.\"_id\"::text AS \"_row_identity\""
        )));
        let mut answer = SqlFragment::text("SELECT ");
        answer
            .push(out_cols.join(", "))
            .push(", ")
            .append(SqlFragment::join(key_columns, ", "))
            .push(" FROM (")
            .append(std::mem::take(&mut state.rel))
            .push(format!(") {r} ORDER BY "))
            .append(order);
        if let Some(n) = state.limit {
            answer.push(" LIMIT ").param(n as i64);
        }
        Ok(Compiled {
            answer,
            count,
            columns: state.columns,
            keys,
            withheld_columns: state.withheld_columns,
        })
    }

    /// Apply a pending sort and limit by wrapping the relation.
    fn wrap(
        &mut self,
        state: &mut CompileState,
        r: &str,
        cols: &Cols,
        take_sort: bool,
    ) -> Result<SqlFragment> {
        let rel = std::mem::take(&mut state.rel);
        let sort = if take_sort {
            state.sort.take()
        } else {
            state.sort.clone()
        };
        let Some(n) = state.limit.take() else {
            return Ok(rel);
        };
        let order = self.order(
            cols,
            sort.as_deref(),
            state.is_grouped,
            state.ranked,
            &state.group_order,
        )?;
        let mut f = SqlFragment::text("SELECT * FROM (");
        f.append(rel)
            .push(format!(") {r} ORDER BY "))
            .append(order)
            .push(" LIMIT ")
            .param(n as i64);
        Ok(f)
    }

    /// The order of rows: the explicit sort with unknowns last, then the
    /// default (relevance when ranked, else time), then the row's own key.
    fn order(
        &self,
        cols: &Cols,
        sort: Option<&[sinex_query::SortKey]>,
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
        sort: Option<&[sinex_query::SortKey]>,
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
