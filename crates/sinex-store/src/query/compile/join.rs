//! Pair relations and order predicates over the actual output of an inner query.
//! Joins retain every matching pair; a single positive order predicate also
//! exposes one deterministic nearest matching row without changing the outer grain.
use super::*;
use sinex_model::query::JoinOn;

pub(super) struct Joined {
    pub lateral: SqlFragment,
    pub columns: Vec<(String, String)>,
    pub hidden_columns: Vec<(String, String)>,
    pub identity: Option<String>,
    pub ordinal: Option<String>,
    pub parents: Option<String>,
}

impl Compiler<'_> {
    fn inner_compiler(&self, query: &Query) -> Result<Compiler<'_>> {
        for stage in &query.stages {
            if let Stage::AsOf(context) = stage {
                if !matches!(context,sinex_model::query::AsOf::Seq(position) if *position == self.seq)
                {
                    return Err(invalid(
                        "an inner query's as of context must resolve to the outer read position",
                    ));
                }
            }
        }
        let mut compiler = Compiler::new(self.ctx, self.seq);
        compiler.calendar = self.calendar.clone();
        Ok(compiler)
    }

    fn compile_inner(&self, join: &Join) -> Result<(Compiled, std::collections::BTreeSet<String>)> {
        let original = self.inner_compiler(&join.inner)?.compile(&join.inner)?;
        let display = original
            .columns
            .iter()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>();
        if join.required.iter().all(|name| {
            display.contains(name) || !join.columns.iter().any(|column| column.column() == *name)
        }) {
            return Ok((original, display));
        }
        let mut names = original.columns.clone();
        names.extend(
            join.required
                .iter()
                .filter(|name| !display.contains(*name))
                .cloned(),
        );
        let mut projected = vec![];
        for name in names {
            if let Some(column) = join.columns.iter().find(|column| column.column() == name) {
                projected.push(column.clone());
            } else if name == "snippet" {
                projected.push(Operand::Named {
                    operand: Box::new(Operand::Field(FieldRef::TypedGroup(
                        sinex_model::query::BoundColumn::generated("snippet", FieldType::Text),
                    ))),
                    name,
                });
            }
        }
        let mut query = (*join.inner).clone();
        query.stages.push(Stage::Fields(projected));
        Ok((self.inner_compiler(&query)?.compile(&query)?, display))
    }

    fn matched_point(at: TimePoint) -> bool {
        matches!(
            at,
            TimePoint::MatchWhen | TimePoint::MatchStart | TimePoint::MatchEnd
        )
    }

    fn join_point(&self, outer: &Cols, at: TimePoint) -> Result<SqlFragment> {
        let (start, end, _, _) = self.span_metadata(outer)?;
        Ok(match at {
            TimePoint::Start => start,
            TimePoint::End => end,
            _ => self.main_time(outer)?,
        })
    }

    fn inner_point(at: TimePoint, i: &str) -> String {
        match at {
            TimePoint::MatchWhen => format!("{i}.\"_t\""),
            TimePoint::MatchStart => format!("CASE WHEN {i}.\"_has_span\" THEN {i}.\"_s\" END"),
            TimePoint::MatchEnd => format!("CASE WHEN {i}.\"_has_span\" THEN {i}.\"_e\" END"),
            _ => format!("{i}.\"_s\""),
        }
    }

    fn row_identity(cols: &Cols) -> String {
        match cols {
            Cols::Base { table, .. } => Self::template(cols, fields::tiebreak(*table)),
            Cols::Named { r, .. } => format!("{r}.\"_id\""),
        }
    }

    fn join_condition(
        &self,
        outer: &Cols,
        on: &JoinOn,
        inner: &Compiled,
        i: &str,
    ) -> Result<SqlFragment> {
        let (relation, within, at, outer_point, inner_point) = match on {
            JoinOn::Key { left, right } => {
                let (left, left_kind) = self.field(outer, left)?;
                let right_kind = Kind::Typed(
                    Operand::Field(right.clone())
                        .field_type()
                        .map_err(|error| invalid(error.to_string()))?,
                );
                let right_column = inner.output_column(&right.name())?;
                return self.compare_operands(
                    left,
                    left_kind,
                    Op::Eq,
                    SqlFragment::text(format!("{i}.{right_column}")),
                    right_kind,
                );
            }
            JoinOn::Order {
                relation,
                within_secs,
                at,
            } => (
                *relation,
                within_secs.as_deref(),
                *at,
                self.join_point(outer, *at)?,
                Self::inner_point(*at, i),
            ),
        };
        let mut conditions = vec![];
        let mut condition = SqlFragment::text("(");
        match relation {
            JoinKind::Near => {
                condition
                    .push("CASE WHEN ")
                    .append(outer_point.clone())
                    .push(format!(
                        " IS NULL OR ({inner_point}) IS NULL THEN NULL::boolean ELSE true END"
                    ));
            }
            JoinKind::Before | JoinKind::After => {
                condition
                    .append(outer_point.clone())
                    .push(if relation == JoinKind::Before {
                        " < "
                    } else {
                        " > "
                    })
                    .push(&inner_point);
            }
            JoinKind::During => {
                if Self::matched_point(at) {
                    condition.push(format!(
                        "CASE WHEN NOT {i}.\"_has_span\" THEN NULL::boolean ELSE "
                    ));
                }
                condition
                    .append(outer_point.clone())
                    .push(format!(" >= {i}.\"_s\" AND ({i}.\"_e\" IS NULL OR "))
                    .append(outer_point.clone())
                    .push(format!(" < {i}.\"_e\" OR ({i}.\"_inclusive\" AND "))
                    .append(outer_point.clone())
                    .push(format!(" = {i}.\"_e\"))"));
                if Self::matched_point(at) {
                    condition.push(" END");
                }
            }
            JoinKind::Containing => {
                let (start, end, inclusive, has_span) = self.span_metadata(outer)?;
                if Self::matched_point(at) {
                    condition
                        .push("CASE WHEN ")
                        .append(self.main_time(outer)?)
                        .push(" IS NULL OR NOT ")
                        .append(has_span)
                        .push(" THEN NULL::boolean ELSE (")
                        .push(&inner_point)
                        .push(") >= ")
                        .append(start)
                        .push(" AND (")
                        .append(end.clone())
                        .push(" IS NULL OR (")
                        .push(&inner_point)
                        .push(") < ")
                        .append(end.clone())
                        .push(" OR (")
                        .append(inclusive)
                        .push(" AND (")
                        .push(&inner_point)
                        .push(") = ")
                        .append(end)
                        .push(")) END");
                } else {
                    condition
                        .push(&inner_point)
                        .push(" >= ")
                        .append(start)
                        .push(" AND (")
                        .append(end.clone())
                        .push(" IS NULL OR (CASE WHEN ")
                        .push(format!(
                            "{i}.\"_has_span\" AND {i}.\"_e\" IS NULL THEN false ELSE {i}.\"_e\" < "
                        ))
                        .append(end.clone())
                        .push(format!(" OR ({i}.\"_e\" = "))
                        .append(end)
                        .push(" AND (")
                        .append(inclusive)
                        .push(format!(" OR NOT {i}.\"_inclusive\")) END))"));
                }
            }
        }
        condition.push(")");
        conditions.push(condition);
        if let Some(seconds) = within {
            let mut window = SqlFragment::text("abs(extract(epoch from (");
            let distance_point = if relation == JoinKind::During && Self::matched_point(at) {
                format!("{i}.\"_s\"")
            } else {
                inner_point
            };
            window
                .push(distance_point)
                .push(" - ")
                .append(outer_point)
                .push("))) <= ")
                .param(seconds)
                .push("::numeric");
            conditions.push(window);
        }
        Ok(SqlFragment::join(conditions, " AND "))
    }

    /// A true candidate wins; absent that, any unknown comparison keeps the
    /// semijoin unknown. The inner query is evaluated once.
    pub(super) fn order_predicate(&self, outer: &Cols, join: &Join) -> Result<SqlFragment> {
        let (inner, _display) = self.compile_inner(join)?;
        let condition = self.join_condition(outer, &join.on, &inner, "order_inner")?;
        let known = match &join.on {
            JoinOn::Key { left, .. } => self.field(outer, left)?.0,
            JoinOn::Order { at, .. } => self.join_point(outer, *at)?,
        };
        let mut sql = SqlFragment::text(
            "(SELECT CASE WHEN bool_or(order_candidate.predicate) IS TRUE THEN true WHEN count(*) FILTER (WHERE order_candidate.predicate IS NULL) > 0 OR ",
        );
        sql.append(known)
            .push(" IS NULL THEN NULL::boolean ELSE false END FROM (SELECT ")
            .append(condition)
            .push(" AS predicate FROM (")
            .append(inner.answer)
            .push(") order_inner) order_candidate)");
        Ok(sql)
    }

    /// Event contributors of successful positive order predicates. Ordinary
    /// predicates add none; negation never invents a positive match.
    pub(super) fn predicate_parents(&self, outer: &Cols, expression: &Expr) -> Result<SqlFragment> {
        query_language::depth::deeper(|| {
            match expression {
                Expr::Order(join) => {
                    let (inner, _display) = self.compile_inner(join)?;
                    let condition = self.join_condition(outer, &join.on, &inner, "proof_inner")?;
                    let mut out = SqlFragment::text(
                        "ARRAY(SELECT DISTINCT unnest(proof_inner.\"_parents\") FROM (",
                    );
                    out.append(inner.answer)
                        .push(") proof_inner WHERE ")
                        .append(condition)
                        .push(")");
                    Ok(out)
                }
                Expr::And(left, right) | Expr::Or(left, right) => {
                    let left = self.predicate_parents(outer, left)?;
                    let right = self.predicate_parents(outer, right)?;
                    // Calling this helper for a scalar-only condition emits no
                    // second evaluation of that predicate.
                    let empty = "ARRAY[]::uuid[]";
                    if left.render().0 == empty && right.render().0 == empty {
                        return Ok(SqlFragment::text(empty));
                    }
                    let mut out = SqlFragment::text("(CASE WHEN ");
                    out.append(self.expr(outer, expression)?)
                        .push(" IS TRUE THEN ARRAY(SELECT DISTINCT unnest(")
                        .append(left)
                        .push(" || ")
                        .append(right)
                        .push(")) ELSE ARRAY[]::uuid[] END)");
                    Ok(out)
                }
                _ => Ok(SqlFragment::text("ARRAY[]::uuid[]")),
            }
        })
    }

    /// Exactly one positive top-level order exposes a matching row. This is the
    /// same AND-only traversal used by shared scope binding and evaluation.
    pub(super) fn matched_rows(&mut self, outer: &Cols, expression: &Expr) -> Result<Vec<Joined>> {
        match expression.exposed_order() {
            Some(join) => Ok(vec![self.join_lateral(outer, join, true)?]),
            None => Ok(vec![]),
        }
    }

    fn join_distance(&self, outer: &Cols, on: &JoinOn, i: &str) -> Result<Option<SqlFragment>> {
        let JoinOn::Order { relation, at, .. } = on else {
            return Ok(None);
        };
        let inner_point = if *relation == JoinKind::During && Self::matched_point(*at) {
            format!("{i}.\"_s\"")
        } else {
            Self::inner_point(*at, i)
        };
        let mut out = SqlFragment::text(format!("extract(epoch from (({inner_point}) - "));
        out.append(self.join_point(outer, *at)?).push("))");
        Ok(Some(out))
    }

    pub(super) fn join(&mut self, outer: &Cols, join: &Join, _n: usize) -> Result<Joined> {
        self.join_lateral(outer, join, false)
    }

    fn join_lateral(&mut self, outer: &Cols, join: &Join, nearest: bool) -> Result<Joined> {
        let (inner, display) = self.compile_inner(join)?;
        let i = self.alias("joined");
        let condition = self.join_condition(outer, &join.on, &inner, &i)?;
        let distance = self.join_distance(outer, &join.on, &i)?;
        let order: Vec<String> = inner
            .keys
            .iter()
            .map(|(key, descending)| {
                format!(
                    "inner_order.{} {} NULLS LAST",
                    quote(key),
                    if *descending { "DESC" } else { "ASC" }
                )
            })
            .collect();
        let mut relation = SqlFragment::text("SELECT inner_order.*, row_number() OVER (ORDER BY ");
        relation
            .push(order.join(", "))
            .push(") AS \"_ordinal\" FROM (")
            .append(inner.answer.clone())
            .push(") inner_order");
        if let Some(distance) = &distance {
            let mut candidates = SqlFragment::text(format!("SELECT {i}.*, "));
            candidates
                .append(distance.clone())
                .push(format!(" AS \"_distance\", false AS \"_coarse\" FROM ("))
                .append(relation)
                .push(format!(") {i}"));
            relation = candidates;
        }
        let mut lateral = SqlFragment::text(if nearest {
            "LEFT JOIN LATERAL (SELECT "
        } else {
            "JOIN LATERAL (SELECT "
        });
        lateral.push(format!("{i}.*"));
        lateral
            .push(" FROM (")
            .append(relation)
            .push(format!(") {i} WHERE "))
            .append(condition);
        if nearest {
            lateral.push(" ORDER BY ");
            if join.pick.is_empty() {
                if distance.is_some() {
                    lateral.push(format!("abs({i}.\"_distance\"), "));
                }
            } else {
                let mut renamed = inner
                    .columns
                    .iter()
                    .enumerate()
                    .map(|(index, name)| (name.clone(), format!("_value{index}")))
                    .collect::<std::collections::BTreeMap<_, _>>();
                for (logical, physical) in [
                    ("match.id", "_id"),
                    ("match.distance", "_distance"),
                    ("match.coarse", "_coarse"),
                ] {
                    renamed.insert(logical.into(), physical.into());
                }
                let candidate = Cols::Named {
                    r: i.clone(),
                    table: join.inner.source.table(),
                    grouped: false,
                    renamed,
                    ordinal: None,
                };
                for key in &join.pick {
                    let SortBy::Operand(operand) = &key.key else {
                        return Err(invalid(
                            "choosing the matched row needs explicit candidate expressions",
                        ));
                    };
                    lateral
                        .append(self.operand(&candidate, operand)?.0)
                        .push(if key.descending {
                            " DESC NULLS LAST, "
                        } else {
                            " ASC NULLS LAST, "
                        });
                }
            }
            lateral.push(format!("{i}.\"_id\" LIMIT 1"));
        }
        lateral.push(format!(") {i} ON true"));
        let mut columns = vec![];
        let mut hidden_columns = vec![];
        for (index, name) in inner.columns.iter().enumerate() {
            let column = (
                format!("match.{name}"),
                format!("{i}.{}", quote(&format!("_value{index}"))),
            );
            if display.contains(name) {
                columns.push(column);
            } else {
                hidden_columns.push(column);
            }
        }
        let expose_distance = match &join.on {
            JoinOn::Order { within_secs, .. } => !nearest || within_secs.is_some(),
            _ => false,
        };
        if expose_distance {
            columns.push(("match.distance".into(), format!("{i}.\"_distance\"")));
        }
        Ok(Joined {
            lateral,
            columns,
            hidden_columns,
            identity: (!nearest).then(|| {
                format!(
                    "jsonb_build_array({}, {i}.\"_id\")::text",
                    Self::row_identity(outer)
                )
            }),
            ordinal: (!nearest).then(|| format!("{i}.\"_ordinal\"")),
            parents: (!nearest).then(|| match outer {
                Cols::Named { r, .. } => {
                    format!("ARRAY(SELECT DISTINCT unnest({r}.\"_parents\" || {i}.\"_parents\"))")
                }
                Cols::Base { table, .. } if table.is_events() => format!(
                    "ARRAY(SELECT DISTINCT unnest(ARRAY[{}]::uuid[] || {i}.\"_parents\"))",
                    Self::row_identity(outer)
                ),
                _ => format!("{i}.\"_parents\""),
            }),
        })
    }
}
