//! Pair relations and order predicates over the actual output of an inner query.
//! Joins retain every matching pair; a single positive order predicate also
//! exposes one deterministic nearest witness without changing the outer grain.
use super::*;
use sinex_model::query::JoinOn;

pub(super) struct Joined {
    pub lateral: SqlFragment,
    pub columns: Vec<(String, String)>,
    pub identity: Option<String>,
    pub ordinal: Option<String>,
    pub parents: Option<String>,
}

impl Compiler<'_> {
    fn inner_compiler(&self, query: &Query) -> Result<Compiler<'_>> {
        for stage in &query.stages {
            if let Stage::AsOf(context) = stage {
                if !matches!(context,sinex_model::query::AsOf::Seq(position) if *position == self.seq) {
                    return Err(invalid("an inner query's as of context must resolve to the outer read position"));
                }
            }
        }
        let mut compiler = Compiler::new(self.ctx, self.seq);
        compiler.calendar = self.calendar.clone();
        Ok(compiler)
    }

    fn join_point(&self, outer: &Cols, at: TimePoint) -> Result<SqlFragment> {
        let (start,end,_,_) = self.span_metadata(outer)?;
        Ok(if at == TimePoint::End { end } else { start })
    }

    fn row_identity(cols: &Cols) -> String {
        match cols {
            Cols::Base { table, .. } => Self::template(cols, fields::tiebreak(*table)),
            Cols::Named { r, .. } => format!("{r}.\"_id\""),
        }
    }

    fn join_condition(&self, outer: &Cols, on: &JoinOn, inner: &Compiled, i: &str) -> Result<SqlFragment> {
        let (relation, within, outer_point, inner_point) = match on {
            JoinOn::Key { left, right } => {
                let (left,left_kind) = self.field(outer,left)?;
                let right_kind = Kind::Typed(Operand::Field(right.clone()).field_type().map_err(|error|invalid(error.to_string()))?);
                let right_column = inner.output_column(&right.name())?;
                return self.compare_operands(left,left_kind,Op::Eq,SqlFragment::text(format!("{i}.{right_column}")),right_kind);
            }
            JoinOn::Order { relation, within_secs, at } =>
                (*relation, *within_secs, self.join_point(outer, *at)?, format!("{i}.\"_s\"")),

        };
        let mut conditions = vec![];
        let mut condition = SqlFragment::text("(");
        match relation {
            JoinKind::Near => {
                condition.append(outer_point.clone()).push(format!(" IS NOT NULL AND {inner_point} IS NOT NULL"));
            }
            JoinKind::Before | JoinKind::After => {
                condition.append(outer_point.clone())
                    .push(if relation == JoinKind::Before { " < " } else { " > " })
                    .push(&inner_point);
            }
            JoinKind::During => {
                condition.append(outer_point.clone()).push(format!(" >= {i}.\"_s\" AND ({i}.\"_e\" IS NULL OR "))
                    .append(outer_point.clone()).push(format!(" < {i}.\"_e\" OR ({i}.\"_inclusive\" AND "))
                    .append(outer_point.clone()).push(format!(" = {i}.\"_e\"))"));
            }
            JoinKind::Containing => {
                let (start,end,inclusive,_) = self.span_metadata(outer)?;
                let inner_end = format!("{i}.\"_e\"");
                condition.push(&inner_point).push(" >= ").append(start).push(" AND (")
                    .append(end.clone()).push(" IS NULL OR ").push(&inner_end).push(" <= ").append(end.clone())
                    .push(" OR (").append(inclusive).push(" AND ").push(inner_end).push(" = ").append(end).push("))");
            }
        }
        condition.push(")");
        conditions.push(condition);
        if let Some(seconds) = within {
            let mut window = SqlFragment::text("abs(extract(epoch from (");
            window.push(inner_point).push(" - ").append(outer_point).push("))) <= ").param(V::Double(Some(seconds)));
            conditions.push(window);
        }
        Ok(SqlFragment::join(conditions, " AND "))
    }

    /// A semijoin is an EXISTS over the same predicate as the pair relation.
    pub(super) fn order_predicate(&self, outer: &Cols, join: &Join) -> Result<SqlFragment> {
        let mut compiler = self.inner_compiler(&join.inner)?;
        let inner = compiler.compile(&join.inner)?;
        let condition = self.join_condition(outer, &join.on, &inner, "order_inner")?;
        let known = match &join.on {
            JoinOn::Key { left, .. } => self.field(outer,left)?.0,
            JoinOn::Order { at, .. } => self.join_point(outer,*at)?,
        };
        let mut sql = SqlFragment::text("(CASE WHEN ");
        sql.append(known).push(" IS NULL THEN NULL::boolean ELSE EXISTS (SELECT 1 FROM (");
        sql.append(inner.answer).push(") order_inner WHERE ").append(condition).push(") END)");
        Ok(sql)
    }

    /// Event contributors of successful positive order predicates. Ordinary
    /// predicates add none; negation never invents a positive witness.
    pub(super) fn predicate_parents(&self, outer: &Cols, expression: &Expr) -> Result<SqlFragment> {
        match expression {
            Expr::Order(join) => {
                let inner = self.inner_compiler(&join.inner)?.compile(&join.inner)?;
                let condition = self.join_condition(outer,&join.on,&inner,"proof_inner")?;
                let mut out = SqlFragment::text("ARRAY(SELECT DISTINCT unnest(proof_inner.\"_parents\") FROM (");
                out.append(inner.answer).push(") proof_inner WHERE ").append(condition).push(")");
                Ok(out)
            }
            Expr::And(left,right) | Expr::Or(left,right) => {
                let left = self.predicate_parents(outer,left)?;
                let right = self.predicate_parents(outer,right)?;
                // Calling this helper for a scalar-only condition emits no
                // second evaluation of that predicate.
                let empty = "ARRAY[]::uuid[]";
                if left.render().0 == empty && right.render().0 == empty { return Ok(SqlFragment::text(empty)); }
                let mut out = SqlFragment::text("(CASE WHEN ");
                out.append(self.expr(outer,expression)?).push(" IS TRUE THEN ARRAY(SELECT DISTINCT unnest(")
                    .append(left).push(" || ").append(right).push(")) ELSE ARRAY[]::uuid[] END)");
                Ok(out)
            }
            _ => Ok(SqlFragment::text("ARRAY[]::uuid[]")),
        }
    }

    /// Exactly one positive top-level order exposes a witness. This is the
    /// same AND-only traversal used by shared scope binding and evaluation.
    pub(super) fn order_witnesses(&mut self, outer: &Cols, expression: &Expr) -> Result<Vec<Joined>> {
        fn collect<'a>(expression: &'a Expr, out: &mut Vec<&'a Join>) {
            match expression {
                Expr::And(left,right) => {collect(left,out);collect(right,out);}
                Expr::Order(join) => out.push(join),
                _ => {}
            }
        }
        let mut orders=vec![];
        collect(expression,&mut orders);
        match orders.as_slice() {
            [join] => Ok(vec![self.join_lateral(outer,join,true)?]),
            _ => Ok(vec![]),
        }
    }

    fn join_distance(&self,outer:&Cols,on:&JoinOn,i:&str)->Result<Option<SqlFragment>> {
        let JoinOn::Order {at,..}=on else {return Ok(None)};
        let mut out=SqlFragment::text(format!("extract(epoch from ({i}.\"_s\" - "));
        out.append(self.join_point(outer,*at)?).push("))");
        Ok(Some(out))
    }

    pub(super) fn join(&mut self, outer: &Cols, join: &Join, _n: usize) -> Result<Joined> {
        self.join_lateral(outer,join,false)
    }

    fn join_lateral(&mut self, outer:&Cols, join:&Join, nearest:bool)->Result<Joined> {
        let inner = self.inner_compiler(&join.inner)?.compile(&join.inner)?;
        let i = self.alias("joined");
        let condition = self.join_condition(outer, &join.on, &inner, &i)?;
        let distance=self.join_distance(outer,&join.on,&i)?;
        let order: Vec<String> = inner.keys.iter().map(|(key, descending)| format!("inner_order.{} {} NULLS LAST", quote(key), if *descending {"DESC"} else {"ASC"})).collect();
        let mut relation = SqlFragment::text("SELECT inner_order.*, row_number() OVER (ORDER BY ");
        relation.push(order.join(", ")).push(") AS \"_ordinal\" FROM (").append(inner.answer.clone()).push(") inner_order");
        let mut lateral = SqlFragment::text(if nearest {"LEFT JOIN LATERAL (SELECT "} else {"JOIN LATERAL (SELECT "});
        lateral.push(format!("{i}.*"));
        if let Some(distance)=&distance { lateral.push(", ").append(distance.clone()).push(" AS \"_distance\""); }
        lateral.push(" FROM (").append(relation).push(format!(") {i} WHERE ")).append(condition);
        if nearest {
            lateral.push(" ORDER BY ");
            if let Some(distance)=&distance {lateral.push("abs(").append(distance.clone()).push("), ");}
            lateral.push(format!("{i}.\"_id\" LIMIT 1"));
        }
        lateral.push(format!(") {i} ON true"));
        let mut columns=inner.columns.iter().enumerate().map(|(index,name)|(format!("match.{name}"),format!("{i}.{}",quote(&format!("_value{index}"))))).collect::<Vec<_>>();
        let expose_distance = match &join.on {
            JoinOn::Order {within_secs,..}=> !nearest || within_secs.is_some(),
            _=>false,
        };
        if expose_distance {columns.push(("match.distance".into(),format!("{i}.\"_distance\"")));}
        Ok(Joined {
            lateral,
            columns,
            identity: (!nearest).then(|| format!("jsonb_build_array({}, {i}.\"_id\")::text",Self::row_identity(outer))),
            ordinal: (!nearest).then(|| format!("{i}.\"_ordinal\"")),
            parents: (!nearest).then(|| match outer {
                Cols::Named { r, .. } => format!("ARRAY(SELECT DISTINCT unnest({r}.\"_parents\" || {i}.\"_parents\"))"),
                Cols::Base { table, .. } if table.is_events() => format!("ARRAY(SELECT DISTINCT unnest(ARRAY[{}]::uuid[] || {i}.\"_parents\"))",Self::row_identity(outer)),
                _ => format!("{i}.\"_parents\""),
            }),
        })
    }
}
