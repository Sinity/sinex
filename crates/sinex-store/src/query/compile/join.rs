//! Time joins: an inner query's rows and the lateral subquery that picks the
//! nearest match for each outer row.
use super::*;

impl Compiler<'_> {
    /// The rows of an inner query of a time join, as `_id`, `_t` (the time
    /// the join measures), `_s` and `_e` (the span, when rows have one).
    pub(super) fn inner(&mut self, q: &Query, at: TimePoint) -> Result<SqlFragment> {
        let Source::Table { table, alias } = &q.source else {
            return Err(invalid(
                "the inner query of a time join selects rows of a table",
            ));
        };
        let t = self.alias("i");
        let a = self.alias("ia");
        let cols = Cols::Base {
            table: *table,
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
            (TimePoint::Start, Some((s, _, _))) => s.clone(),
            (TimePoint::End, Some((_, e, _))) => e.clone(),
            (_, None) => {
                return Err(invalid(
                    "`at start` and `at end` need inner rows with a span",
                ));
            }
        };
        let mut f = SqlFragment::text("SELECT ");
        f.push(Self::template(&cols, fields::tiebreak(*table)))
            .push(" AS _id, ")
            .append(point)
            .push(" AS _t, ");
        match span {
            Some((s, e, inclusive)) => {
                f.append(s)
                    .push(" AS _s, ")
                    .append(e)
                    .push(format!(" AS _e, {inclusive} AS _inclusive"));
            }
            None => {
                f.append(when)
                    .push(" AS _s, NULL::timestamptz AS _e, false AS _inclusive");
            }
        }
        f.push(" FROM ")
            .push(Self::template(&cols, fields::from(*table)));
        for l in laterals {
            f.push(" ").append(l);
        }
        if !conds.is_empty() {
            f.push(" WHERE ").append(SqlFragment::join(conds, " AND "));
        }
        Ok(f)
    }

    /// A time join as a lateral subquery picking the nearest match; rows
    /// without one drop out (a semijoin: several matches never multiply a
    /// row). Returns the join and the columns it adds.
    pub(super) fn join(
        &mut self,
        outer: &Cols,
        j: &Join,
        n: usize,
    ) -> Result<(SqlFragment, Vec<(String, String)>)> {
        let inner = self.inner(&j.inner, j.at)?;
        let jl = self.alias("j");
        let suffix = if n == 0 {
            String::new()
        } else {
            format!("_{}", n + 1)
        };
        let ot = self.main_time(outer)?;
        let mut f = SqlFragment::text("JOIN LATERAL (SELECT ");
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
                    .append(ot.clone())
                    .push(format!(" < {jl}._e OR ({jl}._inclusive AND "))
                    .append(ot)
                    .push(format!(" = {jl}._e)) ORDER BY {jl}._s, {jl}._id LIMIT 1"));
                columns.push((format!("during{suffix}"), format!("{jl}.m")));
            }
            JoinKind::Containing => {
                let (os, oe, inclusive) = self
                    .span(outer)
                    .ok_or_else(|| invalid("`containing` needs outer rows with a span: sessions, intervals, commands or runs"))?;
                f.push(format!("{jl}._id AS m FROM ("))
                    .append(inner)
                    .push(format!(") {jl} WHERE {jl}._t >= "))
                    .append(os)
                    .push(" AND (")
                    .append(oe.clone())
                    .push(format!(
                        " IS NULL OR {jl}._t {} ",
                        if inclusive { "<=" } else { "<" }
                    ))
                    .append(oe)
                    .push(format!(") ORDER BY {jl}._t, {jl}._id LIMIT 1"));
                columns.push((format!("containing{suffix}"), format!("{jl}.m")));
            }
        }
        f.push(format!(") {jl} ON true"));
        Ok((f, columns))
    }
}
