//! Fill the requested calendar buckets in SQL, before later filters and paging.
use super::*;
use sinex_model::query::{Aggregate, Fill, Group};

impl Compiler<'_> {
    pub(super) fn fill(
        &mut self,
        grouped: SqlFragment,
        group: &Group,
        fill: &Fill,
    ) -> Result<SqlFragment> {
        let (func, field) = group
            .keys
            .iter()
            .find_map(|k| {
                if let Operand::TimeFn(func, field) = k {
                    Some((*func, field))
                } else {
                    None
                }
            })
            .ok_or_else(|| invalid("fill requires a time key"))?;
        let key = func.name();
        let dimensions: Vec<String> = group
            .keys
            .iter()
            .filter(|k| !matches!(k, Operand::TimeFn(..)))
            .map(Operand::column)
            .collect();
        let interval = if func == TimeFn::Hour {
            "1 hour"
        } else {
            "1 day"
        };
        let shift = if func == TimeFn::Hour {
            "0 hours".into()
        } else {
            format!(
                "{} hours {} minutes",
                self.ctx.day_start.hour(),
                self.ctx.day_start.minute()
            )
        };
        let local = |t| -> Result<SqlFragment> {
            let mut f = SqlFragment::text("(");
            f.param(time_value(t)?)
                .push(" AT TIME ZONE ")
                .param(V::String(Some(self.ctx.zone_name())))
                .push(" - ")
                .param(V::String(Some(shift.clone())))
                .push("::interval)");
            Ok(f)
        };
        let mut ticks =
            SqlFragment::text("SELECT s.tick AS local_at FROM generate_series(date_trunc('");
        ticks
            .push(if func == TimeFn::Hour { "hour" } else { "day" })
            .push("', ")
            .append(local(fill.from)?)
            .push("), date_trunc('")
            .push(if func == TimeFn::Hour { "hour" } else { "day" })
            .push("', ")
            .append(local(
                fill.to
                    .checked_sub(jiff::SignedDuration::from_nanos(1))
                    .map_err(|e| invalid(e.to_string()))?,
            )?)
            .push(format!("), interval '{interval}') AS s(tick)"));
        // Logical day ticks are shifted back to their wall-clock start.
        let wall_time = |next: bool| {
            let mut at = SqlFragment::text("((local_at + ");
            at.param(V::String(Some(shift.clone()))).push("::interval");
            if next {
                at.push(format!(" + interval '{interval}'"));
            }
            at.push(") AT TIME ZONE ")
                .param(V::String(Some(self.ctx.zone_name())))
                .push(")");
            at
        };
        let bucket = self.time_fn(func, wall_time(false));
        let mut out = SqlFragment::text(
            "WITH filled_groups AS (SELECT filled_input.*, true AS _recorded FROM (",
        );
        out.append(grouped)
            .push(") filled_input), filled_ticks AS (")
            .append(ticks)
            .push("), filled_buckets AS (SELECT ")
            .append(bucket)
            .push(format!(" AS {}, greatest(min(", quote(key)))
            .append(wall_time(false))
            .push("), ")
            .param(time_value(fill.from)?)
            .push(") AS _from, least(max(")
            .append(wall_time(true))
            .push("), ")
            .param(time_value(fill.to)?)
            .push(") AS _to FROM filled_ticks GROUP BY 1)");
        let all_keys: Vec<String> = group.keys.iter().map(Operand::column).collect();
        out.push(", filled_keys AS (SELECT b.*");
        if !dimensions.is_empty() {
            out.push(", d.* FROM filled_buckets b CROSS JOIN (SELECT DISTINCT ")
                .push(
                    dimensions
                        .iter()
                        .map(|k| quote(k))
                        .collect::<Vec<_>>()
                        .join(", "),
                )
                .push(" FROM filled_groups) d)");
        } else {
            out.push(" FROM filled_buckets b)");
        }
        let known_opportunity = self.fill_opportunity(fill, field)?;
        let mut select: Vec<SqlFragment> = all_keys
            .iter()
            .map(|k| SqlFragment::text(format!("coalesce(b.{0}, g.{0}) AS {0}", quote(k))))
            .collect();
        for aggregate in &group.aggregates {
            let mut value = SqlFragment::text(format!(
                "CASE WHEN g._recorded IS TRUE THEN g.{}",
                quote(&aggregate.column())
            ));
            if matches!(
                aggregate.function,
                AggFn::Count | AggFn::CountDistinct | AggFn::Sum
            ) {
                value
                    .push(" WHEN ")
                    .append(known_opportunity.clone())
                    .push(" THEN 0");
            }
            value.push(format!(" ELSE NULL END AS {}", quote(&aggregate.column())));
            select.push(value);
        }
        let mut coverage = SqlFragment::text("CASE WHEN g._recorded IS TRUE THEN 'recorded' WHEN ");
        coverage
            .append(known_opportunity)
            .push(" THEN 'observed' ELSE 'no_data' END AS coverage");
        select.push(coverage);
        let same_keys = all_keys
            .iter()
            .map(|k| format!("b.{0} IS NOT DISTINCT FROM g.{0}", quote(k)))
            .collect::<Vec<_>>()
            .join(" AND ");
        out.push(" SELECT ")
            .append(SqlFragment::join(select, ", "))
            .push(" FROM filled_keys b LEFT JOIN filled_groups g ON ")
            .push(same_keys.clone());
        let recorded: Vec<String> = all_keys
            .iter()
            .cloned()
            .chain(group.aggregates.iter().map(Aggregate::column))
            .map(|name| format!("g.{}", quote(&name)))
            .chain(std::iter::once("'recorded'".into()))
            .collect();
        out.push(" UNION ALL SELECT ")
            .push(recorded.join(", "))
            .push(" FROM filled_groups g WHERE NOT EXISTS (SELECT 1 FROM filled_keys b WHERE ")
            .push(same_keys)
            .push(")");
        Ok(out)
    }

    fn fill_opportunity(&self, fill: &Fill, field: &FieldRef) -> Result<SqlFragment> {
        // A run of a registered live stream establishes opportunity. A
        // historical import or a derived source does not establish it.
        if fill.sources.len() != 1 || field.name() != "when" {
            return Ok(SqlFragment::text("false"));
        }
        let mut out = SqlFragment::text(
            "EXISTS (SELECT 1 FROM core.visible_events e JOIN core.runs r ON r.id=e.run_id JOIN registry.sources s ON s.name=e.source WHERE e.source = ",
        );
        out.param(V::String(Some(fill.sources[0].clone())))
            .push(" AND s.driver='live-stream' AND e.commit_seq <= ").param(V::BigInt(Some(self.seq)))
            .push(" AND NOT audit.is_archived_at(e.id, ").param(V::BigInt(Some(self.seq))).push(") AND r.started_at <= ")
            .push("b._from").push(" AND coalesce(r.ended_at, (SELECT max(last.ts_orig) FROM core.visible_events last WHERE last.run_id=r.id AND last.commit_seq <= ")
            .param(V::BigInt(Some(self.seq))).push(")) >= b._to)");
        Ok(out)
    }
}
