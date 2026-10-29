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
        let func = group
            .keys
            .iter()
            .find_map(|k| {
                if let Operand::TimeFn(func, _) = k {
                    Some(*func)
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
        let (ticks, bucket) = if func == TimeFn::Hour {
            let local = |at| -> Result<SqlFragment> {
                let mut out = SqlFragment::text("(");
                out.param(time_value(at)?)
                    .push(" AT TIME ZONE ")
                    .param(V::String(Some(self.ctx.zone_name())))
                    .push(")");
                Ok(out)
            };
            let mut ticks = SqlFragment::text(
                "SELECT s.tick AS local_at FROM generate_series(date_trunc('hour',",
            );
            ticks
                .append(local(fill.from)?)
                .push("),date_trunc('hour',")
                .append(local(
                    fill.to
                        .checked_sub(jiff::SignedDuration::from_nanos(1))
                        .map_err(|e| invalid(e.to_string()))?,
                )?)
                .push("),interval '1 hour') s(tick)");
            (
                ticks,
                SqlFragment::text("extract(hour from local_at)::bigint"),
            )
        } else {
            let day = self.ctx.clock()?.day;
            let from = day
                .logical_day(fill.from)
                .map_err(|e| invalid(e.to_string()))?;
            let through = day
                .logical_day(
                    fill.to
                        .checked_sub(jiff::SignedDuration::from_nanos(1))
                        .map_err(|e| invalid(e.to_string()))?,
                )
                .map_err(|e| invalid(e.to_string()))?;
            let epoch = jiff::civil::date(1970, 1, 1);
            let first = query_language::time::days_between(epoch, from)
                .map_err(|e| invalid(e.to_string()))?;
            let last = query_language::time::days_between(epoch, through)
                .map_err(|e| invalid(e.to_string()))?;
            let setting = self
                .calendar
                .as_ref()
                .ok_or_else(|| invalid("fill requires shared day boundaries"))?;
            let column = match func {
                TimeFn::Day => "day",
                TimeFn::Week => "week",
                TimeFn::Weekday => "weekday",
                TimeFn::Month => "month",
                TimeFn::MonthDay => "month_day",
                TimeFn::Hour => unreachable!(),
            };
            let mut ticks = SqlFragment::text(format!(
                "SELECT {column} AS bucket FROM pg_temp.sinex_query_days WHERE setting="
            ));
            ticks
                .param(V::String(Some(setting.clone())))
                .push(" AND ordinal>=")
                .param(V::BigInt(Some(first)))
                .push(" AND ordinal<=")
                .param(V::BigInt(Some(last)));
            (ticks, SqlFragment::text("bucket"))
        };
        let mut out = SqlFragment::text(
            "WITH filled_groups AS (SELECT filled_input.*, true AS _recorded FROM (",
        );
        out.append(grouped)
            .push(") filled_input), filled_ticks AS (")
            .append(ticks)
            .push("), filled_buckets AS (SELECT ")
            .append(bucket)
            .push(format!(" AS {} FROM filled_ticks GROUP BY 1)", quote(key)));
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
        let mut select: Vec<SqlFragment> = all_keys
            .iter()
            .map(|k| SqlFragment::text(format!("coalesce(b.{0}, g.{0}) AS {0}", quote(k))))
            .collect();
        for aggregate in &group.aggregates {
            select.push(SqlFragment::text(format!(
                "g.{} AS {}",
                quote(&aggregate.column()),
                quote(&aggregate.column())
            )));
        }
        select.push(SqlFragment::text(
            "CASE WHEN g._recorded IS TRUE THEN 'recorded' ELSE 'no_data' END AS coverage",
        ));
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
}
