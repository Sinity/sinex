//! Operation-local SQL lowering of the shared logical-day boundaries.
//! The private temporary relation belongs to one trusted connection. It is
//! initialized before its outer read-only snapshot; preparation writes only
//! temporary rows and reads all operands through that same input snapshot.
use super::*;
use crate::query::sql;
use jiff::civil::Date;
use query_language::time::DaySetting;
use std::{collections::BTreeMap, future::Future, pin::Pin};

const BATCH: usize = 2048;

pub async fn initialize(conn: &mut sqlx::PgConnection) -> Result<()> {
    sqlx::query("CREATE TEMP TABLE IF NOT EXISTS sinex_query_days (setting text NOT NULL, ordinal bigint NOT NULL, day text NOT NULL, week text, weekday text NOT NULL, month text NOT NULL, month_day text NOT NULL, from_ns numeric NOT NULL, until_ns numeric NOT NULL, PRIMARY KEY(setting,ordinal)) ON COMMIT PRESERVE ROWS").execute(&mut *conn).await?;
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS sinex_query_days_start ON sinex_query_days(setting,from_ns)",
    )
    .execute(&mut *conn)
    .await?;
    sqlx::query("TRUNCATE pg_temp.sinex_query_days")
        .execute(conn)
        .await?;
    Ok(())
}

#[derive(Clone, Debug, Default)]
pub(crate) struct Stats {
    pub input_rows: u64,
    pub max_input_batch: usize,
    pub max_boundary_batch: usize,
}

pub(crate) fn setting(ctx: &Context) -> String {
    format!("{} {}", ctx.zone_name(), ctx.day_start)
}

fn calendar_operand(operand: &Operand) -> Option<(TimeFn, FieldRef)> {
    match operand {
        Operand::TimeFn(func, field) if *func != TimeFn::Hour => Some((*func, field.clone())),
        _ => None,
    }
}
fn expression_operands(e: &Expr, out: &mut Vec<(TimeFn, FieldRef)>) {
    match e {
        Expr::And(a, b) | Expr::Or(a, b) => {
            expression_operands(a, out);
            expression_operands(b, out);
        }
        Expr::Not(e) => expression_operands(e, out),
        Expr::Compare { lhs, .. } | Expr::Exists(lhs) | Expr::Within { lhs, .. } => {
            if let Some(op) = calendar_operand(lhs) {
                out.push(op);
            }
        }
        Expr::Truth { operand, .. } => {
            if let Some(op) = calendar_operand(operand) {
                out.push(op);
            }
        }
        _ => {}
    }
}
fn ordinary(e: &Expr) -> Option<Expr> {
    let mut needs = Vec::new();
    expression_operands(e, &mut needs);
    if needs.is_empty() {
        return Some(e.clone());
    }
    if let Expr::And(a, b) = e {
        match (ordinary(a), ordinary(b)) {
            (Some(a), Some(b)) => Some(Expr::And(Box::new(a), Box::new(b))),
            (a, b) => a.or(b),
        }
    } else {
        None
    }
}
fn stage_operands(stage: &Stage) -> Vec<(TimeFn, FieldRef)> {
    let mut out = Vec::new();
    match stage {
        Stage::Where(e) => expression_operands(e, &mut out),
        Stage::Group(g) => {
            for key in &g.keys {
                if let Some(op) = calendar_operand(key) {
                    out.push(op);
                }
            }
        }
        Stage::Sort(keys) => {
            for key in keys {
                if let SortBy::Operand(op) = &key.key
                    && let Some(op) = calendar_operand(op)
                {
                    out.push(op);
                }
            }
        }
        _ => {}
    }
    let mut unique = Vec::new();
    for op in out {
        if !unique.contains(&op) {
            unique.push(op);
        }
    }
    unique
}
fn needs_calendar(q: &Query) -> bool {
    let mut out = Vec::new();
    if let Some(e) = &q.filter {
        expression_operands(e, &mut out);
    }
    !out.is_empty()
        || q.stages.iter().any(|stage| {
            !stage_operands(stage).is_empty()
                || matches!(stage,Stage::Join(join) if needs_calendar(&join.inner))
        })
}

pub(crate) async fn compile(
    conn: &mut sqlx::PgConnection,
    ctx: &Context,
    seq: i64,
    q: &Query,
) -> Result<Compiled> {
    let (compiled, _) = compile_with_stats(conn, ctx, seq, q).await?;
    Ok(compiled)
}
pub(crate) async fn compile_with_stats(
    conn: &mut sqlx::PgConnection,
    ctx: &Context,
    seq: i64,
    q: &Query,
) -> Result<(Compiled, Stats)> {
    if !needs_calendar(q) {
        return Ok((Compiler::new(ctx, seq).compile(q)?, Stats::default()));
    }
    let exists: bool =
        sqlx::query_scalar("SELECT to_regclass('pg_temp.sinex_query_days') IS NOT NULL")
            .fetch_one(&mut *conn)
            .await?;
    if !exists {
        return Err(invalid(
            "initialize the trusted query calendar before its read-only snapshot",
        ));
    }
    let mut prepare = Preparation {
        ctx,
        seq,
        day: ctx.clock()?.day,
        setting: setting(ctx),
        cursor: 0,
        stats: Stats::default(),
    };
    prepare.query(conn, q).await?;
    Ok((Compiler::with_calendar(ctx, seq).compile(q)?, prepare.stats))
}

#[derive(serde::Serialize)]
struct Boundary {
    ordinal: i64,
    day: String,
    week: Option<String>,
    weekday: &'static str,
    month: String,
    month_day: String,
    from_ns: String,
    until_ns: String,
}
struct Preparation<'a> {
    ctx: &'a Context,
    seq: i64,
    day: DaySetting,
    setting: String,
    cursor: usize,
    stats: Stats,
}
impl Preparation<'_> {
    fn row(&self, date: Date, func: TimeFn) -> Result<Boundary> {
        let bounds = self.day.day(date).map_err(|e| invalid(e.to_string()))?;
        let monday = date
            .checked_sub(jiff::Span::new().days(i64::from(date.weekday().to_monday_zero_offset())))
            .ok();
        if func == TimeFn::Week && monday.is_none() {
            return Err(invalid("week is outside the supported calendar"));
        }
        let ordinal = query_language::time::days_between(jiff::civil::date(1970, 1, 1), date)
            .map_err(|e| invalid(e.to_string()))?;
        const DAYS: [&str; 7] = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"];
        Ok(Boundary {
            ordinal,
            day: date.to_string(),
            week: monday.map(|d| d.to_string()),
            weekday: DAYS[date.weekday().to_monday_zero_offset() as usize],
            month: format!("{:04}-{:02}", date.year(), date.month()),
            month_day: format!("{:02}-{:02}", date.month(), date.day()),
            from_ns: bounds.start.as_nanosecond().to_string(),
            until_ns: bounds.end.as_nanosecond().to_string(),
        })
    }
    async fn insert(&mut self, conn: &mut sqlx::PgConnection, rows: Vec<Boundary>) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        self.stats.max_boundary_batch = self.stats.max_boundary_batch.max(rows.len());
        let rows = serde_json::to_string(&rows).map_err(|e| invalid(e.to_string()))?;
        sqlx::query("INSERT INTO pg_temp.sinex_query_days(setting,ordinal,day,week,weekday,month,month_day,from_ns,until_ns) SELECT $1,r.* FROM jsonb_to_recordset($2::jsonb) AS r(ordinal bigint,day text,week text,weekday text,month text,month_day text,from_ns numeric,until_ns numeric) ON CONFLICT(setting,ordinal) DO NOTHING").bind(&self.setting).bind(rows).execute(conn).await?;
        Ok(())
    }
    async fn interval(
        &mut self,
        conn: &mut sqlx::PgConnection,
        from: jiff::Timestamp,
        to: jiff::Timestamp,
        func: TimeFn,
    ) -> Result<()> {
        if from >= to {
            return Ok(());
        }
        let mut date = self
            .day
            .logical_day(from)
            .map_err(|e| invalid(e.to_string()))?;
        let end = self
            .day
            .logical_day(
                to.checked_sub(jiff::SignedDuration::from_nanos(1))
                    .map_err(|e| invalid(e.to_string()))?,
            )
            .map_err(|e| invalid(e.to_string()))?;
        let mut rows = Vec::new();
        loop {
            rows.push(self.row(date, func)?);
            if rows.len() == BATCH {
                self.insert(conn, std::mem::take(&mut rows)).await?;
            }
            if date == end {
                break;
            }
            date = date.tomorrow().map_err(|e| invalid(e.to_string()))?;
        }
        self.insert(conn, rows).await
    }
    async fn field(
        &mut self,
        conn: &mut sqlx::PgConnection,
        prefix: &Query,
        field: &FieldRef,
        func: TimeFn,
    ) -> Result<()> {
        if let Some((from, to)) = prefix.finite_time_bounds(field) {
            // SQL timestamp parameters have a microsecond grid. Include the
            // adjacent grid points in this candidate superset; SQL retains its
            // actual predicates and boundary membership uses exact nanoseconds.
            let from = from
                .checked_sub(jiff::SignedDuration::from_micros(1))
                .unwrap_or(from);
            let to = to
                .checked_add(jiff::SignedDuration::from_micros(1))
                .unwrap_or(to);
            return self.interval(conn, from, to, func).await;
        }
        let mut selected = prefix.clone();
        selected.stages.retain(|s| !matches!(s, Stage::Fields(_)));
        selected.stages.push(Stage::Fields(vec![field.clone()]));
        let (sql, params) = {
            let compiled = Compiler::with_calendar(self.ctx, self.seq).compile(&selected)?;
            let field = format!("q.{}", quote(&field.name()));
            let mut input = SqlFragment::text(format!(
                "SELECT DISTINCT (extract(epoch from {field})*1000000000)::numeric::text FROM ("
            ));
            input
                .append(compiled.answer)
                .push(format!(") q WHERE {field} IS NOT NULL"));
            self.cursor += 1;
            let mut statement = SqlFragment::text(format!(
                "DECLARE sinex_day_inputs_{} NO SCROLL CURSOR FOR ",
                self.cursor
            ));
            statement.append(input);
            statement.render()
        };
        sql::bind_scalar(sqlx::query_scalar::<_, String>(&sql), params)
            .fetch_optional(&mut *conn)
            .await?;
        let name = format!("sinex_day_inputs_{}", self.cursor);
        let result = async {
            loop {
                let values: Vec<String> =
                    sqlx::query_scalar(&format!("FETCH FORWARD {BATCH} FROM {name}"))
                        .fetch_all(&mut *conn)
                        .await?;
                if values.is_empty() {
                    break;
                }
                self.stats.input_rows += values.len() as u64;
                self.stats.max_input_batch = self.stats.max_input_batch.max(values.len());
                let mut dates = BTreeMap::new();
                for value in values {
                    let (integer, fraction) = value.split_once('.').unwrap_or((&value, ""));
                    if !fraction.bytes().all(|b| b == b'0') {
                        return Err(invalid("nonintegral timestamp nanoseconds"));
                    }
                    let ns = integer
                        .parse::<i128>()
                        .map_err(|e| invalid(e.to_string()))?;
                    let at =
                        jiff::Timestamp::from_nanosecond(ns).map_err(|e| invalid(e.to_string()))?;
                    let date = self
                        .day
                        .logical_day(at)
                        .map_err(|e| invalid(e.to_string()))?;
                    dates.entry(date).or_insert(self.row(date, func)?);
                }
                self.insert(conn, dates.into_values().collect()).await?;
            }
            Ok::<_, Error>(())
        }
        .await;
        let closed = sqlx::query(&format!("CLOSE {name}")).execute(conn).await;
        result?;
        closed?;
        Ok(())
    }
    fn query<'a>(
        &'a mut self,
        conn: &'a mut sqlx::PgConnection,
        q: &'a Query,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move {
            let mut prefix = Query {
                source: q.source.clone(),
                filter: q.filter.as_ref().and_then(ordinary),
                stages: vec![],
                ranked: q.ranked,
            };
            if let Some(e) = &q.filter {
                let mut fields = Vec::new();
                expression_operands(e, &mut fields);
                for (func, field) in fields {
                    self.field(conn, &prefix, &field, func).await?;
                }
            }
            prefix.filter = q.filter.clone();
            for stage in &q.stages {
                if let Stage::Join(join) = stage {
                    self.query(conn, &join.inner).await?;
                }
                let mut input = prefix.clone();
                if let Stage::Where(e) = stage
                    && let Some(e) = ordinary(e)
                {
                    input.stages.push(Stage::Where(e));
                }
                for (func, field) in stage_operands(stage) {
                    self.field(conn, &input, &field, func).await?;
                }
                if let Stage::Group(group) = stage
                    && let Some(fill) = &group.fill
                {
                    let func = group
                        .keys
                        .iter()
                        .find_map(|key| {
                            if let Operand::TimeFn(func, _) = key {
                                Some(*func)
                            } else {
                                None
                            }
                        })
                        .ok_or_else(|| invalid("fill needs a time key"))?;
                    if func != TimeFn::Hour {
                        self.interval(conn, fill.from, fill.to, func).await?;
                    }
                }
                prefix.stages.push(stage.clone());
            }
            Ok(())
        })
    }
}
