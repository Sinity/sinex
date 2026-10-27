//! Times in filters: `since`, `until`, `in <range>`, time functions and
//! differences between two time fields.
use super::*;

impl Parser<'_> {
    pub(super) fn since(&mut self, scope: &Scope) -> Result<Expr, QueryError> {
        let at = self.pos();
        let lhs = Operand::Field(self.main_time(scope, at)?);
        let t = self.value_token()?;
        let from = if let Some(secs) = time::duration(&t.text) {
            self.ctx
                .now
                .checked_sub(SignedDuration::from_secs_f64(secs))
                .map_err(|_| {
                    QueryError::at(t.at, "duration exceeds the supported timestamp range")
                })?
        } else {
            self.instant_from(&t)?
        };
        Ok(Expr::Compare {
            lhs,
            op: Op::Ge,
            rhs: Value::Time(from),
        })
    }

    pub(super) fn until(&mut self, scope: &Scope) -> Result<Expr, QueryError> {
        let at = self.pos();
        let lhs = Operand::Field(self.main_time(scope, at)?);
        let t = self.value_token()?;
        // A date includes its whole logical day.
        let to = if let Some(d) = time::date(&t.text) {
            if self.peek().is_some_and(|n| time::clock(&n.text).is_some()) {
                self.at -= 1;
                self.instant_from(&self.toks[self.at].clone())?
            } else {
                self.ctx
                    .day(d)
                    .ok_or_else(|| QueryError::at(t.at, "date out of range"))?
                    .1
            }
        } else {
            self.instant_from(&t)?
        };
        Ok(Expr::Compare {
            lhs,
            op: Op::Lt,
            rhs: Value::Time(to),
        })
    }

    pub(super) fn time_fn_value(
        &mut self,
        func: TimeFn,
        raw: &str,
        at: usize,
    ) -> Result<Value, QueryError> {
        let today = self.ctx.today();
        let date_word = |w: &str| -> Option<jiff::civil::Date> {
            match w {
                "today" => Some(today),
                "yesterday" => today.yesterday().ok(),
                _ => time::date(w),
            }
        };
        let bad = |what: &str| QueryError::at(at, format!("{}() takes {what}", func.name()));
        Ok(match func {
            TimeFn::Day => Value::Text(date_word(raw).ok_or_else(|| bad("a date"))?.to_string()),
            TimeFn::Week => {
                let d = date_word(raw).ok_or_else(|| bad("a date"))?;
                let monday = d
                    .checked_sub(jiff::Span::new().days(d.weekday().to_monday_zero_offset() as i64))
                    .map_err(|_| bad("a date in range"))?;
                Value::Text(monday.to_string())
            }
            TimeFn::Hour => {
                let h: i64 = raw.parse().map_err(|_| bad("an hour, 0 to 23"))?;
                if !(0..=23).contains(&h) {
                    return Err(bad("an hour, 0 to 23"));
                }
                Value::Integer(h)
            }
            TimeFn::Weekday => {
                const DAYS: [&str; 7] = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"];
                let short: String = raw.to_ascii_lowercase().chars().take(3).collect();
                if !DAYS.contains(&short.as_str()) {
                    return Err(bad("a weekday: mon, tue, wed, thu, fri, sat, sun"));
                }
                Value::Text(short)
            }
            TimeFn::Month => {
                if raw == "today" || raw == "now" {
                    Value::Text(format!("{:04}-{:02}", today.year(), today.month()))
                } else {
                    let (y, m) = time::month(raw).ok_or_else(|| bad("a month: 2026-08"))?;
                    Value::Text(format!("{y:04}-{m:02}"))
                }
            }
            TimeFn::MonthDay => {
                if let Some(d) = date_word(raw) {
                    Value::Text(format!("{:02}-{:02}", d.month(), d.day()))
                } else {
                    let ok = raw.len() == 5
                        && raw.as_bytes()[2] == b'-'
                        && raw[..2].parse::<u8>().is_ok_and(|m| (1..=12).contains(&m))
                        && raw[3..].parse::<u8>().is_ok_and(|d| (1..=31).contains(&d));
                    if !ok {
                        return Err(bad("a calendar day: 09-14, or a date"));
                    }
                    Value::Text(raw.to_string())
                }
            }
        })
    }

    /// An instant from a value token, consuming a following clock token when
    /// the value is a date (`2026-03-02 14:05`).
    pub(super) fn instant_from(&mut self, t: &Token) -> Result<Timestamp, QueryError> {
        let raw = t.text.as_str();
        let ctx = self.ctx;
        let today = ctx.today();
        let day_start = |d: jiff::civil::Date| ctx.day(d).map(|r| r.0);
        let resolved = match raw {
            "now" => Some(ctx.now),
            "today" => day_start(today),
            "yesterday" => today.yesterday().ok().and_then(day_start),
            _ => {
                if let Some(d) = time::date(raw) {
                    if let Some(c) = self.peek().and_then(|n| time::clock(&n.text)) {
                        self.at += 1;
                        ctx.local(d.to_datetime(c))
                    } else {
                        day_start(d)
                    }
                } else if let Some(c) = time::clock(raw) {
                    ctx.local(ctx.zoned(ctx.now).date().to_datetime(c))
                } else {
                    time::instant(ctx, raw)
                }
            }
        };
        resolved.ok_or_else(|| {
            QueryError::at(
                t.at,
                format!("`{raw}` is not a time; use a date (2026-03-02), a date and clock time, RFC 3339, now, today or yesterday"),
            )
        })
    }

    /// A range after `in`: `last 7d`, `today`, `yesterday`, `this week`,
    /// `last week`, `this month`, `last month`, a date, a month or a year.
    pub(super) fn range(&mut self) -> Result<(Timestamp, Timestamp), QueryError> {
        let t = self.value_token()?;
        let ctx = self.ctx;
        let today = ctx.today();
        let out_of_range = || QueryError::at(t.at, "range out of range");
        let word = t.text.to_ascii_lowercase();
        let r = match word.as_str() {
            "today" => ctx.day(today),
            "yesterday" => today.yesterday().ok().and_then(|d| ctx.day(d)),
            "this" | "last" => {
                let next = self.value_token()?;
                let what = next.text.to_ascii_lowercase();
                match (word.as_str(), what.as_str()) {
                    ("this", "week") => ctx.week_of(today),
                    ("last", "week") => today
                        .checked_sub(jiff::Span::new().days(7))
                        .ok()
                        .and_then(|d| ctx.week_of(d)),
                    ("this", "month") => ctx.month_of(today.year(), today.month()),
                    ("last", "month") => {
                        let first = today.first_of_month();
                        let prev = first.yesterday().map_err(|_| out_of_range())?;
                        ctx.month_of(prev.year(), prev.month())
                    }
                    ("last", _) => {
                        if let Some(secs) = time::duration(&what) {
                            Some((
                                ctx.now
                                    .checked_sub(SignedDuration::from_secs_f64(secs))
                                    .map_err(|_| out_of_range())?,
                                ctx.now,
                            ))
                        } else if let Ok(n) = what.parse::<i64>() {
                            if n <= 0 {
                                return Err(QueryError::at(
                                    next.at,
                                    "last N days needs a positive day count",
                                ));
                            }
                            self.expect_word("days")?;
                            let from = today
                                .checked_sub(jiff::Span::new().days(n - 1))
                                .map_err(|_| out_of_range())?;
                            ctx.days(from, today)
                        } else {
                            return Err(QueryError::at(
                                next.at,
                                "after `last`: a duration (7d), N days, week or month",
                            ));
                        }
                    }
                    _ => return Err(QueryError::at(next.at, "after `this`: week or month")),
                }
            }
            _ => {
                if let Some(d) = time::date(&word) {
                    ctx.day(d)
                } else if let Some((y, m)) = time::month(&word) {
                    ctx.month_of(y, m)
                } else if let Some(y) = time::year(&word) {
                    ctx.year_of(y)
                } else {
                    return Err(QueryError::at(
                        t.at,
                        "a range: last 7d, today, yesterday, this week, last week, this month, last month, a date, a month (2026-08) or a year",
                    ));
                }
            }
        };
        r.ok_or_else(out_of_range)
    }

    pub(super) fn time_diff(
        &mut self,
        a: FieldRef,
        scope: &Scope,
        at: usize,
    ) -> Result<Expr, QueryError> {
        self.time_field(&a, scope, at)?;
        self.at += 1; // `-`
        let b = self.field_ref(scope)?;
        self.time_field(&b, scope, at)?;
        let op_token = self
            .next()
            .ok_or_else(|| QueryError::at(self.end, "expected an operator"))?;
        let op = match op_token.text.as_str() {
            "<" => Op::Lt,
            "<=" => Op::Le,
            ">" => Op::Gt,
            ">=" => Op::Ge,
            _ => {
                return Err(QueryError::at(
                    op_token.at,
                    "a time difference compares with < <= > >=",
                ));
            }
        };
        let d = self.value_token()?;
        let seconds = time::duration(&d.text).ok_or_else(|| {
            QueryError::at(d.at, "a time difference compares with a duration: 1d")
        })?;
        Ok(Expr::TimeDiff { a, b, op, seconds })
    }
}
