//! Clauses after the filter: time joins, recall, grouping, aggregates,
//! sorting, limits and output fields.
use super::*;

impl Parser<'_> {
    pub(super) fn reference(&mut self) -> Result<Reference, QueryError> {
        let t = self.value_token()?;
        parse_reference(&t.text).map_err(|e| QueryError {
            at: Some(t.at),
            ..e
        })
    }

    pub(super) fn recall(&mut self) -> Result<Source, QueryError> {
        let ctx = self.ctx;
        let default_window = 30.0 * 60.0;
        let Some(t) = self.peek().cloned() else {
            let w = SignedDuration::from_secs_f64(default_window);
            return Ok(Source::Recall {
                from: ctx
                    .now
                    .checked_sub(w)
                    .map_err(|_| QueryError::new("recall start exceeds the timestamp range"))?,
                to: ctx
                    .now
                    .checked_add(w)
                    .map_err(|_| QueryError::new("recall end exceeds the timestamp range"))?,
            });
        };
        let word = t.text.to_ascii_lowercase();
        if word.starts_with("sinex:window/") {
            self.at += 1;
            if let Reference::Window { from, to } = parse_reference(&t.text)? {
                return Ok(Source::Recall { from, to });
            }
        }
        if word == "last" {
            self.at += 1;
            let alias = Alias::from_name("shell-sessions").expect("shell sessions are declared");
            return Ok(Source::RecallLast {
                filter: self.table_filter(&Scope::rows(Table::Events, Some(alias)))?,
            });
        }
        if word == "mark" {
            self.at += 1;
            let number = if self.peek().is_some_and(|t| t.text.parse::<u64>().is_ok()) {
                let token = self.value_token()?;
                token
                    .text
                    .parse::<u64>()
                    .ok()
                    .filter(|n| *n > 0)
                    .ok_or_else(|| QueryError::at(token.at, "mark number must be positive"))?
            } else {
                1
            };
            let mut window = default_window;
            if let Some(t) = self.peek().cloned()
                && let Some(value) = t
                    .text
                    .strip_prefix('±')
                    .or_else(|| t.text.strip_prefix("+-"))
            {
                window = time::duration(value)
                    .filter(|s| *s > 0.0)
                    .ok_or_else(|| QueryError::at(t.at, "a positive mark window: ±20m"))?;
                self.at += 1;
            }
            return Ok(Source::RecallMark { number, window });
        }
        if CLAUSE_WORDS.contains(&word.as_str()) {
            let w = SignedDuration::from_secs_f64(default_window);
            return Ok(Source::Recall {
                from: ctx
                    .now
                    .checked_sub(w)
                    .map_err(|_| QueryError::new("recall start exceeds the timestamp range"))?,
                to: ctx
                    .now
                    .checked_add(w)
                    .map_err(|_| QueryError::new("recall end exceeds the timestamp range"))?,
            });
        }
        // A whole day.
        let day = match word.as_str() {
            "today" => Some(ctx.today()),
            "yesterday" => ctx.today().yesterday().ok(),
            _ => time::date(&word).filter(|_| {
                !self
                    .peek_at(1)
                    .is_some_and(|n| time::clock(&n.text).is_some())
            }),
        };
        if let Some(d) = day {
            self.at += 1;
            let (from, to) = ctx
                .day(d)
                .ok_or_else(|| QueryError::at(t.at, "date out of range"))?;
            return Ok(Source::Recall { from, to });
        }
        self.at += 1;
        let centre = self.instant_from(&t)?;
        let mut window = default_window;
        if let Some(w) = self.peek().cloned()
            && let Some(rest) = w
                .text
                .strip_prefix('±')
                .or_else(|| w.text.strip_prefix("+-"))
        {
            self.at += 1;
            window = time::duration(rest).ok_or_else(|| QueryError::at(w.at, "a window: ±20m"))?;
        }
        let w = SignedDuration::from_secs_f64(window);
        Ok(Source::Recall {
            from: centre
                .checked_sub(w)
                .map_err(|_| QueryError::at(t.at, "recall start exceeds the timestamp range"))?,
            to: centre
                .checked_add(w)
                .map_err(|_| QueryError::at(t.at, "recall end exceeds the timestamp range"))?,
        })
    }

    pub(super) fn clause(&mut self, scope: &mut Scope) -> Result<Stage, QueryError> {
        let t = self.peek().cloned().expect("caller checked for the end");
        let word = t.text.to_ascii_lowercase();
        if t.kind != Kind::Word {
            return Err(QueryError::at(t.at, format!("unexpected `{}`", t.text)));
        }
        self.at += 1;
        match word.as_str() {
            "where" => Ok(Stage::Where(self.filter(scope)?)),
            "since" => Ok(Stage::Where(self.since(scope)?)),
            "until" => Ok(Stage::Where(self.until(scope)?)),
            "near" | "before" | "after" | "during" | "containing" => {
                if matches!(scope, Scope::Grouped(_)) {
                    return Err(QueryError::at(
                        t.at,
                        "time joins apply to rows, not to groups; join before grouping",
                    ));
                }
                let kind = match word.as_str() {
                    "near" => JoinKind::Near,
                    "before" => JoinKind::Before,
                    "after" => JoinKind::After,
                    "during" => JoinKind::During,
                    _ => JoinKind::Containing,
                };
                self.expect_symbol("(")?;
                let inner_at = self.pos();
                let inner = self.query(true)?;
                self.expect_symbol(")")?;
                if !matches!(inner.source, Source::Table { .. })
                    || inner.stages.iter().any(|s| matches!(s, Stage::Group(_)))
                {
                    return Err(QueryError::at(
                        inner_at,
                        "the inner query of a time join selects rows of a table, without grouping",
                    ));
                }
                let mut at = TimePoint::When;
                if self.take_word("at") {
                    let p = self.value_token()?;
                    at = match p.text.as_str() {
                        "when" => TimePoint::When,
                        "start" => TimePoint::Start,
                        "end" => TimePoint::End,
                        _ => return Err(QueryError::at(p.at, "`at` takes when, start or end")),
                    };
                }
                let within_secs =
                    if matches!(kind, JoinKind::Near | JoinKind::Before | JoinKind::After) {
                        self.expect_word("within")?;
                        let d = self.value_token()?;
                        Some(
                            time::duration(&d.text).ok_or_else(|| {
                                QueryError::at(d.at, "`within` takes a duration: 5m")
                            })?,
                        )
                    } else {
                        None
                    };
                if let Scope::Rows { columns, .. } = scope {
                    // Mirrors the compiler: the first join's columns are
                    // `near`, `offset`; later ones get `_2`, `_3`.
                    let n = columns
                        .iter()
                        .filter(|c| {
                            ["near", "during", "containing"].iter().any(|base| {
                                c.as_str() == *base || c.starts_with(&format!("{base}_"))
                            })
                        })
                        .count();
                    let suffix = if n == 0 {
                        String::new()
                    } else {
                        format!("_{}", n + 1)
                    };
                    let names: &[&str] = match kind {
                        JoinKind::Near | JoinKind::Before | JoinKind::After => &["near", "offset"],
                        JoinKind::During => &["during"],
                        JoinKind::Containing => &["containing"],
                    };
                    columns.extend(names.iter().map(|b| format!("{b}{suffix}")));
                }
                Ok(Stage::Join(Join {
                    kind,
                    inner: Box::new(inner),
                    at,
                    within_secs,
                }))
            }
            "as" => {
                self.expect_word("of")?;
                if self.take_word("seq") {
                    let n = self.value_token()?;
                    let seq = n
                        .text
                        .parse::<i64>()
                        .map_err(|_| QueryError::at(n.at, "`as of seq` takes a commit position"))?;
                    return Ok(Stage::AsOf(AsOf::Seq(seq)));
                }
                let v = self.value_token()?;
                Ok(Stage::AsOf(AsOf::Time(self.instant_from(&v)?)))
            }
            "group" => {
                self.expect_word("by")?;
                let mut keys = Vec::new();
                loop {
                    keys.push(self.group_key(scope)?);
                    if !self.take_symbol(",") {
                        break;
                    }
                }
                let mut aggregates = self.aggregates(scope)?;
                if aggregates.is_empty() {
                    aggregates.push(Aggregate {
                        function: AggFn::Count,
                        field: None,
                    });
                }
                let fill = self.take_word("fill").then_some(Fill {
                    from: self.ctx.now,
                    to: self.ctx.now,
                });
                let group = Group {
                    keys,
                    aggregates,
                    fill,
                };
                *scope = Scope::Grouped(group_columns(&group));
                Ok(Stage::Group(group))
            }
            "count" | "sum" | "min" | "max" | "avg" | "count_distinct" => {
                self.at -= 1;
                let aggregates = self.aggregates(scope)?;
                let group = Group {
                    keys: Vec::new(),
                    aggregates,
                    fill: None,
                };
                *scope = Scope::Grouped(group_columns(&group));
                Ok(Stage::Group(group))
            }
            "fill" => Err(QueryError::at(
                t.at,
                "fill follows a time-bucketed group by",
            )),
            "sort" => {
                let mut keys = Vec::new();
                loop {
                    let k = self
                        .peek()
                        .cloned()
                        .ok_or_else(|| QueryError::at(self.end, "expected a sort key"))?;
                    let key = if k.is_word("rank") {
                        self.at += 1;
                        SortBy::Rank
                    } else {
                        SortBy::Operand(self.group_key(scope)?)
                    };
                    let descending = if self.take_word("desc") {
                        true
                    } else {
                        self.take_word("asc");
                        false
                    };
                    keys.push(SortKey { key, descending });
                    if !self.take_symbol(",") {
                        break;
                    }
                }
                Ok(Stage::Sort(keys))
            }
            "limit" => {
                let n = self.value_token()?;
                let limit = n
                    .text
                    .parse::<u64>()
                    .ok()
                    .filter(|n| *n > 0)
                    .ok_or_else(|| QueryError::at(n.at, "`limit` takes a positive integer"))?;
                Ok(Stage::Limit(limit))
            }
            "fields" => {
                let mut fields = Vec::new();
                loop {
                    fields.push(self.field_ref(scope)?);
                    if !self.take_symbol(",") {
                        break;
                    }
                }
                Ok(Stage::Fields(fields))
            }
            _ => Err(QueryError::at(
                t.at,
                format!(
                    "`{}` is not a clause; clauses: {}",
                    t.text,
                    CLAUSE_WORDS.join(", ")
                ),
            )),
        }
    }

    pub(super) fn group_key(&mut self, scope: &Scope) -> Result<Operand, QueryError> {
        let t = self.value_token()?;
        if let Some(func) = TimeFn::from_name(&t.text.to_ascii_lowercase())
            && self.take_symbol("(")
        {
            let field = self.field_ref(scope)?;
            self.expect_symbol(")")?;
            self.time_field(&field, scope, t.at)?;
            return Ok(Operand::TimeFn(func, field));
        }
        Ok(Operand::Field(self.resolve(&t.text, scope, t.at)?))
    }

    pub(super) fn aggregates(&mut self, scope: &Scope) -> Result<Vec<Aggregate>, QueryError> {
        let mut out = Vec::new();
        while let Some(function) = self
            .peek()
            .filter(|t| t.kind == Kind::Word)
            .and_then(|t| AggFn::from_name(&t.text.to_ascii_lowercase()))
        {
            self.at += 1;
            let field = if function == AggFn::Count && !self.is_symbol("(") {
                None
            } else {
                self.expect_symbol("(")?;
                let f = self.field_ref(scope)?;
                self.expect_symbol(")")?;
                if let FieldRef::Field(field) = &f
                    && matches!(function, AggFn::Sum | AggFn::Avg)
                    && !matches!(field.ty, FieldType::Integer | FieldType::Number)
                {
                    return Err(QueryError::new(format!(
                        "{}() needs a number; `{}` is not one",
                        function.name(),
                        field.name
                    )));
                }
                Some(f)
            };
            out.push(Aggregate { function, field });
            self.take_symbol(",");
        }
        Ok(out)
    }
}

pub(super) fn group_columns(group: &Group) -> Vec<String> {
    group
        .keys
        .iter()
        .map(Operand::column)
        .chain(group.aggregates.iter().map(Aggregate::column))
        .chain(group.fill.iter().map(|_| "coverage".to_owned()))
        .collect()
}
