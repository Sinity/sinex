//! Resolve the finite extent of a filled grouping before compiling any SQL.
use super::*;

fn bounds(
    e: &Expr,
    field: &FieldRef,
    from: &mut Option<jiff::Timestamp>,
    to: &mut Option<jiff::Timestamp>,
) {
    let mut lower = |t| *from = Some(from.map_or(t, |old| old.max(t)));
    let mut upper = |t| *to = Some(to.map_or(t, |old| old.min(t)));
    match e {
        Expr::And(a, b) => {
            bounds(a, field, from, to);
            bounds(b, field, from, to);
        }
        Expr::Within {
            lhs: Operand::Field(f),
            from,
            to,
        } if f == field => {
            lower(*from);
            upper(*to);
        }
        Expr::Compare {
            lhs: Operand::Field(f),
            op,
            rhs: Value::Time(t),
        } if f == field => match op {
            Op::Ge => lower(*t),
            Op::Gt => {
                if let Ok(t) = t.checked_add(jiff::SignedDuration::from_nanos(1)) {
                    lower(t);
                }
            }
            Op::Lt => upper(*t),
            Op::Le => {
                if let Ok(t) = t.checked_add(jiff::SignedDuration::from_nanos(1)) {
                    upper(t);
                }
            }
            _ => {}
        },
        _ => {}
    }
}
fn sources(e: &Expr) -> Option<std::collections::BTreeSet<String>> {
    use std::collections::BTreeSet;
    fn values(value: &Value) -> Option<BTreeSet<String>> {
        match value {
            Value::Text(s) => Some(BTreeSet::from([s.clone()])),
            Value::List(items) => {
                let mut out = BTreeSet::new();
                for item in items {
                    out.extend(values(item)?);
                }
                Some(out)
            }
            _ => None,
        }
    }
    match e {
        Expr::And(a, b) => match (sources(a), sources(b)) {
            (Some(a), Some(b)) => Some(a.intersection(&b).cloned().collect()),
            (a, b) => a.or(b),
        },
        Expr::Or(a, b) => Some(sources(a)?.union(&sources(b)?).cloned().collect()),
        Expr::Compare {
            lhs: Operand::Field(FieldRef::Field(f)),
            op: Op::Eq,
            rhs,
        } if f.name == "source" => values(rhs),
        _ => None,
    }
}
pub(super) fn bind(q: &mut Query, ctx: &Context) -> Result<(), QueryError> {
    let mut filters: Vec<Expr> = q.filter.iter().cloned().collect();
    for s in &mut q.stages {
        match s {
            Stage::Join(j) => bind(&mut j.inner, ctx)?,
            Stage::Where(e) => filters.push(e.clone()),
            Stage::Group(g) => {
                if let Some(fill) = &mut g.fill {
                    let times: Vec<_> = g
                        .keys
                        .iter()
                        .filter_map(|k| {
                            if let Operand::TimeFn(func, field) = k {
                                Some((*func, field))
                            } else {
                                None
                            }
                        })
                        .collect();
                    if times.len() != 1 {
                        return Err(QueryError::new(
                            "fill needs one time-bucket key and any dimension keys",
                        ));
                    }
                    let (_, field) = times[0];
                    let (mut from, mut to) = (None, None);
                    let mut selected: Option<std::collections::BTreeSet<String>> = None;
                    for e in &filters {
                        bounds(e, field, &mut from, &mut to);
                        if let Some(names) = sources(e) {
                            selected = Some(selected.map_or(names.clone(), |prior| {
                                prior.intersection(&names).cloned().collect()
                            }));
                        }
                    }
                    fill.sources = selected.unwrap_or_default().into_iter().collect();
                    if let Source::Recall { from: f, to: t } = q.source
                        && field.name() == "when"
                    {
                        from = Some(from.map_or(f, |bound| bound.max(f)));
                        to = Some(to.map_or(t, |bound| bound.min(t)));
                    }
                    fill.from = from.ok_or_else(|| {
                        QueryError::new(
                            "fill needs a lower time bound on its bucket field: when in last 7d",
                        )
                    })?;
                    fill.to = to.unwrap_or(ctx.now);
                    if fill.from >= fill.to {
                        return Err(QueryError::new("fill needs a nonempty time range"));
                    }
                    fill.sources.sort();
                    fill.sources.dedup();
                }
                filters.clear();
            }
            _ => {}
        }
    }
    Ok(())
}
