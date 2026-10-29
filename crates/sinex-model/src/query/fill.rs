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
                    for e in &filters {
                        bounds(e, field, &mut from, &mut to);
                    }
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
                }
                filters.clear();
            }
            _ => {}
        }
    }
    Ok(())
}

/// Bounds that hold for every row at this phase. OR and later groups never
/// supply a narrowing bound; callers can use the result as a safe candidate range.
pub fn finite_time_bounds(q:&Query,field:&FieldRef)->Option<(jiff::Timestamp,jiff::Timestamp)> {
    let (mut from,mut to)=(None,None);
    if let Some(e)=&q.filter{bounds(e,field,&mut from,&mut to);}
    for stage in &q.stages {
        match stage {Stage::Where(e)=>bounds(e,field,&mut from,&mut to),Stage::Group(_)=>{from=None;to=None;},_=>{}}
    }
    match (from,to){(Some(from),Some(to)) if from<to=>Some((from,to)),_=>None}
}
