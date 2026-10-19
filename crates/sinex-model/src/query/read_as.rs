//! Every query reads back as a sentence (joint decision 17): each unit,
//! field, operator and clause has a phrase, joined in order, with the defaults
//! the query relies on stated. No model is involved, so the reading is exact.
use super::ast::*;
use jiff::Timestamp;

pub fn read_as(q: &Query) -> String {
    let mut out = source(&q.source);
    if let Some(f) = &q.filter {
        out.push_str(" where ");
        out.push_str(&expr(f));
    }
    let mut as_of = false;
    for stage in &q.stages {
        out.push_str(", ");
        match stage {
            Stage::Where(e) => {
                out.push_str("keeping those where ");
                out.push_str(&expr(e));
            }
            Stage::Join(j) => out.push_str(&join(j)),
            Stage::AsOf(AsOf::Time(t)) => {
                as_of = true;
                out.push_str(&format!("as Sinex knew them at {}", time(t)));
            }
            Stage::AsOf(AsOf::Seq(s)) => {
                as_of = true;
                out.push_str(&format!("as Sinex knew them at commit {s}"));
            }
            Stage::Group(g) => {
                let aggs: Vec<String> = g.aggregates.iter().map(aggregate).collect();
                if g.keys.is_empty() {
                    out.push_str(&aggs.join(" and "));
                } else {
                    let keys: Vec<String> = g.keys.iter().map(operand).collect();
                    out.push_str(&format!(
                        "grouped by {}, with {}",
                        keys.join(" and "),
                        aggs.join(" and ")
                    ));
                }
            }
            Stage::Sort(keys) => {
                let keys: Vec<String> = keys
                    .iter()
                    .map(|k| {
                        let name = match &k.key {
                            SortBy::Rank => "relevance".to_string(),
                            SortBy::Operand(o) => operand(o),
                        };
                        format!(
                            "{name} {}",
                            if k.descending {
                                "descending"
                            } else {
                                "ascending"
                            }
                        )
                    })
                    .collect();
                out.push_str(&format!("sorted by {}", keys.join(", then ")));
            }
            Stage::Limit(n) => out.push_str(&format!("the first {n}")),
            Stage::Fields(fields) => {
                let names: Vec<String> = fields.iter().map(FieldRef::name).collect();
                out.push_str(&format!("showing {}", names.join(", ")));
            }
        }
    }
    let sorted = q.stages.iter().any(|s| matches!(s, Stage::Sort(_)));
    if !sorted {
        out.push_str(if q.ranked {
            ", most relevant first"
        } else {
            ", in time order"
        });
    }
    if !as_of {
        out.push_str(", as currently recorded");
    }
    out
}

fn source(s: &Source) -> String {
    match s {
        Source::Unit { alias: Some(a), .. } => a.phrase.to_string(),
        Source::Unit { unit, alias: None } => unit.phrase().to_string(),
        Source::Walk { walk, reference } => {
            let what = match walk {
                Walk::Trace => "the chain from",
                Walk::Descendants => "everything derived from",
                Walk::Parents => "the parents of",
            };
            let r = reference_text(reference);
            if *walk == Walk::Trace {
                format!("{what} {r} down to stored bytes")
            } else {
                format!("{what} {r}")
            }
        }
        Source::Recall { from, to } => {
            format!("everything recorded from {} until {}", time(from), time(to))
        }
    }
}

fn reference_text(r: &Reference) -> String {
    match r {
        Reference::Event(id) => format!("event {id}"),
        Reference::Occurrence {
            material, anchor, ..
        } => {
            format!("the record at byte {anchor} of material {material}")
        }
        Reference::Equivalence {
            source,
            event_type,
            key,
        } => {
            format!("the {event_type} of {source} keyed {key}")
        }
    }
}

fn expr(e: &Expr) -> String {
    match e {
        Expr::And(a, b) => format!("{} and {}", grouped(a, true), grouped(b, true)),
        Expr::Or(a, b) => format!("{} or {}", grouped(a, false), grouped(b, false)),
        Expr::Not(inner) => format!("not ({})", expr(inner)),
        Expr::Compare { lhs, op, rhs } => {
            let verb = match op {
                Op::Eq => {
                    if matches!(rhs, Value::List(_)) {
                        "is one of"
                    } else if matches!(rhs, Value::Glob(_)) {
                        "matches"
                    } else {
                        "is"
                    }
                }
                Op::Ne => "is not",
                Op::Lt => "is before or less than",
                Op::Le => "is at most",
                Op::Gt => "is after or more than",
                Op::Ge => "is at least",
                Op::Contains => "contains",
                Op::Prefix => "starts with",
            };
            format!("{} {verb} {}", operand(lhs), value(rhs))
        }
        Expr::Exists(o) => format!("{} is known", operand(o)),
        Expr::Words(w) => {
            let text = w.words.join(" ");
            let star = if w.prefix { "…" } else { "" };
            if w.phrase {
                format!("the text has the phrase \"{text}{star}\"")
            } else {
                format!("the text has the word \"{text}{star}\"")
            }
        }
        Expr::Within { lhs, from, to } => {
            format!("{} is from {} until {}", operand(lhs), time(from), time(to))
        }
        Expr::TimeDiff { a, b, op, seconds } => {
            format!(
                "{} minus {} {} {}s",
                a.name(),
                b.name(),
                op.symbol(),
                seconds
            )
        }
    }
}

fn grouped(e: &Expr, inside_and: bool) -> String {
    match e {
        Expr::Or(..) if inside_and => format!("({})", expr(e)),
        _ => expr(e),
    }
}

fn operand(o: &Operand) -> String {
    match o {
        Operand::Field(FieldRef::Field(f)) => f.phrase.to_string(),
        Operand::Field(other) => other.name(),
        Operand::TimeFn(func, f) => format!(
            "the {} of {}",
            func.name().replace('_', " "),
            field_phrase(f)
        ),
    }
}

fn field_phrase(f: &FieldRef) -> String {
    match f {
        FieldRef::Field(field) => field.phrase.to_string(),
        other => other.name(),
    }
}

fn value(v: &Value) -> String {
    match v {
        Value::Text(s) => format!("\"{s}\""),
        Value::Glob(s) => format!("\"{s}\""),
        Value::Integer(i) => i.to_string(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Time(t) => time(t),
        Value::Id(id) => id.to_string(),
        Value::List(items) => {
            let items: Vec<String> = items.iter().map(value).collect();
            format!("[{}]", items.join(", "))
        }
    }
}

fn aggregate(a: &Aggregate) -> String {
    match &a.field {
        None => "the count".to_string(),
        Some(f) => format!(
            "the {} of {}",
            a.function.name().replace('_', " "),
            field_phrase(f)
        ),
    }
}

fn join(j: &Join) -> String {
    let inner = read_as(&j.inner);
    let point = match j.at {
        TimePoint::When => "",
        TimePoint::Start => " (their start)",
        TimePoint::End => " (their end)",
    };
    match (j.kind, j.within_secs) {
        (JoinKind::Near, Some(s)) => format!("that happened within {s}s of [{inner}]{point}"),
        (JoinKind::Before, Some(s)) => format!("that happened up to {s}s before [{inner}]{point}"),
        (JoinKind::After, Some(s)) => format!("that happened up to {s}s after [{inner}]{point}"),
        (JoinKind::During, _) => format!("that happened during [{inner}]"),
        (JoinKind::Containing, _) => format!("whose span contains [{inner}]{point}"),
        (kind, None) => format!("{} [{inner}]", kind.name()),
    }
}

fn time(t: &Timestamp) -> String {
    t.to_string()
}
