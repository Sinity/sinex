//! Every query reads back as a sentence: each table, field, operator and clause
//! has a phrase, joined in order, with the defaults the query relies on stated.
//! No model is involved, so the reading is exact.
use super::ast::*;
use jiff::{Timestamp, tz::TimeZone};

/// Describe the resolved query in the answer's time zone.
pub fn read_as(q: &Query, zone: &TimeZone) -> String {
    let mut out = source(&q.source, zone);
    if let Some(f) = &q.filter {
        out.push_str(" where ");
        out.push_str(&expr(f, zone));
    }
    let mut as_of = false;
    for stage in &q.stages {
        out.push_str(", ");
        match stage {
            Stage::Where(e) => {
                out.push_str("keeping those where ");
                out.push_str(&expr(e, zone));
            }
            Stage::Join(j) => out.push_str(&join(j, zone)),
            Stage::AsOf(AsOf::Time(t)) => {
                as_of = true;
                out.push_str(&format!("as Sinex knew them at {}", time(t, zone)));
            }
            Stage::AsOf(AsOf::Seq(s)) => {
                as_of = true;
                out.push_str(&format!("as Sinex knew them at commit {s}"));
            }
            Stage::GroupFirst { keys, count } => {
                out.push_str(&format!(
                    "the first {count} in each group by {}",
                    keys.iter()
                        .map(|item| operand(item, zone))
                        .collect::<Vec<_>>()
                        .join(", ")
                ));
            }
            Stage::Group(g) => {
                if g.fill.is_some() {
                    out.push_str("including empty time buckets with their coverage, ");
                }
                let aggs: Vec<String> = g
                    .aggregates
                    .iter()
                    .map(|item| aggregate(item, zone))
                    .collect();
                if g.keys.is_empty() {
                    out.push_str(&aggs.join(" and "));
                } else {
                    let keys: Vec<String> = g.keys.iter().map(|item| operand(item, zone)).collect();
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
                            SortBy::Operand(o) => operand(o, zone),
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
                let names: Vec<String> = fields.iter().map(|item| operand(item, zone)).collect();
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

fn source(s: &Source, zone: &TimeZone) -> String {
    match s {
        Source::Table { alias: Some(a), .. } => a.phrase.to_string(),
        Source::Table { table, alias: None } => table.phrase().to_string(),
        Source::Walk { walk, reference } => {
            let what = match walk {
                Walk::Trace => "the path from",
                Walk::Descendants => "everything derived from",
                Walk::Parents => "the parents of",
            };
            let r = reference_text(reference, zone);
            if *walk == Walk::Trace {
                format!("{what} {r} down to stored bytes")
            } else {
                format!("{what} {r}")
            }
        }
        Source::RecallMark { number, window } => {
            format!("everything within {window}s of moment mark {number}, newest first")
        }
        Source::RecallLast { filter } => {
            let mut s = "everything during the latest recorded shell session".to_string();
            if let Some(f) = filter {
                s.push_str(" whose ");
                s.push_str(&expr(f, zone));
            }
            s
        }
        Source::Recall { from, to } => {
            format!(
                "everything recorded from {} until {}",
                time(from, zone),
                time(to, zone)
            )
        }
    }
}

fn reference_text(r: &Reference, zone: &TimeZone) -> String {
    match r {
        Reference::Event(id) => format!("event {id}"),
        Reference::At { at, event_type } => {
            format!("the live {event_type} containing {}", time(at, zone))
        }
        Reference::Window { from, to } => format!(
            "the window from {} until {}",
            time(from, zone),
            time(to, zone)
        ),
        Reference::Record {
            material, offset, ..
        } => {
            format!("the record at byte {offset} of material {material}")
        }
        Reference::Key {
            source,
            event_type,
            key,
        } => {
            format!("the {event_type} of {source} keyed {key}")
        }
    }
}

fn expr(e: &Expr, zone: &TimeZone) -> String {
    query_language::depth::deeper(|| match e {
        Expr::Order(j) => format!("having a match {}", join(j, zone)),
        Expr::Truth {
            value,
            operand: field,
        } => format!("{} matches {value:?}", operand(field, zone)),
        Expr::And(a, b) => format!("{} and {}", grouped(a, true, zone), grouped(b, true, zone)),
        Expr::Or(a, b) => format!("{} or {}", grouped(a, false, zone), grouped(b, false, zone)),
        Expr::Not(inner) => format!("not ({})", expr(inner, zone)),
        Expr::Compare { lhs, op, rhs } => {
            let verb = match op {
                Op::Eq => {
                    if matches!(rhs, Value::List(_)) {
                        "is one of"
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
            format!(
                "{} {verb} {}",
                operand(lhs, zone),
                comparison_value(rhs, lhs, zone)
            )
        }
        Expr::Exists(o) => format!("{} is known", operand(o, zone)),
        Expr::FieldWords {
            operand: field,
            words,
        } => format!(
            "{} matches words {}",
            operand(field, zone),
            words.words.join(" ")
        ),
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
            format!(
                "{} is from {} until {}",
                operand(lhs, zone),
                time(from, zone),
                time(to, zone)
            )
        }
        Expr::Similar { reference } => format!("ranked by supplied vector sinex:ev/{reference}"),
        Expr::Overlaps { from, to } => match to {
            Some(to) => format!(
                "its span overlaps {} until {}",
                time(from, zone),
                time(to, zone)
            ),
            None => format!("its span contains {}", time(from, zone)),
        },
    })
}

fn grouped(e: &Expr, inside_and: bool, zone: &TimeZone) -> String {
    match e {
        Expr::Or(..) if inside_and => format!("({})", expr(e, zone)),
        _ => expr(e, zone),
    }
}

fn operand(o: &Operand, zone: &TimeZone) -> String {
    query_language::depth::deeper(|| match o {
        Operand::Field(FieldRef::Field(f)) => f.phrase.to_string(),
        Operand::Field(other) => other.name(),
        Operand::TimeFn(func, f) => format!(
            "the {} of {}",
            func.name().replace('_', " "),
            field_phrase(f)
        ),
        Operand::Literal { value: literal, .. } => value(literal, zone),
        Operand::Arithmetic {
            operator,
            left,
            right,
            ..
        } => format!(
            "({} {} {})",
            operand(left, zone),
            operator.symbol(),
            operand(right, zone)
        ),
        Operand::Length(inner) => format!("the length of {}", operand(inner, zone)),
        Operand::Absolute(inner) => format!("the magnitude of {}", operand(inner, zone)),
        Operand::Text(inner) => format!("the text of {}", operand(inner, zone)),
        Operand::Glob { left, right } => format!(
            "{} matches the case-sensitive pattern {}",
            operand(left, zone),
            operand(right, zone)
        ),
        Operand::CaseEqual { left, right, value } => format!(
            "{} when {} is {}",
            operand(value, zone),
            operand(left, zone),
            operand(right, zone)
        ),
        Operand::Named {
            operand: inner,
            name,
        } => format!("{} as {name}", operand(inner, zone)),
    })
}

fn field_phrase(f: &FieldRef) -> String {
    match f {
        FieldRef::Field(field) => field.phrase.to_string(),
        other => other.name(),
    }
}

fn comparison_value(v: &Value, lhs: &Operand, zone: &TimeZone) -> String {
    let ty = match lhs {
        Operand::Field(FieldRef::Field(field)) => Some(field.ty),
        Operand::Field(FieldRef::TypedPayload { ty, .. }) => Some(*ty),
        Operand::Field(FieldRef::TypedGroup(column)) => column.field_type(),
        Operand::TimeFn(TimeFn::Hour | TimeFn::Year, _) => Some(super::FieldType::Integer),
        _ => None,
    };
    if matches!(
        ty,
        Some(super::FieldType::Integer | super::FieldType::Number)
    ) {
        match v {
            Value::Text(raw) => return raw.clone(),
            Value::Operand(o) => return operand(o, zone),
            Value::List(items) => {
                return format!(
                    "[{}]",
                    items
                        .iter()
                        .map(|value| comparison_value(value, lhs, zone))
                        .collect::<Vec<_>>()
                        .join(", ")
                );
            }
            _ => {}
        }
    }
    value(v, zone)
}

fn value(v: &Value, zone: &TimeZone) -> String {
    match v {
        Value::Unknown => "unknown".into(),
        Value::Text(s) | Value::PlaceholderText(s) => format!("\"{s}\""),
        Value::Integer(i) => i.to_string(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Time(t) => time(t, zone),
        Value::Id(id) => id.to_string(),
        Value::Operand(o) => operand(o, zone),
        Value::List(items) => {
            let items: Vec<String> = items.iter().map(|item| value(item, zone)).collect();
            format!("[{}]", items.join(", "))
        }
    }
}

fn aggregate(a: &Aggregate, zone: &TimeZone) -> String {
    match &a.argument {
        None => "the count".to_string(),
        Some(f) => format!(
            "the {} of {}",
            a.function.name().replace('_', " "),
            operand(f, zone)
        ),
    }
}

fn join(j: &Join, zone: &TimeZone) -> String {
    let inner = read_as(&j.inner, zone);
    let (relation, within, point) = match &j.on {
        JoinOn::Key { left, right } => {
            return format!(
                "paired with [{inner}] where {} equals {}",
                left.name(),
                right.name()
            );
        }
        JoinOn::Order {
            relation,
            within_secs,
            at,
        } => (*relation, within_secs.as_deref(), *at),
    };
    let at_point = match point {
        TimePoint::When => "at their occurrence",
        TimePoint::Start => "at their span start",
        TimePoint::End => "at their end",
        TimePoint::MatchWhen => "at the matched occurrence",
        TimePoint::MatchStart => "at the matched span start",
        TimePoint::MatchEnd => "at the matched span end",
    };
    let window = within
        .map(|seconds| format!(" within {seconds}s"))
        .unwrap_or_default();
    let choice = if j.pick.is_empty() {
        String::new()
    } else {
        format!(
            ", choosing first by {}",
            j.pick
                .iter()
                .map(|key| {
                    let expression = match &key.key {
                        SortBy::Rank => "relevance".into(),
                        SortBy::Operand(value) => operand(value, zone),
                    };
                    format!(
                        "{expression} {}",
                        if key.descending {
                            "descending"
                        } else {
                            "ascending"
                        }
                    )
                })
                .collect::<Vec<_>>()
                .join(", ")
        )
    };
    format!(
        "paired {} [{inner}]{window}, {at_point}{choice}",
        relation.name()
    )
}

fn time(t: &Timestamp, zone: &TimeZone) -> String {
    t.to_zoned(zone.clone())
        .strftime("%Y-%m-%d %H:%M:%S%:z")
        .to_string()
}
