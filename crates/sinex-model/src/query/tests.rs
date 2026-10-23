use super::*;
use jiff::Timestamp;

fn ctx() -> Context {
    Context::new(
        "2026-09-14T10:00:00Z".parse().unwrap(),
        jiff::tz::TimeZone::get("Europe/Warsaw").unwrap(),
        jiff::civil::Time::constant(4, 0, 0, 0),
    )
}

fn q(text: &str) -> Query {
    bind(text, &ctx()).unwrap_or_else(|e| panic!("{text}: {}", e.render(text)))
}

fn err(text: &str) -> QueryError {
    bind(text, &ctx()).expect_err(text)
}

fn ts(s: &str) -> Timestamp {
    s.parse().unwrap()
}

#[test]
fn bare_words_search_events() {
    let query = q("tide offset");
    assert_eq!(
        query.source,
        Source::Unit {
            unit: Unit::Events,
            alias: None
        }
    );
    assert!(query.ranked);
    let Some(Expr::And(a, b)) = query.filter else {
        panic!("two words are joined with and")
    };
    assert!(matches!(*a, Expr::Words(ref w) if w.words == ["tide"] && !w.phrase));
    assert!(matches!(*b, Expr::Words(ref w) if w.words == ["offset"]));
}

#[test]
fn quoted_phrases_and_prefixes() {
    let query = q(r#""cargo test" deploy*"#);
    let Some(Expr::And(a, b)) = query.filter else {
        panic!()
    };
    assert!(matches!(*a, Expr::Words(ref w) if w.phrase && w.words == ["cargo", "test"]));
    assert!(matches!(*b, Expr::Words(ref w) if w.prefix && w.words == ["deploy"]));
}

#[test]
fn a_command_word_is_never_a_search() {
    let e = err("related sinex:ev/0190");
    assert!(e.not_built, "{e}");
    // Quoted, it is a search for the word.
    assert!(matches!(q(r#""related""#).filter, Some(Expr::Words(_))));
}

#[test]
fn a_misspelled_unit_is_refused_with_a_suggestion() {
    let e = err("comands since 7d");
    assert!(e.message.contains("commands"), "{e}");
}

#[test]
fn unit_with_filter_and_clauses() {
    let query = q("commands where exit_code != 0 and cwd ^ /src/tide sort when desc limit 20");
    assert_eq!(query.source.alias().unwrap().name, "commands");
    let Some(Expr::And(a, b)) = &query.filter else {
        panic!()
    };
    assert!(matches!(
        **a,
        Expr::Compare {
            op: Op::Ne,
            rhs: Value::Integer(0),
            ..
        }
    ));
    assert!(
        matches!(**b, Expr::Compare { op: Op::Prefix, rhs: Value::Text(ref p), .. } if p == "/src/tide")
    );
    assert_eq!(query.stages.len(), 2);
    assert!(matches!(&query.stages[0], Stage::Sort(k) if k[0].descending));
    assert_eq!(query.stages[1], Stage::Limit(20));
}

#[test]
fn field_colon_value_means_equals() {
    let query = q("events where source:terminal.*");
    assert!(matches!(
        query.filter,
        Some(Expr::Compare { op: Op::Eq, rhs: Value::Glob(ref g), .. }) if g == "terminal.*"
    ));
}

#[test]
fn and_binds_tighter_than_or_and_both_bounds_are_kept() {
    let query = q("events where source = a or source = b and kind = c");
    let Some(Expr::Or(_, right)) = query.filter else {
        panic!("or at the top")
    };
    assert!(matches!(*right, Expr::And(..)));
    // Two time bounds are both required.
    let query = q("events where when > 2026-09-01 and when < 2026-09-10");
    assert!(matches!(query.filter, Some(Expr::And(..))));
}

#[test]
fn unknown_fields_are_refused_listing_the_fields() {
    let e = err("commands where exit_cod = 0");
    assert!(e.message.contains("exit_code"), "{e}");
    assert!(e.at.is_some());
}

#[test]
fn closed_sets_refuse_outside_values_naming_where_they_belong() {
    let e = err("runs where status = hide");
    assert!(e.message.contains("archive.reason"), "{e}");
}

#[test]
fn times_resolve_once_against_the_context() {
    // since 7d is [now - 7d, ...).
    let query = q("events since 7d");
    let Some(Stage::Where(Expr::Compare {
        rhs: Value::Time(t),
        op: Op::Ge,
        ..
    })) = query.stages.first()
    else {
        panic!("{query:?}")
    };
    assert_eq!(*t, ts("2026-09-07T10:00:00Z"));
    // A date is the start of its logical day in the zone: 04:00 Warsaw.
    let query = q("events where when >= 2026-09-14");
    assert!(matches!(
        query.filter,
        Some(Expr::Compare { rhs: Value::Time(t), .. }) if t == ts("2026-09-14T02:00:00Z")
    ));
    // A date with a clock time is one instant.
    let query = q("events where when >= 2026-09-14 14:05");
    assert!(matches!(
        query.filter,
        Some(Expr::Compare { rhs: Value::Time(t), .. }) if t == ts("2026-09-14T12:05:00Z")
    ));
}

#[test]
fn ranges_are_half_open_logical_days() {
    let query = q("events where when in yesterday");
    let Some(Expr::Within { from, to, .. }) = query.filter else {
        panic!()
    };
    assert_eq!(from, ts("2026-09-13T02:00:00Z"));
    assert_eq!(to, ts("2026-09-14T02:00:00Z"));
    let query = q("events where when in last 7d");
    let Some(Expr::Within { from, to, .. }) = query.filter else {
        panic!()
    };
    assert_eq!(to, ts("2026-09-14T10:00:00Z"));
    assert_eq!(to.duration_since(from).as_secs(), 7 * 86_400);
    let query = q("events where when in 2026-08");
    let Some(Expr::Within { from, .. }) = query.filter else {
        panic!()
    };
    assert_eq!(from, ts("2026-08-01T02:00:00Z"));
}

#[test]
fn a_bare_duration_with_an_ordering_is_refused() {
    let e = err("events where when > 7d");
    assert!(e.message.contains("since 7d"), "{e}");
}

#[test]
fn payload_paths_take_their_type_from_the_literal() {
    let query = q("events where payload.exit_code > 0 and payload.url ~ github");
    let Some(Expr::And(a, b)) = query.filter else {
        panic!()
    };
    assert!(
        matches!(*a, Expr::Compare { lhs: Operand::Field(FieldRef::Payload(ref p)), rhs: Value::Integer(0), .. } if p == &["exit_code"])
    );
    assert!(matches!(
        *b,
        Expr::Compare {
            op: Op::Contains,
            rhs: Value::Text(_),
            ..
        }
    ));
    assert!(
        err("materials where payload.x = 1")
            .message
            .contains("no payload")
    );
}

#[test]
fn lists_and_exists() {
    let query =
        q("events where kind = [command.executed, command.imported] and not material exists");
    let Some(Expr::And(a, b)) = query.filter else {
        panic!()
    };
    assert!(matches!(*a, Expr::Compare { rhs: Value::List(ref l), .. } if l.len() == 2));
    assert!(matches!(*b, Expr::Not(ref inner) if matches!(**inner, Expr::Exists(_))));
}

#[test]
fn time_functions_and_differences() {
    let query = q("events where hour(when) >= 1 and weekday(when) = Saturday and seen - when > 1d");
    let Some(Expr::And(left, diff)) = query.filter else {
        panic!()
    };
    assert!(matches!(*diff, Expr::TimeDiff { seconds, .. } if seconds == 86_400.0));
    let Expr::And(hour, weekday) = *left else {
        panic!()
    };
    assert!(matches!(
        *hour,
        Expr::Compare {
            lhs: Operand::TimeFn(TimeFn::Hour, _),
            rhs: Value::Integer(1),
            ..
        }
    ));
    assert!(matches!(*weekday, Expr::Compare { rhs: Value::Text(ref d), .. } if d == "sat"));
}

#[test]
fn joins_take_an_inner_query() {
    let query = q(
        r#"events where source != terminal.* near (commands where command ~ "cargo test") within 5m"#,
    );
    let Some(Stage::Join(j)) = query.stages.first() else {
        panic!()
    };
    assert_eq!(j.kind, JoinKind::Near);
    assert_eq!(j.within_secs, Some(300.0));
    assert_eq!(j.inner.source.alias().unwrap().name, "commands");
    let query = q("pages during (intervals where state = focused)");
    assert!(matches!(query.stages.first(), Some(Stage::Join(j)) if j.kind == JoinKind::During));
    assert!(err("events near (commands) ").message.contains("within"));
    // The columns a join adds can be shown and sorted on.
    let query = q("events near (commands) within 5m fields when, near, offset sort offset");
    assert!(matches!(&query.stages[1], Stage::Fields(f) if f[1] == FieldRef::Group("near".into())));
    // The n-th join's columns carry `_n`.
    let query = q("events near (commands) within 5m during (commands) fields near, during_2");
    assert!(matches!(&query.stages[2], Stage::Fields(f) if f.len() == 2));
    let query =
        q("events near (commands) within 5m after (commands) within 1m fields near_2, offset_2");
    assert!(
        matches!(&query.stages[2], Stage::Fields(f) if f[0] == FieldRef::Group("near_2".into()))
    );
}

#[test]
fn grouping_makes_a_relation_of_groups() {
    let query = q("commands group by cwd count where count > 1 sort count desc limit 5");
    assert!(matches!(&query.stages[0], Stage::Group(g) if g.keys.len() == 1));
    assert!(
        matches!(&query.stages[1], Stage::Where(Expr::Compare { lhs: Operand::Field(FieldRef::Group(c)), .. }) if c == "count")
    );
    // After grouping, row fields are gone.
    assert!(
        err("commands group by cwd where exit_code = 0")
            .message
            .contains("grouped")
    );
    let query = q("commands group by day(when) sum(duration)");
    assert!(
        matches!(&query.stages[0], Stage::Group(g) if g.aggregates[0].column() == "sum_duration")
    );
    assert!(err("commands group by cwd fill").not_built);
    let query = q("commands count");
    assert!(matches!(&query.stages[0], Stage::Group(g) if g.keys.is_empty()));
}

#[test]
fn as_of_takes_a_time_or_a_commit_position() {
    let query = q("commands where when > 2026-02-01 as of 2026-03-01");
    assert!(matches!(
        query.stages.last(),
        Some(Stage::AsOf(AsOf::Time(_)))
    ));
    let query = q("events as of seq 42");
    assert_eq!(query.stages.last(), Some(&Stage::AsOf(AsOf::Seq(42))));
}

#[test]
fn walks_and_references() {
    let id = "0190a0b0-0000-7000-8000-000000000001";
    let query = q(&format!("trace sinex:ev/{id}"));
    assert!(matches!(
        query.source,
        Source::Walk {
            walk: Walk::Trace,
            reference: Reference::Event(_)
        }
    ));
    let query = q(&format!("descendants {id}"));
    assert!(matches!(
        query.source,
        Source::Walk {
            walk: Walk::Descendants,
            ..
        }
    ));
    let r = parse_reference(&format!("sinex:occ/{id}/120/command.executed/0")).unwrap();
    assert!(matches!(
        r,
        Reference::Occurrence {
            anchor: 120,
            ordinal: Some(0),
            ..
        }
    ));
    let r = parse_reference("sinex:eq/terminal.atuin/command.executed/a/b").unwrap();
    assert!(matches!(r, Reference::Equivalence { ref key, .. } if key == "a/b"));
    assert!(
        parse_reference("sinex:at/2026-09-14/session")
            .unwrap_err()
            .not_built
    );
}

#[test]
fn recall_windows() {
    let query = q("recall 2026-03-02 14:05 ±20m");
    let Source::Recall { from, to } = query.source else {
        panic!()
    };
    assert_eq!(from, ts("2026-03-02T12:45:00Z"));
    assert_eq!(to, ts("2026-03-02T13:25:00Z"));
    let query = q("recall yesterday");
    let Source::Recall { from, .. } = query.source else {
        panic!()
    };
    assert_eq!(from, ts("2026-09-13T02:00:00Z"));
    let query = q("recall");
    let Source::Recall { from, to } = query.source else {
        panic!()
    };
    assert_eq!(to.duration_since(from).as_secs(), 3600);
    assert!(matches!(
        q("recall last").source,
        Source::RecallLast { filter: None }
    ));
    assert!(err("recall mark").not_built);
}

#[test]
fn reserved_words_must_be_quoted_to_search() {
    assert!(err("sort algorithm").message.contains("quote"));
    assert!(q(r#""sort" algorithm"#).ranked);
}

#[test]
fn reads_back_as_a_sentence() {
    let s = read_as(&q("commands where exit_code != 0 sort when desc limit 5"));
    assert_eq!(
        s,
        "commands, each once however many shells saw it where exit code is not 0, sorted by time it happened descending, the first 5, as currently recorded"
    );
    let s = read_as(&q("tide"));
    assert!(s.contains("most relevant first"), "{s}");
}

#[test]
fn errors_render_with_a_caret() {
    let text = "commands where exit_cod = 0";
    let e = err(text);
    let rendered = e.render(text);
    assert!(
        rendered.ends_with(&format!("{}^", " ".repeat(15))),
        "{rendered}"
    );
}

#[test]
fn every_field_name_is_unique_within_its_unit() {
    for unit in Unit::ALL {
        let mut names: Vec<&str> = unit.fields().iter().map(|f| f.name).collect();
        names.sort_unstable();
        let before = names.len();
        names.dedup();
        assert_eq!(before, names.len(), "{}", unit.name());
        for d in unit.default_fields() {
            assert!(unit.field(d).is_some(), "{}: default {d}", unit.name());
        }
        assert!(unit.field(unit.main_time()).is_some());
    }
}

mod sessions;
