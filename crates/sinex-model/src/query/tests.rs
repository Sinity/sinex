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

#[devkit::test]
fn bare_words_search_events() {
    let query = q("tide offset");
    assert_eq!(
        query.source,
        Source::Table {
            table: Table::Events,
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

#[devkit::test]
fn quoted_phrases_and_prefixes() {
    let query = q(r#""cargo test" deploy*"#);
    let Some(Expr::And(a, b)) = query.filter else {
        panic!()
    };
    assert!(matches!(*a, Expr::Words(ref w) if w.phrase && w.words == ["cargo", "test"]));
    assert!(matches!(*b, Expr::Words(ref w) if w.prefix && w.words == ["deploy"]));
}

#[devkit::test]
fn a_command_word_is_never_a_search() {
    let e = err("related sinex:ev/0190");
    assert!(e.not_built, "{e}");
    // Quoted, it is a search for the word.
    assert!(matches!(q(r#""related""#).filter, Some(Expr::Words(_))));
}

#[devkit::test]
fn a_misspelled_table_is_refused_with_a_suggestion() {
    let e = err("comands since 7d");
    assert!(e.message.contains("commands"), "{e}");
}

#[devkit::test]
fn table_with_filter_and_clauses() {
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

#[devkit::test]
fn field_colon_value_means_equals() {
    let query = q("events where source:terminal.*");
    assert!(matches!(
        query.filter,
        Some(Expr::Compare { op: Op::Eq, rhs: Value::Glob(ref g), .. }) if g == "terminal.*"
    ));
}

#[devkit::test]
fn and_binds_tighter_than_or_and_both_bounds_are_kept() {
    let query = q("events where source = a or source = b and event_type = c");
    let Some(Expr::Or(_, right)) = query.filter else {
        panic!("or at the top")
    };
    assert!(matches!(*right, Expr::And(..)));
    // Two time bounds are both required.
    let query = q("events where when > 2026-09-01 and when < 2026-09-10");
    assert!(matches!(query.filter, Some(Expr::And(..))));
}

#[devkit::test]
fn unknown_fields_are_refused_listing_the_fields() {
    let e = err("commands where exit_cod = 0");
    assert!(e.message.contains("exit_code"), "{e}");
    assert!(e.at.is_some());
}

#[devkit::test]
fn closed_sets_refuse_outside_values_naming_where_they_belong() {
    let e = err("runs where status = hide");
    assert!(e.message.contains("archive.reason"), "{e}");
}

#[devkit::test]
fn every_written_archive_reason_can_be_asked_for() {
    for reason in <crate::ArchiveReason as crate::DbCheck>::VALUES {
        q(&format!("archive where reason = {reason}"));
    }
    err("archive where reason = unknown_reason");
}

#[devkit::test]
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

#[devkit::test]
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

#[devkit::test]
fn a_bare_duration_with_an_ordering_is_refused() {
    let e = err("events where when > 7d");
    assert!(e.message.contains("since 7d"), "{e}");
}

#[devkit::test]
fn payload_paths_use_their_schema_types() {
    let query = q("events where payload.exit_code > 0 and payload.url ~ github");
    let Some(Expr::And(a, b)) = query.filter else {
        panic!()
    };
    assert!(
        matches!(*a, Expr::Compare { lhs: Operand::Field(FieldRef::TypedPayload { path: ref p, ty: FieldType::Integer }), rhs: Value::Integer(0), .. } if p == &["exit_code"])
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

#[devkit::test]
fn lists_and_exists() {
    let query =
        q("events where event_type = [command.executed, command.imported] and not material exists");
    let Some(Expr::And(a, b)) = query.filter else {
        panic!()
    };
    assert!(matches!(*a, Expr::Compare { rhs: Value::List(ref l), .. } if l.len() == 2));
    assert!(matches!(*b, Expr::Not(ref inner) if matches!(**inner, Expr::Exists(_))));
}

#[devkit::test]
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

#[devkit::test]
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
    let query = q("commands during (intervals where state = focused)");
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

#[devkit::test]
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
    assert!(
        err("commands group by cwd fill")
            .message
            .contains("time-bucket")
    );
    let query = q("commands count");
    assert!(matches!(&query.stages[0], Stage::Group(g) if g.keys.is_empty()));
}

#[devkit::test]
fn as_of_takes_a_time_or_a_commit_position() {
    let query = q("commands where when > 2026-02-01 as of 2026-03-01");
    assert!(matches!(
        query.stages.last(),
        Some(Stage::AsOf(AsOf::Time(_)))
    ));
    let query = q("events as of seq 42");
    assert_eq!(query.stages.last(), Some(&Stage::AsOf(AsOf::Seq(42))));
}

#[devkit::test]
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
    let r = parse_reference(&format!("sinex:record/{id}/120/command.executed/0")).unwrap();
    assert!(matches!(
        r,
        Reference::Record {
            offset: 120,
            output: Some(0),
            ..
        }
    ));
    let r = parse_reference("sinex:key/terminal.atuin/command.executed/a/b").unwrap();
    assert!(matches!(r, Reference::Key { ref key, .. } if key == "a/b"));
    assert!(parse_reference("sinex:at/2026-09-14/session").is_err());
}

#[devkit::test]
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
    assert!(matches!(
        q("recall mark").source,
        Source::RecallMark { number: 1, .. }
    ));
}

#[devkit::test]
fn reserved_words_must_be_quoted_to_search() {
    assert!(err("sort algorithm").message.contains("quote"));
    assert!(q(r#""sort" algorithm"#).ranked);
}

#[devkit::test]
fn reads_back_as_a_sentence() {
    let s = read_as(&q("commands where exit_code != 0 sort when desc limit 5"));
    assert_eq!(
        s,
        "commands, each once however many shells saw it where exit code is not 0, sorted by time it happened descending, the first 5, as currently recorded"
    );
    let s = read_as(&q("tide"));
    assert!(s.contains("most relevant first"), "{s}");
}

#[devkit::test]
fn errors_render_with_a_caret() {
    let text = "commands where exit_cod = 0";
    let e = err(text);
    let rendered = e.render(text);
    assert!(
        rendered.ends_with(&format!("{}^", " ".repeat(15))),
        "{rendered}"
    );
}

#[devkit::test]
fn every_field_name_is_unique_within_its_table() {
    for table in Table::ALL {
        let mut names: Vec<&str> = table.fields().iter().map(|f| f.name).collect();
        names.sort_unstable();
        let before = names.len();
        names.dedup();
        assert_eq!(before, names.len(), "{}", table.name());
        for d in table.default_fields() {
            assert!(table.field(d).is_some(), "{}: default {d}", table.name());
        }
        assert!(table.field(table.main_time()).is_some());
    }
}

mod sessions;

#[devkit::test]
fn every_alias_reads_registered_event_types() {
    for alias in ALIASES {
        for kind in alias.kinds {
            assert!(
                crate::event_types::find(kind).is_some(),
                "{} reads {kind}, which no type declares",
                alias.name
            );
        }
    }
}

mod gaps;

#[devkit::test]
fn record_citations_preserve_wide_unsigned_output_ordinals() {
    let material = uuid::Uuid::nil();
    for output in [
        0,
        32_768,
        65_536,
        u64::try_from(i64::MAX).unwrap(),
        u64::MAX,
    ] {
        let citation = format!("sinex:record/{material}/120/command.imported/{output}");
        assert!(matches!(parse_reference(&citation).unwrap(),
            Reference::Record {offset:120, output:Some(actual),..} if actual==output));
    }
    for output in ["-1", "18446744073709551616"] {
        assert!(
            parse_reference(&format!(
                "sinex:record/{material}/120/command.imported/{output}"
            ))
            .is_err()
        );
    }
}

#[devkit::test]
fn shared_until_dates_and_explicit_comparisons_keep_distinct_boundaries() {
    let until = q("events until 2026-09-14");
    let before = q("events where when < 2026-09-14");
    let get = |q: Query| match q.filter.or_else(|| {
        q.stages.into_iter().find_map(|s| {
            if let Stage::Where(e) = s {
                Some(e)
            } else {
                None
            }
        })
    }) {
        Some(Expr::Compare {
            op: Op::Lt,
            rhs: Value::Time(t),
            ..
        }) => t,
        _ => panic!("exclusive upper time bound"),
    };
    assert_eq!(get(until), ts("2026-09-15T02:00:00Z"));
    assert_eq!(get(before), ts("2026-09-14T02:00:00Z"));
    assert_eq!(
        get(q("events until 2026-09-14T10:00:00Z")),
        ts("2026-09-14T10:00:00Z")
    );
}
#[devkit::test]
fn shared_similarity_directives_cannot_change_filter_membership() {
    let id = "00000000-0000-0000-0000-000000000001";
    let query = q(&format!(
        "events where source = synthetic and similar:sinex:ev/{id}"
    ));
    assert!(query.ranked);
    assert_eq!(
        query.similar_reference().unwrap(),
        Some(id.parse().unwrap())
    );
    assert!(matches!(
        query.filter.unwrap().predicate(),
        Some(Expr::Compare { .. })
    ));
    for text in [
        format!("events where source = synthetic or similar:sinex:ev/{id}"),
        format!("events where not similar:sinex:ev/{id}"),
        "events where similar:unprovided-text".into(),
    ] {
        assert!(bind(&text, &ctx()).is_err(), "{text}");
    }
}

#[devkit::test]
fn shared_repeated_groups_bind_aggregates_to_their_incoming_relation() {
    let query = q("events group by source count group by count sum(count)");
    let Stage::Group(group) = &query.stages[1] else {
        panic!("second group")
    };
    assert!(matches!(&group.aggregates[0].field,Some(FieldRef::Group(name)) if name=="count"));
}

#[devkit::test]
fn shared_match_unknown_and_any_keep_their_explicit_predicate_meanings() {
    assert!(matches!(
        q("events where when:unknown").filter,
        Some(Expr::Not(_))
    ));
    assert!(matches!(
        q("events where when:*").filter,
        Some(Expr::Truth {
            value: query_language::semantics::Truth::True,
            ..
        })
    ));
    assert!(matches!(
        q("events where when = unknown").filter,
        Some(Expr::Compare {
            rhs: Value::Unknown,
            ..
        })
    ));
}
