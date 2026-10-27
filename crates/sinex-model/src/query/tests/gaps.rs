use super::*;
use crate::query::saved::{Parameter, ParameterType, SavedQuery, bind_saved};
use std::collections::BTreeMap;

#[devkit::test]
fn payload_types_are_taken_from_registered_schemas_and_used_by_aggregates() {
    for text in [
        "events where event_type = command.executed and payload.exit_code > x",
        "events where payload.exit_code > 0 and event_type = command.executed sum(payload.command)",
        "events where event_type = command.executed and payload.no_such_field exists",
        "events where event_type = command.executed and payload.exit_code ~ x",
        "events where event_type = command.executed group by day(payload.exit_code)",
    ] {
        assert!(!err(text).not_built, "{text}");
    }
    let query = q(
        "events where payload.exit_code >= 0 and event_type = command.executed sum(payload.duration_ns)",
    );
    assert!(matches!(query.stages[0], Stage::Group(_)));
    let completions = complete("events where event_type = command.executed and payload.dur");
    assert_eq!(
        completions
            .iter()
            .map(|f| f.name.as_str())
            .collect::<Vec<_>>(),
        ["payload.duration_ns"]
    );
    assert_eq!(completions[0].field_type, "integer");
    for text in [
        "events where event_type = command.* and payload.dur",
        "events where event_type = [command.executed, command.fused] and payload.dur",
    ] {
        assert!(
            complete(text)
                .iter()
                .any(|f| f.name == "payload.duration_ns"),
            "{text}"
        );
    }
    assert!(complete("materials payload.").is_empty());
}

#[devkit::test]
fn saved_parameters_cannot_introduce_conditions_or_change_the_question_shape() {
    let saved = SavedQuery {
        name: "by-directory".into(),
        query: "commands where cwd = $root and exit_code >= $code".into(),
        parameters: vec![
            Parameter {
                name: "root".into(),
                field_type: ParameterType::Text,
            },
            Parameter {
                name: "code".into(),
                field_type: ParameterType::Integer,
            },
        ],
    };
    saved.validate(&ctx()).unwrap();
    let definitions = BTreeMap::from([(saved.name.clone(), saved)]);
    let bound = bind_saved(
        r#"query:by-directory(root="x\" or source exists", code=1) count"#,
        &ctx(),
        &definitions,
    )
    .unwrap();
    let Some(Expr::And(a, _)) = bound.filter else {
        panic!()
    };
    assert!(
        matches!(*a, Expr::Compare { rhs:Value::Text(ref s),.. } if s == "x\" or source exists")
    );
    for text in [
        "query:by-directory(root=x)",
        "query:by-directory(root=x,code=no)",
        "query:by-directory(root=x,code=1,extra=x)",
        "query:by-directory(root=x,root=y,code=1)",
    ] {
        assert!(bind_saved(text, &ctx(), &definitions).is_err(), "{text}");
    }
}

#[devkit::test]
fn fill_resolves_bounds_and_keeps_later_group_filters() {
    let query = q(
        "commands where when in last 3d group by day(when), cwd count fill where count >= 1 sort day",
    );
    let Stage::Group(g) = &query.stages[0] else {
        panic!()
    };
    let fill = g.fill.as_ref().unwrap();
    assert_eq!(fill.to, ctx().now);
    assert_eq!(fill.to.as_second() - fill.from.as_second(), 3 * 86400);
    assert!(read_as(&query).contains("empty time buckets"));
    assert!(
        err("commands group by day(when) count fill")
            .message
            .contains("lower time bound")
    );
    assert!(
        err("commands since 7d group by cwd count fill")
            .message
            .contains("time-bucket")
    );
}

#[devkit::test]
fn time_citations_require_absolute_time_and_marks_have_positive_numbers() {
    assert!(matches!(
        parse_reference("sinex:at/2026-09-14T08:00:00Z/shell.session").unwrap(),
        Reference::At { .. }
    ));
    assert!(parse_reference("sinex:at/yesterday/shell.session").is_err());
    assert!(matches!(
        parse_reference("sinex:window/2026-09-14T08:00:00Z/2026-09-14T09:00:00Z").unwrap(),
        Reference::Window { .. }
    ));
    assert!(parse_reference("sinex:window/2026-09-14T09:00:00Z/2026-09-14T08:00:00Z").is_err());
    assert!(matches!(
        q("recall mark 2 ±5m").source,
        Source::RecallMark {
            number: 2,
            window: 300.0
        }
    ));
    assert!(err("recall mark 0").message.contains("positive"));
}

#[devkit::test]
fn parameters_do_not_stand_for_fields_and_text_parameters_stay_text() {
    let saved = SavedQuery {
        name: "typed".into(),
        query: "events where payload.command = $value".into(),
        parameters: vec![Parameter {
            name: "value".into(),
            field_type: ParameterType::Text,
        }],
    };
    saved.validate(&ctx()).unwrap();
    let definitions = BTreeMap::from([("typed".into(), saved.clone())]);
    let bound = bind_saved("query:typed(value=123)", &ctx(), &definitions).unwrap();
    assert!(matches!(bound.filter,Some(Expr::Compare { rhs:Value::Text(ref s),.. }) if s == "123"));
    for query in [
        "events fields id,$value",
        "events sort when,$value",
        "events where $value = x",
        "query:$value",
    ] {
        let mut invalid = saved.clone();
        invalid.query = query.into();
        assert!(invalid.validate(&ctx()).is_err(), "{query}");
    }
    let mut enumeration = saved;
    enumeration.query = "events where class = $value".into();
    enumeration.validate(&ctx()).unwrap();
    let definitions = BTreeMap::from([("typed".into(), enumeration)]);
    assert!(bind_saved("query:typed(value=open)", &ctx(), &definitions).is_ok());
    assert!(bind_saved("query:typed(value=unknown)", &ctx(), &definitions).is_err());
}

#[devkit::test]
fn unrepresentable_time_ranges_are_errors_without_panicking() {
    for text in [
        "events since 99999999999999999999999999999d",
        "recall 9999-12-30T12:00:00Z ±999999999d",
        "events where when in last -9223372036854775808 days",
    ] {
        assert!(bind(text, &ctx()).is_err(), "{text}");
    }
}

#[devkit::test]
fn each_boolean_branch_uses_its_selected_event_schema() {
    let text = "events where (event_type = metric.figure and payload.value > 0) or (event_type = library.assignment and payload.value = synthetic)";
    assert!(bind(text, &ctx()).is_ok());
    assert!(err("events where (event_type = metric.figure and payload.value = synthetic) or (event_type = library.assignment and payload.value = synthetic)").message.contains("number"));
}

#[devkit::test]
fn event_type_globs_select_the_schemas_used_for_payload_fields() {
    assert!(
        bind(
            "events where event_type = metric.* and payload.value > 0",
            &ctx()
        )
        .is_ok()
    );
    assert!(
        bind(
            "events where event_type = [metric.*, health.figure] sum(payload.numerator)",
            &ctx()
        )
        .is_ok()
    );
}

#[devkit::test]
fn fill_recognizes_equivalent_finite_source_filters() {
    for filter in [
        "source = terminal.atuin",
        "source = [terminal.atuin]",
        "(source = terminal.atuin or source = terminal.atuin)",
        "source = [terminal.atuin, synthetic.other] and source = terminal.atuin",
    ] {
        let query = q(&format!(
            "events where {filter} and when in last 3d group by day(when) count fill"
        ));
        let Stage::Group(g) = &query.stages[0] else {
            panic!()
        };
        assert_eq!(
            g.fill.as_ref().unwrap().sources,
            ["terminal.atuin"],
            "{filter}"
        );
    }
    let query = q(
        "events where (source = terminal.atuin or event_type = command.executed) and when in last 3d group by day(when) count fill",
    );
    let Stage::Group(g) = &query.stages[0] else {
        panic!()
    };
    assert!(g.fill.as_ref().unwrap().sources.is_empty());
}

#[devkit::test]
fn recall_last_payload_conditions_use_the_session_schema() {
    let query = q("recall last where payload.host = synthetic");
    assert!(matches!(
        query.source,
        Source::RecallLast {
            filter: Some(Expr::Compare {
                lhs: Operand::Field(FieldRef::TypedPayload {
                    ty: FieldType::Text,
                    ..
                }),
                ..
            })
        }
    ));
    assert!(
        err("recall last where payload.command = synthetic")
            .message
            .contains("not declared")
    );
}

#[devkit::test]
fn text_list_fields_compare_members_without_treating_the_array_as_json_text() {
    for condition in [
        "payload.cwd ~ tide",
        "payload.cwd ^ /src",
        "payload.cwd = [/src/tide, /src/other]",
        "payload.cwd != /src/tide",
    ] {
        assert!(
            bind(
                &format!("events where event_type = polylogue.session and {condition}"),
                &ctx()
            )
            .is_ok(),
            "{condition}"
        );
    }
    assert!(
        err("events where event_type = polylogue.session and payload.cwd > x")
            .message
            .contains("ordered")
    );
    let choices = complete("events where event_type = polylogue.session and payload.cw");
    assert_eq!(choices[0].field_type, "textlist");
}

#[devkit::test]
fn min_and_max_refuse_payload_values_without_an_ordered_scalar_aggregate() {
    for query in [
        "events where event_type = polylogue.session min(payload.cwd)",
        "events where event_type = polylogue.session max(payload.cwd)",
    ] {
        assert!(err(query).message.contains("ordered scalar"));
    }
}

#[devkit::test]
fn saved_definition_errors_do_not_point_inside_unicode_arguments() {
    let saved = SavedQuery {
        name: "bad".into(),
        query: "events where source = $source and when < not-a-time".into(),
        parameters: vec![Parameter {
            name: "source".into(),
            field_type: ParameterType::Text,
        }],
    };
    let definitions = BTreeMap::from([("bad".into(), saved)]);
    let invocation = format!("query:bad(source=\"{}\")", "界".repeat(40));
    let error = bind_saved(&invocation, &ctx(), &definitions).unwrap_err();
    assert!(error.at.is_none());
    assert_eq!(error.render(&invocation), error.message);
    let error = QueryError::at(1, "synthetic error");
    assert_eq!(error.render("界"), "synthetic error");
}

#[devkit::test]
fn recall_fill_bounds_apply_only_to_when_and_intersect_row_time_bounds() {
    let query = q(
        "recall sinex:window/2026-09-13T04:00:00Z/2026-09-16T04:00:00Z where when >= 2026-09-14T04:00:00Z and when < 2026-09-15T04:00:00Z group by day(when) count fill",
    );
    let Stage::Group(group) = query.stages.last().unwrap() else {
        panic!()
    };
    let fill = group.fill.as_ref().unwrap();
    assert_eq!(fill.from, ts("2026-09-14T04:00:00Z"));
    assert_eq!(fill.to, ts("2026-09-15T04:00:00Z"));
    assert!(
        err("recall yesterday group by day(seen) count fill")
            .message
            .contains("lower time bound")
    );
}

#[devkit::test]
fn saved_query_names_use_the_same_tokens_as_the_query_parser() {
    assert_eq!(
        crate::query::saved::reference_name(" QUERY : by-directory(root=x) "),
        Some("by-directory".into())
    );
    assert_eq!(
        crate::query::saved::reference_name("events where source = synthetic"),
        None
    );
}
