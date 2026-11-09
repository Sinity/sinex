use super::*;
use crate::query::saved::{Parameter, ParameterType, SavedQuery, bind_saved};
use std::collections::BTreeMap;

#[devkit::test]
fn payload_types_are_taken_from_registered_schemas_and_used_by_aggregates() {
    for text in [
        "events where event_type = command.executed AND payload.exit_code > x",
        "events where payload.exit_code > 0 AND event_type = command.executed | agg sum(payload.command)",
        "events where event_type = command.executed AND payload.no_such_field:known",
        "events where event_type = command.executed AND payload.exit_code ~ x",
        "events where event_type = command.executed | group by day(payload.exit_code)",
    ] {
        assert!(!err(text).not_built, "{text}");
    }
    let query = q(
        "events where payload.exit_code >= 0 AND event_type = command.executed | agg sum(payload.duration_ns)",
    );
    assert!(matches!(query.stages[0], Stage::Group(_)));
    let completions = complete("events where event_type = command.executed AND payload.dur");
    assert_eq!(
        completions
            .iter()
            .map(|f| f.name.as_str())
            .collect::<Vec<_>>(),
        ["payload.duration_ns"]
    );
    assert_eq!(completions[0].field_type, "integer");
    for text in [
        "events where glob(event_type, \"command.*\") = true AND payload.dur",
        "events where event_type = (command.executed |command.fused) AND payload.dur",
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
    let saved = SavedQuery::from_text(
        "by-directory".into(),
        "commands where cwd = $root AND exit_code >= $code".into(),
        vec![
            Parameter {
                name: "root".into(),
                field_type: ParameterType::Text,
            },
            Parameter {
                name: "code".into(),
                field_type: ParameterType::Integer,
            },
        ],
        &ctx(),
    )
    .unwrap();
    saved.validate(&ctx()).unwrap();
    let definitions = BTreeMap::from([(saved.name.clone(), saved)]);
    let bound = bind_saved(
        r#"query:by-directory(root="x\" or source exists", code=1) | count"#,
        &ctx(),
        &definitions,
    )
    .unwrap();
    let Some(Expr::And(a, b)) = &bound.filter else {
        panic!()
    };
    let terms = [&**a, &**b];
    assert!(terms.iter().any(|term| matches!(term,
        Expr::Compare { lhs: Operand::Field(FieldRef::Field(field)), op: Op::Eq, rhs: Value::Text(value) }
        if field.name == "cwd" && value == "x\" or source exists"
    )));
    assert!(terms.iter().any(|term| matches!(term,
        Expr::Compare { lhs: Operand::Field(FieldRef::Field(field)), op: Op::Ge, rhs: Value::Text(value) }
        if field.name == "exit_code" && field.ty == FieldType::Integer && value == "1"
    )));
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
        "commands where when in last 3d | group by day(when), cwd | count | fill | where count >= 1 | sort by day",
    );
    let Stage::Group(g) = &query.stages[0] else {
        panic!()
    };
    let fill = g.fill.as_ref().unwrap();
    assert_eq!(fill.to, ctx().now);
    assert_eq!(fill.to.as_second() - fill.from.as_second(), 3 * 86400);
    assert!(read_as(&query).contains("empty time buckets"));
    assert!(
        err("commands | group by day(when) | count | fill")
            .message
            .contains("lower time bound")
    );
    assert!(
        err("commands where since:7d | group by cwd | count | fill")
            .message
            .contains("time bucket")
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
        native_q("recall mark 2 ±5m").source,
        Source::RecallMark {
            number: 2,
            window: 300.0
        }
    ));
    assert!(native_err("recall mark 0").message.contains("one"));
}

#[devkit::test]
fn parameters_do_not_stand_for_fields_and_text_parameters_stay_text() {
    let saved = SavedQuery::from_text(
        "typed".into(),
        "commands where payload.command = $value".into(),
        vec![Parameter {
            name: "value".into(),
            field_type: ParameterType::Text,
        }],
        &ctx(),
    )
    .unwrap();
    saved.validate(&ctx()).unwrap();
    let definitions = BTreeMap::from([("typed".into(), saved.clone())]);
    let bound = bind_saved("query:typed(value=123)", &ctx(), &definitions).unwrap();
    assert!(matches!(bound.filter,Some(Expr::Compare { rhs:Value::Text(ref s),.. }) if s == "123"));
    for query in [
        "events | fields id,$value",
        "events | sort by when,$value",
        "events where $value = x",
        "query:$value",
    ] {
        assert!(
            SavedQuery::from_text(
                saved.name.clone(),
                query.into(),
                saved.parameters.clone(),
                &ctx()
            )
            .and_then(|invalid| invalid.validate(&ctx()))
            .is_err(),
            "{query}"
        );
    }
    let mut enumeration = saved;
    enumeration.query = "events where class = $value".into();
    enumeration.template = native::parse(&enumeration.query, &ctx()).unwrap();
    enumeration.validate(&ctx()).unwrap();
    let definitions = BTreeMap::from([("typed".into(), enumeration)]);
    assert!(bind_saved("query:typed(value=open)", &ctx(), &definitions).is_ok());
    assert!(bind_saved("query:typed(value=unknown)", &ctx(), &definitions).is_err());
}

#[devkit::test]
fn unrepresentable_time_ranges_are_errors_without_panicking() {
    for text in [
        "events where since:99999999999999999999999999999d",
        "recall 9999-12-30T12:00:00Z ±999999999d",
        "events where when in last -9223372036854775808 days",
    ] {
        assert!(
            native::parse(text, &ctx())
                .and_then(|template| template.bind(&ctx()))
                .is_err(),
            "{text}"
        );
    }
}

#[devkit::test]
fn each_boolean_branch_uses_its_selected_event_schema() {
    let text = "events where (event_type = metric.figure AND payload.value > 0) OR (event_type = library.curation AND payload.key = synthetic)";
    assert!(bind(text, &ctx()).is_ok());
    assert!(err("events where (event_type = metric.figure AND payload.value = synthetic) OR (event_type = library.curation AND payload.key = synthetic)").message.contains("number"));
}

#[devkit::test]
fn event_type_globs_select_the_schemas_used_for_payload_fields() {
    assert!(
        err("events where event_type = metric.* AND payload.value > 0")
            .message
            .contains("payload.value")
    );
    assert!(
        bind(
            r#"events where glob(event_type, "metric.*") = true AND payload.value > 0"#,
            &ctx()
        )
        .is_ok()
    );
    assert!(
        bind(
            r#"events where (glob(event_type, "metric.*") = true OR event_type = health.figure) | agg sum(payload.numerator)"#,
            &ctx()
        )
        .is_ok()
    );
}

#[devkit::test]
fn recall_last_payload_conditions_use_the_session_schema() {
    let query = native_q("recall last where payload.host = synthetic");
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
        native_err("recall last where payload.command = synthetic")
            .message
            .contains("not declared")
    );
}

#[devkit::test]
fn text_list_fields_compare_members_without_treating_the_array_as_json_text() {
    for condition in [
        "payload.cwds ~ tide",
        "payload.cwds ^ /src",
        "payload.cwds = (/src/tide |/src/other)",
        "payload.cwds != /src/tide",
    ] {
        assert!(
            bind(
                &format!("events where event_type = shell.session AND {condition}"),
                &ctx()
            )
            .is_ok(),
            "{condition}"
        );
    }
    assert!(
        err("events where event_type = shell.session AND payload.cwds > x")
            .message
            .contains("ordered")
    );
    let choices = complete("events where event_type = shell.session AND payload.cw");
    assert_eq!(choices[0].field_type, "textlist");
}

#[devkit::test]
fn min_and_max_refuse_payload_values_without_an_ordered_scalar_aggregate() {
    for query in [
        "events where event_type = shell.session | agg min(payload.cwds)",
        "events where event_type = shell.session | agg max(payload.cwds)",
    ] {
        assert!(err(query).message.contains("ordered scalar"));
    }
}

#[devkit::test]
fn saved_definition_errors_do_not_point_inside_unicode_arguments() {
    let saved = SavedQuery::from_text(
        "bad".into(),
        "events where source = $source AND when < not-a-time".into(),
        vec![Parameter {
            name: "source".into(),
            field_type: ParameterType::Text,
        }],
        &ctx(),
    )
    .unwrap();
    let definitions = BTreeMap::from([("bad".into(), saved)]);
    let invocation = format!("query:bad(source=\"{}\")", "界".repeat(40));
    let error = bind_saved(&invocation, &ctx(), &definitions).unwrap_err();
    assert!(error.at.is_none());
    assert_eq!(error.render(&invocation), error.message);
    let direct = "events where when < not-a-time";
    let error = bind_saved(direct, &ctx(), &definitions).unwrap_err();
    assert!(error.at.is_some());
    assert!(error.render(direct).contains('^'));
    let error = QueryError::at(1, "synthetic error");
    assert_eq!(error.render("界"), "synthetic error");
}

#[devkit::test]
fn recall_fill_bounds_apply_only_to_when_and_intersect_row_time_bounds() {
    let query = native_q(
        "recall sinex:window/2026-09-13T04:00:00Z/2026-09-16T04:00:00Z where when >= 2026-09-14T04:00:00Z AND when < 2026-09-15T04:00:00Z | group by day(when) | count | fill",
    );
    let Stage::Group(group) = query.stages.last().unwrap() else {
        panic!()
    };
    let fill = group.fill.as_ref().unwrap();
    assert_eq!(fill.from, ts("2026-09-14T04:00:00Z"));
    assert_eq!(fill.to, ts("2026-09-15T04:00:00Z"));
    assert!(
        native_err("recall yesterday | group by day(seen) | count | fill")
            .message
            .contains("lower time bound")
    );
}

#[devkit::test]
fn saved_query_names_use_the_same_tokens_as_the_query_parser() {
    for text in [
        " QUERY : by-directory(root=x) ",
        "QUERY:by-directory(root=x)",
        "query :by-directory(root=x)",
    ] {
        assert_eq!(
            crate::query::saved::reference_name(text),
            Some("by-directory".into())
        );
    }
    assert_eq!(
        crate::query::saved::reference_name("events where source = synthetic"),
        None
    );
}

#[devkit::test]
fn saved_definitions_check_fields_and_stages_before_arguments_are_supplied() {
    for query in [
        "events where nonexistent = $value",
        "events where source = $value | fields nonexistent",
        "events where source = $value | sort by nonexistent",
        "events where source = $value | group by nonexistent | count",
        "events where event_type = command.executed AND payload.nonexistent = $value",
        "events where event_type = command.executed AND payload.exit_code = $value",
        "events where source = $value | group by source | count | where nonexistent > 0",
    ] {
        let saved = SavedQuery::from_text(
            "invalid".into(),
            query.into(),
            vec![Parameter {
                name: "value".into(),
                field_type: ParameterType::Text,
            }],
            &ctx(),
        );
        assert!(
            saved.and_then(|saved| saved.validate(&ctx())).is_err(),
            "{query}"
        );
    }
    let saved = SavedQuery::from_text(
        "between".into(),
        "events where when >= $start AND when < $end".into(),
        vec![
            Parameter {
                name: "start".into(),
                field_type: ParameterType::Time,
            },
            Parameter {
                name: "end".into(),
                field_type: ParameterType::Time,
            },
        ],
        &ctx(),
    )
    .unwrap();
    saved.validate(&ctx()).unwrap();
}

#[devkit::test]
fn saved_queries_can_invoke_an_author_qualified_definition() {
    let saved = SavedQuery::from_text(
        "shared".into(),
        "events where source = $source | count".into(),
        vec![Parameter {
            name: "source".into(),
            field_type: ParameterType::Text,
        }],
        &ctx(),
    )
    .unwrap();
    let definitions = BTreeMap::from([("shared@operator".into(), saved)]);
    assert!(
        bind_saved(
            "query:shared@operator(source=synthetic)",
            &ctx(),
            &definitions
        )
        .is_ok()
    );
    let indirect = SavedQuery::from_text(
        "indirect".into(),
        "query:shared@operator(source=$source) | fields nonexistent".into(),
        vec![Parameter {
            name: "source".into(),
            field_type: ParameterType::Text,
        }],
        &ctx(),
    )
    .unwrap();
    assert!(indirect.validate_with(&ctx(), &definitions).is_err());
    assert_eq!(
        crate::query::saved::reference_name("query:shared@operator"),
        Some("shared@operator".into())
    );
}

#[devkit::test]
fn capitalized_words_with_colons_are_search_text() {
    assert!(matches!(q(r#""TODO:""#).filter, Some(Expr::Words(_))));
    assert!(bind("TODO:", &ctx()).is_err());
    let error = err("events where Source:synthetic");
    assert!(error.message.contains("unknown field `Source`"), "{error}");
    assert!(bind("events where source:synthetic", &ctx()).is_ok());
}

#[devkit::test]
fn saved_text_parameters_validate_in_constrained_scalar_positions() {
    for query in [
        "events where id = $value",
        "events where material = $value",
        "events where parents = $value",
        "events where day(when) = $value",
        "events where DAY(when) = $value",
        "events where week(when) = $value",
        "events where weekday(when) = $value",
        "events where month(when) = $value",
        "events where month_day(when) = $value",
        "events where event_type = $value | fields payload.command",
        "events where event_type = metric.figure AND payload.grain = $value",
    ] {
        let saved = SavedQuery::from_text(
            "constrained".into(),
            query.into(),
            vec![Parameter {
                name: "value".into(),
                field_type: ParameterType::Text,
            }],
            &ctx(),
        )
        .unwrap();
        saved
            .validate(&ctx())
            .unwrap_or_else(|error| panic!("{query}: {error}"));
    }
    let saved = SavedQuery::from_text(
        "overlap".into(),
        "events where overlaps $start .. $end".into(),
        vec![
            Parameter {
                name: "start".into(),
                field_type: ParameterType::Time,
            },
            Parameter {
                name: "end".into(),
                field_type: ParameterType::Time,
            },
        ],
        &ctx(),
    )
    .unwrap();
    saved.validate(&ctx()).unwrap();
    let saved = SavedQuery::from_text(
        "position".into(),
        "events | as of seq $position".into(),
        vec![Parameter {
            name: "position".into(),
            field_type: ParameterType::Integer,
        }],
        &ctx(),
    )
    .unwrap();
    saved.validate(&ctx()).unwrap();
}

#[devkit::test]
fn saved_literal_arguments_are_checked_without_placeholder_values() {
    let base = SavedQuery::from_text(
        "base".into(),
        "events where class = $value".into(),
        vec![Parameter {
            name: "value".into(),
            field_type: ParameterType::Text,
        }],
        &ctx(),
    )
    .unwrap();
    let definitions = BTreeMap::from([("base".into(), base)]);
    let mut saved = SavedQuery::from_text(
        "literal".into(),
        "query:base(value=unknown)".into(),
        vec![],
        &ctx(),
    )
    .unwrap();
    assert!(saved.validate_with(&ctx(), &definitions).is_err());
    saved.query = "query:base(value=guarded)".into();
    saved.template = native::parse(&saved.query, &ctx()).unwrap();
    saved.validate_with(&ctx(), &definitions).unwrap();
}

#[devkit::test]
fn nested_parameters_take_examples_from_the_field_that_uses_them() {
    let base = SavedQuery::from_text(
        "base".into(),
        "commands where cwd ^ $class".into(),
        vec![Parameter {
            name: "class".into(),
            field_type: ParameterType::Text,
        }],
        &ctx(),
    )
    .unwrap();
    let saved = SavedQuery::from_text(
        "nested".into(),
        "query:base(class=$class)".into(),
        base.parameters.clone(),
        &ctx(),
    )
    .unwrap();
    saved
        .validate_with(&ctx(), &BTreeMap::from([("base".into(), base)]))
        .unwrap();
}

#[devkit::test]
fn event_type_parameters_use_fields_from_their_own_question() {
    for query in [
        r#"events where event_type = $kind AND payload.command ~ "payload.absent""#,
        "events where event_type = $kind AND payload.command ~ payload.absent",
        "events where event_type = $kind AND payload.command:known | join near (events where event_type = state.interval AND payload.subject_id:known) within 1h",
        "events where event_type = command.executed AND payload.command:known | join near (events where event_type = $kind AND payload.subject_id:known) within 1h",
    ] {
        let saved = SavedQuery::from_text(
            "by-kind".into(),
            query.into(),
            vec![Parameter {
                name: "kind".into(),
                field_type: ParameterType::Text,
            }],
            &ctx(),
        )
        .unwrap();
        saved
            .validate(&ctx())
            .unwrap_or_else(|error| panic!("{query}: {error}"));
    }
}

#[devkit::test]
fn typed_saved_arguments_keep_their_type_when_quoted() {
    for (query, declaration, value) in [
        (
            "events where event_type = command.executed AND payload.exit_code = $value",
            ParameterType::Integer,
            "1",
        ),
        (
            "events where event_type = browser.url AND payload.hidden = $value",
            ParameterType::Bool,
            "true",
        ),
    ] {
        let saved = SavedQuery::from_text(
            "typed".into(),
            query.into(),
            vec![Parameter {
                name: "value".into(),
                field_type: declaration,
            }],
            &ctx(),
        )
        .unwrap();
        let definitions = BTreeMap::from([("typed".into(), saved)]);
        let invocation = format!("query:typed(value=\"{value}\")");
        bind_saved(&invocation, &ctx(), &definitions)
            .unwrap_or_else(|error| panic!("{query}: {error}"));
    }
}

#[devkit::test]
fn event_type_parameters_check_each_invocation_and_require_one_coherent_schema() {
    let saved = SavedQuery::from_text(
        "by-kind".into(),
        "events where event_type = $kind AND payload.command ~ synthetic".into(),
        vec![Parameter {
            name: "kind".into(),
            field_type: ParameterType::Text,
        }],
        &ctx(),
    )
    .unwrap();
    saved.validate(&ctx()).unwrap();
    let definitions = BTreeMap::from([("by-kind".into(), saved.clone())]);
    assert!(bind_saved("query:by-kind(kind=command.executed)", &ctx(), &definitions).is_ok());
    assert!(bind_saved("query:by-kind(kind=window.focused)", &ctx(), &definitions).is_err());
    let invalid = SavedQuery::from_text(
        saved.name.clone(),
        "events where event_type = $kind AND payload.never_declared:known".into(),
        saved.parameters.clone(),
        &ctx(),
    );
    assert!(
        invalid
            .and_then(|invalid| invalid.validate(&ctx()))
            .is_err()
    );
}
