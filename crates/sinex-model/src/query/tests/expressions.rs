use super::*;

#[devkit::test]
fn duration_arithmetic_keeps_nanoseconds_beyond_float_precision() {
    let query = q("events | fields 1000000000.000000001s as elapsed");
    let Stage::Fields(columns) = &query.stages[0] else {
        panic!("projection")
    };
    assert!(matches!(columns[0].unnamed(), Operand::Literal {
        value: Value::Text(seconds), kind: query_language::catalog::FieldKind::Duration,
    } if seconds == "1000000000.000000001"));
}

#[devkit::test]
fn arithmetic_and_row_dependent_comparisons_keep_typed_operands() {
    let query = q("commands where exit_code = len(command)");
    assert!(matches!(query.filter, Some(Expr::Compare {
        lhs: Operand::Field(FieldRef::Field(field)),
        rhs: Value::Operand(ref right), ..
    }) if field.name == "exit_code" && matches!(&**right, Operand::Length(_))));

    let query = q("events | fields seen - when as latency | where latency > 1d");
    assert!(matches!(&query.stages[0], Stage::Fields(columns)
    if columns[0].column() == "latency"
    && matches!(columns[0].unnamed(), Operand::Arithmetic {
        operator: query_language::ast::Arithmetic::Subtract, ..
    })));
    assert!(matches!(&query.stages[1], Stage::Where(Expr::Compare {
        lhs: Operand::Field(FieldRef::TypedGroup(column)),
        rhs: Value::Text(seconds), ..
    }) if column.name() == "latency" && seconds == "86400"));
}

#[devkit::test]
fn expression_group_aliases_are_available_to_later_stages() {
    let query = q(
        "commands | group by cwd as directory | agg sum(duration + 1) as elapsed | where elapsed > 2 | fields directory, elapsed",
    );
    let Stage::Group(group) = &query.stages[0] else {
        panic!("group relation")
    };
    assert_eq!(group.keys[0].column(), "directory");
    assert_eq!(group.aggregates[0].column(), "elapsed");
    assert!(matches!(
        group.aggregates[0].argument.as_ref(),
        Some(Operand::Arithmetic { .. })
    ));
    assert!(matches!(&query.stages[1], Stage::Where(Expr::Compare {
        lhs: Operand::Field(FieldRef::TypedGroup(column)), ..
    }) if column.name() == "elapsed" && column.field_type() == Some(FieldType::Number)));
    assert!(
        err("commands | group by cwd as directory | fields cwd")
            .message
            .contains("cwd")
    );
}

#[devkit::test]
fn case_equal_preserves_its_value_argument_physical_type() {
    let query =
        q("commands | fields case_eq(exit_code, 0, id) as successful | where successful:known");
    let Stage::Fields(columns) = &query.stages[0] else {
        panic!("projection")
    };
    assert!(
        matches!(columns[0].unnamed(), Operand::CaseEqual { value, .. }
        if value.field_type().unwrap() == FieldType::Id)
    );
    assert!(matches!(&query.stages[1], Stage::Where(Expr::Exists(
        Operand::Field(FieldRef::TypedGroup(column))
    )) if column.field_type() == Some(FieldType::Id)));
}

#[devkit::test]
fn natural_matches_follow_the_field_vocabulary() {
    assert!(matches!(
        q("commands where command:build").filter,
        Some(Expr::Compare {
            op: Op::Contains,
            ..
        })
    ));
    assert!(matches!(
        q("commands where command = build").filter,
        Some(Expr::Compare { op: Op::Eq, .. })
    ));
    assert!(matches!(
        q("commands where cwd:/src/tide").filter,
        Some(Expr::Compare { op: Op::Prefix, .. })
    ));
    assert!(matches!(
        q("events where text:harbor").filter,
        Some(Expr::FieldWords { .. })
    ));
    assert!(matches!(
        q("events where when:known").filter,
        Some(Expr::Exists(_))
    ));
    assert!(matches!(
        q("events where when:yesterday").filter,
        Some(Expr::Within { .. })
    ));
}

#[devkit::test]
fn year_is_an_integer_and_native_commands_use_the_operation_boundary() {
    let query = q("events | fields year(when) as calendar_year | where calendar_year >= 2026");
    assert!(matches!(&query.stages[1], Stage::Where(Expr::Compare {
        lhs: Operand::Field(FieldRef::TypedGroup(column)), ..
    }) if column.field_type() == Some(FieldType::Integer)));
    for command in [
        "recall yesterday",
        "trace sinex:ev/0190a0b0-0000-7000-8000-000000000001",
    ] {
        assert!(bind(command, &ctx()).is_err(), "{command}");
        native_q(command);
    }
}

#[devkit::test]
fn saved_authored_evidence_does_not_replace_the_executable_template() {
    use crate::query::saved::{SavedQuery, bind_saved};
    use std::collections::BTreeMap;
    let mut definition = SavedQuery::from_text(
        "retained".into(),
        "events where source = synthetic".into(),
        vec![],
        &ctx(),
    )
    .unwrap();
    definition.query = "events where source = changed".into();
    let bound = bind_saved(
        "query:retained",
        &ctx(),
        &BTreeMap::from([("retained".into(), definition)]),
    )
    .unwrap();
    assert!(
        matches!(bound.filter, Some(Expr::Compare { rhs: Value::Text(ref value), .. }) if value == "synthetic")
    );
}

#[devkit::test]
fn glob_has_boolean_type_and_accepts_row_dependent_patterns() {
    let query = q("commands | fields glob(command, cwd) as matched | where matched = true");
    let Stage::Fields(columns) = &query.stages[0] else {
        panic!("projection")
    };
    assert!(matches!(columns[0].unnamed(), Operand::Glob { .. }));
    assert_eq!(columns[0].field_type().unwrap(), FieldType::Bool);
    assert!(
        err("commands | fields glob(exit_code, cwd)")
            .message
            .contains("text")
    );
}

#[devkit::test]
fn first_in_a_group_keeps_original_fields_and_physical_types() {
    let query = q("commands | sort command desc | group by cwd | first 1 | fields command, id");
    assert!(
        matches!(&query.stages[1], Stage::GroupFirst { keys, count:1 } if keys[0].column() == "cwd")
    );
    let Stage::Fields(columns) = &query.stages[2] else {
        panic!("projection")
    };
    assert_eq!(columns[1].field_type().unwrap(), FieldType::Id);
}

#[devkit::test]
fn matched_payload_fields_are_checked_against_the_inner_kind() {
    let error = err(
        "events | join (events where event_type = operator.mark) on id = id | fields match.payload.exit_code",
    );
    assert!(error.message.contains("exit_code"), "{}", error.message);
}

#[devkit::test]
fn each_join_checks_only_the_matched_columns_it_produces() {
    q(
        "events | join (commands) on id = id | fields id, match.command as first_command | join (events where event_type = operator.mark) on id = id | fields first_command, match.payload.text",
    );
}
