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
    assert_eq!((fill.to - fill.from).as_secs(), 3 * 86400);
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
    let saved = SavedQuery { name:"typed".into(),query:"events where payload.command = $value".into(),parameters:vec![Parameter { name:"value".into(),field_type:ParameterType::Text }] };
    saved.validate(&ctx()).unwrap();
    let definitions = BTreeMap::from([("typed".into(),saved.clone())]);
    let bound = bind_saved("query:typed(value=123)",&ctx(),&definitions).unwrap();
    assert!(matches!(bound.filter,Some(Expr::Compare { rhs:Value::Text(ref s),.. }) if s == "123"));
    for query in ["events fields id,$value","events sort when,$value","events where $value = x"] {
        let mut invalid = saved.clone();
        invalid.query = query.into();
        assert!(invalid.validate(&ctx()).is_err(),"{query}");
    }
    let mut enumeration = saved;
    enumeration.query = "events where class = $value".into();
    enumeration.validate(&ctx()).unwrap();
    let definitions = BTreeMap::from([("typed".into(),enumeration)]);
    assert!(bind_saved("query:typed(value=open)",&ctx(),&definitions).is_ok());
    assert!(bind_saved("query:typed(value=unknown)",&ctx(),&definitions).is_err());
}

#[devkit::test]
fn unrepresentable_time_ranges_are_errors_without_panicking() {
    for text in ["events since 99999999999999999999999999999d","recall 9999-12-30T12:00:00Z ±999999999d","events where when in last -9223372036854775808 days"] { assert!(bind(text,&ctx()).is_err(),"{text}"); }
}
