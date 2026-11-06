use super::*;

fn context() -> Context {
    Context::utc("2026-09-14T12:00:00Z".parse().unwrap())
}

#[devkit::test]
fn native_subjects_lower_without_entering_the_shared_query_grammar() {
    let ctx = context();
    let template = parse(
        "recall last where cwds ^ \"/src/tide (notes)\" | limit 2",
        &ctx,
    )
    .unwrap();
    let bound = template.bind(&ctx).unwrap();
    assert!(matches!(
        bound.source,
        Source::RecallLast { filter: Some(_) }
    ));
    assert!(matches!(bound.stages.as_slice(), [Stage::Limit(2)]));
    assert!(super::super::bind("recall last", &ctx).is_err());
    let mark = parse("recall mark 2 ±5m", &ctx)
        .unwrap()
        .bind(&ctx)
        .unwrap();
    assert!(matches!(
        mark.source,
        Source::RecallMark {
            number: 2,
            window: 300.0
        }
    ));
    let day = parse("recall yesterday", &ctx).unwrap().bind(&ctx).unwrap();
    let Source::Recall { from, to } = day.source else {
        panic!("expected window")
    };
    assert_eq!(from.to_string(), "2026-09-13T00:00:00Z");
    assert_eq!(to.to_string(), "2026-09-14T00:00:00Z");
}

#[devkit::test]
fn saved_execution_uses_the_template_and_keeps_authored_text_as_evidence() {
    let ctx = context();
    let mut saved =
        saved::SavedQuery::from_text("remember".into(), "recall last".into(), vec![], &ctx)
            .unwrap();
    saved.query = "The authored text is retained independently".into();
    let definitions = std::collections::BTreeMap::from([("remember".into(), saved)]);
    let bound = saved::bind_saved("query:remember", &ctx, &definitions).unwrap();
    assert!(matches!(bound.source, Source::RecallLast { .. }));
}

#[devkit::test]
fn template_serialization_keeps_parameter_values_out_of_query_structure() {
    let ctx = context();
    let saved = saved::SavedQuery::from_text(
        "remember".into(),
        "recall last where cwds ^ $root".into(),
        vec![saved::Parameter {
            name: "root".into(),
            field_type: saved::ParameterType::Text,
        }],
        &ctx,
    )
    .unwrap();
    saved.validate(&ctx).unwrap();
    let restored = serde_json::from_value(serde_json::to_value(&saved).unwrap()).unwrap();
    let definitions = std::collections::BTreeMap::from([("remember".into(), restored)]);
    let bound = saved::bind_saved(
        r#"query:remember(root="/src/tide OR source exists")"#,
        &ctx,
        &definitions,
    )
    .unwrap();
    let Source::RecallLast {
        filter: Some(Expr::Compare {
            rhs: Value::Text(root),
            ..
        }),
    } = bound.source
    else {
        panic!("expected one scalar session predicate")
    };
    assert_eq!(root, "/src/tide OR source exists");
}
