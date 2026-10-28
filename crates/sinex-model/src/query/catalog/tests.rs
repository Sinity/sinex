use super::*;
use crate::query::{Expr, FieldRef, FieldType, Stage, Value};
use serde_json::json;

fn declaration(schema: Json) -> KindDeclaration {
    let mut d = KindDeclaration::schemaless(
        KindRef {
            name: "example.reading".into(),
            version: 9,
        },
        "example".into(),
    );
    d.payload_schema = Some(schema);
    d
}

#[test]
fn local_refs_unions_and_recursive_fields_have_owned_descriptions() {
    let d = declaration(json!({"type":"object","properties":{
        "at":{"type":["string","null"],"format":"date-time"},
        "node":{"$ref":"#/$defs/Node"},
        "mixed":{"anyOf":[{"type":"string"},{"type":"number"}]},
        "names":{"type":"array","items":{"type":"string"}}
    },"$defs":{"Node":{"type":"object","properties":{"label":{"type":"string"},"next":{"$ref":"#/$defs/Node"}}}}}));
    let catalog = QueryCatalog::new([d]);
    assert_eq!(catalog.fields["at"].ty, DataType::Time);
    assert!(catalog.fields["at"].nullable);
    assert_eq!(catalog.fields["node.label"].ty, DataType::Text);
    assert_eq!(catalog.fields["node.next"].ty, DataType::Json);
    assert_eq!(catalog.fields["mixed"].ty, DataType::Json);
    assert_eq!(
        catalog.fields["names"].ty,
        DataType::Array(Box::new(DataType::Text))
    );
}

#[test]
fn differing_versions_and_schemaless_values_stay_json() {
    let a = declaration(json!({"type":"object","properties":{"v":{"type":"string"}}}));
    let mut b = declaration(json!({"type":"object","properties":{"v":{"type":"integer"}}}));
    b.kind.version = 10;
    assert_eq!(
        QueryCatalog::new([a.clone(), b]).fields["v"].ty,
        DataType::Json
    );
    let mut historical = KindDeclaration::historical(a.kind.name.clone());
    historical.kind.version = 1;
    assert_eq!(
        QueryCatalog::new([a, historical]).fields["v"].ty,
        DataType::Json
    );
}

#[test]
fn literal_dots_in_properties_do_not_replace_nested_paths() {
    let d = declaration(
        json!({"type":"object","properties":{"a.b":{"type":"string"},"a":{"type":"object","properties":{"b":{"type":"integer"}}}}}),
    );
    let catalog = QueryCatalog::new([d]);
    assert_eq!(catalog.field(&["a.b".into()]).unwrap().ty, DataType::Text);
    assert_eq!(
        catalog.field(&["a".into(), "b".into()]).unwrap().ty,
        DataType::Integer
    );
}

#[test]
fn declared_times_use_the_shared_relative_clock() {
    let d = declaration(
        json!({"type":"object","properties":{"at":{"type":"string","format":"date-time"}}}),
    );
    let mut ctx = Context::utc("2026-10-27T15:00:00Z".parse().unwrap());
    ctx.catalog = Some(QueryCatalog::new([d]));
    for text in [
        "events where payload.at >= yesterday",
        "events where payload.at >= 2026-10-27 12:00",
    ] {
        let query = super::super::bind(text, &ctx).unwrap();
        assert!(matches!(
            query.filter,
            Some(Expr::Compare {
                rhs: Value::Time(_),
                ..
            })
        ));
    }
}

#[test]
fn references_keep_sibling_fields_and_missing_versions_are_nullable() {
    let a = declaration(
        json!({"$ref":"#/$defs/Base","$defs":{"Base":{"type":"object","properties":{"base":{"type":"integer"}},"required":["base"]}},"properties":{"extra":{"type":"string"}},"required":["extra"]}),
    );
    let mut b = declaration(
        json!({"type":"object","properties":{"base":{"type":"integer"}},"required":["base"]}),
    );
    b.kind.version = 10;
    let catalog = QueryCatalog::new([a, b]);
    assert_eq!(catalog.fields["base"].ty, DataType::Integer);
    assert!(!catalog.fields["base"].nullable);
    assert_eq!(catalog.fields["extra"].ty, DataType::Text);
    assert!(catalog.fields["extra"].nullable);
}

#[test]
fn declared_non_time_fields_refuse_time_operations_before_sql() {
    let mut ctx = Context::utc("2026-10-27T15:00:00Z".parse().unwrap());
    ctx.catalog = Some(QueryCatalog::new([declaration(
        json!({"type":"object","properties":{"label":{"type":"string"}}}),
    )]));
    for query in [
        "events where day(payload.label) = 27",
        "events where payload.label in today",
        "events where payload.label - ts_orig > 1h",
    ] {
        assert!(super::super::bind(query, &ctx).is_err(), "{query}");
    }
}

#[test]
fn each_query_and_join_binds_to_its_selected_saved_kinds() {
    let mut text = declaration(
        json!({"type":"object","properties":{"value":{"type":"string"}},"required":["value"]}),
    );
    text.kind.name = "example.text".into();
    let mut number = declaration(
        json!({"type":"object","properties":{"value":{"type":"integer"}},"required":["value"]}),
    );
    number.kind.name = "example.number".into();
    let mut ctx = Context::utc("2026-10-27T15:00:00Z".parse().unwrap());
    ctx.catalog = Some(QueryCatalog::new([text, number]));
    let query = crate::query::bind("events where event_type = example.text and payload.value = 7 near (events where event_type = example.number and payload.value = 7 fields payload.value) within 5m fields payload.value", &ctx).unwrap();
    assert!(query.stages.iter().any(|stage| matches!(stage, Stage::Fields(fields) if matches!(&fields[0], FieldRef::TypedPayload { ty: FieldType::Text, .. }))));
    let inner = query
        .stages
        .iter()
        .find_map(|stage| {
            if let Stage::Join(join) = stage {
                Some(&join.inner)
            } else {
                None
            }
        })
        .unwrap();
    assert!(inner.stages.iter().any(|stage| matches!(stage, Stage::Fields(fields) if matches!(&fields[0], FieldRef::TypedPayload { ty: FieldType::Integer, .. }))));
    assert!(
        crate::query::bind(
            "events where event_type = example.number and payload.value = wrong",
            &ctx
        )
        .is_err()
    );
}

#[test]
fn schema_binding_rechecks_programmatic_payload_fields() {
    let first = declaration(json!({"type":"object","properties":{"value":{"type":"string"}}}));
    let mut ctx = Context::utc("2026-10-27T15:00:00Z".parse().unwrap());
    ctx.catalog = Some(QueryCatalog::new([first]));
    let mut query = crate::query::bind(
        "events where event_type = example.reading fields payload.value",
        &ctx,
    )
    .unwrap();
    let next = declaration(json!({"type":"object","properties":{"value":{"type":"integer"}}}));
    let catalog = QueryCatalog::new([next]);
    catalog.bind(&mut query, &ctx).unwrap();
    assert!(
        matches!(&query.stages[0], Stage::Fields(fields) if matches!(&fields[0], FieldRef::TypedPayload { ty: FieldType::Integer, .. }))
    );
}

#[test]
fn integer_and_number_unions_keep_numeric_fields_and_array_items() {
    let catalog = QueryCatalog::new([declaration(json!({"type":"object","properties":{
        "value":{"anyOf":[{"type":"integer"},{"type":"number"}]},
        "values":{"type":"array","items":{"anyOf":[{"type":"integer"},{"type":"number"}]}}
    }}))]);
    assert_eq!(catalog.fields["value"].ty, DataType::Number);
    assert_eq!(
        catalog.fields["values"].ty,
        DataType::Array(Box::new(DataType::Number))
    );
    let mut ctx = Context::utc("2026-10-27T15:00:00Z".parse().unwrap());
    ctx.catalog = Some(catalog);
    assert!(
        crate::query::bind(
            "events where event_type = example.reading sum(payload.value)",
            &ctx
        )
        .is_ok()
    );
}
