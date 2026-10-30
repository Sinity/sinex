use super::*;
use crate::kinds::KindDeclaration;
use serde_json::json;
use std::collections::BTreeMap;

fn declared_context() -> Context {
    let declaration = |name: &str, mixed: &str| {
        let mut d = KindDeclaration::schemaless(
            crate::KindRef {
                name: name.into(),
                version: 1,
            },
            "example".into(),
        );
        d.payload_schema = Some(json!({"type":"object","properties":{
            "text":{"type":"string"},
            "integer":{"type":"integer"},
            "number":{"type":"number"},
            "flag":{"type":"boolean"},
            "at":{"type":"string","format":"date-time"},
            "mixed":{"type":mixed},
            "opaque":{}
        }}));
        d
    };
    let mut context = ctx();
    context.catalog = Some(QueryCatalog::new([
        declaration("example.text", "string"),
        declaration("example.integer", "integer"),
    ]));
    context
}

fn field_types(query: &Query, name: &str) -> Vec<FieldType> {
    fn collect(expr: &Expr, name: &str, types: &mut Vec<FieldType>) {
        match expr {
            Expr::And(a, b) | Expr::Or(a, b) => {
                collect(a, name, types);
                collect(b, name, types);
            }
            Expr::Compare {
                lhs: Operand::Field(field),
                ..
            } if field.name() == name => {
                types.push(match field {
                    FieldRef::Field(field) => field.ty,
                    FieldRef::TypedPayload { ty, .. } => *ty,
                    FieldRef::TypedGroup(column) => {
                        column.field_type().expect("bound physical type")
                    }
                    _ => panic!("unbound field"),
                });
            }
            _ => {}
        }
    }
    let mut result = Vec::new();
    if let Some(expr) = &query.filter {
        collect(expr, name, &mut result);
    }
    for stage in &query.stages {
        if let Stage::Where(expr) = stage {
            collect(expr, name, &mut result);
        }
    }
    result
}

fn values<'a>(query: &'a Query, field: &str) -> Vec<&'a Value> {
    fn collect<'a>(expr: &'a Expr, field: &str, out: &mut Vec<&'a Value>) {
        match expr {
            Expr::And(a, b) | Expr::Or(a, b) => {
                collect(a, field, out);
                collect(b, field, out);
            }
            Expr::Compare {
                lhs: Operand::Field(f),
                rhs,
                ..
            } if f.name() == field => out.push(rhs),
            _ => {}
        }
    }
    let mut result = Vec::new();
    if let Some(filter) = &query.filter {
        collect(filter, field, &mut result);
    }
    for stage in &query.stages {
        if let Stage::Where(filter) = stage {
            collect(filter, field, &mut result);
        }
    }
    result
}

#[devkit::test]
fn selected_schema_converts_scalars_without_using_their_quote_spelling() {
    let context = declared_context();
    for (field, literal, expected) in [
        ("text", "001", Value::Text("001".into())),
        ("text", "1.00", Value::Text("1.00".into())),
        ("text", "-0.0", Value::Text("-0.0".into())),
        ("text", "false", Value::Text("false".into())),
        ("integer", "001", Value::Text("001".into())),
        ("number", "1.50", Value::Text("1.50".into())),
        ("flag", "false", Value::Bool(false)),
        (
            "at",
            "2026-09-14T10:00:00Z",
            Value::Time(ts("2026-09-14T10:00:00Z")),
        ),
    ] {
        for literal in [literal.to_owned(), format!("\"{literal}\"")] {
            let text =
                format!("events where event_type = example.text AND payload.{field} = {literal}");
            let query = bind(&text, &context).unwrap_or_else(|e| panic!("{text}: {e}"));
            assert_eq!(
                values(&query, &format!("payload.{field}")),
                [&expected],
                "{text}"
            );
            let expected_type = match field {
                "integer" => FieldType::Integer,
                "number" => FieldType::Number,
                "flag" => FieldType::Bool,
                "at" => FieldType::Time,
                _ => FieldType::Text,
            };
            assert_eq!(
                field_types(&query, &format!("payload.{field}")),
                [expected_type]
            );
        }
    }
    for op in ["=", "!=", "~", "^"] {
        let text = format!("events where payload.text {op} alpha*001");
        let query = bind(&text, &context).unwrap();
        assert_eq!(
            values(&query, "payload.text"),
            [&Value::Text("alpha*001".into())]
        );
    }
    for literal in ["42", "\"42\""] {
        let query = bind(
            &format!("events where payload.opaque = {literal}"),
            &context,
        )
        .unwrap();
        assert_eq!(
            values(&query, "payload.opaque"),
            [&Value::Text("42".into())]
        );
    }
    for op in ["~", "^"] {
        let query = bind(&format!("events where payload.opaque {op} 001"), &context).unwrap();
        assert_eq!(
            values(&query, "payload.opaque"),
            [&Value::Text("001".into())]
        );
    }
}

#[devkit::test]
fn branch_narrowing_and_late_catalog_binding_preserve_raw_scalar_text() {
    let context = declared_context();
    let query = bind("events where (event_type = example.text AND payload.mixed = 001) OR (event_type = example.integer AND payload.mixed = 001)", &context).unwrap();
    let mixed = values(&query, "payload.mixed");
    assert_eq!(mixed.len(), 2);
    assert!(
        mixed
            .iter()
            .all(|value| **value == Value::Text("001".into()))
    );
    let types = field_types(&query, "payload.mixed");
    assert!(types.contains(&FieldType::Text));
    assert!(types.contains(&FieldType::Integer));

    let mut query = parse(
        "events where event_type = example.integer AND payload.mixed = \"001\"",
        &context,
    )
    .unwrap();
    assert_eq!(
        values(&query, "payload.mixed"),
        [&Value::Text("001".into())]
    );
    context
        .catalog
        .as_ref()
        .unwrap()
        .bind(&mut query, &context)
        .unwrap();
    assert_eq!(
        values(&query, "payload.mixed"),
        [&Value::Text("001".into())]
    );
    assert_eq!(field_types(&query, "payload.mixed"), [FieldType::Integer]);
}

#[devkit::test]
fn grouped_repeated_and_saved_outputs_use_their_entering_column_types() {
    for text in [
        "commands group by cwd count where cwd = 001 AND count >= 2",
        "commands group by cwd count group by cwd sum(count) where cwd = 001 AND sum_count >= 2",
    ] {
        let query = q(text);
        assert_eq!(values(&query, "cwd"), [&Value::Text("001".into())]);
        let count = if text.contains("sum(count)") {
            "sum_count"
        } else {
            "count"
        };
        assert_eq!(values(&query, count), [&Value::Text("2".into())]);
    }
    let query = q("events group by source count where source = \"terminal.*\"");
    assert_eq!(
        values(&query, "source"),
        [&Value::Glob("terminal.*".into())]
    );
    let query = q("commands group by cwd min(command) where min_command = 001");
    assert_eq!(values(&query, "min_command"), [&Value::Text("001".into())]);

    let saved = crate::query::saved::SavedQuery {
        name: "by-directory".into(),
        query: "commands group by cwd count".into(),
        parameters: vec![],
    };
    let query = crate::query::saved::bind_saved(
        "query:by-directory where cwd = \"001\" AND count >= \"2\"",
        &ctx(),
        &BTreeMap::from([(saved.name.clone(), saved)]),
    )
    .unwrap();
    assert_eq!(values(&query, "cwd"), [&Value::Text("001".into())]);
    assert_eq!(values(&query, "count"), [&Value::Text("2".into())]);
}

#[devkit::test]
fn promoted_and_grouped_boolean_time_columns_keep_their_actual_types() {
    let query = q("events near (commands) within 5m where offset >= \"001.50\"");
    assert_eq!(values(&query, "offset"), [&Value::Text("001.50".into())]);
    let context = declared_context();
    let query = bind("events where event_type = example.text group by payload.flag, payload.at count where payload_flag = false AND payload_at = 2026-09-14T10:00:00Z", &context).unwrap();
    assert_eq!(values(&query, "payload_flag"), [&Value::Bool(false)]);
    assert_eq!(
        values(&query, "payload_at"),
        [&Value::Time(ts("2026-09-14T10:00:00Z"))]
    );
}

#[devkit::test]
fn quoted_identifier_patterns_offer_the_same_declared_payload_completions() {
    let completions = complete("events where event_type = \"command.*\" AND payload.dur");
    assert!(
        completions
            .iter()
            .any(|field| field.name == "payload.duration_ns")
    );
}

#[devkit::test]
fn declared_numeric_literals_keep_uint64_and_decimal_precision() {
    let context = declared_context();
    for (field, literal, ty) in [
        ("integer", "18446744073709551615", FieldType::Integer),
        ("integer", "1.0", FieldType::Integer),
        (
            "number",
            "1.0000000000000000000000000000001",
            FieldType::Number,
        ),
        ("number", "1e1000", FieldType::Number),
    ] {
        let query = bind(
            &format!("events where event_type = example.text AND payload.{field} = {literal}"),
            &context,
        )
        .unwrap();
        assert_eq!(
            values(&query, &format!("payload.{field}")),
            [&Value::Text(literal.into())]
        );
        assert_eq!(field_types(&query, &format!("payload.{field}")), [ty]);
    }
    let query = q("events where output = 18446744073709551615");
    assert_eq!(
        values(&query, "output"),
        [&Value::Text("18446744073709551615".into())]
    );
}

#[devkit::test]
fn readback_distinguishes_exact_numeric_carriers_from_declared_text() {
    let context = declared_context();
    let number = bind(
        "events where payload.integer = 18446744073709551615",
        &context,
    )
    .unwrap();
    assert!(read_as(&number).contains("payload.integer is 18446744073709551615"));
    let text = bind("events where payload.text = 001", &context).unwrap();
    assert!(read_as(&text).contains("payload.text is \"001\""));
}
