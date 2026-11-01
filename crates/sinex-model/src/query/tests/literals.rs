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
        ("integer", "001", Value::Integer(1)),
        ("number", "1.50", Value::Number(1.5)),
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
    assert!(mixed.contains(&&Value::Text("001".into())));
    assert!(mixed.contains(&&Value::Integer(1)));

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
    assert_eq!(values(&query, "payload.mixed"), [&Value::Integer(1)]);
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
        assert_eq!(values(&query, count), [&Value::Integer(2)]);
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
    assert_eq!(values(&query, "count"), [&Value::Integer(2)]);
}

#[devkit::test]
fn promoted_and_grouped_boolean_time_columns_keep_their_actual_types() {
    let query = q("events near (commands) within 5m where offset >= \"001.50\"");
    assert_eq!(values(&query, "offset"), [&Value::Number(1.5)]);
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
