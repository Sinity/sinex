use super::*;
use serde_json::{Value, json};
use sinex_model::{Class, MaterialId, Origin, ReadingOrder, RecordLocation};
use sqlx::Connection;

fn example(schema: &Value, root: &Value, optional: bool) -> Value {
    if let Some(reference) = schema.get("$ref").and_then(Value::as_str) {
        return example(
            root.pointer(reference.strip_prefix('#').unwrap()).unwrap(),
            root,
            optional,
        );
    }
    if let Some(v) = schema.get("const") {
        return v.clone();
    }
    if let Some(values) = schema.get("enum").and_then(Value::as_array) {
        return values[0].clone();
    }
    if let Some(branches) = schema
        .get("anyOf")
        .or_else(|| schema.get("oneOf"))
        .and_then(Value::as_array)
    {
        let branch = if optional {
            branches
                .iter()
                .find(|s| s.get("type").and_then(Value::as_str) != Some("null"))
                .unwrap()
        } else {
            &branches[0]
        };
        return example(branch, root, optional);
    }
    let ty = schema.get("type").unwrap_or(&Value::Null);
    let ty = ty
        .as_str()
        .or_else(|| {
            ty.as_array()
                .and_then(|a| a.iter().filter_map(Value::as_str).find(|s| *s != "null"))
        })
        .unwrap_or("object");
    match ty {
        "string" => match schema.get("format").and_then(Value::as_str) {
            Some("date-time") => json!("2026-10-27T14:00:00+02:00"),
            Some("uuid") => json!("019a26cd-e800-7000-8000-000000000001"),
            _ => json!("sample"),
        },
        "integer" | "number" => schema.get("minimum").cloned().unwrap_or(json!(0)),
        "boolean" => json!(optional),
        "null" => Value::Null,
        "array" => json!([]),
        _ => {
            let mut object = serde_json::Map::new();
            if let Some(fields) = schema.get("properties").and_then(Value::as_object) {
                let required = schema.get("required").and_then(Value::as_array);
                for (name, field) in fields {
                    if optional || required.is_some_and(|fields| fields.contains(&json!(name))) {
                        object.insert(name.clone(), example(field, root, optional));
                    }
                }
            }
            Value::Object(object)
        }
    }
}

#[devkit::test]
async fn every_saved_bundled_rule_matches_the_typed_constructor(db: devkit::pg::TestDatabase) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let saved = crate::kinds::all(&mut conn).await.unwrap();
    assert_eq!(saved.len(), event_types::ALL.len());
    for kind in event_types::ALL {
        let reference = (kind.declaration)().kind;
        let declaration = &saved[&reference];
        assert_eq!(reference.version, 2);
        for optional in [false, true] {
            let schema = declaration.payload_schema.as_ref().unwrap();
            let mut payload = example(schema, schema, optional);
            // The table decoder checks that the event time matches its source row.
            if kind.event_type == "steering.row" && optional {
                payload["values"]["created_at"] = payload["at"].clone();
            }
            let event = (kind.build)(
                &payload,
                "example",
                Origin::Record {
                    location: RecordLocation {
                        material: MaterialId::new(),
                        anchor: 0,
                    },
                    output_index: 0,
                },
                ReadingOrder(0),
                [0; 32],
                Class::Open,
            )
            .unwrap_or_else(|e| panic!("{}: {e}", kind.event_type));
            let evaluated = declaration.evaluate(&event.payload, Class::Open).unwrap();
            assert_eq!(
                evaluated.equivalence_key, event.equivalence_key,
                "{}",
                kind.event_type
            );
            assert_eq!(evaluated.ts_orig, event.ts_orig, "{}", kind.event_type);
            assert_eq!(
                evaluated.record_state, event.record_state,
                "{}",
                kind.event_type
            );
            assert_eq!(
                evaluated.owned_blobs, event.owned_blobs,
                "{}",
                kind.event_type
            );
            assert_eq!(evaluated.class, event.class, "{}", kind.event_type);
        }
    }
}


#[test]
fn field_hints_need_distinct_valid_pointers() {
    let mut declaration = sinex_model::event_types::CommandExecuted::declaration();
    declaration.field_hints = vec![sinex_model::kinds::FieldHint { field: "command".into(), class: Class::Guarded }];
    assert!(kinds::check(&declaration).is_err());
    declaration.field_hints[0].field = "/command~2".into();
    assert!(kinds::check(&declaration).is_err());
    declaration.field_hints[0].field = "/command".into();
    assert!(kinds::check(&declaration).is_ok());
    declaration.field_hints.push(declaration.field_hints[0].clone());
    assert!(kinds::check(&declaration).is_err());
}
