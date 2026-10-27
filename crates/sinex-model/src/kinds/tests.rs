use super::*;
use crate::{EventPayload, MaterialId, Origin, ReadingOrder, RecordLocation};
use serde_json::json;

#[test]
fn hashes_ignore_object_order_at_every_level() {
    let mut d = KindDeclaration::schemaless(
        KindRef {
            name: "test.value".into(),
            version: 9,
        },
        "test".into(),
    );
    d.payload_schema = Some(
        serde_json::from_str(
            r#"{"type":"object","properties":{"value":{"type":"string","title":"value"}}}"#,
        )
        .unwrap(),
    );
    let hash = d.definition_hash();
    d.payload_schema = Some(
        serde_json::from_str(
            r#"{"properties":{"value":{"title":"value","type":"string"}},"type":"object"}"#,
        )
        .unwrap(),
    );
    assert_eq!(hash, d.definition_hash());
    d.kind.version = 10;
    assert_ne!(hash, d.definition_hash());
}

#[test]
fn version_is_a_positive_bigint() {
    for version in [0, u64::MAX, i64::MAX as u64 + 1] {
        assert!(
            KindRef {
                name: "test.value".into(),
                version
            }
            .database_version()
            .is_err()
        );
    }
    for version in [1, 9, 10, i64::MAX as u64] {
        assert_eq!(
            KindRef {
                name: "test.value".into(),
                version
            }
            .database_version()
            .unwrap(),
            version as i64
        );
    }
}

#[test]
fn first_present_time_refuses_a_malformed_selected_value() {
    let mut d = KindDeclaration::schemaless(
        KindRef {
            name: "test.value".into(),
            version: 1,
        },
        "test".into(),
    );
    d.time_rule = Some(TimeRule::FirstPresent {
        fields: vec!["first".into(), "second".into()],
        quality: TimeQuality::Captured,
    });
    assert!(
        d.evaluate(
            &json!({"first":"2026-02-30T00:00:00Z", "second":"2026-10-27T00:00:00Z"}),
            Class::Open
        )
        .is_err()
    );
    assert_eq!(
        d.evaluate(&json!({}), Class::Open).unwrap().ts_orig,
        TsOrig::unknown()
    );
    let evaluated = d
        .evaluate(
            &json!({"first":null, "second":"2026-10-27T02:00:00+02:00"}),
            Class::Open,
        )
        .unwrap();
    assert_eq!(evaluated.ts_orig.at.unwrap().unix_ns(), 1793059200000000000);
}

/// Build small inputs from the declared schemas; optional fields are also
/// exercised as absent and present so conditional rules are compared.
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

#[test]
fn every_bundled_declaration_matches_its_rust_methods() {
    let mut checked = 0;
    for kind in crate::event_types::ALL {
        let d = (kind.declaration)();
        assert!(d.kind.version >= 2, "{}", kind.event_type);
        for optional in [false, true] {
            let schema = d.payload_schema.as_ref().unwrap();
            let mut payload = example(schema, schema, optional);
            // The table decoder checks that the event time matches its source row.
            if kind.event_type == "steering.row" && optional {
                payload["values"]["created_at"] = payload["at"].clone();
            }
            let event = (kind.build)(
                &payload,
                "test",
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
            .unwrap_or_else(|e| panic!("{} optional={optional}: {e}; {payload}", kind.event_type));
            let evaluated = d.evaluate(&event.payload, Class::Open).unwrap();
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
        checked += 1;
    }
    assert_eq!(checked, crate::event_types::ALL.len());
}

#[test]
fn command_without_start_uses_location_identity() {
    let d = crate::event_types::CommandImported::declaration();
    assert!(
        d.evaluate(&json!({"started_at":null}), Class::Open)
            .unwrap()
            .equivalence_key
            .is_none()
    );
    assert!(
        d.evaluate(&json!({"started_at":"2026-10-27T00:00:00Z"}), Class::Open)
            .unwrap()
            .equivalence_key
            .is_some()
    );
}

#[derive(serde::Serialize, serde::Deserialize, schemars::JsonSchema, crate::EventPayload)]
#[event(type = "test.new_kind", key = location, replace = repeat)]
struct NewKind {}

#[test]
fn a_new_kind_starts_at_version_one() {
    assert_eq!(NewKind::VERSION, 1);
    assert_eq!(NewKind::declaration().kind.version, 1);
}

#[test]
fn missing_and_empty_source_identifiers_use_the_same_keys_as_stored_rules() {
    for name in ["mail.message", "note.document"] {
        let kind = crate::event_types::find(name).unwrap();
        let declaration = (kind.declaration)();
        let schema = declaration.payload_schema.as_ref().unwrap();
        for native in [Value::Null, json!(""), json!("  "), json!("native")] {
            for scope in [Value::Null, json!(""), json!("host")] {
                let mut payload = example(schema, schema, false);
                if name == "mail.message" {
                    payload["message_id"] = native.clone();
                    payload["native_id"] = scope.clone();
                } else {
                    payload["host"] = scope.clone();
                    payload["vault_path"] = json!("/synthetic/vault");
                    payload["note_id"] = native.clone();
                }
                let event = (kind.build)(
                    &payload,
                    "source",
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
                .unwrap();
                assert_eq!(
                    declaration
                        .evaluate(&event.payload, Class::Open)
                        .unwrap()
                        .equivalence_key,
                    event.equivalence_key,
                    "{name}: native={native}, scope={scope}"
                );
            }
        }
    }
}

#[test]
fn changed_source_payloads_have_new_versions() {
    for name in [
        "mail.message",
        "note.document",
        "notification.shown",
        "clipboard.copied",
        "player.changed",
        "lake.observed",
        "journal.entry",
        "git.commit",
        "calendar.event",
        "task.revised",
    ] {
        let kind = crate::event_types::find(name).unwrap();
        assert_eq!((kind.declaration)().kind.version, 3, "{name}");
    }
}

#[test]
fn router_current_and_closed_periods_match_their_stored_key_rule() {
    let kind = crate::event_types::find("device_traffic.sampled").unwrap();
    let declaration = (kind.declaration)();
    let schema = declaration.payload_schema.as_ref().unwrap();
    for period in ["current", "2025-01"] {
        let mut payload = example(schema, schema, false);
        payload["period"] = json!(period);
        let event = (kind.build)(
            &payload,
            "router",
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
        .unwrap();
        assert_eq!(
            declaration
                .evaluate(&event.payload, Class::Open)
                .unwrap()
                .equivalence_key,
            event.equivalence_key
        );
        assert_eq!(event.equivalence_key.is_none(), period == "current");
    }
}
