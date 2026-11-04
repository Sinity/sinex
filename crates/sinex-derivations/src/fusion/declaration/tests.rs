use super::*;
use serde_json::json;

fn shipped() -> Value {
    serde_json::from_str(FILE).unwrap()
}

fn refused(text: &Value) -> Refused {
    parse(&text.to_string())
        .err()
        .expect("the declaration is refused")
}

#[devkit::test]
fn the_shipped_declaration_holds_the_defined_families() {
    check().unwrap();
    let names: Vec<_> = families().iter().map(|f| f.name).collect();
    assert_eq!(names, ["command", "mail"]);
    for family in families() {
        assert!(DEFINED.contains(&(family.name, family.output)));
        assert!(family.members.len() >= 2);
        assert!(
            family
                .input_types
                .contains(&sinex_model::event_types::duplicate::Duplicate::EVENT_TYPE)
        );
        assert_eq!(
            family.output_types,
            [family.output, "fusion.considered_pair"]
        );
    }
}

/// law: fus.defined-only
#[devkit::test]
fn a_family_without_a_written_definition_refuses_to_start() {
    let mut text = shipped();
    let mut undefined = text["families"][0].clone();
    undefined["name"] = json!("clipboard");
    undefined["derivation"] = json!("clipboard.fusion");
    text["families"].as_array_mut().unwrap().push(undefined);
    let refusal = refused(&text);
    assert_eq!(refusal.family.as_deref(), Some("clipboard"));
    assert!(
        refusal.reason.contains("no written definition"),
        "{refusal}"
    );

    // A defined family writing another fused kind is refused too.
    let mut text = shipped();
    text["families"][1]["output"] = json!("command.fused");
    let refusal = refused(&text);
    assert_eq!(refusal.family.as_deref(), Some("mail"));
    assert!(refusal.reason.contains("mail.fused"), "{refusal}");
}

#[devkit::test]
fn an_invalid_rule_is_refused_naming_the_rule_and_the_reason() {
    let cases: Vec<(Box<dyn Fn(&mut Value)>, &str)> = vec![
        (
            Box::new(|t| t["families"][0]["members"][0]["event_type"] = json!("no.such.kind")),
            "not a registered kind",
        ),
        (
            Box::new(|t| t["families"][0]["members"][0]["fields"]["colour"] = json!("/command")),
            "not a field of command.fused",
        ),
        (
            Box::new(|t| t["families"][0]["members"][1]["fields"]["cwd"] = json!("/no_such_field")),
            "has no field no_such_field",
        ),
        (
            Box::new(|t| {
                t["families"][0]["members"][2]["fields"]
                    .as_object_mut()
                    .unwrap()
                    .remove("host")
                    .map(drop)
                    .unwrap()
            }),
            "does not supply host",
        ),
        (
            Box::new(|t| t["families"][0]["members"][1]["identifiers"] = json!({})),
            "carried by one member",
        ),
        (
            Box::new(|t| t["families"][0]["fallback_ms"] = json!(1000)),
            "narrower than the window",
        ),
        (
            Box::new(|t| t["families"][0]["scope"] = json!([])),
            "needs scope fields",
        ),
        (
            Box::new(|t| {
                t["families"][1]["members"]
                    .as_array_mut()
                    .unwrap()
                    .truncate(1)
            }),
            "at least two members",
        ),
        (
            Box::new(|t| t["families"][1]["members"][1] = t["families"][1]["members"][0].clone()),
            "declared twice",
        ),
        (
            Box::new(|t| {
                t["families"][0]["members"][0]["fields"]["command"] = json!({"pointer": "command"})
            }),
            "data did not match",
        ),
        (
            Box::new(|t| t["families"][0]["members"][0]["fields"]["command"] = json!("command")),
            "not a payload field",
        ),
        (
            Box::new(|t| t["families"][0]["unknown"] = json!(true)),
            "unknown field",
        ),
        (
            Box::new(|t| {
                t["families"][1]
                    .as_object_mut()
                    .unwrap()
                    .remove("spread_ms");
            }),
            "declared together",
        ),
        (
            Box::new(|t| t["families"][1]["window_ms"] = json!(1000)),
            "identity alone",
        ),
        (
            Box::new(|t| {
                t["families"][1]["members"][0]["fields"]["account"] =
                    json!({"pointer": "/account", "normalize": "trim"})
            }),
            "kept as it is",
        ),
        (
            Box::new(|t| t["families"][1]["token_length"] = json!("subject")),
            "needs a token",
        ),
        (
            Box::new(|t| t["families"][0]["token_length"] = json!("colour")),
            "token length colour is not a field",
        ),
        (
            Box::new(|t| {
                t["families"][0]["members"][0]["fields"]["duration_ns"] = json!("/exit_code")
            }),
            "not carried in its top-level field duration_ns",
        ),
    ];
    for (change, reason) in cases {
        let mut text = shipped();
        change(&mut text);
        let refusal = refused(&text);
        assert!(refusal.to_string().contains(reason), "{reason}: {refusal}");
    }
}

#[devkit::test]
fn a_changed_rule_is_a_new_version() {
    let shipped_families = parse(FILE).unwrap();
    let again = parse(&shipped().to_string()).unwrap();
    assert_eq!(shipped_families[0].version, again[0].version);
    let mut text = shipped();
    text["families"][0]["window_ms"] = json!(3000);
    let changed = parse(&text.to_string()).unwrap();
    assert_ne!(changed[0].version, shipped_families[0].version);
    // Only the changed family moves.
    assert_eq!(changed[1].version, shipped_families[1].version);
}

#[devkit::test]
fn field_readings_normalise_without_merging_meanings() {
    let trim = Field {
        pointer: "/command",
        normalize: Some(Normalize::Trim),
    };
    assert_eq!(
        trim.read(&json!({"command": "  ls  -l "})),
        Some(json!("ls  -l"))
    );
    assert_eq!(trim.read(&json!({"command": "   "})), None);
    assert_eq!(trim.read(&json!({"command": null})), None);
    let host = Field {
        pointer: "/hostname",
        normalize: Some(Normalize::BeforeColon),
    };
    assert_eq!(
        host.read(&json!({"hostname": "box:user"})),
        Some(json!("box"))
    );
    assert_eq!(host.read(&json!({"hostname": "box"})), Some(json!("box")));
    let plain = Field {
        pointer: "/labels",
        normalize: None,
    };
    assert_eq!(plain.read(&json!({"labels": ["a"]})), Some(json!(["a"])));
}
