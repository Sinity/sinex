use super::*;

fn context() -> Context {
    Context::utc("2026-09-14T12:00:00Z".parse().unwrap())
}

#[devkit::test]
fn saved_and_stored_pack_conversion_keeps_original_identity_evidence() {
    let ctx = context();
    let selected = json!({"kind":"session","anchor":{"source":"synthetic","event_type":"shell.session","key":"session-a"},
        "_query":"original bound selector bytes","_fingerprint":"original input hash",
        "_saved":{"remember":{"name":"remember","query":"recall last where cwds ^ /src/tide","parameters":[]}}});
    let selection = json!({"query":"query:remember","now":crate::Timestamp::from_unix_secs(1_789_387_200).unwrap(),
        "read_as":null,"zone":"UTC","day_start":"00:00:00","grants":["guarded"],"selected":selected,"declarations":[]});
    let mut payload =
        json!({"request":"original-request-hash","operation":"recall","selection":selection});
    upgrade_payload(&mut payload, &ctx).unwrap();
    assert_eq!(payload["request"], "original-request-hash");
    assert_eq!(payload["selection"]["selected"], selected);
    assert_eq!(payload["selection"]["query"], "query:remember");
    assert_eq!(
        payload["selection"]["original_question"],
        json!({
            "source":{"kind":"saved","name":"remember","arguments":[],"condition":null},"attach":[],"stages":[]
        })
    );
    let retained: crate::event_types::analysis::QuerySelection =
        serde_json::from_value(payload["selection"].clone()).unwrap();
    assert!(matches!(
        retained.template.bind(&ctx).unwrap().source,
        super::super::Source::RecallLast { filter: Some(_) }
    ));
    assert_eq!(
        retained.definitions["remember"].query,
        "recall last where cwds ^ /src/tide"
    );
    let once = payload.clone();
    upgrade_payload(&mut payload, &ctx).unwrap();
    assert_eq!(payload, once);
    let mut pack = json!({"pack":"original-pack","analysis":{"selected":selection},"trail":[]});
    upgrade_payload(&mut pack, &ctx).unwrap();
    assert_eq!(pack["pack"], "original-pack");
    assert_eq!(pack["analysis"]["selected"]["selected"], selected);
}

#[devkit::test]
fn historical_query_text_uses_the_frozen_decoder_and_current_templates_use_current_grammar() {
    let ctx = context();
    let template = decode_format_one(
        "events where source = synthetic group by source count fields source,count",
        &ctx,
    )
    .unwrap();
    let bound = template.bind(&ctx).unwrap();
    assert!(
        bound
            .stages
            .iter()
            .any(|stage| matches!(stage, super::super::Stage::Group(_)))
    );
    assert!(decode_format_one("recall mark 0", &ctx).is_err());
    assert_eq!(GRAMMAR_REVISION, "8c22e127bb1f5c10d8c1740de4c4f0bfe53ffdb7");
}

#[devkit::test]
fn historical_definitions_expand_before_selecting_output_and_rebind_dynamic_targets() {
    use super::super::saved::{Parameter, ParameterType, SavedQuery};
    let ctx = context();
    let make = |name: &str, text: &str| SavedQuery {
        name: name.into(),
        query: text.into(),
        template: definition_format_one(text, &ctx).unwrap(),
        parameters: vec![],
    };
    let wrapper = make("wrapper", "query:base where host = first fields id");
    let mut definitions = BTreeMap::from([
        ("base".into(), make("base", "events fields id")),
        ("wrapper".into(), wrapper),
    ]);
    let first = super::super::saved::template_saved("query:wrapper", &ctx, &definitions).unwrap();
    let final_fields = first.relation.transforms.last().unwrap();
    assert!(
        matches!(&final_fields.kind,ast::TransformKind::Fields {columns} if columns.iter().map(ast::Column::column).collect::<Vec<_>>()==["id"])
    );
    // A raw definition's authored target remains dynamic rather than being
    // replaced by a copy of the target that happened to exist during replay.
    definitions.insert(
        "base".into(),
        make("base", "events where source = second fields id"),
    );
    let second = super::super::saved::template_saved("query:wrapper", &ctx, &definitions).unwrap();
    assert_ne!(first.relation, second.relation);
    let text = "events where source = $source";
    let definition = SavedQuery {
        name: "pattern".into(),
        query: text.into(),
        template: definition_format_one(text, &ctx).unwrap(),
        parameters: vec![Parameter {
            name: "source".into(),
            field_type: ParameterType::Text,
        }],
    };
    definitions.insert("pattern".into(), definition);
    let template = super::super::saved::template_saved(
        "query:pattern(source=\"terminal.*\")",
        &ctx,
        &definitions,
    )
    .unwrap();
    let encoded = serde_json::to_string(&template).unwrap();
    assert!(encoded.contains("glob"));
    assert!(!encoded.contains("historical_order_v1"));
}

#[devkit::test]
fn a_historical_source_proof_checks_the_decoded_body_as_well_as_text() {
    let ctx = context();
    let mut definition = definition_format_one("events where source = first", &ctx).unwrap();
    let super::super::saved::Definition::FormatOne { body, .. } = &mut definition else {
        panic!("format")
    };
    *body = parse_format_one("events where source = second", &ctx).unwrap();
    assert!(
        definition
            .check_source("events where source = first")
            .is_err()
    );
}
