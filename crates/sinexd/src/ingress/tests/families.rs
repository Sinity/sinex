use super::*;
use sinex_model::{Class, KindDeclaration, KindRef};
use sinex_protocol::{FetchMaterial, FetchMaterialReply, FetchRefusalKind};

fn grant(family: &str, publisher: bool) -> crate::ingress::fetch::Grants {
    crate::ingress::fetch::Grants {
        kind_prefixes: vec![family.into()],
        classes: vec![Class::Open, Class::Guarded],
        write: Some(crate::ingress::authority::WriteGrant {
            family: family.into(),
            kind_prefixes: vec![family.into()],
            classes: vec![Class::Open],
            canonical_prefixes: if publisher {
                vec![format!("{family}.summary")]
            } else {
                vec![]
            },
        }),
    }
}
fn declaration(name: &str, raw: bool) -> KindDeclaration {
    let mut declaration = KindDeclaration::schemaless(
        KindRef {
            name: name.into(),
            version: 1,
        },
        "example".into(),
    );
    declaration.payload_schema = Some(
        json!({"type":"object","properties":{"copy":{"type":"string"},"id":{"type":"string"},"text":{"type":"string"}},"required":["copy","id","text"],"additionalProperties":false}),
    );
    declaration.key_rule = Some(sinex_model::kinds::KeyRule::Fields {
        fields: if raw {
            vec!["copy".into(), "id".into()]
        } else {
            vec!["id".into()]
        },
    });
    declaration
}
fn event(kind: &KindRef, material: MaterialId, copy: &str, text: &str) -> ProducerEvent {
    ProducerEvent {
        kind: kind.clone(),
        payload: json!({"copy":copy,"id":"one","text":text}),
        location: Some(RecordLocation {
            material,
            anchor: 0,
        }),
        output_index: 0,
        parents: vec![],
        reading_order: None,
        class: Class::Open,
    }
}
async fn material(h: &Harness, producer: &str) -> MaterialId {
    let (id, mut frame) = h.material_frame(&format!("material-{producer}"), b"captured source\n");
    if let Ingress::Material { material, .. } = &mut frame {
        material.input.host = producer.into();
    }
    assert_eq!(h.send(producer, &frame).await.refusal, None);
    id
}
#[devkit::test]
async fn two_authenticated_hosts_share_family_definitions_and_preserve_raw_copies(
    db: devkit::pg::TestDatabase,
) {
    let mut capabilities = std::collections::BTreeMap::from([
        ("host-a".into(), grant("example", true)),
        ("host-b".into(), grant("example", false)),
        ("foreign".into(), grant("foreign", false)),
    ]);
    capabilities.insert(
        "reader".into(),
        crate::ingress::fetch::Grants {
            kind_prefixes: vec!["example".into()],
            classes: vec![Class::Open, Class::Guarded],
            write: None,
        },
    );
    let writer = capabilities.get_mut("host-a").unwrap();
    writer.kind_prefixes.clear();
    writer.classes.clear();
    let h = Harness::start_with_grants(db, capabilities).await;
    let raw = declaration("example.raw", true);
    let summary = declaration("example.summary", false);
    let definitions = Ingress::RegisterKinds {
        frame: "definitions".into(),
        declarations: vec![raw.clone(), summary.clone()],
    };
    let first = h.send("host-a", &definitions).await;
    assert_eq!(first.refusal, None);
    let second = h.send("host-b", &definitions).await;
    assert_eq!(second.refusal, None);
    assert_eq!(
        first.outcome["registered_kinds"],
        second.outcome["registered_kinds"]
    );
    for (producer, canonical_publisher) in [("host-a", true), ("host-b", false)] {
        let reply = rpc(
            &h.client,
            h.subjects.connect(producer),
            serde_json::to_vec(&ProducerHello {
                event_types: vec![raw.kind.name.clone()],
                ..Default::default()
            })
            .unwrap()
            .into(),
        )
        .await
        .unwrap();
        let welcome: sinex_protocol::ProducerWelcome =
            serde_json::from_slice(&reply.payload).unwrap();
        assert_eq!(welcome.refusal, None);
        assert_eq!(welcome.family.as_deref(), Some("example"));
        assert_eq!(welcome.canonical_publisher, canonical_publisher);
    }
    let a = material(&h, "host-a").await;
    let b = material(&h, "host-b").await;
    for (producer, id) in [("host-a", a), ("host-b", b)] {
        assert_eq!(
            h.send(
                producer,
                &Ingress::Events {
                    frame: "raw-copy".into(),
                    events: vec![event(&raw.kind, id, producer, "same captured text")],
                    failures: vec![]
                }
            )
            .await
            .refusal,
            None
        );
    }
    assert_eq!(h.count("SELECT count(*) FROM core.events e WHERE e.event_type='example.raw' AND e.source='example' AND NOT EXISTS(SELECT 1 FROM audit.archived a WHERE a.event_id=e.id)").await,2);
    assert_eq!(h.count("SELECT count(DISTINCT i.host) FROM raw.inputs i JOIN raw.materials m ON m.input_id=i.id JOIN core.ingress_frames f ON f.material_id=m.id WHERE f.family='example'").await,2);
    assert_eq!(h.count("SELECT count(DISTINCT r.name) FROM core.events e JOIN core.runs r ON r.id=e.run_id WHERE e.event_type='example.raw'").await,2);
    assert!(
        h.send(
            "host-b",
            &Ingress::Events {
                frame: "unauthorized-summary".into(),
                events: vec![event(&summary.kind, b, "host-b", "forged summary")],
                failures: vec![]
            }
        )
        .await
        .refusal
        .is_some()
    );
    assert_eq!(
        h.send(
            "host-a",
            &Ingress::Events {
                frame: "canonical-summary".into(),
                events: vec![event(&summary.kind, a, "canonical", "summary")],
                failures: vec![]
            }
        )
        .await
        .refusal,
        None
    );
    assert!(h.send("foreign", &definitions).await.refusal.is_some());
    assert!(
        h.send("reader", &definitions)
            .await
            .refusal
            .unwrap()
            .contains("no configured write family"),
        "read rights grant no registration authority"
    );
    let mut conflicting = raw.clone();
    conflicting.search = sinex_model::kinds::Search::None;
    assert!(
        h.send(
            "host-b",
            &Ingress::RegisterKinds {
                frame: "conflicting-definition".into(),
                declarations: vec![conflicting]
            }
        )
        .await
        .refusal
        .is_some()
    );
    let request = FetchMaterial { material: a };
    let reply = rpc(
        &h.client,
        h.subjects.fetch_material("reader"),
        serde_json::to_vec(&request).unwrap().into(),
    )
    .await
    .unwrap();
    let reply: FetchMaterialReply = serde_json::from_slice(&reply.payload).unwrap();
    assert_eq!(
        reply.refusal, None,
        "family ownership comes from stored write provenance, not writer read grants"
    );
}
#[devkit::test]
async fn historical_material_family_is_unknown_until_explicitly_mapped(
    db: devkit::pg::TestDatabase,
) {
    let h = Harness::start_with_grants(
        db,
        std::collections::BTreeMap::from([("host-a".into(), grant("example", true))]),
    )
    .await;
    let raw = declaration("example.raw", true);
    assert_eq!(
        h.send(
            "host-a",
            &Ingress::RegisterKinds {
                frame: "kind".into(),
                declarations: vec![raw]
            }
        )
        .await
        .refusal,
        None
    );
    let id = material(&h, "host-a").await;
    let mut conn = PgConnection::connect(&h.url).await.unwrap();
    sqlx::query!(
        "UPDATE core.ingress_frames SET family=NULL,write_kind_prefixes=NULL WHERE material_id=$1",
        id.0
    )
    .execute(&mut conn)
    .await
    .unwrap();
    let reply = rpc(
        &h.client,
        h.subjects.fetch_material("host-a"),
        serde_json::to_vec(&FetchMaterial { material: id })
            .unwrap()
            .into(),
    )
    .await
    .unwrap();
    let reply: FetchMaterialReply = serde_json::from_slice(&reply.payload).unwrap();
    assert_eq!(reply.refusal_kind, Some(FetchRefusalKind::Unavailable));
    assert!(reply.material.is_none());
    assert!(
        h.send(
            "host-a",
            &Ingress::Events {
                frame: "old-owner-unknown".into(),
                events: vec![event(
                    &KindRef {
                        name: "example.raw".into(),
                        version: 1
                    },
                    id,
                    "host-a",
                    "retained"
                )],
                failures: vec![]
            }
        )
        .await
        .refusal
        .unwrap()
        .contains("material family ownership")
    );
}
