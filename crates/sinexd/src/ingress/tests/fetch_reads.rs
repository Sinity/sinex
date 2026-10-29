use super::*;
use sinex_protocol::{
    FetchEvent, FetchEventReply, FetchMaterial, FetchMaterialReply, FetchRefusalKind,
};

#[devkit::test]
async fn authenticated_fetch_uses_configured_kind_and_class_grants(db: devkit::pg::TestDatabase) {
    let grants = std::collections::BTreeMap::from([
        (
            "kitty".into(),
            crate::ingress::fetch::Grants {
                write: None,
                kind_prefixes: vec!["command".into()],
                classes: vec![sinex_model::Class::Open, sinex_model::Class::Guarded],
            },
        ),
        (
            "replica".into(),
            crate::ingress::fetch::Grants {
                write: None,
                kind_prefixes: vec!["command.".into()],
                classes: vec![sinex_model::Class::Open, sinex_model::Class::Guarded],
            },
        ),
        (
            "near-prefix".into(),
            crate::ingress::fetch::Grants {
                write: None,
                kind_prefixes: vec!["command.import".into()],
                classes: vec![sinex_model::Class::Guarded],
            },
        ),
        (
            "open-only".into(),
            crate::ingress::fetch::Grants {
                write: None,
                kind_prefixes: vec!["command".into()],
                classes: vec![sinex_model::Class::Open],
            },
        ),
    ]);
    let h = Harness::start_with_grants(db, grants).await;
    let (material, frame) = h.material_frame("fetch-material", b"retained command\n");
    assert_eq!(h.send("kitty", &frame).await.refusal, None);
    let mut event = imported(material, 0, "retained command");
    event.class = sinex_model::Class::Guarded;
    let ack = h
        .send(
            "kitty",
            &Ingress::Events {
                frame: "fetch-event".into(),
                events: vec![event],
                failures: vec![],
            },
        )
        .await;
    assert_eq!(ack.refusal, None);
    let id = ack.admitted[0];
    for producer in ["replica", "near-prefix", "open-only", "unconfigured"] {
        let bytes = serde_json::to_vec(&FetchEvent { event: id }).unwrap();
        let message = rpc(&h.client, h.subjects.fetch_event(producer), bytes.into())
            .await
            .unwrap();
        let reply: FetchEventReply = serde_json::from_slice(&message.payload).unwrap();
        if producer == "replica" {
            assert_eq!(reply.refusal, None);
            let event = reply.event.unwrap();
            assert_eq!(event.payload.unwrap()["command"], "retained command");
            assert_eq!(Some(event.commit_seq), ack.commit_seq);
        } else {
            assert_eq!(reply.refusal_kind, Some(FetchRefusalKind::Denied));
            assert!(reply.event.is_none());
        }
    }
    let forged = rpc(
        &h.client,
        h.subjects.fetch_event("unconfigured"),
        serde_json::to_vec(&json!({"event":id,"producer":"replica"}))
            .unwrap()
            .into(),
    )
    .await
    .unwrap();
    let forged: FetchEventReply = serde_json::from_slice(&forged.payload).unwrap();
    assert_eq!(forged.refusal_kind, Some(FetchRefusalKind::Invalid));
    assert!(forged.event.is_none());
    let message = rpc(
        &h.client,
        h.subjects.fetch_material("replica"),
        serde_json::to_vec(&FetchMaterial { material })
            .unwrap()
            .into(),
    )
    .await
    .unwrap();
    let reply: FetchMaterialReply = serde_json::from_slice(&message.payload).unwrap();
    assert_eq!(reply.refusal, None);
    assert_eq!(reply.material.unwrap().id, material);
    assert!(!reply.extents.is_empty());
}

#[devkit::test]
async fn oversized_event_and_material_metadata_use_the_shared_framed_reply(
    db: devkit::pg::TestDatabase,
) {
    let grant = || crate::ingress::fetch::Grants {
        write: None,
        kind_prefixes: vec!["command".into()],
        classes: vec![sinex_model::Class::Open, sinex_model::Class::Guarded],
    };
    let h = Harness::start_with_grants(
        db,
        std::collections::BTreeMap::from([("kitty".into(), grant()), ("replica".into(), grant())]),
    )
    .await;
    let (material, mut frame) = h.material_frame("large-metadata", b"retained source\n");
    let description = "x ".repeat(h.client.server_info().max_payload);
    if let Ingress::Material { material, .. } = &mut frame {
        material.manifest = sinex_model::Manifest::from_json(json!({"description":description}));
    }
    let encoded = serde_json::to_vec(&frame).unwrap();
    let blob = h.fixture_blob(&encoded);
    assert_eq!(
        h.send(
            "kitty",
            &Ingress::LargeFrame {
                frame: "large-metadata".into(),
                blob,
                length: encoded.len() as u64
            }
        )
        .await
        .refusal,
        None
    );
    let command = "y ".repeat(h.client.server_info().max_payload);
    let inner = Ingress::Events {
        frame: "large-event".into(),
        events: vec![imported(material, 0, &command)],
        failures: vec![],
    };
    let encoded = serde_json::to_vec(&inner).unwrap();
    let blob = h.fixture_blob(&encoded);
    let ack = h
        .send(
            "kitty",
            &Ingress::LargeFrame {
                frame: "large-event".into(),
                blob,
                length: encoded.len() as u64,
            },
        )
        .await;
    assert_eq!(ack.refusal, None);
    let event: FetchEventReply = sinex_transport::replies::request_json(
        &h.client,
        h.subjects.fetch_event("replica"),
        "replica",
        &FetchEvent {
            event: ack.admitted[0],
        },
        Duration::from_secs(20),
    )
    .await
    .unwrap();
    assert_eq!(event.refusal, None);
    assert_eq!(event.event.unwrap().payload.unwrap()["command"], command);
    let result: FetchMaterialReply = sinex_transport::replies::request_json(
        &h.client,
        h.subjects.fetch_material("replica"),
        "replica",
        &FetchMaterial { material },
        Duration::from_secs(20),
    )
    .await
    .unwrap();
    assert_eq!(result.refusal, None);
    assert_eq!(
        result.material.unwrap().manifest.detail("description"),
        &json!(description)
    );
    assert!(!result.extents.is_empty());
}
#[devkit::test]
async fn accessible_empty_material_acknowledges_a_real_extent(db: devkit::pg::TestDatabase) {
    let h = Harness::start(db).await;
    let (_material, frame) = h.material_frame("empty-material", b"");
    let ack = h.send("kitty", &frame).await;
    assert_eq!(ack.refusal, None);
    assert_eq!(ack.material_extents.len(), 1);
    let extent = hashed_blobs::Extent::decode(&ack.material_extents[0]).unwrap();
    assert_eq!(extent.decoded_len, 0);
    assert!(extent.len > 0);
}
