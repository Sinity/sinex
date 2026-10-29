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
        let message = h
            .client
            .request(h.subjects.fetch_event(producer), bytes.into())
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
    let forged = h
        .client
        .request(
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
    let message = h
        .client
        .request(
            h.subjects.fetch_material("replica"),
            serde_json::to_vec(&FetchMaterial {
                material,
                range: None,
            })
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
async fn oversized_fetched_payloads_use_verified_bounded_transfers(db: devkit::pg::TestDatabase) {
    use sinex_protocol::{FetchPayload, FetchPayloadReply};
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
    let (material, frame) = h.material_frame("large-fetch-material", b"retained source\n");
    assert_eq!(h.send("kitty", &frame).await.refusal, None);
    let command = "x ".repeat(h.client.server_info().max_payload);
    let inner = Ingress::Events {
        frame: "large-fetch-event".into(),
        events: vec![imported(material, 0, &command)],
        failures: vec![],
    };
    let encoded = serde_json::to_vec(&inner).unwrap();
    let blob = h.fixture_blob(&encoded);
    let ack = h
        .send(
            "kitty",
            &Ingress::LargeFrame {
                frame: "large-fetch-event".into(),
                blob,
                length: encoded.len() as u64,
            },
        )
        .await;
    assert_eq!(ack.refusal, None, "{ack:?}");
    let event = ack.admitted[0];
    let response = h
        .client
        .request(
            h.subjects.fetch_event("replica"),
            serde_json::to_vec(&FetchEvent { event }).unwrap().into(),
        )
        .await
        .unwrap();
    assert!(response.payload.len() <= h.client.server_info().max_payload);
    let reply: FetchEventReply = serde_json::from_slice(&response.payload).unwrap();
    assert_eq!(reply.refusal, None);
    assert!(reply.event.unwrap().payload.is_none());
    let reference = reply.payload_ref.unwrap();
    let mut bytes = Vec::new();
    let mut transfers = 0;
    while (bytes.len() as u64) < reference.length {
        let request = FetchPayload {
            event,
            offset: bytes.len() as u64,
            length: u64::MAX,
            hash: reference.hash.clone(),
        };
        let response = h
            .client
            .request(
                h.subjects.fetch_payload("replica"),
                serde_json::to_vec(&request).unwrap().into(),
            )
            .await
            .unwrap();
        assert!(response.payload.len() <= h.client.server_info().max_payload);
        let reply: FetchPayloadReply = serde_json::from_slice(&response.payload).unwrap();
        assert_eq!(reply.refusal, None);
        assert_eq!(reply.offset, bytes.len() as u64);
        assert_eq!(reply.total, reference.length);
        assert_eq!(reply.hash, reference.hash);
        let data = base64::engine::general_purpose::STANDARD
            .decode(reply.data)
            .unwrap();
        assert!(!data.is_empty());
        bytes.extend(data);
        transfers += 1;
    }
    assert!(transfers > 1);
    assert_eq!(
        format!("blake3:{}", blake3::hash(&bytes).to_hex()),
        reference.hash
    );
    let payload: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(payload["command"], command);
    let request = FetchPayload {
        event,
        offset: 0,
        length: 1,
        hash: "blake3:changed".into(),
    };
    let response = h
        .client
        .request(
            h.subjects.fetch_payload("replica"),
            serde_json::to_vec(&request).unwrap().into(),
        )
        .await
        .unwrap();
    let reply: FetchPayloadReply = serde_json::from_slice(&response.payload).unwrap();
    assert_eq!(reply.refusal_kind, Some(FetchRefusalKind::Changed));
    assert!(reply.data.is_empty());
}

#[devkit::test]
async fn oversized_material_metadata_has_verified_range_transfers(db: devkit::pg::TestDatabase) {
    use sinex_protocol::{FetchRange, MaterialFrame};
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
    let (material, mut frame) = h.material_frame("large-manifest", b"retained source\n");
    let manifest = sinex_model::Manifest::from_json(
        json!({"description":"x ".repeat(h.client.server_info().max_payload)}),
    );
    if let Ingress::Material { material, .. } = &mut frame {
        material.manifest = manifest.clone();
    } else {
        panic!("material frame");
    }
    let encoded = serde_json::to_vec(&frame).unwrap();
    let blob = h.fixture_blob(&encoded);
    assert_eq!(
        h.send(
            "kitty",
            &Ingress::LargeFrame {
                frame: "large-manifest".into(),
                blob,
                length: encoded.len() as u64
            }
        )
        .await
        .refusal,
        None
    );
    let response = h
        .client
        .request(
            h.subjects.fetch_material("replica"),
            serde_json::to_vec(&FetchMaterial {
                material,
                range: None,
            })
            .unwrap()
            .into(),
        )
        .await
        .unwrap();
    let reply: FetchMaterialReply = serde_json::from_slice(&response.payload).unwrap();
    assert_eq!(reply.refusal, None);
    let reference = reply.material_ref.unwrap();
    let mut bytes = Vec::new();
    while (bytes.len() as u64) < reference.length {
        let request = FetchMaterial {
            material,
            range: Some(FetchRange {
                offset: bytes.len() as u64,
                length: u64::MAX,
                hash: reference.hash.clone(),
            }),
        };
        let response = h
            .client
            .request(
                h.subjects.fetch_material("replica"),
                serde_json::to_vec(&request).unwrap().into(),
            )
            .await
            .unwrap();
        assert!(response.payload.len() <= h.client.server_info().max_payload);
        let reply: FetchMaterialReply = serde_json::from_slice(&response.payload).unwrap();
        assert_eq!(reply.refusal, None);
        let transfer = reply.transfer.unwrap();
        assert_eq!(transfer.offset, bytes.len() as u64);
        assert_eq!(transfer.total, reference.length);
        assert_eq!(transfer.hash, reference.hash);
        let data = base64::engine::general_purpose::STANDARD
            .decode(transfer.data)
            .unwrap();
        assert!(!data.is_empty());
        bytes.extend(data);
    }
    assert_eq!(
        format!("blake3:{}", blake3::hash(&bytes).to_hex()),
        reference.hash
    );
    let reply: FetchMaterialReply = serde_json::from_slice(&bytes).unwrap();
    let frame: MaterialFrame = reply.material.unwrap();
    assert_eq!(frame.manifest, manifest);
    assert!(!reply.extents.is_empty());
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
