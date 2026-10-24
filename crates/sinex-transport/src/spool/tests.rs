use super::*;
use sinex_model::{Class, InputRef, MaterialId};
use sinex_protocol::producer_bytes::{Material, MaterialFrame};

fn declaration(bytes: &[u8], frame: &str) -> Publication {
    Publication {
        version: 1,
        frame: frame.into(),
        class: Class::Guarded,
        material: None,
        envelope: Some(Bytes {
            blob: format!("blake3:{}", blake3::hash(bytes).to_hex()),
            length: bytes.len() as u64,
        }),
    }
}

#[test]
fn partial_prefix_survives_reopen_and_conflicting_or_overflowing_resends_refuse() {
    let root = tempfile::tempdir().unwrap();
    let bytes = b"exact frozen bytes across restart";
    let p = declaration(bytes, "final-frame");
    assert!(!receive(root.path(), "node", &p, Role::Envelope, 0, &bytes[..7]).unwrap());
    let partial = directory(root.path(), "node", &p).join("envelope.partial");
    assert_eq!(std::fs::read(&partial).unwrap(), &bytes[..7]);
    assert!(receive(root.path(), "node", &p, Role::Envelope, 0, b"changed").is_err());
    assert!(receive(root.path(), "node", &p, Role::Envelope, 8, b"x").is_err());
    assert!(receive(root.path(), "node", &p, Role::Envelope, u64::MAX, b"xx").is_err());
    assert!(snapshot(root.path(), "node", &p, Role::Envelope).is_err());
    assert!(receive(root.path(), "node", &p, Role::Envelope, 4, &bytes[4..]).unwrap());
    assert!(receive(root.path(), "node", &p, Role::Envelope, 0, &bytes[..7]).unwrap());
    assert!(receive(root.path(), "node", &p, Role::Envelope, 0, b"changed").is_err());
    let mut original = snapshot(root.path(), "node", &p, Role::Envelope).unwrap();
    std::fs::write(
        directory(root.path(), "node", &p).join("envelope"),
        b"provider changed",
    )
    .unwrap();
    let mut retained = Vec::new();
    original.read_to_end(&mut retained).unwrap();
    assert_eq!(retained, bytes, "receiver owns the verified inode");
    assert!(snapshot(root.path(), "node", &p, Role::Envelope).is_err());
    discard(root.path(), "node", &p).unwrap();
    discard(root.path(), "node", &p).unwrap();
}

#[test]
fn byte_roles_and_sources_have_distinct_private_lifetimes() {
    let root = tempfile::tempdir().unwrap();
    let body = b"material bytes";
    let envelope = b"different envelope bytes";
    let mut p = declaration(envelope, "same-final-frame");
    let frame = MaterialFrame {
        id: MaterialId::new(),
        input: InputRef {
            kind: "file".into(),
            locator: "fixture".into(),
            host: "h".into(),
        },
        blob: format!("blake3:{}", blake3::hash(body).to_hex()),
        length: body.len() as u64,
        parser: "zsh".into(),
        parser_version: 1,
        part_index: 0,
        previous_part: None,
        manifest: serde_json::json!({}),
    };
    p.material = Some(Material::from_frame(&frame));
    stage(root.path(), "one", &p, Role::Material, body.as_slice()).unwrap();
    stage(root.path(), "one", &p, Role::Envelope, envelope.as_slice()).unwrap();
    stage(root.path(), "two", &p, Role::Material, body.as_slice()).unwrap();
    let mut a = String::new();
    snapshot(root.path(), "one", &p, Role::Material)
        .unwrap()
        .read_to_string(&mut a)
        .unwrap();
    assert_eq!(a, "material bytes");
    let mut b = String::new();
    snapshot(root.path(), "one", &p, Role::Envelope)
        .unwrap()
        .read_to_string(&mut b)
        .unwrap();
    assert_eq!(b, "different envelope bytes");
    discard(root.path(), "one", &p).unwrap();
    assert!(snapshot(root.path(), "two", &p, Role::Material).is_ok());
    let mut sealed = p.clone();
    sealed.class = Class::Sealed;
    assert!(
        stage(
            root.path(),
            "sealed",
            &sealed,
            Role::Material,
            body.as_slice()
        )
        .is_err()
    );
    assert!(!directory(root.path(), "sealed", &sealed).exists());
}

#[test]
fn empty_and_wrong_named_uploads_do_not_publish_public_cas_bytes() {
    let root = tempfile::tempdir().unwrap();
    let empty = declaration(b"", "empty");
    assert!(receive(root.path(), "node", &empty, Role::Envelope, 0, b"").unwrap());
    let wrong = declaration(b"right", "wrong");
    assert!(receive(root.path(), "node", &wrong, Role::Envelope, 0, b"wrong").is_err());
    assert!(!root.path().join("blobs").exists());
    assert!(
        !directory(root.path(), "node", &wrong)
            .join("envelope")
            .exists()
    );
    assert!(receive(root.path(), "node", &wrong, Role::Envelope, 0, b"right").unwrap());
}

#[test]
fn frozen_sender_identity_survives_receiver_cleanup_and_differing_retry_refuses() {
    let root = tempfile::tempdir().unwrap();
    let body = b"exact material";
    let frame = MaterialFrame {
        id: MaterialId::new(),
        input: InputRef {
            kind: "file".into(),
            locator: "fixture".into(),
            host: "h".into(),
        },
        blob: format!("blake3:{}", blake3::hash(body).to_hex()),
        length: body.len() as u64,
        parser: "zsh".into(),
        parser_version: 1,
        part_index: 0,
        previous_part: None,
        manifest: serde_json::from_str(r#"{"z":0,"a":{"z":3,"a":4}}"#).unwrap(),
    };
    let inner = Ingress::Material {
        frame: "inner".into(),
        material: Box::new(frame),
    };
    let envelope = encode_frame(&inner);
    let mut publication = declaration(&envelope, "final");
    if let Ingress::Material { material, .. } = &inner {
        publication.material = Some(Material::from_frame(material));
    }
    let frozen = Frozen {
        publication,
        frame: inner,
    };
    assert_eq!(freeze(root.path(), "node", &frozen).unwrap(), frozen);
    for (role, _) in frozen.publication.roles() {
        stage(
            root.path(),
            "node",
            &frozen.publication,
            role,
            if role == Role::Material {
                body.as_slice()
            } else {
                envelope.as_slice()
            },
        )
        .unwrap();
    }
    discard_bytes(root.path(), "node", &frozen.publication).unwrap();
    assert_eq!(
        load(root.path(), "node", "final").unwrap(),
        Some(frozen.clone())
    );
    assert!(snapshot(root.path(), "node", &frozen.publication, Role::Material).is_err());
    let mut different = frozen.clone();
    if let Ingress::Material { material, .. } = &mut different.frame {
        material.id = MaterialId::new();
        different.publication.material = Some(Material::from_frame(material));
    }
    let bytes = encode_frame(&different.frame);
    different.publication.envelope = Some(Bytes {
        blob: format!("blake3:{}", blake3::hash(&bytes).to_hex()),
        length: bytes.len() as u64,
    });
    assert!(freeze(root.path(), "node", &different).is_err());
    assert_eq!(
        load(root.path(), "node", "final").unwrap(),
        Some(frozen.clone())
    );
    discard(root.path(), "node", &frozen.publication).unwrap();
    discard(root.path(), "node", &frozen.publication).unwrap();
    assert!(load(root.path(), "node", "final").unwrap().is_none());
}

#[test]
fn pending_artifacts_are_producer_scoped_and_keep_payload_order_through_the_supported_round_trip() {
    let root = tempfile::tempdir().unwrap();
    let frame = Ingress::Events {
        frame: "outer".into(),
        failures: vec![],
        events: vec![sinex_protocol::ProducerEvent {
            event_type: "fixture".into(),
            payload: serde_json::from_str(r#"{"z":0,"a":{"z":3,"a":4}}"#).unwrap(),
            location: None,
            ordinal: 0,
            parents: vec![],
            reading_order: 0,
            class: Class::Guarded,
        }],
    };
    let encoded = encode_frame(&frame);
    let frozen = Frozen {
        publication: declaration(&encoded, "outer"),
        frame,
    };
    freeze(root.path(), "one", &frozen).unwrap();
    freeze(root.path(), "two", &frozen).unwrap();
    let first = pending(root.path(), "one")
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(first, vec![frozen.clone()]);
    assert_eq!(encode_frame(&first[0].frame), encoded);
    let Ingress::Events { events, .. } = &first[0].frame else {
        panic!("events")
    };
    assert_eq!(
        serde_json::to_string(&events[0].payload).unwrap(),
        r#"{"z":0,"a":{"z":3,"a":4}}"#
    );
    let malformed = artifact_directory(root.path(), "one").join(format!("{}.json", "0".repeat(64)));
    std::fs::write(&malformed, b"{not-json").unwrap();
    let entries = pending(root.path(), "one").unwrap().collect::<Vec<_>>();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries.iter().filter(|e| e.is_err()).count(), 1);
    assert_eq!(
        pending(root.path(), "two")
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap(),
        vec![frozen.clone()]
    );
    assert!(pending(root.path(), "absent").unwrap().next().is_none());
    std::fs::remove_file(malformed).unwrap();
    discard(root.path(), "one", &frozen.publication).unwrap();
    assert_eq!(pending(root.path(), "one").unwrap().count(), 0);
    assert_eq!(pending(root.path(), "two").unwrap().count(), 1);
}

mod group;
