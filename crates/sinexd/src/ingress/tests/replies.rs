use super::*;
#[devkit::test]
async fn sixty_five_thousand_outputs_have_one_complete_verified_producer_receipt_without_broadcast_subscribers(
    db: devkit::pg::TestDatabase,
) {
    const OUTPUTS: usize = 65_538;
    let h = Harness::start(db).await;
    let source = serde_json::to_vec(
        &(0..OUTPUTS)
            .map(|index| json!({"id":index,"command":"synthetic"}))
            .collect::<Vec<_>>(),
    )
    .unwrap();
    let (material, mut frame) = h.material_frame("bulk-material", &source);
    if let Ingress::Material { material, .. } = &mut frame {
        material.parser = "synthetic-command-array".into();
        material.parser_version = 1;
        material.input.kind = "provider-array".into();
        material.input.locator = "/synthetic/provider-array.json".into();
        material.manifest =
            sinex_model::Manifest::from_json(json!({"media_type":"application/json"}));
    }
    assert_eq!(h.send("kitty", &frame).await.refusal, None);
    let events = (0..OUTPUTS)
        .map(|index| {
            let mut event = imported(material, 0, "synthetic");
            event.output_index = index as u64;
            event.payload["n"] = json!(index);
            event
        })
        .collect();
    let inner = Ingress::Events {
        source:Some(test_source("kitty")),coverage:None,
        frame: "bulk-outputs".into(),
        events,
        failures: vec![],
    };
    let bytes = serde_json::to_vec(&inner).unwrap();
    let blob = h.fixture_blob(&bytes);
    frame_bytes::write(
        &h.store_dir,
        "kitty",
        "bulk-outputs",
        &blob,
        bytes.len() as u64,
        bytes.as_slice(),
    )
    .unwrap();
    let outer = Ingress::LargeFrame {
        frame: "bulk-outputs".into(),
        blob,
        length: bytes.len() as u64,
    };
    // There is no ack subscriber for this admission. A later frame still completes.
    jetstream::new(h.client.clone())
        .publish(
            h.subjects.ingress("kitty"),
            serde_json::to_vec(&outer).unwrap().into(),
        )
        .await
        .unwrap()
        .await
        .unwrap();
    let mut committed = PgConnection::connect(&h.url).await.unwrap();
    tokio::time::timeout(Duration::from_secs(30),async {
        loop {
            let present=sqlx::query_scalar!(r#"SELECT EXISTS(SELECT 1 FROM core.ingress_frames WHERE producer='kitty' AND frame='bulk-outputs') AS "present!""#).fetch_one(&mut committed).await.unwrap();
            if present {break;}
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.expect("admission commits with no receipt subscriber");
    let (_, probe) = h.material_frame("after-bulk", b"next capture\n");
    assert_eq!(h.send("kitty", &probe).await.refusal, None);
    let mut acks = h.client.subscribe(h.subjects.ack("kitty")).await.unwrap();
    h.client.flush().await.unwrap();
    let receipt = sinex_transport::producer::submit_frame(
        &h.client,
        &h.subjects,
        "kitty",
        &mut acks,
        &outer,
        Duration::from_secs(10),
    )
    .await
    .unwrap();
    assert_eq!(receipt.refusal, None);
    assert_eq!(receipt.admitted.len(), OUTPUTS);
    assert_eq!(receipt.outcome["admitted"], OUTPUTS);
    assert!(receipt.number.is_some());
    assert_eq!(
        receipt
            .admitted
            .iter()
            .collect::<std::collections::HashSet<_>>()
            .len(),
        OUTPUTS
    );
    let producer = sinex_source_core::drivers::producer::Producer::connect(
        h.nats.url(),
        None,
        h.subjects.clone(),
        "kitty",
        &ProducerHello::default(),
    )
    .await
    .unwrap();
    let restored = producer.recorded(&["bulk-outputs".into()]).await.unwrap();
    assert_eq!(restored, vec![receipt.clone()]);
    let repeated = sinex_transport::producer::submit_frame(
        &h.client,
        &h.subjects,
        "kitty",
        &mut acks,
        &outer,
        Duration::from_secs(10),
    )
    .await
    .unwrap();
    assert_eq!(repeated, receipt);
    let mut conn = PgConnection::connect(&h.url).await.unwrap();
    let count = sqlx::query_scalar!(
        "SELECT count(*) FROM core.events WHERE material_id=$1",
        material.0
    )
    .fetch_one(&mut conn)
    .await
    .unwrap();
    assert_eq!(count, Some(OUTPUTS as i64));
    let maximum = sqlx::query_scalar!(
        "SELECT max(output_index) FROM core.events WHERE material_id=$1",
        material.0
    )
    .fetch_one(&mut conn)
    .await
    .unwrap();
    assert_eq!(maximum, Some((OUTPUTS - 1) as i64));
}
#[devkit::test]
async fn request_replies_cannot_be_forwarded_to_a_foreign_principal_inbox(
    db: devkit::pg::TestDatabase,
) {
    let grant = crate::ingress::fetch::Grants { indexed_catalog:None, public_context:false,
        kind_prefixes: vec!["command".into()],
        classes: vec![sinex_model::Class::Open],
        write: None,
    };
    let h = Harness::start_with_grants(
        db,
        std::collections::BTreeMap::from([("kitty".into(), grant)]),
    )
    .await;
    let (material, frame) = h.material_frame("private-material", b"source\n");
    h.send("kitty", &frame).await;
    let receipt = h
        .send(
            "kitty",
            &Ingress::Events {
                source:Some(test_source("kitty")),coverage:None,
                frame: "private-event".into(),
                events: vec![imported(material, 0, "private event")],
                failures: vec![],
            },
        )
        .await;
    let inbox = "_INBOX.phone.foreign-principal";
    let mut foreign = h.client.subscribe(inbox).await.unwrap();
    h.client.flush().await.unwrap();
    h.client
        .publish_with_reply(
            h.subjects.fetch_event("kitty"),
            inbox,
            serde_json::to_vec(&sinex_protocol::FetchEvent {
                event: receipt.admitted[0],
            })
            .unwrap()
            .into(),
        )
        .await
        .unwrap();
    h.client.flush().await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(300), foreign.next())
            .await
            .is_err()
    );
}
