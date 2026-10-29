use super::*;

#[devkit::test]
async fn excision_retains_negative_receipts_for_immutable_siblings_and_never_resurrects_a_frame(
    db: devkit::pg::TestDatabase,
) {
    let declaration = super::kinds::declaration("example.receipt");
    let h = Harness::start_with_source(db, "example", vec![declaration.clone()]).await;
    assert!(
        h.send(
            "example",
            &Ingress::RegisterKinds {
                frame: "receipt-kind".into(),
                declarations: vec![declaration.clone()]
            }
        )
        .await
        .refusal
        .is_none()
    );
    let (first, first_material) =
        h.material_for("example", "first-material", b"first retained input\n");
    let (second, second_material) =
        h.material_for("example", "second-material", b"second retained input\n");
    assert!(h.send("example", &first_material).await.refusal.is_none());
    assert!(h.send("example", &second_material).await.refusal.is_none());
    let event = |id: &str, material: Option<MaterialId>, parents: Vec<EventId>| ProducerEvent {
        kind: declaration.kind.clone(),
        payload: json!({"id":id,"text":"retained reading"}),
        location: material.map(|material| RecordLocation {
            material,
            anchor: 0,
        }),
        output_index: 0,
        parents,
        reading_order: Some(0),
        class: sinex_model::Class::Open,
    };
    let baseline = Ingress::Events {
        frame: "baseline".into(),
        source: Some(test_source("example")),
        coverage: None,
        events: vec![event("baseline", Some(second), vec![])],
        failures: vec![],
    };
    let parent = h.send("example", &baseline).await.admitted[0];
    let mixed = Ingress::Events {
        frame: "mixed".into(),
        source: Some(test_source("example")),
        coverage: None,
        events: vec![
            event("removed", Some(first), vec![]),
            event("sibling", None, vec![parent]),
        ],
        failures: vec![],
    };
    let receipt = h.send("example", &mixed).await;
    assert_eq!(receipt.refusal, None);
    assert_eq!(receipt.admitted.len(), 2);
    let sibling = receipt.admitted[1];
    let remove = |material| Excise {
        materials: vec![material],
    };
    let first_result: ExciseReply = sinex_transport::replies::request_json(
        &h.client,
        h.subjects.excise("example"),
        "example",
        &remove(first),
        Duration::from_secs(20),
    )
    .await
    .unwrap();
    assert_eq!(first_result.refusal, None);
    assert_eq!(first_result.events, 1);
    let partial = h.recorded("example", &["mixed"]).await.pop().unwrap();
    assert_eq!(partial.refusal_kind(), Some(FrameRefusal::PartiallyExcised));
    assert!(partial.admitted.is_empty());
    assert!(partial.material.is_none());
    assert!(partial.material_extents.is_empty());
    assert_eq!(partial.number, receipt.number);
    assert_eq!(partial.content_hash(), receipt.content_hash());
    assert_eq!(
        h.count(&format!(
            "SELECT count(*) FROM core.events WHERE id='{sibling}' AND producer_frame={}",
            receipt.number.unwrap()
        ))
        .await,
        1,
        "the immutable sibling keeps the same receipt FK"
    );
    assert_eq!(h.send("example", &mixed).await, partial);
    let mut changed = mixed.clone();
    let Ingress::Events { events, .. } = &mut changed else {
        unreachable!()
    };
    events[0].payload["text"] = json!("changed body");
    let mut acks = h.client.subscribe(h.subjects.ack("example")).await.unwrap();
    h.client.flush().await.unwrap();
    let conflict = sinex_transport::producer::submit_frame(
        &h.client,
        &h.subjects,
        "example",
        &mut acks,
        &changed,
        Duration::from_secs(20),
    )
    .await
    .unwrap_err();
    let sinex_transport::producer::SubmitError::ReceiptConflict {
        frame,
        offered_hash,
        receipt: prior,
    } = conflict
    else {
        panic!("typed owner conflict evidence")
    };
    assert_eq!(frame, "mixed");
    assert_eq!(
        offered_hash,
        sinex_transport::producer::content_hash(&changed).unwrap()
    );
    assert_eq!(*prior, partial);
    assert_eq!(h.count("SELECT count(*) FROM core.events").await, 2);
    let second_result: ExciseReply = sinex_transport::replies::request_json(
        &h.client,
        h.subjects.excise("example"),
        "example",
        &remove(second),
        Duration::from_secs(20),
    )
    .await
    .unwrap();
    assert_eq!(second_result.refusal, None);
    assert_eq!(second_result.events, 2);
    let removed = h.recorded("example", &["mixed"]).await.pop().unwrap();
    assert_eq!(removed.refusal_kind(), Some(FrameRefusal::Excised));
    assert!(removed.admitted.is_empty());
    assert_eq!(h.send("example", &mixed).await, removed);
    assert_eq!(h.count("SELECT count(*) FROM core.events").await, 0);
    assert_eq!(
        h.send("example", &first_material).await.refusal_kind(),
        Some(FrameRefusal::Excised)
    );
}
