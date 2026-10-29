use super::*;

fn receipt(frame: &Ingress) -> Acknowledgment {
    Acknowledgment {
        number: Some(1),
        frame: frame.frame().into(),
        material: None,
        material_extents: vec![],
        admitted: vec![],
        outcome: serde_json::json!({"frame_content_hash":content_hash(frame).unwrap()}),
        refusal: None,
        commit_seq: Some(1),
    }
}

#[devkit::test]
fn conflict_keeps_the_prior_owned_receipt_distinct_from_the_offered_body() {
    let prior = Ingress::RegisterKinds {
        frame: "same-frame".into(),
        declarations: vec![],
    };
    let offered = Ingress::Events {
        frame: "same-frame".into(),
        source: None,
        coverage: None,
        events: vec![],
        failures: vec![],
    };
    let saved = receipt(&prior);
    assert_eq!(check_receipt(&prior, saved.clone()).unwrap(), saved);
    let SubmitError::ReceiptConflict {
        frame,
        offered_hash,
        receipt: actual,
    } = check_receipt(&offered, saved.clone()).unwrap_err()
    else {
        panic!("typed conflict evidence")
    };
    assert_eq!(frame, offered.frame());
    assert_eq!(offered_hash, content_hash(&offered).unwrap());
    assert_eq!(*actual, saved);
    assert_ne!(actual.content_hash(), Some(offered_hash.as_str()));
}

#[devkit::test]
fn foreign_frame_or_missing_durable_identity_never_exposes_a_prior_receipt_as_conflict_evidence() {
    let offered = Ingress::RegisterKinds {
        frame: "owned-frame".into(),
        declarations: vec![],
    };
    let mut foreign = receipt(&offered);
    foreign.frame = "foreign-frame".into();
    let error = check_receipt(&offered, foreign).unwrap_err();
    assert!(matches!(
        error,
        SubmitError::Refused {
            kind: sinex_model::FrameRefusal::Conflict,
            ..
        }
    ));
    assert!(!error.to_string().contains("foreign-frame"));
    let mut unknown = receipt(&offered);
    unknown.number = None;
    unknown.outcome = serde_json::json!({});
    assert!(matches!(
        check_receipt(&offered, unknown),
        Err(SubmitError::NoReceipt(_))
    ));
}

#[devkit::test]
fn malformed_receipt_requires_an_invalid_name_and_exact_transmitted_envelope() {
    for frame in [
        Ingress::RegisterKinds {
            frame: "".into(),
            declarations: vec![],
        },
        Ingress::LargeFrame {
            frame: "broken\0name".into(),
            blob: "blake3:logical-body".into(),
            length: 42,
        },
    ] {
        let bytes = serde_json::to_vec(&frame).unwrap();
        let malformed = format!("malformed/{}", blake3::hash(&bytes).to_hex());
        let mut saved = receipt(&frame);
        saved.frame = malformed.clone();
        saved.refusal = Some("invalid frame name".into());
        saved.outcome["refusal_kind"] = serde_json::json!("malformed");
        let check = |receipt| {
            check_receipt_for(
                &frame,
                receipt,
                content_hash(&frame).unwrap(),
                Some(&malformed),
            )
        };
        assert_eq!(check(saved.clone()).unwrap(), saved);
        // The public recovery helper has no transmitted-envelope evidence.
        assert!(check_receipt(&frame, saved.clone()).is_err());
        let other_envelope = format!(
            "malformed/{}",
            blake3::hash(b"different transmitted bytes").to_hex()
        );
        assert!(
            check_receipt_for(
                &frame,
                saved.clone(),
                content_hash(&frame).unwrap(),
                Some(&other_envelope)
            )
            .is_err()
        );
        let mut foreign = saved.clone();
        foreign.frame.push('x');
        assert!(check(foreign).is_err());
        let mut wrong_body = saved.clone();
        wrong_body.outcome["frame_content_hash"] = serde_json::json!("blake3:different");
        assert!(check(wrong_body).is_err());
        let mut not_refused = saved.clone();
        not_refused.refusal = None;
        assert!(check(not_refused).is_err());
        let mut undurable = saved;
        undurable.number = None;
        assert!(check(undurable).is_err());
    }
    let valid = Ingress::RegisterKinds {
        frame: "valid-offered-name".into(),
        declarations: vec![],
    };
    let malformed = format!(
        "malformed/{}",
        blake3::hash(&serde_json::to_vec(&valid).unwrap()).to_hex()
    );
    let mut saved = receipt(&valid);
    saved.frame = malformed.clone();
    saved.refusal = Some("invalid content".into());
    saved.outcome["refusal_kind"] = serde_json::json!("malformed");
    assert!(
        check_receipt_for(
            &valid,
            saved,
            content_hash(&valid).unwrap(),
            Some(&malformed)
        )
        .is_err()
    );
}

// The exchange deadline detects waiting for a broadcast that cannot be
// regenerated. It is shorter than the unchanged production hint idle.
const EXCHANGE: Duration = Duration::from_secs(10);
const HINT_IDLE: Duration = Duration::from_secs(30);
const PRODUCER: &str = "capture";

struct Duplicate {
    owner: Client,
    client: Client,
    subjects: Subjects,
    acks: Subscriber,
    stream: jetstream::stream::Stream,
    consumer: jetstream::consumer::PullConsumer,
    message: jetstream::Message,
    frame: Ingress,
    _server: devkit::nats::TestNats,
}
impl Duplicate {
    async fn new(committed: bool) -> Self {
        let server = devkit::nats::test_nats().unwrap();
        let subjects = Subjects::new(server.prefix());
        let owner = async_nats::connect(server.url()).await.unwrap();
        let js = jetstream::new(owner.clone());
        let stream = js
            .create_stream(jetstream::stream::Config {
                name: subjects.ingress_stream(),
                subjects: vec![subjects.ingress_all()],
                storage: jetstream::stream::StorageType::Memory,
                retention: jetstream::stream::RetentionPolicy::WorkQueue,
                duplicate_window: Duration::from_secs(120),
                ..Default::default()
            })
            .await
            .unwrap();
        let consumer = stream
            .create_consumer(jetstream::consumer::pull::Config {
                durable_name: Some("owner".into()),
                ack_policy: jetstream::consumer::AckPolicy::Explicit,
                ..Default::default()
            })
            .await
            .unwrap();
        let frame = Ingress::RegisterKinds {
            frame: "frozen-before-restart".into(),
            declarations: vec![],
        };
        let mut headers = HeaderMap::new();
        headers.insert("Nats-Msg-Id", frame.frame());
        let body = serde_json::to_vec(&frame).unwrap();
        let first = js
            .publish_with_headers(
                subjects.ingress(PRODUCER),
                headers.clone(),
                body.clone().into(),
            )
            .await
            .unwrap()
            .await
            .unwrap();
        assert!(!first.duplicate);
        let message = tokio::time::timeout(EXCHANGE, async {
            consumer
                .batch()
                .max_messages(1)
                .expires(EXCHANGE)
                .messages()
                .await
                .expect("one waiting pull for the committed original")
                .next()
                .await
                .expect("the waiting pull delivers the committed original")
                .expect("the original delivery succeeds")
        })
        .await
        .expect("the committed original is delivered within the exchange deadline");
        assert_eq!(message.payload.as_ref(), body.as_slice());
        if committed {
            message.double_ack().await.unwrap();
            // This completed owner receipt's original hint has no subscriber.
            // A restarted producer cannot receive it as a queued notification.
            owner
                .publish(
                    subjects.ack(PRODUCER),
                    serde_json::to_vec(&ReceiptHint {
                        number: 1,
                        frame_hash: blake3::hash(frame.frame().as_bytes()).to_hex().to_string(),
                    })
                    .unwrap()
                    .into(),
                )
                .await
                .unwrap();
            owner.flush().await.unwrap();
        }
        let duplicate = js
            .publish_with_headers(subjects.ingress(PRODUCER), headers, body.into())
            .await
            .unwrap()
            .await
            .unwrap();
        assert!(duplicate.duplicate, "the real broker suppresses this retry");
        assert_eq!(duplicate.sequence, first.sequence);
        let client = async_nats::connect(server.url()).await.unwrap();
        let acks = client.subscribe(subjects.ack(PRODUCER)).await.unwrap();
        client.flush().await.unwrap();
        Self {
            owner,
            client,
            subjects,
            acks,
            stream,
            consumer,
            message,
            frame,
            _server: server,
        }
    }
    async fn submit(&mut self, frame: &Ingress) -> Result<Acknowledgment, SubmitError> {
        tokio::time::timeout(
            EXCHANGE,
            submit_frame(
                &self.client,
                &self.subjects,
                PRODUCER,
                &mut self.acks,
                frame,
                HINT_IDLE,
            ),
        )
        .await
        .expect("owned receipt exchange waited for an unrepeated broadcast hint")
    }
    async fn once(&mut self) {
        assert_eq!(self.stream.info().await.unwrap().state.last_sequence, 1);
        let info = self.consumer.info().await.unwrap();
        assert_eq!(
            info.delivered.consumer_sequence, 1,
            "no new ingress delivery"
        );
        assert_eq!(info.num_pending, 0);
        assert_eq!(info.num_ack_pending, 0);
    }
}
async fn answer_recorded(
    owner: &Client,
    requests: &mut Subscriber,
    acknowledgments: Vec<Acknowledgment>,
) -> RecordedQuery {
    let request = requests.next().await.unwrap();
    let query: RecordedQuery = serde_json::from_slice(&request.payload).unwrap();
    assert_eq!(query.after, 0);
    crate::replies::publish_json(
        owner,
        request.reply.unwrap().to_string(),
        RecordedPage {
            acknowledgments,
            next: None,
            ..Default::default()
        },
        HINT_IDLE,
    )
    .await
    .unwrap();
    query
}

#[devkit::test]
async fn duplicate_after_restart_resolves_owned_receipt_without_a_regenerated_hint() {
    use futures::FutureExt;
    let mut fixture = Duplicate::new(true).await;
    let saved = receipt(&fixture.frame);
    let expected = saved.clone();
    let mut requests = fixture
        .owner
        .subscribe(fixture.subjects.recorded(PRODUCER))
        .await
        .unwrap();
    fixture.owner.flush().await.unwrap();
    let owner = fixture.owner.clone();
    let name = fixture.frame.frame().to_owned();
    let responder = tokio::spawn(async move {
        let query = answer_recorded(&owner, &mut requests, vec![saved]).await;
        assert!(query.frames.contains(&name));
        assert!(query.numbers.is_empty());
    });
    let offered = fixture.frame.clone();
    assert_eq!(fixture.submit(&offered).await.unwrap(), expected);
    responder.await.unwrap();
    assert!(
        fixture.acks.next().now_or_never().is_none(),
        "no fresh hint was emitted"
    );
    fixture.once().await;
}

#[devkit::test]
async fn duplicate_without_owner_admission_still_waits_for_the_durable_receipt() {
    let mut fixture = Duplicate::new(false).await;
    let saved = receipt(&fixture.frame);
    let expected = saved.clone();
    let mut requests = fixture
        .owner
        .subscribe(fixture.subjects.recorded(PRODUCER))
        .await
        .unwrap();
    fixture.owner.flush().await.unwrap();
    let owner = fixture.owner.clone();
    let name = fixture.frame.frame().to_owned();
    let ack_subject = fixture.subjects.ack(PRODUCER);
    let original = fixture.message.clone();
    let responder = tokio::spawn(async move {
        let query = answer_recorded(&owner, &mut requests, vec![]).await;
        assert!(query.frames.contains(&name));
        // Only after the empty lookup does this owner commit the original.
        original.double_ack().await.unwrap();
        owner
            .publish(
                ack_subject,
                serde_json::to_vec(&ReceiptHint {
                    number: 1,
                    frame_hash: blake3::hash(name.as_bytes()).to_hex().to_string(),
                })
                .unwrap()
                .into(),
            )
            .await
            .unwrap();
        owner.flush().await.unwrap();
        let query = answer_recorded(&owner, &mut requests, vec![saved]).await;
        assert_eq!(query.numbers, vec![1]);
        assert!(query.frames.is_empty());
    });
    let offered = fixture.frame.clone();
    assert_eq!(fixture.submit(&offered).await.unwrap(), expected);
    responder.await.unwrap();
    fixture.once().await;
}

#[devkit::test]
async fn duplicate_receipt_with_different_bytes_remains_typed_conflict_evidence() {
    let mut fixture = Duplicate::new(true).await;
    let prior = receipt(&fixture.frame);
    let expected = prior.clone();
    let mut requests = fixture
        .owner
        .subscribe(fixture.subjects.recorded(PRODUCER))
        .await
        .unwrap();
    fixture.owner.flush().await.unwrap();
    let owner = fixture.owner.clone();
    let responder =
        tokio::spawn(async move { answer_recorded(&owner, &mut requests, vec![prior]).await });
    let offered = Ingress::Events {
        frame: fixture.frame.frame().into(),
        source: None,
        coverage: None,
        events: vec![],
        failures: vec![],
    };
    let SubmitError::ReceiptConflict {
        frame,
        offered_hash,
        receipt,
    } = fixture.submit(&offered).await.unwrap_err()
    else {
        panic!("duplicate is not admission of the changed offer")
    };
    assert_eq!(frame, offered.frame());
    assert_eq!(offered_hash, content_hash(&offered).unwrap());
    assert_eq!(*receipt, expected);
    responder.await.unwrap();
    fixture.once().await;
}

#[devkit::test]
async fn duplicate_broker_ack_with_unavailable_owner_never_becomes_an_admission_receipt() {
    let mut fixture = Duplicate::new(true).await;
    let offered = fixture.frame.clone();
    assert!(matches!(fixture.submit(&offered).await,
        Err(SubmitError::Reply(crate::replies::ReplyError::Transport(reason))) if reason.contains("no responders")));
    fixture.once().await;
}

#[devkit::test]
async fn duplicate_refusal_preserves_the_owned_terminal_disposition() {
    for kind in [
        sinex_model::FrameRefusal::Content,
        sinex_model::FrameRefusal::Excised,
        sinex_model::FrameRefusal::PartiallyExcised,
    ] {
        let mut fixture = Duplicate::new(true).await;
        let mut saved = receipt(&fixture.frame);
        saved.refusal = Some("synthetic owner refusal".into());
        saved.outcome["refusal_kind"] = serde_json::to_value(kind).unwrap();
        if matches!(
            kind,
            sinex_model::FrameRefusal::Excised | sinex_model::FrameRefusal::PartiallyExcised
        ) {
            saved
                .outcome
                .as_object_mut()
                .unwrap()
                .remove("frame_content_hash");
        }
        let expected = saved.clone();
        let mut requests = fixture
            .owner
            .subscribe(fixture.subjects.recorded(PRODUCER))
            .await
            .unwrap();
        fixture.owner.flush().await.unwrap();
        let owner = fixture.owner.clone();
        let responder =
            tokio::spawn(async move { answer_recorded(&owner, &mut requests, vec![saved]).await });
        let offered = fixture.frame.clone();
        assert_eq!(fixture.submit(&offered).await.unwrap(), expected);
        responder.await.unwrap();
        fixture.once().await;
    }
}

#[devkit::test]
async fn optional_continuations_preserve_ordinary_wire_and_explicit_rejection_is_not_retried() {
    #[derive(serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    struct PreviousQuery {
        frames: Vec<String>,
        numbers: Vec<i64>,
        after: i64,
    }
    let server = devkit::nats::test_nats().unwrap();
    let client = async_nats::connect(server.url()).await.unwrap();
    let subjects = Subjects::new(server.prefix());
    let mut requests = client.subscribe(subjects.recorded(PRODUCER)).await.unwrap();
    client.flush().await.unwrap();
    let owner = client.clone();
    let responder = tokio::spawn(async move {
        for explicit in [false, true] {
            let request = requests.next().await.unwrap();
            let previous = serde_json::from_slice::<PreviousQuery>(&request.payload);
            let value = if explicit {
                assert!(
                    previous.is_err(),
                    "old deny_unknown_fields peer refuses the explicit feature"
                );
                serde_json::json!({"refusal":"continuations unsupported"})
            } else {
                let previous = previous.unwrap();
                assert_eq!(previous.frames, vec!["owned"]);
                assert!(previous.numbers.is_empty());
                assert_eq!(previous.after, 0);
                serde_json::json!({"acknowledgments":[],"next":null})
            };
            let _ = crate::replies::publish_json(
                &owner,
                request.reply.unwrap().to_string(),
                value,
                HINT_IDLE,
            )
            .await;
        }
    });
    let ordinary = RecordedQuery {
        frames: vec!["owned".into()],
        ..Default::default()
    };
    let page: RecordedPage = crate::replies::request_json(
        &client,
        subjects.recorded(PRODUCER),
        PRODUCER,
        &ordinary,
        HINT_IDLE,
    )
    .await
    .unwrap();
    assert!(page.continuations.is_empty());
    let explicit = RecordedQuery {
        continuations: vec![sinex_protocol::RecordedContinuationRequest {
            frame: "retained-material".into(),
            frame_content_hash: "blake3:expected-body".into(),
            material: sinex_model::MaterialId::new(),
            blob: "blake3:expected-bytes".into(),
            length: 0,
            uses: vec![],
        }],
        ..Default::default()
    };
    let result = crate::replies::request_json::<RecordedPage>(
        &client,
        subjects.recorded(PRODUCER),
        PRODUCER,
        &explicit,
        HINT_IDLE,
    )
    .await;
    assert!(
        result.is_err(),
        "unsupported evidence remains unavailable, never a fresh ordinary ACK"
    );
    responder.await.unwrap();
}
