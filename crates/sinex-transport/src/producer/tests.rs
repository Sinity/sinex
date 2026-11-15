use super::*;

fn ack(frame: &Ingress) -> Acknowledgment {
    Acknowledgment {
        number: 1,
        frame: frame.frame().into(),
        content_hash: Some(content_hash(frame).unwrap()),
        result: sinex_protocol::FrameResult::Admitted {
            material: None,
            material_extents: vec![],
            admitted: vec![],
            counts: Default::default(),
            commit_seq: Some(1),
        },
    }
}

fn refused(kind: sinex_model::FrameRefusal, reason: &str) -> sinex_protocol::FrameResult {
    sinex_protocol::FrameResult::Refused {
        kind,
        reason: reason.into(),
    }
}

#[devkit::test]
fn conflict_reports_earlier_ack_apart_from_offered_body() {
    let prior = Ingress::RegisterKinds {
        frame: "same-frame".into(),
        declarations: vec![],
    };
    let offered = Ingress::Events {
        frame: "same-frame".into(),
        source: None,
        events: vec![],
        failures: vec![],
    };
    let saved = ack(&prior);
    assert_eq!(check_ack(&prior, saved.clone()).unwrap(), saved);
    let SubmitError::AckConflict {
        frame,
        offered_hash,
        ack: recorded,
    } = check_ack(&offered, saved.clone()).unwrap_err()
    else {
        panic!("typed conflict")
    };
    assert_eq!(frame, offered.frame());
    assert_eq!(offered_hash, content_hash(&offered).unwrap());
    assert_eq!(*recorded, saved);
    assert_ne!(recorded.content_hash, Some(offered_hash));
}

#[devkit::test]
fn other_frame_or_unsaved_ack_is_not_a_conflict() {
    let offered = Ingress::RegisterKinds {
        frame: "offered-frame".into(),
        declarations: vec![],
    };
    let mut other = ack(&offered);
    other.frame = "other-frame".into();
    let error = check_ack(&offered, other).unwrap_err();
    assert!(matches!(
        error,
        SubmitError::Refused {
            kind: sinex_model::FrameRefusal::Conflict,
            ..
        }
    ));
    assert!(!error.to_string().contains("other-frame"));
    let mut unknown = ack(&offered);
    unknown.number = 0;
    unknown.content_hash = None;
    assert!(matches!(
        check_ack(&offered, unknown),
        Err(SubmitError::NoAck(_))
    ));
}

#[devkit::test]
fn malformed_ack_needs_invalid_name_and_sent_envelope() {
    for frame in [
        Ingress::RegisterKinds {
            frame: "".into(),
            declarations: vec![],
        },
        Ingress::LargeFrame {
            frame: "broken\0name".into(),
            blob: "blake3:frame-body".into(),
            length: 42,
        },
    ] {
        let bytes = serde_json::to_vec(&frame).unwrap();
        let malformed = format!("malformed/{}", blake3::hash(&bytes).to_hex());
        let mut saved = ack(&frame);
        saved.frame = malformed.clone();
        saved.result = refused(sinex_model::FrameRefusal::Malformed, "invalid frame name");
        let check =
            |ack| check_ack_for(&frame, ack, content_hash(&frame).unwrap(), Some(&malformed));
        assert_eq!(check(saved.clone()).unwrap(), saved);
        // The public recovery helper does not know the sent envelope's digest.
        assert!(check_ack(&frame, saved.clone()).is_err());
        let other_envelope = format!(
            "malformed/{}",
            blake3::hash(b"different transmitted bytes").to_hex()
        );
        assert!(
            check_ack_for(
                &frame,
                saved.clone(),
                content_hash(&frame).unwrap(),
                Some(&other_envelope)
            )
            .is_err()
        );
        let mut other = saved.clone();
        other.frame.push('x');
        assert!(check(other).is_err());
        let mut wrong_body = saved.clone();
        wrong_body.content_hash = Some("blake3:different".into());
        assert!(check(wrong_body).is_err());
        let mut not_refused = saved.clone();
        not_refused.result = ack(&frame).result;
        assert!(check(not_refused).is_err());
        let mut unsaved = saved;
        unsaved.number = 0;
        assert!(check(unsaved).is_err());
    }
    let valid = Ingress::RegisterKinds {
        frame: "valid-offered-name".into(),
        declarations: vec![],
    };
    let malformed = format!(
        "malformed/{}",
        blake3::hash(&serde_json::to_vec(&valid).unwrap()).to_hex()
    );
    let mut saved = ack(&valid);
    saved.frame = malformed.clone();
    saved.result = refused(sinex_model::FrameRefusal::Malformed, "invalid content");
    assert!(
        check_ack_for(
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
    daemon: Client,
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
        let daemon = async_nats::connect(server.url()).await.unwrap();
        let js = jetstream::new(daemon.clone());
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
                durable_name: Some("daemon".into()),
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
            // The hint for this completed acknowledgment had no subscriber.
            // A restarted producer cannot receive it as a queued notification.
            daemon
                .publish(
                    subjects.ack(PRODUCER),
                    serde_json::to_vec(&AckHint {
                        number: 1,
                        frame_hash: blake3::hash(frame.frame().as_bytes()).to_hex().to_string(),
                    })
                    .unwrap()
                    .into(),
                )
                .await
                .unwrap();
            daemon.flush().await.unwrap();
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
            daemon,
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
        .expect("acknowledgment exchange waited for an unrepeated broadcast hint")
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
    daemon: &Client,
    requests: &mut Subscriber,
    acknowledgments: Vec<Acknowledgment>,
) -> RecordedQuery {
    let request = requests.next().await.unwrap();
    let query: RecordedQuery = serde_json::from_slice(&request.payload).unwrap();
    assert_eq!(query.after, 0);
    crate::replies::publish_json(
        daemon,
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
async fn duplicate_after_restart_finds_ack_without_new_hint() {
    use futures::FutureExt;
    let mut fixture = Duplicate::new(true).await;
    let saved = ack(&fixture.frame);
    let expected = saved.clone();
    let mut requests = fixture
        .daemon
        .subscribe(fixture.subjects.recorded(PRODUCER))
        .await
        .unwrap();
    fixture.daemon.flush().await.unwrap();
    let daemon = fixture.daemon.clone();
    let name = fixture.frame.frame().to_owned();
    let responder = tokio::spawn(async move {
        let query = answer_recorded(&daemon, &mut requests, vec![saved]).await;
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
async fn duplicate_before_admission_waits_for_ack() {
    let mut fixture = Duplicate::new(false).await;
    let saved = ack(&fixture.frame);
    let expected = saved.clone();
    let mut requests = fixture
        .daemon
        .subscribe(fixture.subjects.recorded(PRODUCER))
        .await
        .unwrap();
    fixture.daemon.flush().await.unwrap();
    let daemon = fixture.daemon.clone();
    let name = fixture.frame.frame().to_owned();
    let ack_subject = fixture.subjects.ack(PRODUCER);
    let original = fixture.message.clone();
    let responder = tokio::spawn(async move {
        let query = answer_recorded(&daemon, &mut requests, vec![]).await;
        assert!(query.frames.contains(&name));
        // Only after the empty lookup does the daemon commit the original.
        original.double_ack().await.unwrap();
        daemon
            .publish(
                ack_subject,
                serde_json::to_vec(&AckHint {
                    number: 1,
                    frame_hash: blake3::hash(name.as_bytes()).to_hex().to_string(),
                })
                .unwrap()
                .into(),
            )
            .await
            .unwrap();
        daemon.flush().await.unwrap();
        let query = answer_recorded(&daemon, &mut requests, vec![saved]).await;
        assert_eq!(query.numbers, vec![1]);
        assert!(query.frames.is_empty());
    });
    let offered = fixture.frame.clone();
    assert_eq!(fixture.submit(&offered).await.unwrap(), expected);
    responder.await.unwrap();
    fixture.once().await;
}

#[devkit::test]
async fn duplicate_with_different_bytes_is_typed_conflict() {
    let mut fixture = Duplicate::new(true).await;
    let prior = ack(&fixture.frame);
    let expected = prior.clone();
    let mut requests = fixture
        .daemon
        .subscribe(fixture.subjects.recorded(PRODUCER))
        .await
        .unwrap();
    fixture.daemon.flush().await.unwrap();
    let daemon = fixture.daemon.clone();
    let responder =
        tokio::spawn(async move { answer_recorded(&daemon, &mut requests, vec![prior]).await });
    let offered = Ingress::Events {
        frame: fixture.frame.frame().into(),
        source: None,
        events: vec![],
        failures: vec![],
    };
    let SubmitError::AckConflict {
        frame,
        offered_hash,
        ack,
    } = fixture.submit(&offered).await.unwrap_err()
    else {
        panic!("duplicate is not admission of the changed offer")
    };
    assert_eq!(frame, offered.frame());
    assert_eq!(offered_hash, content_hash(&offered).unwrap());
    assert_eq!(*ack, expected);
    responder.await.unwrap();
    fixture.once().await;
}

#[devkit::test]
async fn broker_duplicate_without_daemon_is_not_admission() {
    let mut fixture = Duplicate::new(true).await;
    let offered = fixture.frame.clone();
    assert!(matches!(fixture.submit(&offered).await,
        Err(SubmitError::Reply(crate::replies::ReplyError::Transport(reason))) if reason.contains("no responders")));
    fixture.once().await;
}

#[devkit::test]
async fn duplicate_refusal_keeps_recorded_outcome() {
    for kind in [
        sinex_model::FrameRefusal::Content,
        sinex_model::FrameRefusal::Conflict,
    ] {
        let mut fixture = Duplicate::new(true).await;
        let mut saved = ack(&fixture.frame);
        saved.result = refused(kind, "synthetic refusal");
        let expected = saved.clone();
        let mut requests = fixture
            .daemon
            .subscribe(fixture.subjects.recorded(PRODUCER))
            .await
            .unwrap();
        fixture.daemon.flush().await.unwrap();
        let daemon = fixture.daemon.clone();
        let responder =
            tokio::spawn(async move { answer_recorded(&daemon, &mut requests, vec![saved]).await });
        let offered = fixture.frame.clone();
        assert_eq!(fixture.submit(&offered).await.unwrap(), expected);
        responder.await.unwrap();
        fixture.once().await;
    }
}

#[devkit::test]
async fn query_without_continuations_keeps_old_wire_shape() {
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
    let daemon = client.clone();
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
                assert_eq!(previous.frames, vec!["recorded"]);
                assert!(previous.numbers.is_empty());
                assert_eq!(previous.after, 0);
                serde_json::json!({"acknowledgments":[],"next":null})
            };
            let _ = crate::replies::publish_json(
                &daemon,
                request.reply.unwrap().to_string(),
                value,
                HINT_IDLE,
            )
            .await;
        }
    });
    let plain = RecordedQuery {
        frames: vec!["recorded".into()],
        ..Default::default()
    };
    let page: RecordedPage = crate::replies::request_json(
        &client,
        subjects.recorded(PRODUCER),
        PRODUCER,
        &plain,
        HINT_IDLE,
    )
    .await
    .unwrap();
    assert!(page.continuations.is_empty());
    let explicit = RecordedQuery {
        continuations: vec![sinex_protocol::RecordedContinuationRequest {
            frame: "stored-material".into(),
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
        "an unsupported continuation stays unavailable, never a fresh acknowledgment"
    );
    responder.await.unwrap();
}
