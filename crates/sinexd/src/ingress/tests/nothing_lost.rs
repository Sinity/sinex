//! Nothing a producer sent is lost before admission: not to an age limit, a
//! full stream, or a writer that stalls.
use super::*;

/// The ingress stream keeps every frame until it is admitted: no age limit, a
/// full stream refuses the next publish instead of dropping an older frame,
/// and the writer's consumer redelivers without a cap. A stream created with
/// other settings is brought to these.
#[devkit::test]
async fn frames_never_expire_and_a_full_stream_refuses(bus: devkit::nats::TestNats) {
    let subjects = Subjects::new(bus.prefix());
    let client = async_nats::connect(bus.url()).await.unwrap();
    let js = jetstream::new(client);
    // Bring an existing stream and consumer to the configured settings.
    let existing = js
        .create_stream(stream::Config {
            name: subjects.ingress_stream(),
            subjects: vec![subjects.ingress_all()],
            retention: stream::RetentionPolicy::WorkQueue,
            ..Default::default()
        })
        .await
        .unwrap();
    existing
        .create_consumer(consumer::pull::Config {
            durable_name: Some(DURABLE.into()),
            ack_policy: consumer::AckPolicy::Explicit,
            ack_wait: Duration::from_secs(60),
            max_deliver: 10,
            ..Default::default()
        })
        .await
        .unwrap();
    let mut consumer = ensure_stream(&js, &subjects).await.unwrap();
    let mut stream = js.get_stream(subjects.ingress_stream()).await.unwrap();
    let config = stream.info().await.unwrap().config.clone();
    assert_eq!(config.max_age, Duration::ZERO, "frames never expire");
    assert_eq!(config.discard, stream::DiscardPolicy::New);
    let info = consumer.info().await.unwrap();
    assert_eq!(
        info.config.max_deliver, -1,
        "redelivered without a delivery cap"
    );

    // With a limit set, a full stream refuses and keeps what it holds.
    js.update_stream(stream::Config {
        max_messages: 2,
        ..config
    })
    .await
    .unwrap();
    for frame in ["one", "two"] {
        js.publish(subjects.ingress("kitty"), frame.into())
            .await
            .unwrap()
            .await
            .unwrap();
    }
    let refused = async {
        js.publish(subjects.ingress("kitty"), "three".into())
            .await?
            .await
    }
    .await;
    assert!(refused.is_err(), "a full stream took a third frame");
    assert_eq!(stream.info().await.unwrap().state.messages, 2);
    let first = stream.get_raw_message(1).await.unwrap();
    assert_eq!(&first.payload[..], b"one", "the oldest frame is kept");
}

/// While the writer is stalled, producers keep publishing and the frames wait
/// on the stream; once it resumes, each is admitted exactly once.
#[devkit::test]
async fn frames_sent_while_the_writer_stalls_are_admitted_once(db: devkit::pg::TestDatabase) {
    let h = Harness::start(db).await;
    let bytes: Vec<u8> = (0..40)
        .flat_map(|i| format!(": {}:0;cmd {i}\n", 1_760_788_800 + i).into_bytes())
        .collect();
    let (material, frame) = h.material_frame("m1", &bytes);
    assert_eq!(h.send("kitty", &frame).await.refusal, None);
    let before = h.count("SELECT count(*) FROM core.events").await;

    let mut blocker = PgConnection::connect(&h.url).await.unwrap();
    sqlx::query("BEGIN").execute(&mut blocker).await.unwrap();
    sqlx::query("LOCK TABLE core.events IN SHARE MODE")
        .execute(&mut blocker)
        .await
        .unwrap();
    let js = jetstream::new(h.client.clone());
    let names: Vec<String> = (0..20).map(|i| format!("e{i}")).collect();
    for (i, name) in names.iter().enumerate() {
        let frame = Ingress::Events {
            source:Some(test_source("kitty")),coverage:None,
            frame: name.clone(),
            events: vec![
                imported(material, 2 * i as u64, &format!("cmd {i}")),
                imported(material, 2 * i as u64 + 1, &format!("again {i}")),
            ],
            failures: vec![],
        };
        js.publish(
            h.subjects.ingress("kitty"),
            serde_json::to_vec(&frame).unwrap().into(),
        )
        .await
        .unwrap()
        .await
        .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    let mut stream = js.get_stream(h.subjects.ingress_stream()).await.unwrap();
    assert!(
        stream.info().await.unwrap().state.messages >= 19,
        "the frames wait on the stream"
    );

    sqlx::query("ROLLBACK").execute(&mut blocker).await.unwrap();
    let mut admitted = 0;
    for _ in 0..600 {
        admitted = h.count("SELECT count(*) FROM core.events").await - before;
        if admitted >= 40 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        admitted, 40,
        "each event admitted once after the writer resumed"
    );
    let frames: Vec<&str> = names.iter().map(String::as_str).collect();
    let acknowledged = h.recorded("kitty", &frames).await;
    assert_eq!(acknowledged.len(), frames.len(), "every frame acknowledged");
}
