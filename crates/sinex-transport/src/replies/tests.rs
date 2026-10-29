use super::*;
use serde::ser::SerializeSeq;

struct Generated(usize);
impl Serialize for Generated {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut sequence = serializer.serialize_seq(Some(self.0))?;
        for value in 0..self.0 {
            sequence.serialize_element(&value)?;
        }
        sequence.end()
    }
}
struct Counted {
    count: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    values: Generated,
}
impl Serialize for Counted {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.values.serialize(serializer)
    }
}
#[devkit::test]
async fn large_json_is_serialized_once_in_server_sized_units_and_verified_before_return() {
    let server = devkit::nats::Server::start_with(&devkit::nats::Options {
        config: "max_payload: 4096".into(),
    })
    .unwrap();
    let client = async_nats::connect(server.url()).await.unwrap();
    let responder = client.clone();
    let mut requests = client.subscribe("rpc.reader").await.unwrap();
    let mut observed = client.subscribe("_INBOX.reader.>").await.unwrap();
    client.flush().await.unwrap();
    let count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let serialized = count.clone();
    let task = tokio::spawn(async move {
        let request = requests.next().await.unwrap();
        publish_json(
            &responder,
            request.reply.unwrap().to_string(),
            Counted {
                count: serialized,
                values: Generated(100_000),
            },
            Duration::from_secs(10),
        )
        .await
        .unwrap();
    });
    let values: Vec<usize> = request_json(
        &client,
        "rpc.reader",
        "reader",
        &serde_json::json!({}),
        Duration::from_secs(10),
    )
    .await
    .unwrap();
    assert_eq!(values.len(), 100_000);
    assert!(
        values
            .iter()
            .enumerate()
            .all(|(index, value)| *value == index)
    );
    task.await.unwrap();
    assert_eq!(count.load(std::sync::atomic::Ordering::SeqCst), 1);
    let mut frames = 0;
    loop {
        let message = tokio::time::timeout(Duration::from_secs(10), observed.next())
            .await
            .unwrap()
            .unwrap();
        if let Ok(frame) = serde_json::from_slice::<ReplyFrame>(&message.payload) {
            assert!(message.payload.len() <= 4096);
            match frame {
                ReplyFrame::Data { .. } => frames += 1,
                ReplyFrame::End { .. } => break,
                ReplyFrame::Abort { .. } => panic!("unexpected abort"),
            }
        }
    }
    assert!(frames > 1);
}
#[devkit::test]
async fn corruption_discards_the_partial_reply_and_a_retry_uses_the_original_request() {
    let server = devkit::nats::test_nats().unwrap();
    let client = async_nats::connect(server.url()).await.unwrap();
    let responder = client.clone();
    let mut requests = client.subscribe("corrupt.reader").await.unwrap();
    client.flush().await.unwrap();
    let task = tokio::spawn(async move {
        let mut first = None;
        for corrupt in [true, false] {
            let request = requests.next().await.unwrap();
            if let Some(body) = &first {
                assert_eq!(&request.payload, body);
            } else {
                first = Some(request.payload.clone());
            }
            let inbox = request.reply.unwrap().to_string();
            let stream = stream_id(&inbox);
            let flow = format!("{inbox}.flow");
            let bytes = b"[1,2]";
            let data = ReplyFrame::Data {
                stream: stream.clone(),
                sequence: 0,
                offset: 0,
                data: base64::engine::general_purpose::STANDARD.encode(bytes),
            };
            responder
                .publish_with_reply(
                    inbox.clone(),
                    flow.clone(),
                    frame_bytes(&data).unwrap().into(),
                )
                .await
                .unwrap();
            let end = ReplyFrame::End {
                stream,
                sequence: 1,
                total: bytes.len() as u64,
                hash: if corrupt {
                    "0".repeat(64)
                } else {
                    blake3::hash(bytes).to_hex().to_string()
                },
            };
            responder
                .publish_with_reply(inbox, flow, frame_bytes(&end).unwrap().into())
                .await
                .unwrap();
        }
    });
    let request = serde_json::json!({"frame":"same-idempotent-frame"});
    assert!(
        request_json::<Vec<u8>>(
            &client,
            "corrupt.reader",
            "reader",
            &request,
            Duration::from_secs(10)
        )
        .await
        .is_err()
    );
    assert_eq!(
        request_json::<Vec<u8>>(
            &client,
            "corrupt.reader",
            "reader",
            &request,
            Duration::from_secs(10)
        )
        .await
        .unwrap(),
        vec![1, 2]
    );
    task.await.unwrap();
}
#[devkit::test]
async fn a_disconnected_receiver_releases_the_serializer_after_idle_cancellation() {
    let server = devkit::nats::test_nats().unwrap();
    let client = async_nats::connect(server.url()).await.unwrap();
    let inbox = "_INBOX.reader.disconnected".to_owned();
    let mut receiver = client.subscribe(inbox.clone()).await.unwrap();
    client.flush().await.unwrap();
    let publisher = client.clone();
    let task = tokio::spawn(async move {
        publish_json(
            &publisher,
            inbox,
            Generated(100_000),
            Duration::from_millis(200),
        )
        .await
    });
    let first = receiver.next().await.unwrap();
    assert!(matches!(
        serde_json::from_slice::<ReplyFrame>(&first.payload).unwrap(),
        ReplyFrame::Data { sequence: 0, .. }
    ));
    drop(receiver);
    assert!(matches!(task.await.unwrap(), Err(ReplyError::Idle)));
}
#[devkit::test]
async fn a_wrong_flow_ack_does_not_acknowledge_another_stream_or_extend_its_deadline() {
    let server = devkit::nats::test_nats().unwrap();
    let client = async_nats::connect(server.url()).await.unwrap();
    let inbox = "_INBOX.reader.wrong-flow".to_owned();
    let mut receiver = client.subscribe(inbox.clone()).await.unwrap();
    client.flush().await.unwrap();
    let publisher = client.clone();
    let task = tokio::spawn(async move {
        publish_json(
            &publisher,
            inbox,
            Generated(100_000),
            Duration::from_millis(200),
        )
        .await
    });
    let first = receiver.next().await.unwrap();
    client
        .publish(
            first.reply.unwrap(),
            serde_json::to_vec(&FlowAck::Accepted {
                stream: "foreign".into(),
                sequence: 0,
            })
            .unwrap()
            .into(),
        )
        .await
        .unwrap();
    assert!(matches!(task.await.unwrap(), Err(ReplyError::Idle)));
}

#[devkit::test(multi_thread)]
async fn the_final_flow_ack_reaches_the_server_before_a_caller_runtime_quiesces() {
    let server = devkit::nats::test_nats().unwrap();
    let responder = async_nats::connect(server.url()).await.unwrap();
    let mut requests = responder.subscribe("current-thread.reader").await.unwrap();
    responder.flush().await.unwrap();
    let responding = tokio::spawn(async move {
        let request = requests.next().await.unwrap();
        publish_json(
            &responder,
            request.reply.unwrap().to_string(),
            vec![1u8, 2, 3],
            Duration::from_millis(300),
        )
        .await
    });
    let url = server.url().to_owned();
    let (ready_send, ready) = tokio::sync::oneshot::channel();
    let (resume, wait) = std::sync::mpsc::channel();
    let caller = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let client = runtime.block_on(async_nats::connect(url)).unwrap();
        let value: Vec<u8> = runtime
            .block_on(request_json(
                &client,
                "current-thread.reader",
                "reader",
                &serde_json::json!({}),
                Duration::from_secs(5),
            ))
            .unwrap();
        assert_eq!(value, vec![1, 2, 3]);
        ready_send.send(()).unwrap();
        wait.recv().unwrap();
        drop(client);
        drop(runtime);
    });
    ready.await.unwrap();
    let result = tokio::time::timeout(Duration::from_secs(5), responding).await;
    resume.send(()).unwrap();
    caller.join().unwrap();
    assert!(
        result.unwrap().unwrap().is_ok(),
        "server observes the completed flow while caller runtime is idle"
    );
}
