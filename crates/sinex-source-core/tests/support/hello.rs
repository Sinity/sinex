use futures::StreamExt;
use sinex_protocol::{ProducerHello, ProducerWelcome, Subjects};

/// Accept only the configuration handshake; leave ingress publishing unavailable.
pub(crate) async fn hello(nats: &devkit::nats::TestNats, subjects: &Subjects, producer: &str) {
    let client = async_nats::connect(nats.url()).await.unwrap();
    let mut requests = client.subscribe(subjects.connect(producer)).await.unwrap();
    client.flush().await.unwrap();
    tokio::spawn(async move {
        while let Some(message) = requests.next().await {
            let hello: ProducerHello = serde_json::from_slice(&message.payload).unwrap();
            assert_eq!(hello.version, sinex_protocol::PRODUCER_VERSION);
            client
                .publish(
                    message.reply.unwrap(),
                    serde_json::to_vec(&ProducerWelcome {
                        refusal: None,
                        ..Default::default()
                    })
                    .unwrap()
                    .into(),
                )
                .await
                .unwrap();
        }
    });
}
