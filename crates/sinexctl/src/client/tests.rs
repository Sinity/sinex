use super::*;

#[devkit::test]
fn endpoints() {
    assert!(matches!(
        Endpoint::parse("unix:/run/sinex/s.sock").unwrap(),
        Endpoint::Unix(_)
    ));
    assert!(matches!(
        Endpoint::parse("/run/sinex/s.sock").unwrap(),
        Endpoint::Unix(_)
    ));
    assert!(
        matches!(Endpoint::parse("http://127.0.0.1:7171/").unwrap(), Endpoint::Tcp(h) if h == "127.0.0.1:7171")
    );
    assert!(Endpoint::parse("relative").is_err());
    assert_eq!(percent_encode("a b&c"), "a%20b%26c");
}

#[devkit::test]
async fn dropping_an_unbounded_call_closes_its_http_driver() {
    use tokio::io::AsyncReadExt;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (received, waiting) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut bytes = Vec::new();
        loop {
            let mut chunk = [0; 1024];
            let count = stream.read(&mut chunk).await.unwrap();
            assert!(count > 0);
            bytes.extend_from_slice(&chunk[..count]);
            if let Some(at) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
                let headers = std::str::from_utf8(&bytes[..at]).unwrap();
                let length: usize = headers
                    .lines()
                    .find_map(|line| {
                        line.split_once(':')
                            .filter(|(name, _)| name.eq_ignore_ascii_case("content-length"))
                            .map(|(_, value)| value.trim().parse().unwrap())
                    })
                    .unwrap();
                if bytes.len() >= at + 4 + length {
                    break;
                }
            }
        }
        received.send(()).unwrap();
        let mut byte = [0];
        assert_eq!(
            tokio::time::timeout(std::time::Duration::from_secs(2), stream.read(&mut byte))
                .await
                .unwrap()
                .unwrap(),
            0,
            "cancelled call kept its HTTP socket alive"
        );
    });
    let client = Client {
        endpoint: Endpoint::Tcp(address.to_string()),
        token: "synthetic".into(),
        deadline: command_surface::Deadline::None,
    };
    let call = tokio::spawn(async move { client.call(&Request::Status).await });
    waiting.await.unwrap();
    call.abort();
    let _ = call.await;
    server.await.unwrap();
}
