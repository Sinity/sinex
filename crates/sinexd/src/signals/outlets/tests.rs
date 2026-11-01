use super::*;
use ::signals::DeliveryOutcome;
use axum::{Router, http::StatusCode, routing::post};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

struct Server {
    url: String,
    requests: Arc<AtomicUsize>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn server(
    status: StatusCode,
    redirect: Option<String>,
    entered: Option<Arc<tokio::sync::Notify>>,
) -> Server {
    let requests = Arc::new(AtomicUsize::new(0));
    let counter = requests.clone();
    let app = Router::new().route(
        "/",
        post(move || {
            let counter = counter.clone();
            let redirect = redirect.clone();
            let entered = entered.clone();
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
                if let Some(entered) = entered {
                    entered.notify_one();
                    std::future::pending::<()>().await;
                }
                let mut response = axum::response::Response::new(axum::body::Body::empty());
                *response.status_mut() = status;
                if let Some(location) = redirect {
                    response
                        .headers_mut()
                        .insert("location", location.parse().unwrap());
                }
                response
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    Server {
        url,
        requests,
        task,
    }
}
const DEADLINE: Duration = Duration::from_secs(5);

#[devkit::test]
async fn async_value_http_has_one_request_and_distinguishes_success_from_refusal() {
    for (status, expected) in [
        (StatusCode::OK, DeliveryOutcome::Delivered),
        (
            StatusCode::FORBIDDEN,
            DeliveryOutcome::NotSent("value_http_status".into()),
        ),
    ] {
        let server = server(status, None, None).await;
        let result = http_value(
            &server.url,
            b"synthetic-value".to_vec(),
            DEADLINE,
        )
        .await;
        assert_eq!(result, expected);
        assert_eq!(server.requests.load(Ordering::SeqCst), 1);
    }
}

#[devkit::test]
async fn async_value_http_does_not_follow_a_redirect() {
    let destination = server(StatusCode::OK, None, None).await;
    let redirect = server(
        StatusCode::TEMPORARY_REDIRECT,
        Some(destination.url.clone()),
        None,
    )
    .await;
    assert_eq!(
        http_value(
            &redirect.url,
            b"synthetic-value".to_vec(),
            DEADLINE
        )
        .await,
        DeliveryOutcome::NotSent("value_http_status".into())
    );
    assert_eq!(redirect.requests.load(Ordering::SeqCst), 1);
    assert_eq!(destination.requests.load(Ordering::SeqCst), 0);
}

/// A request already sent when the deadline passes may have reached its
/// target: its outcome is unknown, and it is never sent again.
#[devkit::test]
async fn value_http_past_its_deadline_after_sending_is_unknown_and_sent_once() {
    let entered = Arc::new(tokio::sync::Notify::new());
    let server = server(StatusCode::OK, None, Some(entered.clone())).await;
    let url = server.url.clone();
    let task = tokio::spawn(async move {
        http_value(&url, b"synthetic-value".to_vec(), Duration::from_millis(500)).await
    });
    tokio::time::timeout(Duration::from_secs(3), entered.notified())
        .await
        .unwrap();
    assert_eq!(task.await.unwrap(), DeliveryOutcome::Unknown);
    assert_eq!(server.requests.load(Ordering::SeqCst), 1);
}

#[devkit::test]
async fn file_value_is_synced_and_published_once_with_private_staging_cleanup() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("value");
    assert_eq!(
        file_value(&path, b"synthetic-value")
        .await,
        DeliveryOutcome::Delivered
    );
    assert_eq!(std::fs::read(&path).unwrap(), b"synthetic-value");
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 1);
}

#[devkit::test]
async fn value_https_preserves_certificate_validation_and_sanitizes_its_receipt() {
    use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
    use tokio_rustls::rustls;
    // Deliberately untrusted synthetic certificate with a matching loopback SAN.
    let cert =
        CertificateDer::from(include_bytes!("fixtures/untrusted-loopback-cert.der").to_vec());
    let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
        include_bytes!("fixtures/synthetic-loopback-key.der").to_vec(),
    ));
    let config = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_no_client_auth()
    .with_single_cert(vec![cert], key)
    .unwrap();
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!(
        "https://{}/?synthetic-private-query",
        listener.local_addr().unwrap()
    );
    let requests = Arc::new(AtomicUsize::new(0));
    let observed = requests.clone();
    let task = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        if let Ok(mut tls) = acceptor.accept(socket).await {
            let mut request = [0; 1024];
            if tls.read(&mut request).await.unwrap_or(0) > 0 {
                observed.fetch_add(1, Ordering::SeqCst);
                let _ = tls
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
                    .await;
            }
        }
    });
    let receipt = http_value(
        &url,
        b"synthetic-value".to_vec(),
        DEADLINE,
    )
    .await;
    assert_eq!(
        receipt,
        DeliveryOutcome::NotSent("value_http_connect".into())
    );
    assert!(
        !serde_json::to_string(&receipt)
            .unwrap()
            .contains("synthetic-private-query")
    );
    assert_eq!(requests.load(Ordering::SeqCst), 0);
    tokio::time::timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap();
}

#[devkit::test]
async fn accepted_value_http_does_not_require_an_unrelated_streaming_response_body() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = [0; 4096];
        assert!(socket.read(&mut request).await.unwrap() > 0);
        socket
            .write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n1\r\nx\r\n")
            .await
            .unwrap();
        std::future::pending::<()>().await;
    });
    let result = http_value(
        &url,
        b"synthetic-value".to_vec(),
        DEADLINE,
    )
    .await;
    task.abort();
    let _ = task.await;
    assert_eq!(result, DeliveryOutcome::Delivered);
}

/// A command that exits unsuccessfully sent nothing it vouches for; one
/// still running at its deadline may have acted, so it is stopped and its
/// outcome stays unknown.
#[devkit::test]
async fn failed_command_is_not_sent_and_a_command_past_its_deadline_stays_unknown() {
    use std::os::unix::fs::PermissionsExt;
    let directory = tempfile::tempdir().unwrap();
    let script = directory.path().join("controlled");
    let sink = directory.path().join("actions");
    std::fs::write(
        &script,
        b"#!/usr/bin/env bash\nprintf '%s\\n' \"$2\" >> \"$1\"\nif [[ \"$2\" == fail ]]; then exit 3; fi\nexec sleep 30\n",
    )
    .unwrap();
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o700)).unwrap();
    let command = Command {
        program: script,
        arguments: vec![sink.display().to_string()],
    };
    assert_eq!(
        command_effect(&command, "fail", DEADLINE).await,
        DeliveryOutcome::NotSent("action_exit".into())
    );
    assert_eq!(
        command_effect(&command, "synthetic-reference", Duration::from_millis(500)).await,
        DeliveryOutcome::Unknown
    );
    assert_eq!(
        std::fs::read_to_string(&sink).unwrap(),
        "fail\nsynthetic-reference\n"
    );
}
