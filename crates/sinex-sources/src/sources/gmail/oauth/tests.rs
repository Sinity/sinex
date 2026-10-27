use super::*;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

fn files() -> (tempfile::TempDir, CredentialFiles) {
    let dir = tempfile::tempdir().unwrap();
    for (name, contents) in [
        ("id", "client-id\n"),
        ("secret", "client-secret"),
        ("refresh", "refresh-secret"),
    ] {
        std::fs::write(dir.path().join(name), contents).unwrap();
    }
    let files = CredentialFiles {
        client_id_file: dir.path().join("id"),
        client_secret_file: dir.path().join("secret"),
        refresh_token_file: dir.path().join("refresh"),
    };
    (dir, files)
}

struct FakeClock(AtomicU64);
impl Clock for FakeClock {
    fn now(&self) -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_secs(self.0.load(Ordering::SeqCst))
    }
}
struct FakeExchange(AtomicUsize);
impl Exchange for FakeExchange {
    fn exchange(&self, _: &Credentials) -> Result<Token, Error> {
        let n = self.0.fetch_add(1, Ordering::SeqCst);
        Ok(Token {
            access_token: format!("access-secret-{n}"),
            expires_in: 60,
            token_type: "Bearer".into(),
            scope: Some(FULL_SCOPE.into()),
        })
    }
}

#[test]
fn fresh_reuse_stale_refresh_and_invalidation() {
    let (_dir, files) = files();
    let provider = TokenProvider::new(
        files,
        FakeExchange(AtomicUsize::new(0)),
        FakeClock(AtomicU64::new(0)),
    );
    assert_eq!(provider.bearer().unwrap(), "access-secret-0");
    assert_eq!(provider.bearer().unwrap(), "access-secret-0");
    provider.clock.0.store(31, Ordering::SeqCst);
    assert_eq!(provider.bearer().unwrap(), "access-secret-1");
    provider.invalidate();
    assert_eq!(provider.bearer().unwrap(), "access-secret-2");
}

#[test]
fn missing_empty_and_debug_never_reveal_secrets() {
    let (_dir, files) = files();
    let credentials = files.load().unwrap();
    let debug = format!("{credentials:?}");
    for secret in ["client-id", "client-secret", "refresh-secret"] {
        assert!(!debug.contains(secret));
    }
    std::fs::write(&files.refresh_token_file, " \n").unwrap();
    assert!(matches!(files.load(), Err(Error::Missing("refresh_token"))));
    std::fs::remove_file(&files.refresh_token_file).unwrap();
    assert!(matches!(files.load(), Err(Error::Missing("refresh_token"))));
}

fn server(status: u16, body: &'static str) -> (String, std::thread::JoinHandle<String>) {
    use std::io::{Read, Write};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}/token", listener.local_addr().unwrap());
    let handle = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut bytes = Vec::new();
        let mut length = None;
        loop {
            let mut chunk = [0; 4096];
            let n = stream.read(&mut chunk).unwrap();
            if n == 0 {
                break;
            }
            bytes.extend_from_slice(&chunk[..n]);
            if let Some(end) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                let headers = String::from_utf8_lossy(&bytes[..end]).to_ascii_lowercase();
                length = length.or_else(|| {
                    headers.lines().find_map(|line| {
                        line.strip_prefix("content-length:")
                            .and_then(|n| n.trim().parse::<usize>().ok())
                    })
                });
                if bytes.len() >= end + 4 + length.unwrap_or(0) {
                    break;
                }
            }
        }
        write!(
            stream,
            "HTTP/1.1 {status} response\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
        .unwrap();
        String::from_utf8(bytes).unwrap()
    });
    (endpoint, handle)
}

#[test]
fn google_exchange_uses_full_scope_and_parses_token() {
    let (_dir, files) = files();
    let (endpoint, handle) = server(
        200,
        r#"{"access_token":"access-secret","expires_in":3600,"token_type":"Bearer","scope":"https://mail.google.com/"}"#,
    );
    let exchange = GoogleExchange {
        agent: agent(),
        endpoint,
    };
    let token = exchange.exchange(&files.load().unwrap()).unwrap();
    assert_eq!(token.access_token, "access-secret");
    let request = handle.join().unwrap();
    assert!(request.contains("grant_type=refresh_token"));
    assert!(request.contains("scope=https%3A%2F%2Fmail.google.com%2F"));
    assert!(request.contains("refresh_token=refresh-secret"));
}

#[test]
fn rejected_and_transient_errors_are_secret_free() {
    let (_dir, files) = files();
    for (status, expected) in [
        (400, Error::Rejected),
        (401, Error::Rejected),
        (403, Error::Rejected),
        (429, Error::Unavailable),
        (500, Error::Unavailable),
    ] {
        let (endpoint, handle) = server(status, "refresh-secret client-secret access-secret");
        let exchange = GoogleExchange {
            agent: agent(),
            endpoint,
        };
        let error = exchange.exchange(&files.load().unwrap()).err().unwrap();
        assert_eq!(error, expected);
        assert!(!format!("{error:?} {error}").contains("secret"));
        handle.join().unwrap();
    }
}

struct NarrowExchange;
impl Exchange for NarrowExchange {
    fn exchange(&self, _: &Credentials) -> Result<Token, Error> {
        Ok(Token {
            access_token: "secret".into(),
            expires_in: 60,
            token_type: "Bearer".into(),
            scope: Some("gmail.metadata".into()),
        })
    }
}

#[test]
fn narrow_scope_is_refused_and_authorization_requests_full_scope() {
    let (_dir, files) = files();
    let provider = TokenProvider::new(files, NarrowExchange, SystemClock);
    assert_eq!(provider.bearer().unwrap_err(), Error::InvalidResponse);
    let url = authorization_url("client &id", "http://localhost/callback", "state+value");
    assert!(url.contains("client_id=client%20%26id"));
    assert!(url.contains("scope=https%3A%2F%2Fmail.google.com%2F"));
    assert!(url.contains("access_type=offline"));
    assert!(url.contains("state=state%2Bvalue"));
}
