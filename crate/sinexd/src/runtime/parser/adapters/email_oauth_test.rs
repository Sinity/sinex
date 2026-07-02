use super::*;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use xtask::sandbox::prelude::sinex_test;

struct FakeExchange {
    calls: AtomicUsize,
    responses: StdMutex<Vec<Result<OAuthTokenResponse, OAuthError>>>,
}

impl FakeExchange {
    fn new(responses: Vec<Result<OAuthTokenResponse, OAuthError>>) -> Self {
        Self {
            calls: AtomicUsize::new(0),
            responses: StdMutex::new(responses),
        }
    }

    fn call_count(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

impl OAuthTokenExchange for FakeExchange {
    async fn exchange(
        &self,
        _credentials: &GmailOAuthCredentials,
    ) -> Result<OAuthTokenResponse, OAuthError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let mut responses = self.responses.lock().unwrap();
        assert!(
            !responses.is_empty(),
            "FakeExchange ran out of queued responses"
        );
        responses.remove(0)
    }
}

fn creds() -> GmailOAuthCredentials {
    GmailOAuthCredentials::new(
        "client-id".to_string(),
        "client-secret".to_string(),
        "refresh-token".to_string(),
    )
}

fn token(access: &str, expires_in: i64) -> OAuthTokenResponse {
    OAuthTokenResponse {
        access_token: access.to_string(),
        expires_in: Some(expires_in),
        token_type: Some("Bearer".to_string()),
        scope: None,
    }
}

#[sinex_test]
async fn caches_fresh_token_without_re_exchanging() -> xtask::sandbox::TestResult<()> {
    let exchange = FakeExchange::new(vec![Ok(token("access-1", 3_600))]);
    let provider = OAuthTokenProvider::new(creds(), exchange);

    assert_eq!(provider.bearer_token().await?, "access-1");
    // Second call is served from cache — no second response is queued, so a
    // re-exchange would panic in FakeExchange.
    assert_eq!(provider.bearer_token().await?, "access-1");
    assert_eq!(provider.exchange.call_count(), 1);
    Ok(())
}

#[sinex_test]
async fn re_exchanges_when_cached_token_is_stale() -> xtask::sandbox::TestResult<()> {
    // First token already expired (negative lifetime -> expires in the past),
    // second token fresh.
    let exchange = FakeExchange::new(vec![Ok(token("stale", -10)), Ok(token("fresh", 3_600))]);
    let provider = OAuthTokenProvider::new(creds(), exchange);

    // First call exchanges and caches an already-stale token.
    assert_eq!(provider.bearer_token().await?, "stale");
    // Stale -> re-exchange yields the fresh token.
    assert_eq!(provider.bearer_token().await?, "fresh");
    assert_eq!(provider.exchange.call_count(), 2);
    Ok(())
}

#[sinex_test]
async fn invalidate_forces_re_exchange() -> xtask::sandbox::TestResult<()> {
    let exchange = FakeExchange::new(vec![Ok(token("a", 3_600)), Ok(token("b", 3_600))]);
    let provider = OAuthTokenProvider::new(creds(), exchange);
    assert_eq!(provider.bearer_token().await?, "a");
    assert_eq!(provider.bearer_token().await?, "a");
    assert_eq!(provider.exchange.call_count(), 1);
    provider.invalidate().await;
    assert_eq!(provider.bearer_token().await?, "b");
    assert_eq!(provider.exchange.call_count(), 2);
    Ok(())
}

#[sinex_test]
async fn invalid_grant_maps_to_rejected() -> xtask::sandbox::TestResult<()> {
    let exchange = FakeExchange::new(vec![Err(OAuthError::Status {
        status: reqwest::StatusCode::BAD_REQUEST,
        error_code: Some("invalid_grant".to_string()),
        body: "{\"error\":\"invalid_grant\"}".to_string(),
    })]);
    let provider = OAuthTokenProvider::new(creds(), exchange);
    let error = provider.bearer_token().await.unwrap_err();
    assert_eq!(error.authorization_state(), EmailAuthorizationState::Rejected);
    assert!(!error.is_retryable());
    Ok(())
}

#[sinex_test]
async fn missing_credential_maps_to_missing() -> xtask::sandbox::TestResult<()> {
    let error = OAuthError::MissingCredential {
        field: "refresh_token",
    };
    assert_eq!(error.authorization_state(), EmailAuthorizationState::Missing);
    assert!(!error.is_retryable());
    Ok(())
}

#[sinex_test]
async fn server_error_maps_to_unknown_and_is_retryable() -> xtask::sandbox::TestResult<()> {
    let error = OAuthError::Status {
        status: reqwest::StatusCode::INTERNAL_SERVER_ERROR,
        error_code: None,
        body: String::new(),
    };
    assert_eq!(error.authorization_state(), EmailAuthorizationState::Unknown);
    assert!(error.is_retryable());
    Ok(())
}

#[sinex_test]
async fn debug_redacts_secret_and_refresh_token() -> xtask::sandbox::TestResult<()> {
    let rendered = format!("{:?}", creds());
    assert!(rendered.contains("client-id"));
    assert!(!rendered.contains("client-secret"));
    assert!(!rendered.contains("refresh-token"));
    assert!(rendered.contains("<redacted>"));
    Ok(())
}

#[sinex_test]
async fn load_from_files_rejects_empty_and_loads_trimmed() -> xtask::sandbox::TestResult<()> {
    let dir = std::env::temp_dir().join(format!(
        "sinex-oauth-test-{}-{}",
        std::process::id(),
        "load"
    ));
    tokio::fs::create_dir_all(&dir).await?;
    let id_path = dir.join("id");
    let secret_path = dir.join("secret");
    let empty_path = dir.join("empty");
    tokio::fs::write(&id_path, "client-id\n").await?;
    tokio::fs::write(&secret_path, "client-secret\n").await?;
    tokio::fs::write(&empty_path, "   \n").await?;

    let id_str = id_path.to_string_lossy().into_owned();
    let secret_str = secret_path.to_string_lossy().into_owned();

    let error =
        GmailOAuthCredentials::load_from_files(&id_str, &secret_str, &empty_path.to_string_lossy())
            .await
            .unwrap_err();
    assert!(matches!(
        error,
        OAuthError::MissingCredential {
            field: "refresh_token"
        }
    ));

    // A wholly-missing file also maps to MissingCredential, not an IO panic.
    let absent = dir.join("does-not-exist");
    let error = GmailOAuthCredentials::load_from_files(
        &id_str,
        &secret_str,
        &absent.to_string_lossy(),
    )
    .await
    .unwrap_err();
    assert_eq!(error.authorization_state(), EmailAuthorizationState::Missing);

    // Trimmed contents load cleanly when present.
    let refresh_path = dir.join("refresh");
    tokio::fs::write(&refresh_path, "  refresh-token  ").await?;
    let loaded = GmailOAuthCredentials::load_from_files(
        &id_str,
        &secret_str,
        &refresh_path.to_string_lossy(),
    )
    .await?;
    assert_eq!(loaded.client_id, "client-id");
    assert_eq!(loaded.refresh_token, "refresh-token");

    tokio::fs::remove_dir_all(&dir).await.ok();
    Ok(())
}

#[sinex_test]
async fn google_client_parses_token_over_http() -> xtask::sandbox::TestResult<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}/token", listener.local_addr()?);
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let mut buf = [0_u8; 2048];
        let _ = stream.read(&mut buf).await?;
        let body = b"{\"access_token\":\"live-access\",\"expires_in\":3600,\"token_type\":\"Bearer\"}";
        let header = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n",
            body.len()
        );
        stream.write_all(header.as_bytes()).await?;
        stream.write_all(body).await?;
        stream.shutdown().await
    });
    let client = GoogleOAuthClient::with_endpoint(reqwest::Client::new(), endpoint);
    let response = client.exchange(&creds()).await?;
    server.await??;
    assert_eq!(response.access_token, "live-access");
    assert_eq!(response.expires_in, Some(3_600));
    Ok(())
}

#[sinex_test]
async fn google_client_maps_invalid_grant_status() -> xtask::sandbox::TestResult<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}/token", listener.local_addr()?);
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let mut buf = [0_u8; 2048];
        let _ = stream.read(&mut buf).await?;
        let body = b"{\"error\":\"invalid_grant\",\"error_description\":\"Token has been expired or revoked.\"}";
        let header = format!(
            "HTTP/1.1 400 Bad Request\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n",
            body.len()
        );
        stream.write_all(header.as_bytes()).await?;
        stream.write_all(body).await?;
        stream.shutdown().await
    });
    let client = GoogleOAuthClient::with_endpoint(reqwest::Client::new(), endpoint);
    let error = client.exchange(&creds()).await.unwrap_err();
    server.await??;
    assert_eq!(error.authorization_state(), EmailAuthorizationState::Rejected);
    // Error display must not leak the refresh token or client secret.
    let rendered = error.to_string();
    assert!(!rendered.contains("refresh-token"));
    assert!(!rendered.contains("client-secret"));
    Ok(())
}
