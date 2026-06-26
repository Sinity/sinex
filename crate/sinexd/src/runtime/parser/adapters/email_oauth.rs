//! OAuth2 refresh-token runtime for Gmail provider sync.
//!
//! The Gmail cursor/material client
//! ([`super::email_gmail_api::GmailHttpClient`]) consumes a bearer *access
//! token* and explicitly leaves OAuth refresh and secret lookup to the runtime
//! (`email_gmail_api.rs` doc comment: "OAuth refresh and secret lookup stay
//! outside the adapter"). This module is that runtime.
//!
//! It loads operator-owned refresh credentials from secret files, exchanges a
//! refresh token for a short-lived access token at the provider token endpoint,
//! caches the access token until it is near expiry, and refreshes on demand.
//! A bare access-token file expires within ~1 hour with no recovery; an
//! operator-owned refresh credential plus this runtime keeps live Gmail sync
//! working past that window.
//!
//! Errors classify into the provider authorization states the email package
//! family already surfaces ([`EmailAuthorizationState`]):
//!
//! | Failure | `authorization_state()` |
//! |---------|-------------------------|
//! | missing/empty credential file | `Missing` |
//! | provider returns `invalid_grant` / `invalid_client` | `Rejected` (operator must re-authorize) |
//! | transport / decode / 5xx / other | `Unknown` (transient runtime fault, not a settled auth verdict) |
//!
//! Secrets never appear in emitted records or errors: [`OAuthError`] carries the
//! provider error *code* and HTTP body but never the client secret or refresh
//! token, and credential file contents are held only in memory.

use std::error::Error;
use std::fmt;
use std::future::Future;

use serde::Deserialize;
use sinex_primitives::events::payloads::email::EmailAuthorizationState;
use time::OffsetDateTime;

/// Default Google OAuth2 token endpoint for the `refresh_token` grant.
pub const GOOGLE_OAUTH_TOKEN_URL: &str = "https://oauth2.googleapis.com/token";

/// Access tokens with no `expires_in` are treated as valid for this long.
const DEFAULT_TOKEN_LIFETIME_SECS: i64 = 3_600;

/// A cached access token is refreshed when it falls within this skew of expiry.
const DEFAULT_REFRESH_SKEW_SECS: i64 = 60;

/// Operator-owned Gmail OAuth refresh credentials.
///
/// These are read from operator-owned secret files (the deployment boundary,
/// #1738, stages them at `/run/agenix/...`). They are never serialized into
/// events, provider records, or errors.
#[derive(Clone)]
pub struct GmailOAuthCredentials {
    pub client_id: String,
    pub client_secret: String,
    pub refresh_token: String,
}

impl fmt::Debug for GmailOAuthCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Never leak the secret or refresh token through Debug.
        f.debug_struct("GmailOAuthCredentials")
            .field("client_id", &self.client_id)
            .field("client_secret", &"<redacted>")
            .field("refresh_token", &"<redacted>")
            .finish()
    }
}

impl GmailOAuthCredentials {
    /// Build credentials from explicit values.
    #[must_use]
    pub fn new(client_id: String, client_secret: String, refresh_token: String) -> Self {
        Self {
            client_id,
            client_secret,
            refresh_token,
        }
    }

    /// Read each credential field from an operator-owned secret file.
    ///
    /// File contents are trimmed; an unreadable, missing, or whitespace-only
    /// file yields [`OAuthError::MissingCredential`] so the caller can report
    /// `EmailAuthorizationState::Missing` rather than attempting an exchange.
    pub async fn load_from_files(
        client_id_file: &str,
        client_secret_file: &str,
        refresh_token_file: &str,
    ) -> Result<Self, OAuthError> {
        Ok(Self {
            client_id: read_secret_file("client_id", client_id_file).await?,
            client_secret: read_secret_file("client_secret", client_secret_file).await?,
            refresh_token: read_secret_file("refresh_token", refresh_token_file).await?,
        })
    }
}

async fn read_secret_file(field: &'static str, path: &str) -> Result<String, OAuthError> {
    let raw = tokio::fs::read_to_string(path)
        .await
        .map_err(|_| OAuthError::MissingCredential { field })?;
    let value = raw.trim().to_string();
    if value.is_empty() {
        return Err(OAuthError::MissingCredential { field });
    }
    Ok(value)
}

/// Raw token-endpoint success response (provider JSON shape).
#[derive(Debug, Clone, Deserialize)]
pub struct OAuthTokenResponse {
    pub access_token: String,
    #[serde(default)]
    pub expires_in: Option<i64>,
    #[serde(default)]
    pub token_type: Option<String>,
    #[serde(default)]
    pub scope: Option<String>,
}

/// Token-endpoint error body (`{ "error": ..., "error_description": ... }`).
#[derive(Debug, Clone, Default, Deserialize)]
struct OAuthErrorBody {
    error: Option<String>,
    #[allow(dead_code)]
    error_description: Option<String>,
}

/// A cached access token with its computed wall-clock expiry.
#[derive(Debug, Clone)]
struct CachedToken {
    access_token: String,
    expires_at: OffsetDateTime,
}

impl CachedToken {
    fn from_response(response: OAuthTokenResponse, now: OffsetDateTime) -> Self {
        let lifetime = response.expires_in.unwrap_or(DEFAULT_TOKEN_LIFETIME_SECS);
        Self {
            access_token: response.access_token,
            expires_at: now + time::Duration::seconds(lifetime),
        }
    }

    /// Whether the token is still usable at `now` allowing for `skew`.
    fn is_fresh(&self, now: OffsetDateTime, skew: time::Duration) -> bool {
        now + skew < self.expires_at
    }
}

/// Errors raised while obtaining a bearer access token.
///
/// Secrets are never included. [`OAuthError::Status`] carries the provider error
/// *code* and HTTP body (which the provider must not populate with secrets) but
/// not the credentials used in the request.
#[derive(Debug)]
pub enum OAuthError {
    /// A required credential file was missing, unreadable, or empty.
    MissingCredential { field: &'static str },
    /// Network/transport failure talking to the token endpoint.
    Transport(reqwest::Error),
    /// The endpoint returned a body that could not be decoded as token JSON.
    Decode(reqwest::Error),
    /// The endpoint returned a non-success HTTP status.
    Status {
        status: reqwest::StatusCode,
        error_code: Option<String>,
        body: String,
    },
    /// The endpoint returned success but an empty access token.
    EmptyAccessToken,
}

impl OAuthError {
    /// Map the failure onto the provider authorization state surfaced by
    /// coverage/debt rows.
    #[must_use]
    pub fn authorization_state(&self) -> EmailAuthorizationState {
        match self {
            Self::MissingCredential { .. } => EmailAuthorizationState::Missing,
            Self::Status { error_code, .. }
                if matches!(
                    error_code.as_deref(),
                    Some("invalid_grant" | "invalid_client" | "unauthorized_client")
                ) =>
            {
                EmailAuthorizationState::Rejected
            }
            Self::EmptyAccessToken
            | Self::Status { .. }
            | Self::Transport(_)
            | Self::Decode(_) => EmailAuthorizationState::Unknown,
        }
    }

    /// Whether re-running with the same credentials could plausibly succeed.
    /// `Rejected`/`Missing` need operator action; transient faults may retry.
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        matches!(
            self.authorization_state(),
            EmailAuthorizationState::Unknown | EmailAuthorizationState::Expired
        )
    }
}

impl fmt::Display for OAuthError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingCredential { field } => {
                write!(f, "missing OAuth credential: {field}")
            }
            Self::Transport(error) => write!(f, "OAuth transport error: {error}"),
            Self::Decode(error) => write!(f, "OAuth response decode error: {error}"),
            Self::Status {
                status,
                error_code,
                body,
            } => match error_code {
                Some(code) => write!(f, "OAuth token endpoint returned HTTP {status} ({code})"),
                None if body.trim().is_empty() => {
                    write!(f, "OAuth token endpoint returned HTTP {status}")
                }
                None => write!(f, "OAuth token endpoint returned HTTP {status}: {body}"),
            },
            Self::EmptyAccessToken => write!(f, "OAuth token endpoint returned an empty access token"),
        }
    }
}

impl Error for OAuthError {}

/// Runtime-provided token exchange: trade a refresh token for an access token.
///
/// Mirrors [`super::email_gmail_api::GmailApiClient`]: the production
/// implementation is a thin reqwest client; tests inject a fake.
pub trait OAuthTokenExchange: Send + Sync {
    fn exchange(
        &self,
        credentials: &GmailOAuthCredentials,
    ) -> impl Future<Output = Result<OAuthTokenResponse, OAuthError>> + Send;
}

/// Reqwest-backed Google OAuth2 token client.
#[derive(Clone)]
pub struct GoogleOAuthClient {
    http: reqwest::Client,
    token_url: String,
}

impl GoogleOAuthClient {
    /// Client against the production Google token endpoint.
    #[must_use]
    pub fn new() -> Self {
        Self::with_endpoint(reqwest::Client::new(), GOOGLE_OAUTH_TOKEN_URL.to_string())
    }

    /// Client against an explicit token endpoint (used by tests).
    #[must_use]
    pub fn with_endpoint(http: reqwest::Client, token_url: String) -> Self {
        Self { http, token_url }
    }
}

impl Default for GoogleOAuthClient {
    fn default() -> Self {
        Self::new()
    }
}

impl OAuthTokenExchange for GoogleOAuthClient {
    async fn exchange(
        &self,
        credentials: &GmailOAuthCredentials,
    ) -> Result<OAuthTokenResponse, OAuthError> {
        // The workspace `reqwest` is built without the feature that provides
        // `RequestBuilder::form`, so the `application/x-www-form-urlencoded`
        // body is encoded by hand (matching the query encoding in
        // `email_gmail_api.rs`).
        let body = [
            ("client_id", credentials.client_id.as_str()),
            ("client_secret", credentials.client_secret.as_str()),
            ("refresh_token", credentials.refresh_token.as_str()),
            ("grant_type", "refresh_token"),
        ]
        .iter()
        .map(|(key, value)| {
            format!(
                "{}={}",
                urlencoding::encode(key),
                urlencoding::encode(value)
            )
        })
        .collect::<Vec<_>>()
        .join("&");
        let response = self
            .http
            .post(&self.token_url)
            .header(
                reqwest::header::CONTENT_TYPE,
                "application/x-www-form-urlencoded",
            )
            .body(body)
            .send()
            .await
            .map_err(OAuthError::Transport)?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            let error_code = serde_json::from_str::<OAuthErrorBody>(&body)
                .ok()
                .and_then(|parsed| parsed.error);
            return Err(OAuthError::Status {
                status,
                error_code,
                body,
            });
        }
        let token = response
            .json::<OAuthTokenResponse>()
            .await
            .map_err(OAuthError::Decode)?;
        if token.access_token.trim().is_empty() {
            return Err(OAuthError::EmptyAccessToken);
        }
        Ok(token)
    }
}

/// Caching bearer-token provider built once per operation and reused across the
/// messages/rows of a sync run.
///
/// [`Self::bearer_token`] returns the cached access token while it is fresh and
/// re-exchanges only when it is missing or within the refresh skew of expiry.
pub struct OAuthTokenProvider<X: OAuthTokenExchange> {
    credentials: GmailOAuthCredentials,
    exchange: X,
    refresh_skew: time::Duration,
    cached: tokio::sync::Mutex<Option<CachedToken>>,
}

impl<X: OAuthTokenExchange> OAuthTokenProvider<X> {
    /// Provider with the default 60s refresh skew.
    #[must_use]
    pub fn new(credentials: GmailOAuthCredentials, exchange: X) -> Self {
        Self::with_refresh_skew(
            credentials,
            exchange,
            time::Duration::seconds(DEFAULT_REFRESH_SKEW_SECS),
        )
    }

    /// Provider with an explicit refresh skew (used by tests).
    #[must_use]
    pub fn with_refresh_skew(
        credentials: GmailOAuthCredentials,
        exchange: X,
        refresh_skew: time::Duration,
    ) -> Self {
        Self {
            credentials,
            exchange,
            refresh_skew,
            cached: tokio::sync::Mutex::new(None),
        }
    }

    /// Return a usable bearer access token, refreshing when stale.
    pub async fn bearer_token(&self) -> Result<String, OAuthError> {
        let now = OffsetDateTime::now_utc();
        let mut cached = self.cached.lock().await;
        if let Some(token) = cached.as_ref() {
            if token.is_fresh(now, self.refresh_skew) {
                return Ok(token.access_token.clone());
            }
        }
        let response = self.exchange.exchange(&self.credentials).await?;
        let fresh = CachedToken::from_response(response, now);
        let access_token = fresh.access_token.clone();
        *cached = Some(fresh);
        Ok(access_token)
    }

    /// Drop any cached token so the next [`Self::bearer_token`] re-exchanges.
    pub async fn invalidate(&self) {
        *self.cached.lock().await = None;
    }
}

#[cfg(test)]
mod tests {
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
}
