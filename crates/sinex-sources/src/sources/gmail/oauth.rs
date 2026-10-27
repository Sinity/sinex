//! OAuth secrets are read from operator-owned files and kept only in memory.
use serde::Deserialize;
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::{Duration, SystemTime};

pub const FULL_SCOPE: &str = "https://mail.google.com/";
const TOKEN_URL: &str = "https://oauth2.googleapis.com/token";

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CredentialFiles {
    pub client_id_file: PathBuf,
    pub client_secret_file: PathBuf,
    pub refresh_token_file: PathBuf,
}

pub struct Credentials {
    client_id: String,
    client_secret: String,
    refresh_token: String,
}

impl std::fmt::Debug for Credentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Credentials(<redacted>)")
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    #[error("Gmail OAuth credential missing or empty: {0}")]
    Missing(&'static str),
    #[error("Gmail OAuth authorization rejected")]
    Rejected,
    #[error("Gmail OAuth service unavailable")]
    Unavailable,
    #[error("Gmail OAuth token response invalid or missing full Gmail scope")]
    InvalidResponse,
}

impl CredentialFiles {
    pub fn load(&self) -> Result<Credentials, Error> {
        fn read(path: &std::path::Path, name: &'static str) -> Result<String, Error> {
            let text = std::fs::read_to_string(path).map_err(|_| Error::Missing(name))?;
            let text = text.trim();
            if text.is_empty() { return Err(Error::Missing(name)); }
            Ok(text.to_owned())
        }
        Ok(Credentials {
            client_id: read(&self.client_id_file, "client_id")?,
            client_secret: read(&self.client_secret_file, "client_secret")?,
            refresh_token: read(&self.refresh_token_file, "refresh_token")?,
        })
    }
}

#[derive(Deserialize)]
pub struct Token {
    access_token: String,
    expires_in: u64,
    token_type: String,
    scope: Option<String>,
}

pub trait Exchange: Send + Sync {
    fn exchange(&self, credentials: &Credentials) -> Result<Token, Error>;
}

pub trait Clock: Send + Sync {
    fn now(&self) -> SystemTime;
}

pub struct SystemClock;
impl Clock for SystemClock {
    fn now(&self) -> SystemTime { SystemTime::now() }
}

pub struct GoogleExchange {
    agent: ureq::Agent,
    endpoint: String,
}

pub(super) fn agent() -> ureq::Agent {
    ureq::Agent::config_builder()
        .timeout_global(Some(Duration::from_secs(60)))
        .http_status_as_error(false)
        .max_redirects(0)
        .build().into()
}

impl Default for GoogleExchange {
    fn default() -> Self { Self { agent: agent(), endpoint: TOKEN_URL.into() } }
}

impl Exchange for GoogleExchange {
    fn exchange(&self, credentials: &Credentials) -> Result<Token, Error> {
        let mut response = self.agent.post(&self.endpoint).send_form([
            ("grant_type", "refresh_token"),
            ("client_id", credentials.client_id.as_str()),
            ("client_secret", credentials.client_secret.as_str()),
            ("refresh_token", credentials.refresh_token.as_str()),
            ("scope", FULL_SCOPE),
        ]).map_err(|_| Error::Unavailable)?;
        match response.status().as_u16() {
            200 => {},
            400 | 401 | 403 => return Err(Error::Rejected),
            _ => return Err(Error::Unavailable),
        }
        let bytes = response.body_mut().with_config().limit(u64::MAX).read_to_vec()
            .map_err(|_| Error::Unavailable)?;
        serde_json::from_slice(&bytes).map_err(|_| Error::InvalidResponse)
    }
}

struct CachedToken { access: String, until: SystemTime }

pub struct TokenProvider<E = GoogleExchange, C = SystemClock> {
    files: CredentialFiles,
    exchange: E,
    clock: C,
    cached: Mutex<Option<CachedToken>>,
}

impl<E: Exchange, C: Clock> TokenProvider<E, C> {
    pub fn new(files: CredentialFiles, exchange: E, clock: C) -> Self {
        Self { files, exchange, clock, cached: Mutex::new(None) }
    }

    pub fn bearer(&self) -> Result<String, Error> {
        let mut cache = self.cached.lock().map_err(|_| Error::Unavailable)?;
        let now = self.clock.now();
        if let Some(token) = cache.as_ref() && token.until > now {
            return Ok(token.access.clone());
        }
        *cache = None;
        let token = self.exchange.exchange(&self.files.load()?)?;
        if token.access_token.is_empty() || !token.token_type.eq_ignore_ascii_case("bearer")
            || token.scope.as_ref().is_some_and(|s| !s.split_whitespace().any(|s| s == FULL_SCOPE)) {
            return Err(Error::InvalidResponse);
        }
        let until = now.checked_add(Duration::from_secs(token.expires_in.saturating_sub(30)))
            .ok_or(Error::InvalidResponse)?;
        let access = token.access_token;
        *cache = Some(CachedToken { access: access.clone(), until });
        Ok(access)
    }

    pub fn invalidate(&self) {
        if let Ok(mut cache) = self.cached.lock() { *cache = None; }
    }
}

/// The operator opens this URL and supplies the resulting refresh token file.
pub fn authorization_url(client_id: &str, redirect_uri: &str, state: &str) -> String {
    format!("https://accounts.google.com/o/oauth2/v2/auth?client_id={}&redirect_uri={}&state={}&scope={}&response_type=code&access_type=offline&prompt=consent", encode(client_id), encode(redirect_uri), encode(state), encode(FULL_SCOPE))
}

pub(super) fn encode(text: &str) -> String {
    text.bytes().map(|b| if b.is_ascii_alphanumeric() || b"-._~".contains(&b) {
        (b as char).to_string()
    } else { format!("%{b:02X}") }).collect()
}

#[cfg(test)]
mod tests;
