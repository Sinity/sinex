use super::*;

pub(super) fn send_token_watcher_ready(
    ready_tx: &mut Option<tokio::sync::oneshot::Sender<SinexResult<()>>>,
    result: SinexResult<()>,
    phase: &str,
) -> bool {
    if let Some(tx) = ready_tx.take()
        && tx.send(result).is_err()
    {
        warn!(
            phase,
            "RPC token file watcher readiness receiver was dropped before initialization completed"
        );
        return false;
    }
    true
}

#[derive(Clone)]
pub(crate) struct GatewayAuth {
    pub(super) token: Arc<RwLock<Option<String>>>,
    token_path: Option<PathBuf>,
}

impl GatewayAuth {
    pub(super) fn store_token(token: &RwLock<Option<String>>, new_token: String) {
        let mut token_guard = token.write();
        *token_guard = Some(new_token);
    }

    pub(super) fn reload_token_from_path(token: &RwLock<Option<String>>, path: &Path) {
        match std::fs::read_to_string(path) {
            Ok(new_token) => {
                let trimmed = new_token.trim().to_string();
                if trimmed.is_empty() {
                    warn!("Token file {:?} is empty after reload", path);
                } else {
                    Self::store_token(token, trimmed);
                    info!("RPC token reloaded from {:?}", path);
                }
            }
            Err(error) => {
                error!(
                    target: "sinex_metrics",
                    metric = "gateway.token_file_watch_failures_total",
                    path = ?path,
                    error = %error,
                    "Failed to read token file after modification"
                );
            }
        }
    }

    pub(super) fn from_config(config: &GatewayConfig) -> SinexResult<Self> {
        let (token, token_path) = config
            .auth_token_from_config()
            .map_err(|err| SinexError::configuration(err.to_string()))?;

        if let Some(ref t) = token {
            if t.trim().is_empty() {
                return Err(SinexError::configuration(
                    "SINEX_API_TOKEN (or token file) is set but empty; refusing to start without a token",
                ));
            }
        } else {
            return Err(SinexError::configuration(
                "SINEX_API_TOKEN is not set. Export a token (or SINEX_API_ADMIN_TOKEN_FILE / SINEX_API_TOKEN_FILE) so the gateway can authenticate RPC clients.",
            ));
        }

        Ok(Self {
            token: Arc::new(RwLock::new(token)),
            token_path,
        })
    }

    pub(super) async fn start_file_watcher(
        self,
        shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> SinexResult<Self> {
        if let Some(ref path) = self.token_path {
            let token_clone = Arc::clone(&self.token);
            let path_clone = path.clone();
            let path_for_closure = path.clone();

            // Bridge the async shutdown watch into a sync channel so the OS-thread
            // watcher can block cleanly instead of polling with sleep().
            let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();
            {
                let mut shutdown_clone = shutdown.clone();
                tokio::spawn(async move {
                    // wait_for blocks until the predicate matches or the sender is dropped.
                    if shutdown_clone.wait_for(|v| *v).await.is_err() {
                        warn!(
                            "RPC token file watcher shutdown channel dropped before explicit shutdown"
                        );
                    }
                    if done_tx.send(()).is_err() {
                        debug!(
                            "RPC token file watcher shutdown bridge receiver was already dropped"
                        );
                    }
                });
            }

            let (ready_tx, ready_rx) = tokio::sync::oneshot::channel::<SinexResult<()>>();

            std::thread::spawn(move || {
                use notify::{Event, EventKind, RecursiveMode, Watcher};
                let mut ready_tx = Some(ready_tx);

                let watcher = notify::recommended_watcher(
                    move |res: std::result::Result<Event, notify::Error>| {
                        match res {
                            Ok(event) => {
                                match event.kind {
                                    EventKind::Modify(_) | EventKind::Create(_) => {
                                        Self::reload_token_from_path(
                                            &token_clone,
                                            &path_for_closure,
                                        );
                                    }
                                    EventKind::Remove(_) => {
                                        // File was deleted — keep last valid token (fail-closed).
                                        // Do NOT clear the token, as that would disable auth entirely,
                                        // allowing unauthenticated access. If the file is recreated,
                                        // the Create/Modify handler will reload it.
                                        error!(
                                            target: "sinex_metrics",
                                            metric = "gateway.token_file_watch_failures_total",
                                            path = ?path_for_closure,
                                            "RPC token file deleted! Keeping last valid token. Re-create the file to update the token."
                                        );
                                    }
                                    _ => {
                                        // Ignore other events (access, metadata changes, etc.)
                                    }
                                }
                            }
                            Err(e) => {
                                error!(
                                    target: "sinex_metrics",
                                    metric = "gateway.token_file_watch_failures_total",
                                    error = %e,
                                    "Token file watch error"
                                );
                            }
                        }
                    },
                );

                let mut watcher = match watcher {
                    Ok(w) => w,
                    Err(e) => {
                        send_token_watcher_ready(
                            &mut ready_tx,
                            Err(SinexError::configuration("Failed to create file watcher")
                                .with_std_error(&e)),
                            "create",
                        );
                        error!(
                            target: "sinex_metrics",
                            metric = "gateway.token_file_watch_failures_total",
                            error = %e,
                            "Failed to create file watcher"
                        );
                        return;
                    }
                };

                if let Err(e) = watcher.watch(&path_clone, RecursiveMode::NonRecursive) {
                    send_token_watcher_ready(
                        &mut ready_tx,
                        Err(SinexError::configuration("Failed to watch token file")
                            .with_context("path", path_clone.display().to_string())
                            .with_std_error(&e)),
                        "watch",
                    );
                    error!(
                        target: "sinex_metrics",
                        metric = "gateway.token_file_watch_failures_total",
                        path = ?path_clone,
                        error = %e,
                        "Failed to watch token file"
                    );
                    return;
                }

                send_token_watcher_ready(&mut ready_tx, Ok(()), "ready");
                info!("Watching token file {:?} for changes", path_clone);

                // Block until the shutdown signal fires; no busy-polling.
                if done_rx.recv().is_err() {
                    warn!(
                        "RPC token file watcher shutdown bridge disconnected before explicit shutdown"
                    );
                }
                debug!("Token file watcher shutting down");
            });

            match tokio::time::timeout(Duration::from_secs(2), ready_rx).await {
                Ok(Ok(Ok(()))) => {}
                Ok(Ok(Err(err))) => return Err(err),
                Ok(Err(err)) => {
                    return Err(SinexError::channel_receive(
                        "token file watcher readiness channel closed",
                    )
                    .with_std_error(&err));
                }
                Err(_) => {
                    return Err(SinexError::timeout(format!(
                        "Timed out waiting for token file watcher to initialize for {}",
                        path.display()
                    )));
                }
            }
        }

        Ok(self)
    }

    /// Verify the bearer token in the request headers.
    /// Returns the verified token string on success so callers need not re-extract it.
    pub(crate) fn verify(&self, headers: &HeaderMap) -> Result<String, AuthError> {
        let provided = extract_token(headers).ok_or(AuthError::Missing)?;

        let token_guard = self.token.read();
        if let Some(expected) = token_guard.as_ref() {
            if constant_time_eq(provided.as_bytes(), expected.as_bytes()) {
                Ok(provided)
            } else {
                Err(AuthError::Invalid)
            }
        } else {
            warn!("No token configured - rejecting request");
            Err(AuthError::Missing)
        }
    }

    #[cfg(test)]
    pub(super) fn with_test_token(token: &str) -> Self {
        Self {
            token: Arc::new(RwLock::new(Some(token.to_string()))),
            token_path: None,
        }
    }
}

pub(super) fn read_token_and_path_from_env() -> SinexResult<(Option<String>, Option<PathBuf>)> {
    if let Some(path_str) = shared_env::strict_var("SINEX_API_ADMIN_TOKEN_FILE")? {
        let path = PathBuf::from(&path_str);
        let contents = std::fs::read_to_string(&path).map_err(|e| {
            SinexError::configuration("Failed to read SINEX_API_ADMIN_TOKEN_FILE")
                .with_context("path", path.display().to_string())
                .with_std_error(&e)
        })?;
        return Ok((Some(contents.trim().to_string()), Some(path)));
    }

    if let Some(path_str) = shared_env::strict_var("SINEX_API_TOKEN_FILE")? {
        let path = PathBuf::from(&path_str);
        let contents = std::fs::read_to_string(&path).map_err(|e| {
            SinexError::configuration("Failed to read SINEX_API_TOKEN_FILE")
                .with_context("path", path.display().to_string())
                .with_std_error(&e)
        })?;
        return Ok((Some(contents.trim().to_string()), Some(path)));
    }

    if let Some(token) = shared_env::strict_var("SINEX_API_TOKEN")? {
        return Ok((Some(token.trim().to_string()), None));
    }

    Ok((None, None))
}

pub(crate) fn extract_token(headers: &HeaderMap) -> Option<String> {
    if let Some(value) = headers.get(header::AUTHORIZATION)
        && let Ok(as_str) = value.to_str()
    {
        let trimmed = as_str.trim();
        if let Some(rest) = trimmed.strip_prefix("Bearer ") {
            return Some(rest.trim().to_string());
        }
    }

    None
}

// Issue 137: Use constant-time comparison from subtle crate
pub(crate) fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    use subtle::ConstantTimeEq;
    bool::from(a.ct_eq(b))
}

pub(crate) enum AuthError {
    Missing,
    Invalid,
}

impl AuthError {
    pub(super) fn into_response(self) -> (StatusCode, Json<JsonRpcResponse>) {
        let message = match self {
            AuthError::Missing => {
                "Authentication required. Provide SINEX_API_TOKEN via Authorization header."
            }
            AuthError::Invalid => "Authentication failed: invalid token.",
        };

        (
            StatusCode::UNAUTHORIZED,
            Json(JsonRpcResponse::error(None, -32002, message.to_string())),
        )
    }
}

impl std::fmt::Display for AuthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AuthError::Missing => write!(
                f,
                "authentication required: provide SINEX_API_TOKEN via Authorization header"
            ),
            AuthError::Invalid => write!(f, "authentication failed: invalid token"),
        }
    }
}

/// Authorization context passed to RPC handlers
///
/// Contains actor information derived from the authenticated token,
/// allowing handlers to perform authorization checks and audit logging.
#[derive(Debug, Clone)]
pub struct RpcAuthContext {
    /// First 8 characters of the token for audit logging
    pub token_prefix: String,
    /// Stable actor identity for access audit records
    pub actor_id: String,
    /// Timestamp when authentication occurred
    pub authenticated_at: Timestamp,
    /// Role extracted from token (determines permissions)
    pub role: crate::api::auth::Role,
}

impl RpcAuthContext {
    /// Create an auth context from a validated token
    ///
    /// Parses the role from the token suffix (e.g., `sinex_xxx:readonly`)
    pub(crate) fn from_token(token: &str) -> Result<Self, crate::api::auth::TokenRoleError> {
        let (base, role) = crate::api::auth::Role::from_token(token)?;
        let token_prefix = base.chars().take(8).collect::<String>();
        Ok(Self {
            actor_id: format!("token:{token_prefix}"),
            token_prefix,
            authenticated_at: Timestamp::now(),
            role,
        })
    }

    /// Create a system auth context for native messaging or internal calls
    ///
    /// Native messaging uses stdin/stdout and doesn't go through HTTP auth,
    /// so we use a special "system" context to indicate trusted local calls.
    /// System context always has Admin role.
    #[must_use]
    pub fn system() -> Self {
        Self {
            token_prefix: "system".to_string(),
            actor_id: "system:local".to_string(),
            authenticated_at: Timestamp::now(),
            role: crate::api::auth::Role::Admin,
        }
    }

    /// Create an auth context for a native messaging extension
    ///
    /// Used when native messaging can attribute calls to specific browser extensions.
    /// The role is determined by the `SINEX_NATIVE_MESSAGING_EXTENSION_ROLES` env var.
    /// Unknown extensions default to `ReadOnly` for defense in depth.
    #[must_use]
    pub fn extension(extension_id: &str, role: crate::api::auth::Role) -> Self {
        Self {
            token_prefix: format!("ext:{}", extension_id.chars().take(8).collect::<String>()),
            actor_id: format!("extension:{extension_id}"),
            authenticated_at: Timestamp::now(),
            role,
        }
    }

    #[must_use]
    pub fn actor_id(&self) -> &str {
        &self.actor_id
    }

    #[must_use]
    pub fn replay_actor(&self) -> String {
        if self.actor_id.starts_with("system:") {
            return self.actor_id.clone();
        }

        let replay_role = match self.role {
            crate::api::auth::Role::Admin => "admin",
            crate::api::auth::Role::Write => "operator",
            crate::api::auth::Role::ReadOnly => "user",
        };
        format!("{replay_role}:{}", self.actor_id)
    }

    /// Check if the token has at least the required role permission
    #[must_use]
    pub fn has_permission(&self, required: crate::api::auth::Role) -> bool {
        self.role.has_permission(required)
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum AccessOutcome {
    Success,
    Failed,
    Unauthenticated,
    Rejected,
    RateLimited,
    InvalidRequest,
    Forbidden,
    Unavailable,
}

impl AccessOutcome {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Failed => "failed",
            Self::Unauthenticated => "unauthenticated",
            Self::Rejected => "rejected",
            Self::RateLimited => "rate_limited",
            Self::InvalidRequest => "invalid_request",
            Self::Forbidden => "forbidden",
            Self::Unavailable => "unavailable",
        }
    }
}

pub(crate) fn log_access_audit(
    surface: &'static str,
    operation: &str,
    outcome: AccessOutcome,
    auth: Option<&RpcAuthContext>,
    detail: Option<&str>,
) {
    let actor = auth.map_or("anonymous", RpcAuthContext::actor_id);
    let role = auth.map_or("none", |ctx| ctx.role.as_str());

    match (outcome, detail) {
        (AccessOutcome::Success, _) => info!(
            event = "gateway.access",
            surface,
            operation,
            outcome = outcome.as_str(),
            actor,
            role,
            "Gateway access allowed"
        ),
        (_, Some(detail)) => warn!(
            event = "gateway.access",
            surface,
            operation,
            outcome = outcome.as_str(),
            actor,
            role,
            detail,
            "Gateway access denied or failed"
        ),
        _ => warn!(
            event = "gateway.access",
            surface,
            operation,
            outcome = outcome.as_str(),
            actor,
            role,
            "Gateway access denied or failed"
        ),
    }
}

/// Rate limiter that can be either in-memory or distributed via NATS KV.
///
/// Both backends enforce per-role quotas keyed on `(token, role)`:
///
/// 1. **This enum** — covers the HTTP RPC path (`/rpc`, `/events/stream`).
///    - `InMemory`: `TokenRateLimiter` — per-(token, role) RPS+burst via `governor`.
///      Role quotas apply: admin < write < readonly by default.
///    - `Distributed`: `DistributedRateLimiter` — per-(token, role) RPM via NATS KV.
///      Each role has its own KV key and window budget derived from the same
///      per-role RPS settings, so switching backends does not change effective quotas.
///
/// 2. **`native_messaging::RateLimiter`** (private, in that module) — covers the
///    browser-extension native-messaging path. Keyed on `extension_id`, enforces
///    a per-minute sliding window. Config comes from `ExtensionCapabilities`, not
///    `GatewayConfig`. It does not share state or semantics with this enum.
///
/// Residual divergence: a quota tightened in `RateLimitConfig` does **not**
/// constrain native messaging. That gap is tracked separately (#1578).
#[derive(Clone)]
pub(crate) enum RateLimiter {
    /// In-memory rate limiter (fast, but state lost on restart).
    /// Applies per-role RPS+burst quotas via `governor`.
    InMemory(Arc<TokenRateLimiter>),
    /// Distributed rate limiter via NATS KV (shared across instances, survives restarts).
    /// Applies per-role RPM quotas with NATS KV keys scoped per (token, role).
    Distributed(Arc<DistributedRateLimiter>),
}

impl RateLimiter {
    /// Check if a request is allowed for the given (token, role).
    ///
    /// Both backends enforce per-role quotas: `InMemory` uses RPS+burst via `governor`;
    /// `Distributed` uses per-window RPM via NATS KV with keys scoped per (token, role).
    pub(super) async fn check(&self, token: &str, role: crate::api::auth::Role) -> bool {
        match self {
            RateLimiter::InMemory(limiter) => limiter.check(token, role).is_ok(),
            RateLimiter::Distributed(limiter) => limiter.check_and_increment(token, role).await,
        }
    }
}

#[allow(dead_code)]
pub(crate) struct AppStateAuthExports;
