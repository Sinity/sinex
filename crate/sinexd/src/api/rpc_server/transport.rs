use super::auth::read_token_and_path_from_env;
use super::*;

#[derive(Debug, Clone, Copy)]
pub(crate) struct RpcServerLimits {
    pub(crate) concurrency_limit: usize,
    pub(crate) request_timeout: Duration,
    pub(crate) max_body_bytes: Bytes,
}

impl RpcServerLimits {
    pub(crate) fn from_config(config: &GatewayConfig) -> Self {
        Self {
            concurrency_limit: config.max_concurrency,
            request_timeout: config.request_timeout(),
            max_body_bytes: Bytes::from_bytes(config.max_body_bytes),
        }
    }

    pub(crate) fn apply_pool_limit(self, pool_max: usize) -> Self {
        if pool_max == 0 {
            return self;
        }

        Self {
            concurrency_limit: self.concurrency_limit.min(pool_max),
            ..self
        }
    }

    #[cfg(test)]
    pub(super) fn test_limits(
        concurrency_limit: usize,
        timeout: Duration,
        max_body_bytes: Bytes,
    ) -> Self {
        Self {
            concurrency_limit,
            request_timeout: timeout,
            max_body_bytes,
        }
    }
}

/// Server bind address configuration
#[derive(Debug)]
pub(crate) enum BindAddress {
    Tcp { host: String, port: u16 },
}

impl BindAddress {
    /// Create bind address from loaded gateway configuration.
    pub(crate) fn from_config(config: &GatewayConfig) -> SinexResult<Self> {
        let (host, port) = parse_tcp_listen(&config.tcp_listen)?;
        Ok(BindAddress::Tcp { host, port })
    }
}

pub(crate) fn parse_tcp_listen(spec: &str) -> SinexResult<(String, u16)> {
    if let Ok(addr) = SocketAddr::from_str(spec) {
        return Ok((addr.ip().to_string(), addr.port()));
    }

    if let Some(idx) = spec.rfind(':') {
        let (host_part, port_part) = spec.split_at(idx);
        let port = port_part[1..].parse::<u16>().map_err(|error| {
            SinexError::configuration(format!("Invalid TCP port in {spec}")).with_std_error(&error)
        })?;
        let host = host_part.trim_matches(|c| c == '[' || c == ']').trim();
        if host.is_empty() {
            return Err(SinexError::configuration(format!(
                "TCP host is empty in {spec}"
            )));
        }
        return Ok((host.to_string(), port));
    }

    Err(SinexError::configuration(format!(
        "Invalid TCP listen specification '{spec}'. Expected host:port."
    )))
}

/// Read RPC token from environment variables.
/// Priority: `SINEX_API_ADMIN_TOKEN_FILE` > `SINEX_API_TOKEN_FILE` > `SINEX_API_TOKEN`
///
/// Used by test support utilities and external consumers that need token access.
pub fn read_token_from_env() -> SinexResult<Option<String>> {
    let (token, _) = read_token_and_path_from_env()?;
    Ok(token)
}

/// Backlog size for the TCP listener.
///
/// 128 matches the traditional `SOMAXCONN` default and is sufficient for gateway
/// workloads. The kernel may clamp this to the system-configured maximum.
const TCP_LISTEN_BACKLOG: u32 = 128;

/// Bind a TCP listener with explicit address reuse.
pub(crate) fn bind_tcp_listener(addr: &str) -> std::io::Result<tokio::net::TcpListener> {
    use std::net::SocketAddr;
    use tokio::net::TcpSocket;

    let socket_addr: SocketAddr = addr
        .parse()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;

    let socket = if socket_addr.is_ipv4() {
        TcpSocket::new_v4()?
    } else {
        TcpSocket::new_v6()?
    };

    socket.set_reuseaddr(true)?;
    socket.bind(socket_addr)?;
    socket.listen(TCP_LISTEN_BACKLOG)
}

pub(crate) fn tls_paths_from_config(
    config: &GatewayConfig,
) -> SinexResult<(String, String, Option<String>)> {
    let cert = config.tls_cert.clone().ok_or_else(|| {
        SinexError::configuration(
            "SINEX_API_TLS_CERT is required for TCP bindings\n\n\
            For local development, run `xtask doctor --fix` to auto-generate certificates.\n\
            For production, provide proper certificates via environment variables.",
        )
    })?;
    let key = config.tls_key.clone().ok_or_else(|| {
        SinexError::configuration(
            "SINEX_API_TLS_KEY is required for TCP bindings\n\n\
            For local development, run `xtask doctor --fix` to auto-generate certificates.\n\
            For production, provide proper certificates via environment variables.",
        )
    })?;
    let client_ca = config.tls_client_ca.clone();
    Ok((cert, key, client_ca))
}

pub(crate) fn load_rustls_config(
    cert_path: &str,
    key_path: &str,
    client_ca_path: Option<&str>,
) -> SinexResult<rustls::ServerConfig> {
    ensure_rustls_crypto_provider()?;

    let cert_chain: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(cert_path)
        .map_err(|error| {
            SinexError::configuration(format!("Failed to open TLS certificate from {cert_path}"))
                .with_std_error(&error)
        })?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| {
            SinexError::configuration(format!("Failed to read TLS certificate from {cert_path}"))
                .with_std_error(&error)
        })?;

    let key = PrivateKeyDer::from_pem_file(key_path).map_err(|error| {
        SinexError::configuration(format!("Failed to read TLS private key from {key_path}"))
            .with_std_error(&error)
    })?;

    if let Some(ca_path) = client_ca_path {
        let client_certs: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(ca_path)
            .map_err(|error| {
                SinexError::configuration(format!("Failed to open client CA bundle from {ca_path}"))
                    .with_std_error(&error)
            })?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| {
                SinexError::configuration(format!("Failed to read client CA bundle from {ca_path}"))
                    .with_std_error(&error)
            })?;
        let mut roots = rustls::RootCertStore::empty();
        let (added, _ignored) = roots.add_parsable_certificates(client_certs);
        if added == 0 {
            return Err(SinexError::configuration(format!(
                "No valid client CA certs found in {ca_path}"
            )));
        }

        let verifier = WebPkiClientVerifier::builder(Arc::new(roots))
            .build()
            .map_err(|error| {
                SinexError::configuration("Failed to build client verifier").with_std_error(&error)
            })?;

        rustls::ServerConfig::builder()
            .with_client_cert_verifier(verifier)
            .with_single_cert(cert_chain, key)
            .map_err(|error| {
                SinexError::configuration("Invalid TLS cert/key").with_std_error(&error)
            })
    } else {
        rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(cert_chain, key)
            .map_err(|error| {
                SinexError::configuration("Invalid TLS cert/key").with_std_error(&error)
            })
    }
}

/// Install the process-global rustls crypto provider (idempotent).
///
/// Must run before the first `reqwest::Client` / rustls `ClientConfig` /
/// `ServerConfig` is built anywhere in the process — reqwest is compiled with
/// `rustls-no-provider`, so building a client without a default provider
/// panics. Called once at daemon startup (`sinexd` main) so every entry point
/// (supervisor, standalone gateway, source scans) is covered regardless of
/// which subsystem builds the first TLS client.
pub fn ensure_rustls_crypto_provider() -> SinexResult<()> {
    if rustls::crypto::CryptoProvider::get_default().is_some() {
        return Ok(());
    }

    match rustls::crypto::aws_lc_rs::default_provider().install_default() {
        Ok(()) => Ok(()),
        Err(_) if rustls::crypto::CryptoProvider::get_default().is_some() => Ok(()),
        Err(_) => Err(SinexError::configuration(
            "Failed to install Rustls crypto provider for gateway TLS configuration",
        )),
    }
}

fn is_loopback_host(host: &str) -> bool {
    if host.eq_ignore_ascii_case("localhost") {
        return true;
    }
    if let Ok(addr) = host.parse::<IpAddr>() {
        return addr.is_loopback();
    }
    false
}

/// Enforce mTLS requirements based on bind address and configuration
///
/// # Security Note (Issue 151 - LOW)
///
/// The gateway currently requires mTLS for all TCP bindings. For deployments
/// behind a reverse proxy (nginx, `HAProxy`, Envoy), the proxy should handle
/// TLS termination and client authentication. In this configuration:
///
/// - Bind gateway to 127.0.0.1 (loopback only)
/// - Configure reverse proxy with TLS certificates
/// - Set up client certificate verification in the proxy
/// - Use `SINEX_API_REQUIRE_CLIENT_TLS=0` if proxy handles mTLS
///
/// For direct TLS support without a proxy, native rustls integration is already
/// implemented in this file (see `load_rustls_config` and TLS acceptor logic).
pub(crate) fn require_mtls_for_remote(
    bind_address: &BindAddress,
    require_client_tls: bool,
    client_ca: Option<&str>,
) -> SinexResult<()> {
    let host_requires = match bind_address {
        BindAddress::Tcp { host, .. } => !is_loopback_host(host),
    };

    if (host_requires || require_client_tls) && client_ca.is_none() {
        return Err(SinexError::configuration(
            "SINEX_API_TLS_CLIENT_CA is required when mTLS is enforced (non-loopback or SINEX_API_REQUIRE_CLIENT_TLS=1)",
        ));
    }
    Ok(())
}

pub(crate) fn warn_if_remote_bind(bind_address: &BindAddress) {
    let BindAddress::Tcp { host, .. } = bind_address;
    if !is_loopback_host(host) {
        warn!(
            bind_host = %host,
            "Gateway RPC is exposed beyond loopback; ensure mTLS and firewalling are configured"
        );
    }
}

pub(super) fn request_id_for_span<B>(request: &Request<B>) -> Cow<'_, str> {
    match request.headers().get("x-request-id") {
        Some(value) => match value.to_str() {
            Ok(request_id) => Cow::Borrowed(request_id),
            Err(_) => Cow::Borrowed("<invalid x-request-id>"),
        },
        None => Cow::Borrowed("unknown"),
    }
}

pub(crate) fn apply_rpc_layers<S>(
    router: Router<S>,
    limits: &RpcServerLimits,
    cors_origins: &[String],
) -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    let request_id_header = HeaderName::from_static("x-request-id");

    // Configure CORS: if no origins specified, allow localhost only
    let cors = if cors_origins.is_empty() {
        CorsLayer::new()
            .allow_origin(AllowOrigin::predicate(|origin, _| {
                origin.to_str().is_ok_and(is_localhost_origin)
            }))
            .allow_methods([Method::POST, Method::GET, Method::OPTIONS])
            .allow_headers([header::CONTENT_TYPE, header::AUTHORIZATION])
    } else {
        let origins = parse_cors_origin_values(cors_origins);
        CorsLayer::new()
            .allow_origin(origins)
            .allow_methods([Method::POST, Method::GET, Method::OPTIONS])
            .allow_headers([header::CONTENT_TYPE, header::AUTHORIZATION])
    };

    // Note: TimeoutLayer is NOT applied here — it's applied per-route-group
    // in build_app() so that SSE (long-lived) routes are exempt from timeout.
    router
        .layer(
            ServiceBuilder::new()
                .layer(HandleErrorLayer::new(handle_layer_error))
                .layer(LoadShedLayer::new())
                .layer(ConcurrencyLimitLayer::new(limits.concurrency_limit))
                .layer(RequestBodyLimitLayer::new(limits.max_body_bytes.as_usize()))
                .layer(cors)
                .into_inner(),
        )
        .layer(
            TraceLayer::new_for_http().make_span_with(|request: &Request<_>| {
                tracing::info_span!(
                    "rpc.request",
                    method = %request.method(),
                    uri = %request.uri(),
                    request_id = %request_id_for_span(request)
                )
            }),
        )
        .layer(PropagateRequestIdLayer::new(request_id_header.clone()))
        .layer(SetRequestIdLayer::new(request_id_header, MakeRequestUuid))
}

/// Return true if the given HTTP origin is a valid localhost or loopback origin.
///
/// Accepts `http://localhost:<port>` and `http://127.0.0.1:<port>` where `<port>`
/// consists only of ASCII digits. Rejects strings like `http://localhost:evil.com`
/// that pass a naive `starts_with` check.
fn is_localhost_origin(origin: &str) -> bool {
    for prefix in &["http://localhost:", "http://127.0.0.1:"] {
        if let Some(rest) = origin.strip_prefix(prefix) {
            // The remainder must be a non-empty sequence of ASCII digits only
            if !rest.is_empty() && rest.bytes().all(|b| b.is_ascii_digit()) {
                return true;
            }
        }
    }
    false
}

pub(super) fn parse_cors_origin_values(cors_origins: &[String]) -> Vec<HeaderValue> {
    cors_origins
        .iter()
        .filter_map(|origin| match HeaderValue::from_str(origin) {
            Ok(value) => Some(value),
            Err(error) => {
                warn!(
                    origin = %origin,
                    %error,
                    "Ignoring invalid CORS origin override"
                );
                None
            }
        })
        .collect()
}

pub(crate) async fn handle_layer_error(err: BoxError) -> impl IntoResponse {
    if err.is::<tower::timeout::error::Elapsed>() {
        return rpc_layer_error_response(
            StatusCode::GATEWAY_TIMEOUT,
            -32000,
            "RPC request exceeded timeout".to_string(),
        );
    }

    if err.is::<Overloaded>() {
        return rpc_layer_error_response(
            StatusCode::TOO_MANY_REQUESTS,
            -32001,
            "RPC server is busy; please retry".to_string(),
        );
    }

    let message = format!("Unhandled middleware error: {err}");
    rpc_layer_error_response(StatusCode::INTERNAL_SERVER_ERROR, -32099, message)
}

fn rpc_layer_error_response(status: StatusCode, code: i32, message: String) -> impl IntoResponse {
    (status, Json(JsonRpcResponse::error(None, code, message)))
}
