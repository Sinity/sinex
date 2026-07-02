use super::*;
use axum::{
    Json, Router,
    http::{HeaderMap, HeaderValue},
    routing::post,
};
use reqwest::Client;
use serde_json::json;
use std::net::SocketAddr;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use xtask::sandbox::sinex_test;
static ENV_LOCK: std::sync::LazyLock<Mutex<()>> = std::sync::LazyLock::new(|| Mutex::new(()));

fn clear_tcp_env() {
    unsafe { std::env::remove_var("SINEX_API_TCP_LISTEN") };
}

fn gateway_config_from_env() -> GatewayConfig {
    GatewayConfig::load().expect("gateway config should load in test env")
}

fn clear_auth_env() {
    unsafe {
        std::env::remove_var("SINEX_API_TOKEN");
        std::env::remove_var("SINEX_API_TOKEN_FILE");
        std::env::remove_var("SINEX_API_ADMIN_TOKEN_FILE");
    }
}

fn bearer_headers(token: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    let value =
        HeaderValue::from_str(&format!("Bearer {token}")).expect("valid bearer header value");
    headers.insert(header::AUTHORIZATION, value);
    headers
}

fn build_test_router(limits: RpcServerLimits) -> Router {
    let base = Router::new()
        .route(
            "/",
            post(|| async move {
                tokio::time::sleep(Duration::from_millis(100)).await;
                Json(json!({"status": "ok"}))
            }),
        )
        .layer(
            ServiceBuilder::new()
                .layer(HandleErrorLayer::new(handle_layer_error))
                .layer(TimeoutLayer::new(limits.request_timeout))
                .into_inner(),
        );
    apply_rpc_layers(base, &limits, &[])
}

#[sinex_test]
async fn rpc_error_projection_preserves_kind_without_private_context() -> TestResult<()> {
    let err = SinexError::database("SELECT token FROM auth")
        .with_context("operation", "events.query")
        .with_context("path", "/home/sinity/.ssh/id_ed25519")
        .with_source("postgresql://user:pass@localhost failed");

    let (code, public) = sinex_error_to_rpc_code(&err);
    assert_eq!(code, -32810);
    assert_eq!(
        public.kind,
        sinex_primitives::error::SinexErrorKind::Database
    );
    assert_eq!(public.kind_name, "database");
    assert_eq!(public.message, "A database error occurred");
    assert_eq!(
        public.context.get("operation"),
        Some(&"events.query".to_string())
    );
    assert!(!public.context.contains_key("path"));

    let data = rpc_error_data(Uuid::now_v7(), &public, &err);
    let rendered = data.to_string();
    assert!(rendered.contains("database"));
    #[cfg(not(feature = "dev-errors"))]
    {
        assert!(!rendered.contains("id_ed25519"));
        assert!(!rendered.contains("postgresql://"));
        assert!(!rendered.contains("SELECT token"));
    }
    Ok(())
}

#[sinex_test]
async fn parse_cors_origin_values_keeps_valid_entries_and_rejects_invalid_ones()
-> TestResult<()> {
    let origins = parse_cors_origin_values(&[
        "http://localhost:3000".to_string(),
        "bad\norigin".to_string(),
        "https://example.com".to_string(),
    ]);

    let parsed: Vec<_> = origins
        .iter()
        .map(|origin| origin.to_str().expect("valid header value"))
        .collect();

    assert_eq!(parsed, vec!["http://localhost:3000", "https://example.com"]);
    Ok(())
}

#[sinex_test]
async fn parse_cors_origin_values_rejects_all_invalid_entries() -> TestResult<()> {
    let origins = parse_cors_origin_values(&["bad\norigin".to_string(), "\u{7f}".to_string()]);
    assert!(origins.is_empty());
    Ok(())
}

async fn spawn_router(router: Router) -> (SocketAddr, JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        axum::serve(listener, router.into_make_service())
            .await
            .unwrap();
    });
    (addr, handle)
}

#[sinex_test]
async fn concurrency_limit_returns_429() -> TestResult<()> {
    let limits =
        RpcServerLimits::test_limits(1, Duration::from_secs(5), Bytes::from_mebibytes(1));
    let router = build_test_router(limits);
    let (addr, handle) = spawn_router(router).await;
    let client = Client::new();

    let first = {
        let client = client.clone();
        let url = format!("http://{addr}/");
        tokio::spawn(async move {
            client
                .post(&url)
                .header("content-type", "application/json")
                .body("{}")
                .send()
                .await
                .unwrap()
        })
    };

    tokio::time::sleep(Duration::from_millis(10)).await;

    let resp = client
        .post(format!("http://{addr}/"))
        .header("content-type", "application/json")
        .body("{}")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);
    let as_str = resp.text().await.unwrap();
    assert!(as_str.contains("server is busy"));

    first.await.unwrap();
    handle.abort();
    Ok(())
}

#[sinex_test]
async fn timeout_layer_returns_504() -> TestResult<()> {
    let limits =
        RpcServerLimits::test_limits(8, Duration::from_millis(20), Bytes::from_mebibytes(1));
    let router = build_test_router(limits);
    let (addr, handle) = spawn_router(router).await;
    let client = Client::new();

    let resp = client
        .post(format!("http://{addr}/"))
        .header("content-type", "application/json")
        .body("{}")
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::GATEWAY_TIMEOUT);
    let body = resp.text().await.unwrap();
    assert!(body.contains("timeout"));

    handle.abort();
    Ok(())
}

#[sinex_test]
async fn body_limit_returns_413() -> TestResult<()> {
    let limits = RpcServerLimits::test_limits(8, Duration::from_secs(5), Bytes::from_bytes(16));
    let router = build_test_router(limits);
    let big_payload = format!("{{\"payload\":\"{}\"}}", "x".repeat(32));

    let (addr, handle) = spawn_router(router).await;
    let client = Client::new();

    let resp = client
        .post(format!("http://{addr}/"))
        .header("content-type", "application/json")
        .body(big_payload)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);

    handle.abort();
    Ok(())
}

#[sinex_test]
async fn rpc_responses_include_request_id_header() -> TestResult<()> {
    let limits =
        RpcServerLimits::test_limits(4, Duration::from_secs(1), Bytes::from_bytes(1024));
    let router = build_test_router(limits);
    let (addr, handle) = spawn_router(router).await;
    let client = Client::new();

    let resp = client
        .post(format!("http://{addr}/"))
        .header("content-type", "application/json")
        .body("{}")
        .send()
        .await?;

    assert!(
        resp.headers().contains_key("x-request-id"),
        "Gateway RPC responses should include an x-request-id header for structured logging"
    );

    handle.abort();
    Ok(())
}

#[sinex_test]
async fn request_id_for_span_marks_invalid_headers() -> TestResult<()> {
    let request = Request::builder()
        .uri("/")
        .header("x-request-id", HeaderValue::from_bytes(b"\xff")?)
        .body(())?;

    assert_eq!(request_id_for_span(&request), "<invalid x-request-id>");
    Ok(())
}

#[sinex_test]
async fn request_id_for_span_marks_missing_headers_as_unknown() -> TestResult<()> {
    let request = Request::builder().uri("/").body(())?;

    assert_eq!(request_id_for_span(&request), "unknown");
    Ok(())
}

#[sinex_test]
async fn wait_for_background_tasks_rejects_join_failures() -> TestResult<()> {
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let failing = tokio::spawn(async move {
        panic!("metrics task panicked");
    });

    let error = RpcServer::wait_for_background_tasks_with_timeout(
        Some(RpcServer::monitor_background_task(
            "Metrics emission task",
            failing,
            shutdown_rx,
        )),
        None,
        None,
        Duration::from_millis(50),
    )
    .await
    .expect_err("background task join failure must fail shutdown honestly");

    let message = error.to_string();
    assert!(message.contains("Background task shutdown failed"));
    assert!(message.contains("Metrics emission task"));
    drop(shutdown_tx);
    Ok(())
}

#[sinex_test]
async fn monitor_background_task_rejects_early_exit_before_shutdown() -> TestResult<()> {
    let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let completed = tokio::spawn(async move {});

    let error =
        RpcServer::monitor_background_task("Metrics emission task", completed, shutdown_rx)
            .await
            .expect("monitor join should succeed")
            .expect_err(
                "background task that exits before shutdown must be treated as a failure",
            );

    assert!(error.to_string().contains("exited before gateway shutdown"));
    Ok(())
}

#[sinex_test]
async fn monitor_background_task_rejects_dropped_shutdown_channel_without_signal()
-> TestResult<()> {
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let completed = tokio::spawn(async move {});
    drop(shutdown_tx);

    let error = RpcServer::monitor_background_task(
        "SSE subscription bus",
        completed,
        shutdown_rx,
    )
    .await
    .expect("monitor join should succeed")
    .expect_err(
        "background task that exits after shutdown channel drop without a shutdown signal must fail",
    );

    let rendered = error.to_string();
    assert!(
        rendered.contains("exited before gateway shutdown")
            || rendered.contains("shutdown channel closed without a shutdown signal")
    );
    Ok(())
}

#[sinex_test]
async fn monitor_background_task_allows_dropped_shutdown_channel_after_signal() -> TestResult<()>
{
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    shutdown_tx.send(true)?;
    drop(shutdown_tx);
    let completed = tokio::spawn(async move {});

    RpcServer::monitor_background_task("SSE subscription bus", completed, shutdown_rx)
        .await
        .expect("monitor join should succeed")?;

    Ok(())
}

#[sinex_test]
async fn monitor_background_task_retains_pending_handle_after_shutdown_signal() -> TestResult<()>
{
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
    let task = tokio::spawn(async move {
        let _ = release_rx.await;
    });
    let monitor = RpcServer::monitor_background_task("SSE subscription bus", task, shutdown_rx);

    tokio::time::sleep(Duration::from_millis(10)).await;
    shutdown_tx.send(true)?;
    tokio::time::sleep(Duration::from_millis(10)).await;
    let _ = release_tx.send(());

    RpcServer::wait_for_background_tasks_with_timeout(
        None,
        None,
        Some(monitor),
        Duration::from_millis(200),
    )
    .await?;

    Ok(())
}

#[sinex_test]
async fn tcp_binding_defaults_to_loopback() -> TestResult<()> {
    let _guard = ENV_LOCK.lock().await;
    clear_tcp_env();

    let addr = BindAddress::from_config(&gateway_config_from_env())?;
    match addr {
        BindAddress::Tcp { host, port } => {
            assert_eq!(&host, "127.0.0.1");
            assert_eq!(port, 9999);
        }
    }

    Ok(())
}

#[sinex_test]
async fn mtls_configuration_is_loaded() -> TestResult<()> {
    let _guard = ENV_LOCK.lock().await;

    unsafe {
        std::env::set_var("SINEX_API_TLS_CERT", "cert.pem");
        std::env::set_var("SINEX_API_TLS_KEY", "key.pem");
        std::env::set_var("SINEX_API_TLS_CLIENT_CA", "ca.pem");
    }

    let (cert, key, ca) = tls_paths_from_config(&gateway_config_from_env())?;
    assert_eq!(cert, "cert.pem");
    assert_eq!(key, "key.pem");
    assert_eq!(ca, Some("ca.pem".to_string()));

    unsafe { std::env::remove_var("SINEX_API_TLS_CLIENT_CA") };
    let (_, _, ca) = tls_paths_from_config(&gateway_config_from_env())?;
    assert!(ca.is_none());

    Ok(())
}

#[sinex_test]
async fn tcp_binding_env_opt_in_respected() -> TestResult<()> {
    let _guard = ENV_LOCK.lock().await;
    clear_tcp_env();
    unsafe { std::env::set_var("SINEX_API_TCP_LISTEN", "127.0.0.1:7777") };

    let addr = BindAddress::from_config(&gateway_config_from_env())?;

    let BindAddress::Tcp { host, port } = addr;
    assert_eq!(&host, "127.0.0.1");
    assert_eq!(port, 7777);

    clear_tcp_env();
    Ok(())
}

#[sinex_test]
async fn tcp_binding_cli_override_wins() -> TestResult<()> {
    let _guard = ENV_LOCK.lock().await;
    clear_tcp_env();
    unsafe { std::env::set_var("SINEX_API_TCP_LISTEN", "127.0.0.1:7777") };

    let addr = BindAddress::from_config(&GatewayConfig {
        tcp_listen: "127.0.0.1:8888".to_string(),
        ..gateway_config_from_env()
    })?;

    let BindAddress::Tcp { host, port } = addr;
    assert_eq!(&host, "127.0.0.1");
    assert_eq!(port, 8888);

    clear_tcp_env();
    Ok(())
}

#[sinex_test]
async fn tcp_binding_invalid_cli_spec_rejected() -> TestResult<()> {
    let _guard = ENV_LOCK.lock().await;
    clear_tcp_env();

    let result = BindAddress::from_config(&GatewayConfig {
        tcp_listen: "not-a-valid-spec".to_string(),
        ..gateway_config_from_env()
    });

    assert!(result.is_err());
    Ok(())
}

#[sinex_test]
async fn mtls_required_for_non_loopback_bind() -> TestResult<()> {
    let remote = BindAddress::Tcp {
        host: "0.0.0.0".to_string(),
        port: 8080,
    };
    assert!(require_mtls_for_remote(&remote, false, None).is_err());
    assert!(require_mtls_for_remote(&remote, false, Some("ca.pem")).is_ok());

    let loopback = BindAddress::Tcp {
        host: "127.0.0.1".to_string(),
        port: 8080,
    };
    assert!(require_mtls_for_remote(&loopback, false, None).is_ok());
    Ok(())
}

#[sinex_test]
async fn mtls_override_requires_client_ca() -> TestResult<()> {
    let loopback = BindAddress::Tcp {
        host: "127.0.0.1".to_string(),
        port: 8080,
    };
    assert!(require_mtls_for_remote(&loopback, true, None).is_err());
    assert!(require_mtls_for_remote(&loopback, true, Some("ca.pem")).is_ok());
    Ok(())
}

#[sinex_test]
async fn tls_paths_must_be_set_for_tcp() -> TestResult<()> {
    // Ensure env is clean
    let _guard = ENV_LOCK.lock().await;
    unsafe {
        std::env::remove_var("SINEX_API_TLS_CERT");
        std::env::remove_var("SINEX_API_TLS_KEY");
    }

    assert!(
        tls_paths_from_config(&gateway_config_from_env()).is_err(),
        "TLS paths should be required when binding TCP"
    );
    Ok(())
}

#[sinex_test]
async fn gateway_auth_blocks_missing_token() -> TestResult<()> {
    let auth = GatewayAuth::with_test_token("secret");
    let headers = HeaderMap::new();
    assert!(matches!(auth.verify(&headers), Err(AuthError::Missing)));
    Ok(())
}

#[sinex_test]
async fn gateway_auth_accepts_bearer_header() -> TestResult<()> {
    let auth = GatewayAuth::with_test_token("secret");
    let headers = bearer_headers("secret");
    assert!(auth.verify(&headers).is_ok());
    Ok(())
}

#[cfg(unix)]
#[sinex_test]
async fn token_env_rejects_non_utf8_values() -> TestResult<()> {
    use std::ffi::OsString;
    use std::os::unix::ffi::OsStringExt;

    let _guard = ENV_LOCK.lock().await;
    clear_auth_env();
    unsafe {
        std::env::set_var(
            "SINEX_API_TOKEN",
            OsString::from_vec(vec![0x73, 0x80, 0x65]),
        );
    }

    let error =
        read_token_and_path_from_env().expect_err("non-UTF-8 token env should be rejected");
    assert!(error.to_string().contains("SINEX_API_TOKEN"));

    clear_auth_env();
    Ok(())
}

#[sinex_test]
async fn gateway_auth_reloads_token_file_without_restart() -> TestResult<()> {
    let _guard = ENV_LOCK.lock().await;
    clear_auth_env();

    let temp_dir = tempfile::tempdir()?;
    let token_file = temp_dir.path().join("gateway-token");
    std::fs::write(&token_file, "initial-token")?;
    unsafe {
        std::env::set_var(
            "SINEX_API_TOKEN_FILE",
            token_file
                .to_str()
                .expect("token path should be valid UTF-8"),
        );
    }

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let auth = GatewayAuth::from_config(&gateway_config_from_env())?
        .start_file_watcher(shutdown_rx)
        .await?;

    assert!(auth.verify(&bearer_headers("initial-token")).is_ok());
    assert!(matches!(
        auth.verify(&bearer_headers("wrong-token")),
        Err(AuthError::Invalid)
    ));

    std::fs::write(&token_file, "rotated-token")?;

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let old_rejected = matches!(
                auth.verify(&bearer_headers("initial-token")),
                Err(AuthError::Invalid)
            );
            let new_accepted = auth.verify(&bearer_headers("rotated-token")).is_ok();

            if old_rejected && new_accepted {
                break;
            }

            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("token watcher should reload updated token");

    let _ = shutdown_tx.send(true);
    clear_auth_env();
    Ok(())
}

#[sinex_test]
async fn gateway_auth_keeps_last_token_when_reload_file_is_empty() -> TestResult<()> {
    let auth = GatewayAuth::with_test_token("initial-token");
    let temp_dir = tempfile::tempdir()?;
    let token_file = temp_dir.path().join("gateway-token");
    std::fs::write(&token_file, " \n\t")?;

    GatewayAuth::reload_token_from_path(&auth.token, &token_file);

    assert!(auth.verify(&bearer_headers("initial-token")).is_ok());
    assert!(matches!(
        auth.verify(&bearer_headers("wrong-token")),
        Err(AuthError::Invalid)
    ));
    Ok(())
}

#[sinex_test]
async fn send_token_watcher_ready_reports_dropped_receiver() -> TestResult<()> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    drop(rx);
    let mut ready_tx = Some(tx);

    assert!(!super::send_token_watcher_ready(
        &mut ready_tx,
        Ok(()),
        "ready"
    ));
    assert!(ready_tx.is_none());
    Ok(())
}

#[sinex_test]
async fn send_token_watcher_ready_delivers_result() -> TestResult<()> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    let mut ready_tx = Some(tx);

    assert!(super::send_token_watcher_ready(
        &mut ready_tx,
        Ok(()),
        "ready"
    ));
    assert!(ready_tx.is_none());
    rx.await??;
    Ok(())
}
