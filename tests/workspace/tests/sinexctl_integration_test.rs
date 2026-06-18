use async_nats::jetstream;
use sinex_workspace_tests::built_binary;
use sinexd::api::{ServiceContainer, rpc_server};
use std::net::TcpListener;
use std::process::Stdio;
use std::time::Duration;
use tempfile::NamedTempFile;
use tokio::io::AsyncReadExt;
use tokio::process::Command;
use tokio::sync::watch;
use xtask::sandbox::{TestContext, sinex_test, timing::Timeouts};

fn reserve_port() -> color_eyre::Result<u16> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let port = listener.local_addr()?.port();
    drop(listener);
    Ok(port)
}

/// Wait until the TCP port is accepting connections, up to `timeout`.
async fn wait_for_port(port: u16, timeout: Duration) -> color_eyre::Result<()> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        match tokio::net::TcpStream::connect(format!("127.0.0.1:{port}")).await {
            Ok(_) => return Ok(()),
            Err(_) if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(e) => {
                return Err(color_eyre::eyre::eyre!(
                    "Gateway port {port} not ready after {timeout:?}: {e}"
                ));
            }
        }
    }
}

async fn assert_process_stays_running(
    child: &mut tokio::process::Child,
    duration: Duration,
    label: &str,
) -> color_eyre::Result<()> {
    match tokio::time::timeout(duration, child.wait()).await {
        Err(_) => Ok(()),
        Ok(Ok(status)) => {
            let mut stderr = String::new();
            if let Some(mut stream) = child.stderr.take() {
                stream.read_to_string(&mut stderr).await.ok();
            }
            Err(color_eyre::eyre::eyre!(
                "{label} exited before remaining stable for {duration:?}: {status}\nstderr: {stderr}"
            ))
        }
        Ok(Err(error)) => Err(color_eyre::eyre::eyre!(
            "failed while waiting for {label} process state: {error}"
        )),
    }
}

/// Resources held for the lifetime of a test gateway.
///
/// The caller MUST hold onto this struct — dropping `shutdown_tx` stops the
/// server, and dropping the temp files removes the TLS certificates.
struct TestGateway {
    port: u16,
    _env: xtask::sandbox::EnvGuard,
    _shutdown_tx: watch::Sender<bool>,
    handle: tokio::task::JoinHandle<()>,
    _cert_file: NamedTempFile,
    _key_file: NamedTempFile,
}

/// Start a test gateway with auto-generated TLS certificates.
async fn start_test_gateway(ctx: &TestContext) -> color_eyre::Result<TestGateway> {
    // Generate self-signed TLS certificates for the gateway.
    let subject_alt_names = vec!["localhost".to_string(), "127.0.0.1".to_string()];
    let cert = rcgen::generate_simple_self_signed(subject_alt_names)?;
    let cert_file = NamedTempFile::new()?;
    let key_file = NamedTempFile::new()?;
    tokio::fs::write(cert_file.path(), cert.cert.pem()).await?;
    tokio::fs::write(key_file.path(), cert.key_pair.serialize_pem()).await?;

    let mut env = xtask::sandbox::EnvGuard::with_keys(&[
        "SINEX_API_TLS_CERT",
        "SINEX_API_TLS_KEY",
        "SINEX_API_TLS_CLIENT_CA",
        "SINEX_API_TOKEN",
        "SINEX_NATS_URL",
    ]);
    env.set(
        "SINEX_API_TLS_CERT",
        cert_file.path().to_string_lossy().to_string(),
    );
    env.set(
        "SINEX_API_TLS_KEY",
        key_file.path().to_string_lossy().to_string(),
    );
    env.clear("SINEX_API_TLS_CLIENT_CA");
    env.set("SINEX_API_TOKEN", "test-token:admin");
    env.set("SINEX_NATS_URL", ctx.nats_handle()?.client_url());

    let port = reserve_port()?;
    let mut config =
        sinexd::api::config::GatewayConfig::load_with_database_url(ctx.database_url().to_string())?;
    config.tcp_listen = format!("127.0.0.1:{port}");
    config.rpc_rate_limit_enabled = false;
    let services = ServiceContainer::new(&config).await?;
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut server_handle = tokio::spawn({
        let services = services.clone();
        let config = config.clone();
        async move {
            if let Err(e) = rpc_server::run(&config, services, shutdown_rx).await {
                eprintln!("Gateway startup failed: {e:#}");
            }
        }
    });

    // Race port readiness against server exit — if the server errors during
    // setup, we detect it immediately instead of waiting the full timeout.
    let port_timeout = Duration::from_secs(Timeouts::STANDARD);
    tokio::select! {
        result = wait_for_port(port, port_timeout) => {
            result?;
        }
        join_result = &mut server_handle => {
            // Server task exited before port was ready — it failed during setup
            match join_result {
                Ok(()) => return Err(color_eyre::eyre::eyre!(
                    "Gateway server exited before binding port {port} (check stderr for details)"
                )),
                Err(e) => return Err(color_eyre::eyre::eyre!(
                    "Gateway server task panicked: {e}"
                )),
            }
        }
    }

    Ok(TestGateway {
        port,
        _env: env,
        _shutdown_tx: shutdown_tx,
        handle: server_handle,
        _cert_file: cert_file,
        _key_file: key_file,
    })
}

async fn ensure_dlq_stream(ctx: &TestContext) -> color_eyre::Result<jetstream::stream::Stream> {
    let js = ctx.jetstream().await?;
    let env = ctx.env();
    let stream_name = env.nats_stream_name("SINEX_RAW_EVENTS_DLQ");
    let dlq_subject = env.nats_subject("events.dlq.>");
    let stream = js
        .get_or_create_stream(jetstream::stream::Config {
            name: stream_name,
            subjects: vec![dlq_subject],
            retention: jetstream::stream::RetentionPolicy::Limits,
            max_messages: 1000,
            storage: jetstream::stream::StorageType::Memory,
            allow_direct: true,
            ..Default::default()
        })
        .await?;
    Ok(stream)
}

async fn publish_dlq_message(
    ctx: &TestContext,
    event_id: &str,
    payload: &str,
    retry_count: u32,
) -> color_eyre::Result<()> {
    let client = ctx.nats_handle()?.connect().await?;
    let env = ctx.env();
    let original_subject = env.nats_subject("events.raw.workspace-test");
    let mut headers = async_nats::HeaderMap::new();
    headers.insert("Retry-Count", retry_count.to_string().as_str());
    headers.insert("Original-Subject", original_subject.as_str());
    headers.insert("Event-Id", event_id);

    let subject = env.nats_subject(&format!("events.dlq.workspace-test.{event_id}"));
    client
        .publish_with_headers(subject, headers, payload.to_owned().into())
        .await?;
    client.flush().await?;
    Ok(())
}

async fn wait_for_dlq_messages(ctx: &TestContext, expected: u64) -> color_eyre::Result<()> {
    let js = ctx.jetstream().await?;
    let stream_name = ctx.env().nats_stream_name("SINEX_RAW_EVENTS_DLQ");
    let mut stream = js.get_stream(&stream_name).await?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(Timeouts::SHORT);
    loop {
        let info = stream.info().await?;
        if info.state.messages >= expected {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(color_eyre::eyre::eyre!(
                "DLQ stream {stream_name} had {} message(s), expected at least {expected}",
                info.state.messages
            ));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn parse_json_stdout(output: &std::process::Output, label: &str) -> serde_json::Value {
    let stdout = String::from_utf8_lossy(&output.stdout);
    serde_json::from_str(&stdout).unwrap_or_else(|e| {
        panic!(
            "failed to parse {label} JSON output: {e}\nstdout: {stdout}\nstderr: {}",
            String::from_utf8_lossy(&output.stderr)
        )
    })
}

async fn sinexctl_rpc_output(url: &str, args: &[&str]) -> std::process::Output {
    let mut command = Command::new(built_binary("sinexctl"));
    command
        .arg("--token")
        .arg("test-token:admin")
        .arg("--insecure")
        .arg("--timeout")
        .arg("5")
        .arg("--rpc-url")
        .arg(url);
    for arg in args {
        command.arg(arg);
    }
    command
        .output()
        .await
        .expect("sinexctl binary should be executable")
}

#[sinex_test]
async fn sinexctl_dlq_list_command_reports_entries(ctx: TestContext) -> color_eyre::Result<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    ensure_dlq_stream(&ctx).await?;
    publish_dlq_message(
        &ctx,
        "00000000-0000-7000-8000-000000000101",
        r#"{"event_id":"00000000-0000-7000-8000-000000000101"}"#,
        0,
    )
    .await?;
    publish_dlq_message(
        &ctx,
        "00000000-0000-7000-8000-000000000102",
        r#"{"event_id":"00000000-0000-7000-8000-000000000102"}"#,
        2,
    )
    .await?;
    wait_for_dlq_messages(&ctx, 2).await?;

    let gw = start_test_gateway(&ctx).await?;
    let url = format!("https://127.0.0.1:{}/rpc", gw.port);

    let output = sinexctl_rpc_output(&url, &["ops", "dlq", "list", "-f", "json"]).await;

    gw.handle.abort();

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "`sinexctl ops dlq list` should succeed, got: stdout={}, stderr={}",
        String::from_utf8_lossy(&output.stdout),
        stderr,
    );
    let response = parse_json_stdout(&output, "ops dlq list");
    assert_eq!(response["total_messages"].as_u64(), Some(2));
    assert!(
        response["total_bytes"]
            .as_u64()
            .is_some_and(|bytes| bytes > 0),
        "DLQ list should report stored bytes: {response}"
    );
    assert_eq!(response["first_seq"].as_u64(), Some(1));
    assert_eq!(response["last_seq"].as_u64(), Some(2));
    Ok(())
}

#[sinex_test]
async fn sinexctl_watch_command_streams_events(ctx: TestContext) -> color_eyre::Result<()> {
    let ctx = ctx.with_nats().shared().await?;
    // `sinexctl events watch` is an infinite polling loop — it never exits.
    // We spawn it as a child process and verify it starts successfully
    // (connects to the gateway), then kill it after a brief window.
    let gw = start_test_gateway(&ctx).await?;
    let url = format!("https://127.0.0.1:{}", gw.port);

    let mut child = Command::new(built_binary("sinexctl"))
        .arg("--token")
        .arg("test-token:admin")
        .arg("--insecure")
        .arg("--rpc-url")
        .arg(&url)
        .arg("events")
        .arg("watch")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("sinexctl binary should be executable");

    assert_process_stays_running(
        &mut child,
        Duration::from_secs(2),
        "`sinexctl events watch`",
    )
    .await?;
    child.kill().await.ok();
    let _ = child.wait().await;

    gw.handle.abort();
    Ok(())
}

#[sinex_test]
async fn sinexctl_dlq_peek_command_reports_entries(ctx: TestContext) -> color_eyre::Result<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    ensure_dlq_stream(&ctx).await?;
    let event_id = "00000000-0000-7000-8000-000000000201";
    publish_dlq_message(
        &ctx,
        event_id,
        r#"{"event_id":"00000000-0000-7000-8000-000000000201","marker":"workspace-dlq-peek"}"#,
        3,
    )
    .await?;
    wait_for_dlq_messages(&ctx, 1).await?;

    let gw = start_test_gateway(&ctx).await?;
    let url = format!("https://127.0.0.1:{}/rpc", gw.port);

    let output =
        sinexctl_rpc_output(&url, &["ops", "dlq", "peek", "-n", "1", "-f", "json"]).await;

    gw.handle.abort();

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "`sinexctl ops dlq peek` should succeed, got: stdout={}, stderr={}",
        String::from_utf8_lossy(&output.stdout),
        stderr,
    );
    let message = parse_json_stdout(&output, "ops dlq peek");
    assert!(
        message["subject"]
            .as_str()
            .is_some_and(|subject| subject.ends_with(event_id)),
        "peeked DLQ subject should end with event id {event_id}: {message}"
    );
    assert_eq!(message["retry_count"].as_u64(), Some(3));
    assert!(
        message["original_subject"]
            .as_str()
            .is_some_and(|subject| subject.ends_with("events.raw.workspace-test")),
        "peeked DLQ message should preserve original subject: {message}"
    );
    assert!(
        message["payload_preview"]
            .as_str()
            .is_some_and(|payload| payload.contains("workspace-dlq-peek")),
        "peeked DLQ message should include payload preview marker: {message}"
    );
    Ok(())
}
