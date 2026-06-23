mod support;

use async_nats::jetstream;
use sinex_workspace_tests::built_binary;
use std::process::Stdio;
use std::time::Duration;
use support::{TEST_TOKEN, start_test_gateway};
use tokio::io::AsyncReadExt;
use tokio::process::Command;
use xtask::sandbox::{TestContext, sinex_test, timing::Timeouts};

async fn assert_process_stays_running(
    child: &mut tokio::process::Child,
    duration: Duration,
    label: &str,
) -> color_eyre::Result<()> {
    match tokio::time::timeout(duration, child.wait()).await {
        Err(_) => Ok(()),
        Ok(Ok(status)) => {
            let mut stderr = String::new();
            if let Some(mut stream) = child.stderr.take()
                && let Err(error) = stream.read_to_string(&mut stderr).await
            {
                stderr = format!("<failed to read child stderr: {error}>");
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
        .arg(TEST_TOKEN)
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
    let url = gw.rpc_url();

    let output = sinexctl_rpc_output(&url, &["ops", "dlq", "list", "-f", "json"]).await;

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
    let url = gw.base_url();

    let mut child = Command::new(built_binary("sinexctl"))
        .arg("--token")
        .arg(TEST_TOKEN)
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
    if let Err(error) = child.kill().await {
        if let Some(status) = child.try_wait()? {
            return Err(color_eyre::eyre::eyre!(
                "`sinexctl events watch` exited before cleanup after the stability window: {status}"
            ));
        }
        return Err(color_eyre::eyre::eyre!(
            "failed to stop `sinexctl events watch` after stability assertion: {error}"
        ));
    }
    child.wait().await?;
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
    let url = gw.rpc_url();

    let output = sinexctl_rpc_output(&url, &["ops", "dlq", "peek", "-n", "1", "-f", "json"]).await;

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
