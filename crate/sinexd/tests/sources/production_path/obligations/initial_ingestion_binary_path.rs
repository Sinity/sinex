use sinex_primitives::privacy::{RuntimePrivateModeState, save_private_mode_state};
use sinex_primitives::temporal::Timestamp;
use xtask::sandbox::prelude::*;

const WEECHAT_MESSAGE: &str = "hello from source host binary";
const WEECHAT_SUPPRESSED_MESSAGE: &str = "private mode should suppress this";
const WEECHAT_MALFORMED_STATE_MESSAGE: &str =
    "malformed private-mode state should suppress this";
const WEECHAT_OUT_OF_SCOPE_MESSAGE: &str = "desktop scope should not suppress terminal";
const BASH_SUPPRESSED_COMMAND: &str = "echo private mode should suppress bash history";

fn weechat_runtime_config(log_path: &std::path::Path) -> serde_json::Value {
    serde_json::json!({
        "path": log_path,
        "skip_empty": true,
    })
}

fn append_only_runtime_config(path: &std::path::Path) -> serde_json::Value {
    serde_json::json!({
        "path": path,
        "skip_empty": true,
    })
}

struct SourceDriverHostIngestStack {
    event_engine: TestEventEngineHandle,
    _work_dir: tempfile::TempDir,
}

impl SourceDriverHostIngestStack {
    async fn start(ctx: &Sandbox) -> TestResult<Self> {
        ctx.reset_database_slot().await?;

        let nats = ctx.nats_handle()?;
        let work_dir = tempfile::tempdir()?;
        let event_engine = start_test_event_engine_with_config(
            TestEventEngineConfig {
                nats: nats.connection_config(),
                database_url: ctx.database_url().to_string(),
                work_dir: Some(work_dir.path().to_path_buf()),
                namespace: Some(ctx.pipeline_namespace().prefix().to_string()),
                consumer_fetch_max_messages: 32,
                consumer_fetch_timeout_ms: 50,
                database_pool_size: 4,
                reject_initial_replay: false,
            },
            Some(ctx),
        )
        .await?;

        Ok(Self {
            event_engine,
            _work_dir: work_dir,
        })
    }

    async fn shutdown(mut self) -> TestResult<()> {
        self.event_engine.stop().await?;
        Ok(())
    }
}

async fn write_weechat_fixture(log_path: &std::path::Path, message: &str) -> TestResult<()> {
    tokio::fs::write(
        log_path,
        format!("2024-01-15 14:23:45\tsinity\t{message}\n"),
    )
    .await?;
    Ok(())
}

async fn write_bash_fixture(history_path: &std::path::Path, command: &str) -> TestResult<()> {
    tokio::fs::write(history_path, format!("{command}\n")).await?;
    Ok(())
}

async fn count_irc_messages(ctx: &Sandbox, message: &str) -> TestResult<i64> {
    let count = sqlx::query_scalar(
        r"
        SELECT COUNT(*)::bigint
        FROM core.events
        WHERE source = 'irc'
          AND event_type = 'irc.message'
          AND payload->>'message' = $1
        ",
    )
    .bind(message)
    .fetch_one(ctx.pool())
    .await?;
    Ok(count)
}

async fn count_bash_commands(ctx: &Sandbox, command: &str) -> TestResult<i64> {
    let count = sqlx::query_scalar(
        r"
        SELECT COUNT(*)::bigint
        FROM core.events
        WHERE source = 'shell.history'
          AND event_type = 'command.imported'
          AND payload->>'command' = $1
        ",
    )
    .bind(command)
    .fetch_one(ctx.pool())
    .await?;
    Ok(count)
}

async fn run_weechat_scan(
    ctx: &Sandbox,
    tempdir: &tempfile::TempDir,
    case: &str,
    runtime_config: serde_json::Value,
) -> TestResult<String> {
    let worker_dir = tempdir.path().join(format!("worker-{case}"));
    tokio::fs::create_dir_all(&worker_dir).await?;

    let mut config = TestSourceDriverConfig::new("weechat");
    config.nats = ctx.nats_handle()?.connection_config();
    config.database_url = ctx.database_url().to_string();
    config.namespace = Some(ctx.pipeline_namespace().prefix().to_string());
    config.work_dir = Some(worker_dir);
    config.runtime_config = Some(runtime_config.to_string());

    let output = run_test_source_scan(config, &[], Some(ctx)).await?;
    Ok(output.stdout)
}

async fn run_bash_scan(
    ctx: &Sandbox,
    tempdir: &tempfile::TempDir,
    case: &str,
    runtime_config: serde_json::Value,
) -> TestResult<String> {
    let worker_dir = tempdir.path().join(format!("worker-{case}"));
    tokio::fs::create_dir_all(&worker_dir).await?;

    let mut config = TestSourceDriverConfig::new("terminal.bash-history");
    config.nats = ctx.nats_handle()?.connection_config();
    config.database_url = ctx.database_url().to_string();
    config.namespace = Some(ctx.pipeline_namespace().prefix().to_string());
    config.work_dir = Some(worker_dir);
    config.runtime_config = Some(runtime_config.to_string());

    let output = run_test_source_scan(config, &[], Some(ctx)).await?;
    Ok(output.stdout)
}

/// Proves the real `sinexd scan-source` path for adapter-backed
/// source contracts: binary launch, adapter config, parser, NATS publish,
/// event_engine persistence, DB payload visibility, and private-mode policy.
#[sinex_test(timeout = 120)]
async fn source_driver_host_scan_private_mode_matrix(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let stack = SourceDriverHostIngestStack::start(&ctx).await?;
    let tempdir = tempfile::tempdir()?;

    let baseline_log_path = tempdir.path().join("weechat.log");
    write_weechat_fixture(&baseline_log_path, WEECHAT_MESSAGE).await?;

    let private_log_path = tempdir.path().join("weechat-private.log");
    write_weechat_fixture(&private_log_path, WEECHAT_SUPPRESSED_MESSAGE).await?;
    let private_state_dir = tempdir.path().join("weechat-state");
    save_private_mode_state(
        &private_state_dir,
        &RuntimePrivateModeState::enabled_by(
            "test-operator",
            vec!["weechat".to_string()],
            Timestamp::UNIX_EPOCH,
        ),
    )?;
    let mut private_config = weechat_runtime_config(&private_log_path);
    private_config["private_mode_state_dir"] =
        serde_json::Value::String(private_state_dir.display().to_string());

    let malformed_log_path = tempdir.path().join("weechat-malformed-state.log");
    write_weechat_fixture(&malformed_log_path, WEECHAT_MALFORMED_STATE_MESSAGE).await?;
    let malformed_state_dir = tempdir.path().join("malformed-state");
    let private_mode_path =
        sinex_primitives::privacy::private_mode_state_path(&malformed_state_dir);
    let private_mode_parent = private_mode_path
        .parent()
        .ok_or_else(|| color_eyre::eyre::eyre!("private-mode state path must have parent"))?;
    tokio::fs::create_dir_all(private_mode_parent).await?;
    tokio::fs::write(&private_mode_path, b"{not-json").await?;
    let mut malformed_config = weechat_runtime_config(&malformed_log_path);
    malformed_config["private_mode_state_dir"] =
        serde_json::Value::String(malformed_state_dir.display().to_string());

    let history_path = tempdir.path().join(".bash_history");
    write_bash_fixture(&history_path, BASH_SUPPRESSED_COMMAND).await?;
    let bash_state_dir = tempdir.path().join("terminal-state");
    save_private_mode_state(
        &bash_state_dir,
        &RuntimePrivateModeState::enabled_by(
            "test-operator",
            vec!["terminal".to_string()],
            Timestamp::UNIX_EPOCH,
        ),
    )?;
    let mut bash_config = append_only_runtime_config(&history_path);
    bash_config["private_mode_state_dir"] =
        serde_json::Value::String(bash_state_dir.display().to_string());

    let out_of_scope_log_path = tempdir.path().join("weechat-out-of-scope.log");
    write_weechat_fixture(&out_of_scope_log_path, WEECHAT_OUT_OF_SCOPE_MESSAGE).await?;
    let out_of_scope_state_dir = tempdir.path().join("desktop-state");
    save_private_mode_state(
        &out_of_scope_state_dir,
        &RuntimePrivateModeState::enabled_by(
            "test-operator",
            vec!["desktop".to_string()],
            Timestamp::UNIX_EPOCH,
        ),
    )?;
    let mut out_of_scope_config = weechat_runtime_config(&out_of_scope_log_path);
    out_of_scope_config["private_mode_state_dir"] =
        serde_json::Value::String(out_of_scope_state_dir.display().to_string());

    let (baseline_output, private_output, malformed_output, bash_output, out_of_scope_output) =
        tokio::try_join!(
            run_weechat_scan(
                &ctx,
                &tempdir,
                "baseline",
                weechat_runtime_config(&baseline_log_path),
            ),
            run_weechat_scan(&ctx, &tempdir, "weechat-private", private_config),
            run_weechat_scan(&ctx, &tempdir, "weechat-malformed", malformed_config),
            run_bash_scan(&ctx, &tempdir, "bash-private", bash_config),
            run_weechat_scan(&ctx, &tempdir, "weechat-out-of-scope", out_of_scope_config),
        )?;

    ctx.assert("source host scan processed one event").that(
        baseline_output.contains("Events processed: 1"),
        "scan output should report one processed event",
    )?;
    WaitHelpers::wait_for_condition(
        || async {
            let count = count_irc_messages(&ctx, WEECHAT_MESSAGE)
                .await
                .map_err(|error| sqlx::Error::Protocol(error.to_string()))?;
            Ok::<bool, sqlx::Error>(count >= 1)
        },
        10,
    )
    .await?;

    ctx.assert("private-mode scan suppressed all events").that(
        private_output.contains("Events processed: 0"),
        "scan output should report no processed events when source private mode is active",
    )?;
    let count = count_irc_messages(&ctx, WEECHAT_SUPPRESSED_MESSAGE).await?;
    ctx.assert("suppressed private-mode scan persisted no irc.message events")
        .eq(&count, &0)?;

    ctx.assert("malformed private-mode state suppressed all events")
        .that(
            malformed_output.contains("Events processed: 0"),
            "scan output should report no processed events when private-mode state is unreadable",
        )?;
    let count = count_irc_messages(&ctx, WEECHAT_MALFORMED_STATE_MESSAGE).await?;
    ctx.assert("fail-closed malformed state persisted no irc.message events")
        .eq(&count, &0)?;

    ctx.assert("private-mode bash scan suppressed all events").that(
        bash_output.contains("Events processed: 0"),
        "scan output should report no processed events when terminal private mode is active",
    )?;
    let count = count_bash_commands(&ctx, BASH_SUPPRESSED_COMMAND).await?;
    ctx.assert("suppressed private-mode scan persisted no shell.history events")
        .eq(&count, &0)?;

    ctx.assert("out-of-scope private mode preserves acquisition").that(
        out_of_scope_output.contains("Events processed: 1"),
        "scan output should report one processed event when private mode is scoped elsewhere",
    )?;
    WaitHelpers::wait_for_condition(
        || async {
            let count = count_irc_messages(&ctx, WEECHAT_OUT_OF_SCOPE_MESSAGE)
                .await
                .map_err(|error| sqlx::Error::Protocol(error.to_string()))?;
            Ok::<bool, sqlx::Error>(count >= 1)
        },
        10,
    )
    .await?;

    stack.shutdown().await?;
    Ok(())
}
