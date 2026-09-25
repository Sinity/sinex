use sinex_db::{DbPoolExt, SourceMaterialRecord};
use sinex_primitives::Id;
use sinex_primitives::events::EventPayload;
use sinex_primitives::events::payloads::{ActuationStatus, HyprlandWorkspaceSwitchedPayload};
use sinex_primitives::nats::{JetStreamEventLane, JetStreamTopology};
use sinex_primitives::rpc::instructions::HyprlandWorkspaceSwitchRequest;
use sinexd::api::config::GatewayConfig;
use sinexd::api::handlers::handle_hyprland_workspace_switch;
use sinexd::api::rpc_server::RpcAuthContext;
use sinexd::api::service_container::ServiceContainer;
use sinexd::event_engine::{IngestEventValidator, JetStreamConsumer};
use std::sync::Arc;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::UnixListener,
};
use xtask::sandbox::prelude::*;

#[path = "common/mod.rs"]
mod common;
#[path = "../event_engine/support.rs"]
mod event_engine_support;

struct ConfirmedConsumerGuard(tokio::task::JoinHandle<sinexd::event_engine::EventEngineResult<()>>);

impl Drop for ConfirmedConsumerGuard {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn services_for_test(
    ctx: TestContext,
) -> TestResult<(
    TestContext,
    ServiceContainer,
    EnvGuard,
    ConfirmedConsumerGuard,
)> {
    let ctx = ctx.with_nats().dedicated().await?;
    services_for_nats_test_context(ctx).await
}

async fn services_for_nats_test_context(
    ctx: TestContext,
) -> TestResult<(
    TestContext,
    ServiceContainer,
    EnvGuard,
    ConfirmedConsumerGuard,
)> {
    let mut env_guard = EnvGuard::new();
    env_guard.set("SINEX_NATS_URL", ctx.nats_handle()?.client_url());

    let namespace = ctx.pipeline_namespace().prefix().to_string();
    let mut config = GatewayConfig::load()?;
    config.database_url = ctx.database_url().to_string();
    config.nats.url = ctx.nats_handle()?.client_url().to_string();
    config.namespace = Some(namespace.clone());
    let services = ServiceContainer::new(&config).await?;

    // Seed a registered material before the consumer starts. Individual tests
    // may register additional observation materials before invoking the handler.
    ctx.create_source_material(Some("hyprland-instruction-consumer-seed"))
        .await?;

    let nats = ctx.nats_handle()?;
    let nats_client = ctx.nats_client();
    let env = ctx.env().clone();
    let base_stream = env.nats_stream_name_with_namespace(Some(&namespace), "SINEX_RAW_EVENTS");
    let topology = JetStreamTopology::for_lane(
        &env,
        base_stream,
        ctx.pipeline_namespace()
            .consumer_name("hyprland-instructions"),
        Some(&namespace),
        JetStreamEventLane::Activity,
    );
    let js = nats.jetstream_with_client(nats_client.clone());
    let consumer = JetStreamConsumer::new(
        nats_client,
        ctx.pool().clone(),
        Arc::new(tokio::sync::RwLock::new(IngestEventValidator::new(false))),
        topology.clone(),
    );
    let handle =
        event_engine_support::spawn_consumer_and_wait_ready(&ctx, &js, &topology, consumer).await?;

    Ok((ctx, services, env_guard, ConfirmedConsumerGuard(handle)))
}

#[sinex_test]
async fn hyprland_workspace_switch_records_unavailable_without_observation(
    ctx: TestContext,
) -> TestResult<()> {
    let (ctx, services, _env, _consumer) = services_for_test(ctx).await?;
    common::seed_rpc_handler_product_declarations(ctx.pool()).await?;
    let auth = RpcAuthContext::system();

    let response = handle_hyprland_workspace_switch(
        &services,
        HyprlandWorkspaceSwitchRequest {
            instruction_id: None,
            desired_workspace_id: 4,
            deadline: None,
            dry_run: false,
            command_socket_path: None,
        },
        &auth,
    )
    .await?;

    assert_eq!(response.instruction.desired_workspace_id, 4);
    assert_eq!(response.instruction.actor_id, auth.actor_id());
    assert!(!response.observation_ready);
    assert_eq!(response.current_workspace_id, None);
    assert_eq!(response.command_socket_response, None);
    assert_eq!(response.attempt.status, ActuationStatus::Unavailable);
    assert!(response.attempt.command_summary.command.is_none());

    let material = ctx
        .pool()
        .source_materials()
        .get_by_id(Id::<SourceMaterialRecord>::from_uuid(
            response.material_id.to_uuid(),
        ))
        .await?
        .ok_or_else(|| color_eyre::eyre::eyre!("instruction material not persisted"))?;
    assert_eq!(material.staged_by.as_deref(), Some(auth.actor_id()));
    assert_eq!(
        material.metadata["instruction_target"],
        "desktop.hyprland.workspace"
    );

    let instruction_event_id = response
        .instruction_event
        .id
        .ok_or_else(|| color_eyre::eyre::eyre!("instruction event missing id"))?;
    let attempt_parent = response
        .attempt_event
        .get_source_event_ids()
        .and_then(|parents| parents.first().copied())
        .ok_or_else(|| color_eyre::eyre::eyre!("attempt event missing instruction parent"))?;
    assert_eq!(attempt_parent, instruction_event_id);
    Ok(())
}

#[sinex_test]
async fn hyprland_workspace_switch_dispatches_typed_command_when_observation_ready(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    common::seed_rpc_handler_product_declarations(ctx.pool()).await?;
    let material_id = ctx
        .create_source_material(Some("hyprland-workspace-observation"))
        .await?;
    let observed = HyprlandWorkspaceSwitchedPayload {
        from_workspace_id: Some(1),
        to_workspace_id: 2,
        workspace_name: None,
        monitor_id: Some(0),
        active_window_id: None,
    }
    .from_material(material_id)
    .build()?;
    ctx.pool().events().insert(observed).await?;
    let (_ctx, services, _env, _consumer) = services_for_nats_test_context(ctx).await?;

    let temp = tempfile::Builder::new()
        .prefix("sinex-hypr-")
        .tempdir_in("/tmp")?;
    let socket_path = temp.path().join("hyprland-command.sock");
    let listener = UnixListener::bind(&socket_path)?;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let mut request = String::new();
        stream.read_to_string(&mut request).await?;
        stream.write_all(b"ok").await?;
        Ok::<_, std::io::Error>(request)
    });

    let response = handle_hyprland_workspace_switch(
        &services,
        HyprlandWorkspaceSwitchRequest {
            instruction_id: None,
            desired_workspace_id: 4,
            deadline: None,
            dry_run: false,
            command_socket_path: Some(socket_path.display().to_string()),
        },
        &RpcAuthContext::system(),
    )
    .await?;
    let request = server.await??;

    assert_eq!(request, "dispatch workspace 4");
    assert!(response.observation_ready);
    assert_eq!(response.current_workspace_id, Some(2));
    assert_eq!(response.attempt.status, ActuationStatus::Accepted);
    assert_eq!(response.command_socket_response.as_deref(), Some("ok"));
    assert_eq!(
        response
            .attempt
            .command_summary
            .command
            .as_ref()
            .map(|command| command.workspace_id),
        Some(4)
    );
    Ok(())
}

#[sinex_test]
async fn hyprland_workspace_switch_fresh_instruction_id_retry_does_not_redispatch(
    ctx: TestContext,
) -> TestResult<()> {
    let (ctx, services, _env, _consumer) = services_for_test(ctx).await?;
    common::seed_rpc_handler_product_declarations(ctx.pool()).await?;
    let material_id = ctx
        .create_source_material(Some("hyprland-workspace-observation-retry"))
        .await?;
    let observed = HyprlandWorkspaceSwitchedPayload {
        from_workspace_id: Some(1),
        to_workspace_id: 2,
        workspace_name: None,
        monitor_id: Some(0),
        active_window_id: None,
    }
    .from_material(material_id)
    .build()?;
    ctx.pool().events().insert(observed).await?;

    let temp = tempfile::Builder::new()
        .prefix("sinex-hypr-retry-")
        .tempdir_in("/tmp")?;
    let socket_path = temp.path().join("hyprland-command.sock");
    let listener = UnixListener::bind(&socket_path)?;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let mut request = String::new();
        stream.read_to_string(&mut request).await?;
        stream.write_all(b"ok").await?;
        Ok::<_, std::io::Error>(request)
    });

    let first = handle_hyprland_workspace_switch(
        &services,
        HyprlandWorkspaceSwitchRequest {
            instruction_id: None,
            desired_workspace_id: 4,
            deadline: None,
            dry_run: false,
            command_socket_path: Some(socket_path.display().to_string()),
        },
        &RpcAuthContext::system(),
    )
    .await?;
    server.await??;
    assert_eq!(first.attempt.status, ActuationStatus::Accepted);

    // The gateway retry path creates a fresh instruction ID. The source to
    // target transition key still identifies the same side effect.
    let retry_temp = tempfile::Builder::new()
        .prefix("sinex-hypr-retry2-")
        .tempdir_in("/tmp")?;
    let retry_socket_path = retry_temp.path().join("hyprland-command.sock");
    let retry_listener = UnixListener::bind(&retry_socket_path)?;
    let second = handle_hyprland_workspace_switch(
        &services,
        HyprlandWorkspaceSwitchRequest {
            instruction_id: None,
            desired_workspace_id: 4,
            deadline: None,
            dry_run: false,
            command_socket_path: Some(retry_socket_path.display().to_string()),
        },
        &RpcAuthContext::system(),
    )
    .await?;

    assert_eq!(
        second.attempt.status,
        ActuationStatus::Rejected,
        "retrying with a fresh instruction_id must not re-dispatch"
    );
    assert_ne!(
        first.instruction.instruction_id,
        second.instruction.instruction_id
    );
    assert_eq!(second.command_socket_response, None);
    assert!(
        tokio::time::timeout(
            std::time::Duration::from_millis(100),
            retry_listener.accept()
        )
        .await
        .is_err(),
        "same-ID retry must not connect to the second actuator socket"
    );

    Ok(())
}

#[sinex_test]
async fn hyprland_workspace_switch_rejects_duplicate_active_idempotency_key(
    ctx: TestContext,
) -> TestResult<()> {
    let (ctx, services, _env, _consumer) = services_for_test(ctx).await?;
    common::seed_rpc_handler_product_declarations(ctx.pool()).await?;
    let material_id = ctx
        .create_source_material(Some("hyprland-workspace-observation"))
        .await?;
    let observed = HyprlandWorkspaceSwitchedPayload {
        from_workspace_id: Some(1),
        to_workspace_id: 2,
        workspace_name: None,
        monitor_id: Some(0),
        active_window_id: None,
    }
    .from_material(material_id)
    .build()?;
    ctx.pool().events().insert(observed).await?;

    let temp = tempfile::Builder::new()
        .prefix("sinex-hypr-")
        .tempdir_in("/tmp")?;
    let socket_path = temp.path().join("hyprland-command.sock");
    let listener = UnixListener::bind(&socket_path)?;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let mut request = String::new();
        stream.read_to_string(&mut request).await?;
        stream.write_all(b"ok").await?;
        Ok::<_, std::io::Error>(request)
    });

    let first = handle_hyprland_workspace_switch(
        &services,
        HyprlandWorkspaceSwitchRequest {
            instruction_id: None,
            desired_workspace_id: 4,
            deadline: None,
            dry_run: false,
            command_socket_path: Some(socket_path.display().to_string()),
        },
        &RpcAuthContext::system(),
    )
    .await?;
    let request = server.await??;

    assert_eq!(request, "dispatch workspace 4");
    assert_eq!(first.attempt.status, ActuationStatus::Accepted);

    let second = handle_hyprland_workspace_switch(
        &services,
        HyprlandWorkspaceSwitchRequest {
            instruction_id: None,
            desired_workspace_id: 4,
            deadline: None,
            dry_run: false,
            command_socket_path: Some("/tmp/should-not-be-opened.sock".to_string()),
        },
        &RpcAuthContext::system(),
    )
    .await?;

    assert_eq!(second.attempt.status, ActuationStatus::Rejected);
    assert!(second.attempt.command_summary.command.is_none());
    assert_eq!(second.command_socket_response, None);
    assert!(
        second
            .attempt
            .error
            .as_deref()
            .is_some_and(|error| error.contains("duplicate active workspace instruction"))
    );

    let persisted_attempt = ctx
        .pool()
        .events()
        .get_by_id(
            second
                .attempt_event
                .id
                .ok_or_else(|| color_eyre::eyre::eyre!("attempt event missing id"))?,
        )
        .await?
        .ok_or_else(|| color_eyre::eyre::eyre!("attempt event not persisted"))?;
    assert_eq!(
        persisted_attempt.payload["status"],
        serde_json::json!("rejected")
    );
    assert!(persisted_attempt.payload["command_summary"]["command"].is_null());

    Ok(())
}

#[sinex_test]
async fn hyprland_workspace_switch_noops_when_already_satisfied(
    ctx: TestContext,
) -> TestResult<()> {
    let (ctx, services, _env, _consumer) = services_for_test(ctx).await?;
    common::seed_rpc_handler_product_declarations(ctx.pool()).await?;
    let material_id = ctx
        .create_source_material(Some("hyprland-workspace-observation"))
        .await?;
    let observed = HyprlandWorkspaceSwitchedPayload {
        from_workspace_id: Some(1),
        to_workspace_id: 4,
        workspace_name: None,
        monitor_id: Some(0),
        active_window_id: None,
    }
    .from_material(material_id)
    .build()?;
    ctx.pool().events().insert(observed).await?;

    let response = handle_hyprland_workspace_switch(
        &services,
        HyprlandWorkspaceSwitchRequest {
            instruction_id: None,
            desired_workspace_id: 4,
            deadline: None,
            dry_run: false,
            command_socket_path: None,
        },
        &RpcAuthContext::system(),
    )
    .await?;

    assert!(response.observation_ready);
    assert_eq!(response.current_workspace_id, Some(4));
    assert_eq!(
        response.attempt.status,
        ActuationStatus::NoopAlreadySatisfied
    );
    assert!(response.attempt.command_summary.command.is_none());
    assert_eq!(response.command_socket_response, None);

    let persisted_attempt = ctx
        .pool()
        .events()
        .get_by_id(
            response
                .attempt_event
                .id
                .ok_or_else(|| color_eyre::eyre::eyre!("attempt event missing id"))?,
        )
        .await?
        .ok_or_else(|| color_eyre::eyre::eyre!("attempt event not persisted"))?;
    assert_eq!(
        persisted_attempt.payload["status"],
        serde_json::json!("noop_already_satisfied")
    );
    assert!(persisted_attempt.payload["command_summary"]["command"].is_null());
    Ok(())
}

#[sinex_test]
async fn hyprland_workspace_switch_dry_run_records_plan_without_dispatch(
    ctx: TestContext,
) -> TestResult<()> {
    let (ctx, services, _env, _consumer) = services_for_test(ctx).await?;
    common::seed_rpc_handler_product_declarations(ctx.pool()).await?;
    let material_id = ctx
        .create_source_material(Some("hyprland-workspace-observation"))
        .await?;
    let observed = HyprlandWorkspaceSwitchedPayload {
        from_workspace_id: Some(1),
        to_workspace_id: 2,
        workspace_name: None,
        monitor_id: Some(0),
        active_window_id: None,
    }
    .from_material(material_id)
    .build()?;
    ctx.pool().events().insert(observed).await?;

    let response = handle_hyprland_workspace_switch(
        &services,
        HyprlandWorkspaceSwitchRequest {
            instruction_id: None,
            desired_workspace_id: 4,
            deadline: None,
            dry_run: true,
            command_socket_path: None,
        },
        &RpcAuthContext::system(),
    )
    .await?;

    assert!(response.observation_ready);
    assert_eq!(response.current_workspace_id, Some(2));
    assert_eq!(response.attempt.status, ActuationStatus::DryRun);
    assert_eq!(response.command_socket_response, None);
    assert_eq!(
        response
            .attempt
            .command_summary
            .command
            .as_ref()
            .map(|command| command.workspace_id),
        Some(4)
    );

    let persisted_attempt = ctx
        .pool()
        .events()
        .get_by_id(
            response
                .attempt_event
                .id
                .ok_or_else(|| color_eyre::eyre::eyre!("attempt event missing id"))?,
        )
        .await?
        .ok_or_else(|| color_eyre::eyre::eyre!("attempt event not persisted"))?;
    assert_eq!(
        persisted_attempt.payload["status"],
        serde_json::json!("dry_run")
    );
    assert_eq!(
        persisted_attempt.payload["command_summary"]["command"]["workspace_id"],
        serde_json::json!(4)
    );
    assert!(persisted_attempt.payload["error"].is_null());
    Ok(())
}

#[sinex_test]
async fn hyprland_workspace_switch_records_failed_attempt_on_socket_rejection(
    ctx: TestContext,
) -> TestResult<()> {
    let (ctx, services, _env, _consumer) = services_for_test(ctx).await?;
    common::seed_rpc_handler_product_declarations(ctx.pool()).await?;
    let material_id = ctx
        .create_source_material(Some("hyprland-workspace-observation"))
        .await?;
    let observed = HyprlandWorkspaceSwitchedPayload {
        from_workspace_id: Some(1),
        to_workspace_id: 2,
        workspace_name: None,
        monitor_id: Some(0),
        active_window_id: None,
    }
    .from_material(material_id)
    .build()?;
    ctx.pool().events().insert(observed).await?;

    let temp = tempfile::Builder::new()
        .prefix("sinex-hypr-")
        .tempdir_in("/tmp")?;
    let socket_path = temp.path().join("hyprland-command.sock");
    let listener = UnixListener::bind(&socket_path)?;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let mut request = String::new();
        stream.read_to_string(&mut request).await?;
        stream.write_all(b"unknown dispatcher").await?;
        Ok::<_, std::io::Error>(request)
    });

    let response = handle_hyprland_workspace_switch(
        &services,
        HyprlandWorkspaceSwitchRequest {
            instruction_id: None,
            desired_workspace_id: 4,
            deadline: None,
            dry_run: false,
            command_socket_path: Some(socket_path.display().to_string()),
        },
        &RpcAuthContext::system(),
    )
    .await?;
    let request = server.await??;

    assert_eq!(request, "dispatch workspace 4");
    assert_eq!(response.attempt.status, ActuationStatus::Attempted);
    assert_eq!(
        response.command_socket_response.as_deref(),
        Some("unknown dispatcher")
    );
    assert!(
        response
            .attempt
            .error
            .as_deref()
            .is_some_and(|error| error.contains("rejected workspace dispatch"))
    );

    let persisted_attempt = ctx
        .pool()
        .events()
        .get_by_id(
            response
                .attempt_event
                .id
                .ok_or_else(|| color_eyre::eyre::eyre!("attempt event missing id"))?,
        )
        .await?
        .ok_or_else(|| color_eyre::eyre::eyre!("attempt event not persisted"))?;
    assert_eq!(
        persisted_attempt.payload["status"],
        serde_json::json!("failed")
    );
    assert!(
        persisted_attempt.payload["error"]
            .as_str()
            .is_some_and(|error| error.contains("rejected workspace dispatch"))
    );
    Ok(())
}

#[sinex_serial_test]
async fn hyprland_workspace_switch_resolves_default_socket_from_runtime_env(
    ctx: TestContext,
) -> TestResult<()> {
    let (ctx, services, _env, _consumer) = services_for_test(ctx).await?;
    common::seed_rpc_handler_product_declarations(ctx.pool()).await?;
    let material_id = ctx
        .create_source_material(Some("hyprland-workspace-observation"))
        .await?;
    let observed = HyprlandWorkspaceSwitchedPayload {
        from_workspace_id: Some(1),
        to_workspace_id: 2,
        workspace_name: None,
        monitor_id: Some(0),
        active_window_id: None,
    }
    .from_material(material_id)
    .build()?;
    ctx.pool().events().insert(observed).await?;

    let temp = tempfile::Builder::new()
        .prefix("sinex-hypr-runtime-")
        .tempdir_in("/tmp")?;
    let instance_dir = temp.path().join("hypr/instance-1");
    std::fs::create_dir_all(&instance_dir)?;
    let socket_path = instance_dir.join(".socket.sock");
    let listener = UnixListener::bind(&socket_path)?;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let mut request = String::new();
        stream.read_to_string(&mut request).await?;
        stream.write_all(b"ok").await?;
        Ok::<_, std::io::Error>(request)
    });

    let mut env = EnvGuard::new();
    env.set("XDG_RUNTIME_DIR", temp.path().display().to_string());
    env.set("HYPRLAND_INSTANCE_SIGNATURE", "instance-1");

    let response = handle_hyprland_workspace_switch(
        &services,
        HyprlandWorkspaceSwitchRequest {
            instruction_id: None,
            desired_workspace_id: 4,
            deadline: None,
            dry_run: false,
            command_socket_path: None,
        },
        &RpcAuthContext::system(),
    )
    .await?;
    let request = server.await??;

    assert_eq!(request, "dispatch workspace 4");
    assert_eq!(response.attempt.status, ActuationStatus::Accepted);
    assert_eq!(response.command_socket_response.as_deref(), Some("ok"));
    Ok(())
}
