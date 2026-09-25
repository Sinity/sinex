use std::path::Path;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use sinex_primitives::events::payloads::instruction::{HyprlandDispatch, HyprlandWorkspaceCommand};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::UnixListener,
};
use xtask::sandbox::prelude::*;

use super::*;

#[sinex_test]
async fn instruction_socket_path_rejects_invalid_path_before_connection() -> TestResult<()> {
    let invalid = Path::new("/tmp/hyprland\0.sock");
    assert!(validate_instruction_socket_path(invalid).is_err());

    let valid = Path::new("/run/user/1000/hyprland.sock");
    assert_eq!(validate_instruction_socket_path(valid)?.as_path(), valid);
    Ok(())
}

#[sinex_test]
async fn workspace_dispatch_persists_attempt_before_socket_side_effect() -> TestResult<()> {
    let temp = tempfile::Builder::new()
        .prefix("sinex-instruction-receipt-")
        .tempdir_in("/tmp")?;
    let socket_path = temp.path().join("hyprland-command.sock");
    let listener = UnixListener::bind(&socket_path)?;
    let receipt_persisted = Arc::new(AtomicBool::new(false));
    let server_receipt_persisted = Arc::clone(&receipt_persisted);
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let mut request = String::new();
        stream.read_to_string(&mut request).await?;
        if !server_receipt_persisted.load(Ordering::SeqCst) {
            return Err(std::io::Error::other(
                "command bytes were sent before the dispatch receipt",
            ));
        }
        stream.write_all(b"ok").await?;
        Ok::<_, std::io::Error>(request)
    });
    let command = HyprlandWorkspaceCommand {
        dispatch: HyprlandDispatch::Workspace,
        workspace_id: 5,
    };

    let connection = connect_hyprland_command_socket(&socket_path).await?;
    let response = dispatch_with_attempt_receipt(connection, &command, || async {
        receipt_persisted.store(true, Ordering::SeqCst);
        Ok(())
    })
    .await?;
    assert_eq!(server.await??, "dispatch workspace 5");
    assert_eq!(response.response, "ok");
    Ok(())
}

#[sinex_test]
async fn transition_idempotency_key_blocks_same_transition_but_allows_later_transition()
-> TestResult<()> {
    let first = workspace_transition_idempotency_key(Some(2), 5);
    let retry = workspace_transition_idempotency_key(Some(2), 5);
    let later = workspace_transition_idempotency_key(Some(5), 2);

    assert_eq!(first, retry);
    assert_ne!(first, later);
    Ok(())
}

#[sinex_test]
async fn instruction_authority_reflects_caller_origin() -> TestResult<()> {
    use sinex_primitives::events::payloads::instruction::InstructionAuthorityClass;

    let (system_authority, system_policy) = instruction_authority(&RpcAuthContext::system());
    assert_eq!(system_authority, InstructionAuthorityClass::OperatorDirect);
    assert_eq!(
        system_policy,
        "desktop.hyprland.workspace-switch.operator-direct"
    );

    let token_auth = RpcAuthContext::from_token("sinex-test-token:write")
        .expect("write token context should be valid");
    let (token_authority, token_policy) = instruction_authority(&token_auth);
    assert_eq!(token_authority, InstructionAuthorityClass::UserDeclared);
    assert_eq!(
        token_policy,
        "desktop.hyprland.workspace-switch.authenticated-user"
    );
    Ok(())
}

#[sinex_test]
async fn stale_workspace_observation_is_not_ready_for_actuation(
    ctx: xtask::sandbox::TestContext,
) -> TestResult<()> {
    let material_id = ctx
        .create_source_material(Some("hyprland-stale-workspace-observation"))
        .await?;
    let stale_at = Timestamp::from_unix_timestamp(Timestamp::now().unix_timestamp() - 86_400)
        .expect("one day ago is a valid timestamp");
    let observed = HyprlandWorkspaceSwitchedPayload {
        from_workspace_id: Some(1),
        to_workspace_id: 2,
        workspace_name: None,
        monitor_id: Some(0),
        active_window_id: None,
    }
    .from_material(material_id)
    .at_time(stale_at)
    .build()?;
    ctx.pool().events().insert(observed).await?;

    assert!(
        latest_hyprland_workspace(ctx.pool()).await?.is_none(),
        "a stale source observation must not authorize workspace actuation"
    );
    Ok(())
}

#[sinex_test]
async fn workspace_dispatch_lock_serializes_same_transition(
    ctx: xtask::sandbox::TestContext,
) -> TestResult<()> {
    let key = format!(
        "test-workspace-dispatch:{}",
        sinex_primitives::Uuid::now_v7()
    );
    let first = acquire_workspace_dispatch_lock(ctx.pool(), &key).await?;
    let pool = ctx.pool().clone();
    let second_key = key.clone();
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let second = tokio::spawn(async move {
        let _ = started_tx.send(());
        acquire_workspace_dispatch_lock(&pool, &second_key).await
    });

    started_rx.await?;
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(!second.is_finished(), "same-key advisory lock must wait");

    first.commit().await?;
    let second = second.await??;
    drop(second);
    Ok(())
}

#[sinex_test]
async fn active_dispatch_receipt_blocks_retry_with_same_instruction_id(
    ctx: xtask::sandbox::TestContext,
) -> TestResult<()> {
    crate::automata::product_declarations::reconcile_declarations(
        ctx.pool(),
        "instructions-rpc",
        INSTRUCTIONS_OUTPUT_DECLARATIONS,
    )
    .await?;
    crate::automata::product_declarations::reconcile_declarations(
        ctx.pool(),
        "instruction-reconciler",
        crate::automata::instruction_reconciler::INSTRUCTION_RECONCILER_OUTPUT_DECLARATIONS,
    )
    .await?;

    let auth = RpcAuthContext::system();
    let instruction_id = sinex_primitives::Uuid::now_v7();
    let desired_workspace_id = 100 + (instruction_id.as_u128() % 100_000) as i32;
    let mut instruction = DesktopWorkspaceSwitchInstructionPayload::hyprland_operator_direct(
        instruction_id,
        desired_workspace_id,
        auth.actor_id(),
        None,
        false,
    )?;
    instruction.idempotency_key =
        workspace_transition_idempotency_key(Some(desired_workspace_id - 1), desired_workspace_id);

    let material_id = register_instruction_material(ctx.pool(), &auth, &instruction).await?;
    let instruction_event = instruction
        .clone()
        .from_material(Id::<SourceMaterial>::from_uuid(material_id))
        .at_time(Timestamp::now())
        .build()?;
    let inserted = ctx.pool().events().insert(instruction_event).await?;
    let instruction_event_id = inserted
        .id
        .expect("instruction event insert must return its event id");
    let attempt = plan_hyprland_workspace_switch(
        &instruction,
        Some(desired_workspace_id - 1),
        true,
        Timestamp::now(),
    );
    assert_eq!(attempt.status, ActuationStatus::Attempted);

    let mut transaction =
        acquire_workspace_dispatch_lock(ctx.pool(), &instruction.idempotency_key).await?;
    persist_dispatch_receipt(
        ctx.pool(),
        &mut transaction,
        instruction_event_id,
        &instruction,
        &attempt,
    )
    .await?;
    transaction.commit().await?;

    let timed_out_at = Timestamp::now();
    let timed_out =
        sinex_primitives::events::payloads::instruction::InstructionExpectationStatusPayload {
            instruction_id,
            desired_event_source: instruction.desired_event_source.clone(),
            desired_event_type: instruction.desired_event_type.clone(),
            status:
                sinex_primitives::events::payloads::instruction::InstructionExpectationStatus::TimedOut,
            matched_event_ids: Vec::new(),
            caveat: Some("test timeout does not prove the dispatch stopped".to_string()),
            evaluated_at: timed_out_at,
        };
    let mut timed_out_event = timed_out
        .from_parents([instruction_event_id])?
        .at_time(timed_out_at)
        .build()?;
    let declaration =
        crate::automata::instruction_reconciler::INSTRUCTION_RECONCILER_OUTPUT_DECLARATIONS[0];
    timed_out_event.product_class = Some(declaration.product_class);
    timed_out_event.claim_support = Some(declaration.default_support.instantiate(1, 0, 1, 0));
    timed_out_event.derivation_declaration_id = Some(declaration.declaration_id.to_string());
    ctx.pool().events().insert(timed_out_event).await?;

    // A terminal expectation timeout does not prove the command stopped. The
    // retry uses the same caller-provided instruction ID, and the durable
    // dispatch receipt must continue to block it.
    let mut retry_transaction =
        acquire_workspace_dispatch_lock(ctx.pool(), &instruction.idempotency_key).await?;
    let active =
        active_hyprland_workspace_instruction(&mut retry_transaction, &instruction).await?;
    assert!(
        active.is_some(),
        "same-ID retry must remain blocked after expectation timeout"
    );
    retry_transaction.rollback().await?;
    Ok(())
}

#[sinex_test]
async fn active_dispatch_receipt_releases_after_post_receipt_fulfillment_without_attempt(
    ctx: xtask::sandbox::TestContext,
) -> TestResult<()> {
    crate::automata::product_declarations::reconcile_declarations(
        ctx.pool(),
        "instructions-rpc",
        INSTRUCTIONS_OUTPUT_DECLARATIONS,
    )
    .await?;
    crate::automata::product_declarations::reconcile_declarations(
        ctx.pool(),
        "instruction-reconciler",
        crate::automata::instruction_reconciler::INSTRUCTION_RECONCILER_OUTPUT_DECLARATIONS,
    )
    .await?;

    let auth = RpcAuthContext::system();
    let instruction_id = sinex_primitives::Uuid::now_v7();
    let desired_workspace_id = 100 + (instruction_id.as_u128() % 100_000) as i32;
    let mut instruction = DesktopWorkspaceSwitchInstructionPayload::hyprland_operator_direct(
        instruction_id,
        desired_workspace_id,
        auth.actor_id(),
        None,
        false,
    )?;
    instruction.idempotency_key =
        workspace_transition_idempotency_key(Some(desired_workspace_id - 1), desired_workspace_id);

    let material_id = register_instruction_material(ctx.pool(), &auth, &instruction).await?;
    let instruction_event = instruction
        .clone()
        .from_material(Id::<SourceMaterial>::from_uuid(material_id))
        .at_time(Timestamp::now())
        .build()?;
    let inserted_instruction = ctx.pool().events().insert(instruction_event).await?;
    let instruction_event_id = inserted_instruction
        .id
        .expect("instruction event insert must return its event id");
    let attempt = plan_hyprland_workspace_switch(
        &instruction,
        Some(desired_workspace_id - 1),
        true,
        Timestamp::now(),
    );
    assert_eq!(attempt.status, ActuationStatus::Attempted);

    let mut transaction =
        acquire_workspace_dispatch_lock(ctx.pool(), &instruction.idempotency_key).await?;
    persist_dispatch_receipt(
        ctx.pool(),
        &mut transaction,
        instruction_event_id,
        &instruction,
        &attempt,
    )
    .await?;
    transaction.commit().await?;

    let receipt_at = sqlx::query_scalar::<_, Timestamp>(
        "SELECT ts_orig FROM core.events \
         WHERE source = 'runtime.instruction' \
           AND event_type = 'actuation.dispatch_started' \
           AND payload->>'instruction_id' = $1 \
         ORDER BY ts_coided DESC LIMIT 1",
    )
    .bind(instruction_id.to_string())
    .fetch_one(ctx.pool())
    .await?;

    let observation_material_id = ctx
        .create_source_material(Some("hyprland-post-receipt-fulfillment-observation"))
        .await?;
    let observed_at = Timestamp::now();
    assert!(observed_at >= receipt_at);
    let observation = HyprlandWorkspaceSwitchedPayload {
        from_workspace_id: Some(desired_workspace_id - 1),
        to_workspace_id: desired_workspace_id,
        workspace_name: None,
        monitor_id: Some(0),
        active_window_id: None,
    }
    .from_material(observation_material_id)
    .at_time(observed_at)
    .build()?;
    let inserted_observation = ctx.pool().events().insert(observation).await?;
    let observation_event_id = inserted_observation
        .id
        .expect("workspace observation insert must return its event id");

    let expectation =
        sinex_primitives::events::payloads::instruction::InstructionExpectationStatusPayload {
            instruction_id,
            desired_event_source: instruction.desired_event_source.clone(),
            desired_event_type: instruction.desired_event_type.clone(),
            status:
                sinex_primitives::events::payloads::instruction::InstructionExpectationStatus::Fulfilled,
            matched_event_ids: vec![*observation_event_id.as_uuid()],
            caveat: None,
            evaluated_at: observed_at,
        };
    let mut expectation_event = expectation
        .from_parents([instruction_event_id, observation_event_id])?
        .at_time(observed_at)
        .build()?;
    let declaration =
        crate::automata::instruction_reconciler::INSTRUCTION_RECONCILER_OUTPUT_DECLARATIONS[0];
    expectation_event.product_class = Some(declaration.product_class);
    expectation_event.claim_support = Some(declaration.default_support.instantiate(2, 0, 1, 0));
    expectation_event.derivation_declaration_id = Some(declaration.declaration_id.to_string());
    ctx.pool().events().insert(expectation_event).await?;

    let attempt_exists = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS (SELECT 1 FROM core.events \
         WHERE source = 'runtime.instruction' \
           AND event_type = 'actuation.attempted' \
           AND $1 = ANY(source_event_ids))",
    )
    .bind(*instruction_event_id.as_uuid())
    .fetch_one(ctx.pool())
    .await?;
    assert!(
        !attempt_exists,
        "the release proof must not require an attempt event"
    );

    let mut retry_transaction =
        acquire_workspace_dispatch_lock(ctx.pool(), &instruction.idempotency_key).await?;
    let active =
        active_hyprland_workspace_instruction(&mut retry_transaction, &instruction).await?;
    assert!(
        active.is_none(),
        "a matched observation after dispatch receipt proves fulfillment even when the reply/attempt was lost"
    );
    retry_transaction.rollback().await?;
    Ok(())
}
