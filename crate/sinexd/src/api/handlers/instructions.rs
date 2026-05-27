//! Instruction/actuator-loop RPC handlers.

use serde_json::json;
use sinex_db::DbPoolExt;
use sinex_db::repositories::SourceMaterial as DbSourceMaterial;
use sinex_node_sdk::{
    dispatch_hyprland_workspace_command, probe_hyprland_command_socket,
    resolve_hyprland_command_socket_path,
};
use sinex_primitives::events::payloads::{
    ActuationAttemptPayload, ActuationStatus, DesktopWorkspaceSwitchInstructionPayload,
    HyprlandWorkspaceSwitchedPayload, plan_hyprland_workspace_switch,
};
use sinex_primitives::events::{Event, EventPayload, SourceMaterial};
use sinex_primitives::rpc::instructions::{
    HyprlandWorkspaceSwitchRequest, HyprlandWorkspaceSwitchResponse,
};
use sinex_primitives::{Id, JsonValue, Result, SinexError, Timestamp, Uuid};
use sqlx::PgPool;

use crate::api::rpc_server::RpcAuthContext;

pub async fn handle_hyprland_workspace_switch(
    pool: &PgPool,
    req: HyprlandWorkspaceSwitchRequest,
    auth: &RpcAuthContext,
) -> Result<HyprlandWorkspaceSwitchResponse> {
    let instruction_id = req.instruction_id.unwrap_or_else(Uuid::now_v7);
    let observed_at = Timestamp::now();
    let instruction = DesktopWorkspaceSwitchInstructionPayload::hyprland_operator_direct(
        instruction_id,
        req.desired_workspace_id,
        auth.actor_id(),
        req.deadline,
        req.dry_run,
    )?;
    let material_id = register_instruction_material(pool, auth, &instruction).await?;
    let instruction_event = instruction
        .clone()
        .from_material(Id::<SourceMaterial>::from_uuid(material_id))
        .at_time(observed_at)
        .build()?;
    let inserted_instruction = pool.events().insert(instruction_event).await?;
    let instruction_event_id = inserted_instruction.id.ok_or_else(|| {
        SinexError::invalid_state(
            "instructions.hyprland.workspace_switch: persisted instruction event missing id",
        )
    })?;

    let current_workspace_id = latest_hyprland_workspace(pool).await?;
    let observation_ready = current_workspace_id.is_some();
    let mut attempt = plan_hyprland_workspace_switch(
        &instruction,
        current_workspace_id,
        observation_ready,
        Timestamp::now(),
    );
    let mut command_socket_response = None;

    if attempt.status == ActuationStatus::Attempted
        && let Some(active_instruction_id) =
            active_hyprland_workspace_instruction(pool, &instruction).await?
    {
        attempt.status = ActuationStatus::Rejected;
        attempt.command_summary.command = None;
        attempt.error = Some(format!(
            "duplicate active workspace instruction with idempotency key {} is already pending observation: {active_instruction_id}",
            instruction.idempotency_key
        ));
        return persist_attempt(
            pool,
            PendingInstructionAttempt {
                instruction,
                instruction_event: inserted_instruction,
                attempt,
                material_id,
                observation_ready,
                current_workspace_id,
                command_socket_response,
                instruction_event_id,
            },
        )
        .await;
    }

    if attempt.status == ActuationStatus::Attempted
        && let Some(command) = attempt.command_summary.command.clone()
    {
        let Some(socket_path) =
            resolve_hyprland_command_socket_path(req.command_socket_path.as_deref())
        else {
            attempt.status = ActuationStatus::Unavailable;
            attempt.error = Some(
                "Hyprland command socket path is required for live workspace dispatch; pass command_socket_path or set XDG_RUNTIME_DIR and HYPRLAND_INSTANCE_SIGNATURE".to_string(),
            );
            return persist_attempt(
                pool,
                PendingInstructionAttempt {
                    instruction,
                    instruction_event: inserted_instruction,
                    attempt,
                    material_id,
                    observation_ready,
                    current_workspace_id,
                    command_socket_response,
                    instruction_event_id,
                },
            )
            .await;
        };

        let probe = probe_hyprland_command_socket(&socket_path).await;
        if probe.available {
            match dispatch_hyprland_workspace_command(&socket_path, &command).await {
                Ok(response) => {
                    let socket_response = response.response;
                    if socket_response.trim() != "ok" {
                        attempt.status = ActuationStatus::Failed;
                        attempt.error = Some(format!(
                            "Hyprland command socket rejected workspace dispatch: {socket_response}"
                        ));
                    }
                    command_socket_response = Some(socket_response);
                }
                Err(error) => {
                    attempt.status = ActuationStatus::Failed;
                    attempt.error = Some(error.to_string());
                }
            }
        } else {
            attempt.status = ActuationStatus::Unavailable;
            attempt.error = probe.caveat;
        }
    }

    persist_attempt(
        pool,
        PendingInstructionAttempt {
            instruction,
            instruction_event: inserted_instruction,
            attempt,
            material_id,
            observation_ready,
            current_workspace_id,
            command_socket_response,
            instruction_event_id,
        },
    )
    .await
}

struct PendingInstructionAttempt {
    instruction: DesktopWorkspaceSwitchInstructionPayload,
    instruction_event: Event<JsonValue>,
    attempt: ActuationAttemptPayload,
    material_id: Uuid,
    observation_ready: bool,
    current_workspace_id: Option<i32>,
    command_socket_response: Option<String>,
    instruction_event_id: Id<Event<JsonValue>>,
}

async fn persist_attempt(
    pool: &PgPool,
    pending: PendingInstructionAttempt,
) -> Result<HyprlandWorkspaceSwitchResponse> {
    let attempt_event = pending
        .attempt
        .clone()
        .from_parents([pending.instruction_event_id])?
        .at_time(pending.attempt.attempted_at)
        .build()?;
    let inserted_attempt = pool.events().insert(attempt_event).await?;
    let _attempt_event_id = inserted_attempt.id.ok_or_else(|| {
        SinexError::invalid_state(
            "instructions.hyprland.workspace_switch: persisted attempt event missing id",
        )
    })?;

    Ok(HyprlandWorkspaceSwitchResponse {
        instruction: pending.instruction,
        instruction_event: pending.instruction_event,
        attempt: pending.attempt,
        attempt_event: inserted_attempt,
        material_id: Id::<SourceMaterial>::from_uuid(pending.material_id),
        observation_ready: pending.observation_ready,
        current_workspace_id: pending.current_workspace_id,
        command_socket_response: pending.command_socket_response,
    })
}

async fn latest_hyprland_workspace(pool: &PgPool) -> Result<Option<i32>> {
    let row = sqlx::query!(
        r#"
        SELECT payload
        FROM core.events
        WHERE source = 'wm.hyprland'
          AND event_type = 'workspace.switched'
        ORDER BY ts_orig DESC, id DESC
        LIMIT 1
        "#
    )
    .fetch_optional(pool)
    .await
    .map_err(|error| {
        SinexError::database("failed to query latest Hyprland workspace observation")
            .with_std_error(&error)
    })?;

    let Some(row) = row else {
        return Ok(None);
    };
    let payload: HyprlandWorkspaceSwitchedPayload =
        serde_json::from_value(row.payload).map_err(|error| {
            SinexError::serialization("latest Hyprland workspace observation payload is invalid")
                .with_std_error(&error)
        })?;
    Ok(Some(payload.to_workspace_id))
}

async fn active_hyprland_workspace_instruction(
    pool: &PgPool,
    instruction: &DesktopWorkspaceSwitchInstructionPayload,
) -> Result<Option<Uuid>> {
    sqlx::query_scalar!(
        r#"
        SELECT i.id as "id!: Uuid"
        FROM core.events i
        WHERE i.source = 'runtime.instruction'
          AND i.event_type = 'desktop.workspace.switch_requested'
          AND i.payload->>'idempotency_key' = $1
          AND i.payload->>'instruction_id' <> $2
          AND COALESCE((i.payload->>'dry_run')::boolean, false) = false
          AND EXISTS (
              SELECT 1
              FROM core.events a
              WHERE a.source = 'runtime.instruction'
                AND a.event_type = 'actuation.attempted'
                AND i.id = ANY(a.source_event_ids)
                AND a.payload->>'status' = 'attempted'
          )
          AND NOT EXISTS (
              SELECT 1
              FROM core.events s
              WHERE s.source = 'runtime.instruction'
                AND s.event_type = 'expectation.status'
                AND s.payload->>'instruction_id' = i.payload->>'instruction_id'
                AND s.payload->>'status' IN (
                    'already_satisfied',
                    'fulfilled',
                    'timed_out',
                    'contradicted',
                    'impossible',
                    'cancelled'
                )
          )
        ORDER BY i.ts_orig DESC, i.id DESC
        LIMIT 1
        "#,
        instruction.idempotency_key,
        instruction.instruction_id.to_string(),
    )
    .fetch_optional(pool)
    .await
    .map_err(|error| {
        SinexError::database("failed to query active Hyprland workspace instructions")
            .with_context("idempotency_key", instruction.idempotency_key.clone())
            .with_std_error(&error)
    })
}

async fn register_instruction_material(
    pool: &PgPool,
    auth: &RpcAuthContext,
    instruction: &DesktopWorkspaceSwitchInstructionPayload,
) -> Result<Uuid> {
    let material_id = Uuid::now_v7();
    let source_uri = format!(
        "sinexctl://instructions/hyprland-workspace/{}/{}",
        instruction.desired_workspace_id, material_id
    );
    let material = DbSourceMaterial::blob_text(source_uri.clone())
        .with_content_preview(format!(
            "workspace switch request: {}",
            instruction.desired_workspace_id
        ))
        .with_metadata(json!({
            "source_uri": source_uri,
            "instruction_id": instruction.instruction_id,
            "instruction_target": "desktop.hyprland.workspace",
            "desired_workspace_id": instruction.desired_workspace_id,
            "capture_surface": "sinexctl",
        }))
        .with_staged_by(auth.actor_id().to_string());
    let record = pool
        .source_materials()
        .register_external_material(material_id, material)
        .await
        .map_err(|error| {
            SinexError::processing("failed to register instruction source material")
                .with_context("instruction_id", instruction.instruction_id.to_string())
                .with_std_error(&error)
        })?;
    Ok(record.id)
}
