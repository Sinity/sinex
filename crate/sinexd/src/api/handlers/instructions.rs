//! Instruction/actuator-loop RPC handlers.

use std::{
    future::Future,
    path::{Path, PathBuf},
};

use crate::runtime::{
    HyprlandCommandSocketConnection, HyprlandCommandSocketResponse, RuntimeResult,
    connect_hyprland_command_socket, resolve_hyprland_command_socket_path,
};
use serde_json::json;
use sinex_db::DbPoolExt;
use sinex_db::repositories::SourceMaterial as DbSourceMaterial;
use sinex_primitives::derivation::{
    ClaimSupportTemplate, ClaimTemporalQuality, DerivationOutputDeclaration,
    DerivationWriteSurface, DerivedProductClass, InputEligibility, SourceCoverage, SupportLevel,
};
use sinex_primitives::events::payloads::{
    ActuationAttemptPayload, ActuationDispatchReceiptPayload, ActuationStatus,
    DesktopWorkspaceSwitchInstructionPayload, HyprlandWorkspaceSwitchedPayload,
    plan_hyprland_workspace_switch,
};
use sinex_primitives::events::{Event, EventPayload, SourceMaterial};
use sinex_primitives::rpc::instructions::{
    HyprlandWorkspaceSwitchRequest, HyprlandWorkspaceSwitchResponse,
};
use sinex_primitives::{
    DEFAULT_RUNTIME_LIVENESS_STALE_AFTER_SECS, Id, JsonValue, Result, RuntimeLivenessPolicy,
    RuntimeLivenessSignals, SinexError, Timestamp, Uuid, evaluate_runtime_liveness,
};
use sqlx::{PgPool, Postgres, Transaction};

use crate::api::rpc_server::RpcAuthContext;
use crate::api::service_container::ServiceContainer;

/// Derivation control-plane declaration for the instructions RPC handlers
/// (sinex-q46n), reconciled the same way as `curation::CURATION_OUTPUT_DECLARATIONS`
/// (see its doc comment for the general shape/rationale) — this handler builds
/// its own `ActuationAttemptPayload` event directly rather than through the
/// automaton adapter.
///
/// `runtime.instruction/actuation.attempted`: a sanitized record of an
/// actuator decision or command-socket attempt — `ReportArtifact` (a
/// generated receipt of what the actuator did), `artifact_writer`.
pub const INSTRUCTIONS_OUTPUT_DECLARATIONS: &[DerivationOutputDeclaration] = &[
    ACTUATION_ATTEMPT_DECLARATION,
    ACTUATION_DISPATCH_RECEIPT_DECLARATION,
];

const ACTUATION_ATTEMPT_DECLARATION: DerivationOutputDeclaration = DerivationOutputDeclaration {
    declaration_id: "instructions-rpc.actuation.attempted",
    owner: "instructions-rpc",
    product_class: DerivedProductClass::ReportArtifact,
    write_surface: DerivationWriteSurface::ArtifactWriter,
    output_source: None,
    output_event_type: None,
    projection_kind: None,
    artifact_kind: None,
    proposal_kind: None,
    semantics_version: "1.0.0",
    input_eligibility: InputEligibility::NeverInput,
    default_support: ClaimSupportTemplate::new(
        SupportLevel::Direct,
        SourceCoverage::Covered,
        ClaimTemporalQuality::RealtimeCapture,
    ),
    verification_command: "xtask test -p sinexd -E 'test(hyprland_workspace_switch_dispatches_typed_command_when_observation_ready)'",
};

const ACTUATION_DISPATCH_RECEIPT_DECLARATION: DerivationOutputDeclaration =
    DerivationOutputDeclaration {
        declaration_id: "instructions-rpc.actuation.dispatch_started",
        owner: "instructions-rpc",
        product_class: DerivedProductClass::ReportArtifact,
        write_surface: DerivationWriteSurface::ArtifactWriter,
        output_source: None,
        output_event_type: None,
        projection_kind: None,
        artifact_kind: None,
        proposal_kind: None,
        semantics_version: "1.0.0",
        input_eligibility: InputEligibility::NeverInput,
        default_support: ClaimSupportTemplate::new(
            SupportLevel::Direct,
            SourceCoverage::Covered,
            ClaimTemporalQuality::RealtimeCapture,
        ),
        verification_command: "xtask test -p sinexd -E 'test(instructions)'",
    };

pub async fn handle_hyprland_workspace_switch(
    services: &ServiceContainer,
    req: HyprlandWorkspaceSwitchRequest,
    auth: &RpcAuthContext,
) -> Result<HyprlandWorkspaceSwitchResponse> {
    let pool = services.pool();
    let instruction_id = req.instruction_id.unwrap_or_else(Uuid::now_v7);
    let observed_at = Timestamp::now();
    let mut instruction = DesktopWorkspaceSwitchInstructionPayload::hyprland_operator_direct(
        instruction_id,
        req.desired_workspace_id,
        auth.actor_id(),
        req.deadline,
        req.dry_run,
    )?;
    let (authority, safety_policy_ref) = instruction_authority(auth);
    instruction.authority = authority;
    instruction.safety_policy_ref = safety_policy_ref.to_string();
    let current_workspace = latest_hyprland_workspace(pool).await?;
    let current_workspace_id = current_workspace.map(|(workspace_id, _)| workspace_id);
    let observation_ready = current_workspace_id.is_some();
    // The insert-only instruction path cannot rely on a reconciler expectation
    // to release an idempotency key. Scope the key to the observed transition:
    // retries from the same source state stay blocked, while a later transition
    // becomes a distinct operation after the observation lane catches up.
    instruction.idempotency_key = workspace_transition_idempotency_key(
        current_workspace_id,
        instruction.desired_workspace_id,
    );
    let material_id = register_instruction_material(pool, auth, &instruction).await?;
    let mut instruction_event = instruction
        .clone()
        .from_material(Id::<SourceMaterial>::from_uuid(material_id))
        .at_time(observed_at)
        .build()?;
    instruction_event.id =
        Some(Id::<Event<DesktopWorkspaceSwitchInstructionPayload>>::from_uuid(Uuid::now_v7()));
    let typed_instruction_event_id = instruction_event.id.ok_or_else(|| {
        SinexError::invalid_state(
            "instructions.hyprland.workspace_switch: built instruction event missing id",
        )
    })?;
    let instruction_event_id =
        Id::<Event<JsonValue>>::from_uuid(*typed_instruction_event_id.as_uuid());
    let instruction_event: Event<JsonValue> =
        serde_json::from_value(serde_json::to_value(instruction_event).map_err(SinexError::from)?)
            .map_err(SinexError::from)?;
    let inserted_instruction = services
        .publish_and_confirm_activity_event(instruction_event, "instructions-rpc")
        .await?;

    let mut attempt = plan_hyprland_workspace_switch(
        &instruction,
        current_workspace_id,
        observation_ready,
        Timestamp::now(),
    );
    let mut command_socket_response = None;
    let mut dispatch_transaction =
        if !instruction.dry_run && attempt.status == ActuationStatus::Attempted {
            Some(acquire_workspace_dispatch_lock(pool, &instruction.idempotency_key).await?)
        } else {
            None
        };

    if attempt.status == ActuationStatus::Attempted {
        let Some(transaction) = dispatch_transaction.as_mut() else {
            return Err(SinexError::invalid_state(
                "workspace dispatch is missing its advisory-lock transaction",
            ));
        };
        if let Some(active_instruction_id) =
            active_hyprland_workspace_instruction(transaction, &instruction).await?
        {
            drop(dispatch_transaction.take());
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
    }

    if attempt.status == ActuationStatus::Attempted
        && let Some(command) = attempt.command_summary.command.clone()
    {
        let Some(socket_path) =
            resolve_hyprland_command_socket_path(req.command_socket_path.as_deref())
        else {
            drop(dispatch_transaction.take());
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

        // Validate caller-provided paths at this RPC boundary as well as in
        // the runtime socket helper. This keeps malformed paths out of the
        // dispatch flow before any connection attempt is made.
        let connection = match validate_instruction_socket_path(&socket_path) {
            Ok(path) => connect_hyprland_command_socket(path).await,
            Err(error) => Err(error),
        };
        match connection {
            Err(error) => {
                drop(dispatch_transaction.take());
                // No command bytes were sent, so this failure is safe to retry.
                attempt.status = ActuationStatus::Unavailable;
                attempt.error = Some(error.to_string());
            }
            Ok(connection) => {
                let Some(mut transaction) = dispatch_transaction.take() else {
                    return Err(SinexError::invalid_state(
                        "workspace dispatch is missing its advisory-lock transaction",
                    ));
                };
                let receipt_instruction = instruction.clone();
                let receipt_attempt = attempt.clone();
                let receipt_pool = pool.clone();
                match dispatch_with_attempt_receipt(connection, &command, move || async move {
                    persist_dispatch_receipt(
                        &receipt_pool,
                        &mut transaction,
                        instruction_event_id,
                        &receipt_instruction,
                        &receipt_attempt,
                    )
                    .await?;
                    transaction.commit().await.map_err(|error| {
                        SinexError::database("failed to commit Hyprland dispatch receipt")
                            .with_std_error(&error)
                    })?;
                    Ok(())
                })
                .await
                {
                    Ok(response) => {
                        let socket_response = response.response;
                        if socket_response.trim() != "ok" {
                            // A non-ok response is still post-write evidence;
                            // conservatively retain the receipt as an active
                            // guard in case the command was partially applied.
                            attempt.status = ActuationStatus::Attempted;
                            attempt.error = Some(format!(
                                "Hyprland command socket rejected workspace dispatch: {socket_response}"
                            ));
                        } else {
                            // A successful socket acknowledgement plus a later
                            // matching observation is required before the guard
                            // can release this transition key.
                            attempt.status = ActuationStatus::Accepted;
                            attempt.attempted_at = Timestamp::now();
                        }
                        command_socket_response = Some(socket_response);
                    }
                    Err(error) => {
                        // A write/read error after the durable dispatch receipt is
                        // uncertain: leave status attempted so the guard blocks.
                        attempt.status = ActuationStatus::Attempted;
                        attempt.error = Some(error.to_string());
                    }
                }
            }
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

fn validate_instruction_socket_path(path: &Path) -> Result<PathBuf> {
    let path = path.to_str().ok_or_else(|| {
        SinexError::validation("Hyprland command socket path must be valid UTF-8")
    })?;
    sinex_primitives::validation::validate_path(path).map(PathBuf::from)
}

async fn acquire_workspace_dispatch_lock(
    pool: &PgPool,
    idempotency_key: &str,
) -> Result<Transaction<'static, Postgres>> {
    let mut transaction = pool.begin().await.map_err(|error| {
        SinexError::database("failed to begin Hyprland workspace dispatch transaction")
            .with_std_error(&error)
    })?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(idempotency_key)
        .execute(&mut *transaction)
        .await
        .map_err(|error| {
            SinexError::database("failed to acquire Hyprland workspace dispatch lock")
                .with_context("idempotency_key", idempotency_key)
                .with_std_error(&error)
        })?;
    Ok(transaction)
}

async fn dispatch_with_attempt_receipt<F, Fut>(
    connection: HyprlandCommandSocketConnection,
    command: &sinex_primitives::events::payloads::instruction::HyprlandWorkspaceCommand,
    persist_attempt: F,
) -> RuntimeResult<HyprlandCommandSocketResponse>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<()>>,
{
    persist_attempt().await?;
    connection.dispatch(command).await
}

async fn persist_dispatch_receipt(
    pool: &PgPool,
    transaction: &mut Transaction<'_, Postgres>,
    instruction_event_id: Id<Event<JsonValue>>,
    instruction: &DesktopWorkspaceSwitchInstructionPayload,
    attempt: &ActuationAttemptPayload,
) -> Result<()> {
    let receipt = ActuationDispatchReceiptPayload {
        instruction_id: instruction.instruction_id,
        idempotency_key: instruction.idempotency_key.clone(),
        actuator_id: attempt.actuator_id.clone(),
        capability: attempt.capability.clone(),
        command_summary: attempt.command_summary.clone(),
        dispatch_started_at: Timestamp::now(),
    };
    let dispatch_started_at = receipt.dispatch_started_at;
    let mut event = receipt
        .from_parents([instruction_event_id])?
        .at_time(dispatch_started_at)
        .build()?;
    event.product_class = Some(ACTUATION_DISPATCH_RECEIPT_DECLARATION.product_class);
    event.claim_support = Some(
        ACTUATION_DISPATCH_RECEIPT_DECLARATION
            .default_support
            .instantiate(1, 0, 1, 0),
    );
    event.derivation_declaration_id = Some(
        ACTUATION_DISPATCH_RECEIPT_DECLARATION
            .declaration_id
            .to_string(),
    );
    let inserted = pool.events().insert_with_tx(transaction, event).await?;
    if inserted.id.is_none() {
        return Err(SinexError::invalid_state(
            "instructions.hyprland.workspace_switch: dispatch receipt insert returned no event id",
        ));
    }
    Ok(())
}

fn workspace_transition_idempotency_key(
    current_workspace_id: Option<i32>,
    desired_workspace_id: i32,
) -> String {
    format!(
        "desktop.hyprland.workspace:{}:{desired_workspace_id}",
        current_workspace_id.map_or_else(
            || "unknown".to_string(),
            |workspace_id| workspace_id.to_string()
        )
    )
}

#[cfg(test)]
#[path = "instructions_test.rs"]
mod tests;

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
    let inserted_attempt =
        insert_attempt_event(pool, pending.instruction_event_id, &pending.attempt).await?;

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

async fn insert_attempt_event(
    pool: &PgPool,
    instruction_event_id: Id<Event<JsonValue>>,
    attempt: &ActuationAttemptPayload,
) -> Result<Event<JsonValue>> {
    let mut attempt_event = attempt
        .clone()
        .from_parents([instruction_event_id])?
        .at_time(attempt.attempted_at)
        .build()?;
    attempt_event.product_class = Some(ACTUATION_ATTEMPT_DECLARATION.product_class);
    attempt_event.claim_support = Some(
        ACTUATION_ATTEMPT_DECLARATION
            .default_support
            .instantiate(1, 0, 1, 0),
    );
    attempt_event.derivation_declaration_id =
        Some(ACTUATION_ATTEMPT_DECLARATION.declaration_id.to_string());
    let inserted_attempt = pool.events().insert(attempt_event).await?;
    let _attempt_event_id = inserted_attempt.id.ok_or_else(|| {
        SinexError::invalid_state(
            "instructions.hyprland.workspace_switch: persisted attempt event missing id",
        )
    })?;

    Ok(inserted_attempt)
}

async fn latest_hyprland_workspace(pool: &PgPool) -> Result<Option<(i32, Timestamp)>> {
    let row = sqlx::query!(
        r#"
        SELECT payload, ts_orig as "observed_at!: Timestamp"
        FROM core.events
        WHERE source = 'wm.hyprland'
          AND event_type = 'workspace.switched'
          AND ts_orig IS NOT NULL
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
    let liveness = evaluate_runtime_liveness(
        RuntimeLivenessSignals {
            run_status: None,
            health_status: None,
            last_heartbeat_at: None,
            last_output_at: Some(row.observed_at),
        },
        RuntimeLivenessPolicy::new(DEFAULT_RUNTIME_LIVENESS_STALE_AFTER_SECS),
        Timestamp::now(),
    );
    Ok(liveness
        .status
        .is_live()
        .then_some((payload.to_workspace_id, row.observed_at)))
}

fn instruction_authority(
    auth: &RpcAuthContext,
) -> (
    sinex_primitives::events::payloads::instruction::InstructionAuthorityClass,
    &'static str,
) {
    use sinex_primitives::events::payloads::instruction::InstructionAuthorityClass;

    // A token or browser extension identifies an authenticated caller, not a
    // direct operator action. Keep that distinction in the durable instruction
    // record; only the explicit local system context receives OperatorDirect.
    if auth.actor_id().starts_with("system:") {
        (
            InstructionAuthorityClass::OperatorDirect,
            "desktop.hyprland.workspace-switch.operator-direct",
        )
    } else if auth.actor_id().starts_with("extension:") {
        (
            InstructionAuthorityClass::UserDeclared,
            "desktop.hyprland.workspace-switch.extension-user",
        )
    } else {
        (
            InstructionAuthorityClass::UserDeclared,
            "desktop.hyprland.workspace-switch.authenticated-user",
        )
    }
}

async fn active_hyprland_workspace_instruction(
    transaction: &mut Transaction<'_, Postgres>,
    instruction: &DesktopWorkspaceSwitchInstructionPayload,
) -> Result<Option<Uuid>> {
    sqlx::query_scalar!(
        r#"
        SELECT i.id as "id!: Uuid"
        FROM core.events i
        WHERE i.source = 'runtime.instruction'
          AND i.event_type = 'desktop.workspace.switch_requested'
          AND i.payload->>'idempotency_key' = $1
          AND COALESCE((i.payload->>'dry_run')::boolean, false) = false
          AND EXISTS (
              SELECT 1
              FROM core.events a
              WHERE a.source = 'runtime.instruction'
                AND a.event_type = 'actuation.dispatch_started'
                AND a.payload->>'idempotency_key' = $1
                AND i.id = ANY(a.source_event_ids)
          )
          AND NOT EXISTS (
              SELECT 1
              FROM core.events s
              WHERE s.source = 'runtime.instruction'
                AND s.event_type = 'expectation.status'
                AND s.payload->>'instruction_id' = i.payload->>'instruction_id'
                AND s.payload->>'status' = 'fulfilled'
                AND EXISTS (
                    SELECT 1
                    FROM core.events receipt
                    WHERE receipt.source = 'runtime.instruction'
                      AND receipt.event_type = 'actuation.dispatch_started'
                      AND receipt.payload->>'idempotency_key' = $1
                      AND i.id = ANY(receipt.source_event_ids)
                      AND EXISTS (
                          SELECT 1
                          FROM core.events observation
                          WHERE EXISTS (
                              SELECT 1
                              FROM jsonb_array_elements_text(
                                  COALESCE(s.payload->'matched_event_ids', '[]'::jsonb)
                              ) AS matched(event_id)
                              WHERE matched.event_id = observation.id::text
                          )
                            AND observation.source = 'wm.hyprland'
                            AND observation.event_type = 'workspace.switched'
                            AND observation.ts_orig >= receipt.ts_orig
                      )
                )
          )
        ORDER BY i.ts_orig DESC, i.id DESC
        LIMIT 1
        "#,
        instruction.idempotency_key,
    )
    .fetch_optional(&mut **transaction)
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
