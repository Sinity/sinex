# Instruction And Actuator Loops

Sinex is primarily an observation system. Active loops are allowed only when the
desired state, authority, actuation attempt, observation closure, and failure
semantics are explicit.

This record defines instruction events and actuator loops without making active
inference the core architecture.

## Decision

Represent instructions as typed events plus operation/audit records, not as
freeform shell commands and not as observations.

Rejected alternatives:

| Alternative | Rejected because |
| --- | --- |
| Arbitrary command payloads | Unsafe, unauditable, impossible to authorize per capability. |
| Reusing observation event types with an `intent` flag | Makes history ambiguous: desired state and observed fact become too easy to confuse. |
| Gateway RPC only | Loses event lineage and replay/audit visibility. |
| DB command table only | Bypasses the event-driven provenance and trace model. |

Instructions are desired-state records. Observations remain normal captured
facts from source units. A reconciler compares the two.

## Concepts

```rust
pub struct InstructionPayload {
    pub instruction_id: Uuid,
    pub target: InstructionTarget,
    pub desired_event_type: EventType,
    pub desired_payload: JsonValue,
    pub actor: ActorRef,
    pub authority: AuthorityClass,
    pub idempotency_key: String,
    pub deadline: Option<Timestamp>,
    pub dry_run: bool,
    pub safety_policy: SafetyPolicyRef,
}

pub struct ActuationAttemptPayload {
    pub instruction_id: Uuid,
    pub actuator_id: String,
    pub capability: String,
    pub status: ActuationStatus,
    pub command_summary: JsonValue,
    pub error: Option<String>,
}

pub struct ExpectationStatusPayload {
    pub instruction_id: Uuid,
    pub status: ExpectationStatus,
    pub matched_event_ids: Vec<Uuid>,
    pub prediction_error: Option<JsonValue>,
}
```

| Concept | Meaning |
| --- | --- |
| Instruction | Desired state or requested action with actor, authority, idempotency, deadline, and safety policy. |
| Actuation attempt | Actuator accepted, rejected, dry-ran, nooped, attempted, failed, or completed the external operation. |
| Observation | Ordinary source-captured event proving what happened in the world. |
| Expectation status | Reconciler output: fulfilled, timed out, contradicted, impossible, cancelled, or already satisfied. |

## Event Families

Use dedicated event families:

| Event | Role |
| --- | --- |
| `instruction.requested` | Desired state admitted by gateway/CLI or approved finalizer. |
| `actuation.attempted` | Actuator attempt result and sanitized command summary. |
| `instruction.fulfilled` | Observation matched desired state. |
| `instruction.failed` | Timeout, contradiction, impossible state, or rejected authority. |

Capability-specific payloads can wrap these generic roles. For example,
`desktop.workspace.switch_requested` can be an instruction payload whose desired
observation is `desktop.workspace.switched`.

## Authority

| Authority class | Allowed source |
| --- | --- |
| `operator_direct` | Authenticated local operator or admin RPC. |
| `user_declared` | User declaration with captured material. |
| `deterministic_policy` | Narrow allowlisted rule with bounded effect. |
| `approved_proposal` | Finalizer output after proposal/judgment approval. |
| `model_suggested` | Not executable until judged/finalized. |

Model-generated or agent-generated actions require proposal/judgment/finalizer
unless a deterministic policy explicitly whitelists the exact capability and
scope. No actuator accepts arbitrary shell commands from event payloads.

## Loop Prevention

Every actuator must implement:

1. capability declaration;
2. dry-run/preview where meaningful;
3. idempotency key check;
4. current-state pre-check when observable;
5. no-op result when the desired state is already satisfied;
6. bounded retry policy;
7. deadline handling;
8. audit event for rejection/failure;
9. reconciler observation matching after attempt.

The reconciler, not the actuator, decides fulfillment. This prevents "I ran the
command" from being conflated with "the desired world state became true."

## QoS And Privacy

Instruction and actuation events belong in a high-priority control lane, but
they must not weaken privacy.

| Concern | Rule |
| --- | --- |
| QoS | Control/instruction signals are lossless and high priority. |
| Private mode | May block both actuation and observation depending on capability policy. |
| Readiness | If the observing source is not ready, instruction status is caveated or impossible. |
| Continuity gaps | Fulfillment must report when observation may be missing. |
| Audit | Rejected and failed attempts still emit audit evidence. |

## Hyprland Proof Slice

First proof target: local desktop workspace switch.

Instruction:

```json
{
  "target": "desktop.hyprland.workspace",
  "desired_event_type": "desktop.workspace.switched",
  "desired_payload": {"workspace": "3"},
  "authority": "operator_direct",
  "idempotency_key": "desktop.hyprland.workspace:3",
  "dry_run": false
}
```

Actuator:

- uses declared Hyprland command capability, not shell passthrough;
- checks current workspace first;
- if already on workspace 3, emits `noop_already_satisfied`;
- otherwise dispatches the workspace switch through the Hyprland command socket;
- emits `actuation.attempted` with sanitized summary.

Observation:

- desktop capture emits `desktop.workspace.switched` through normal source
  capture;
- expectation reconciler matches workspace 3 before deadline;
- emits `instruction.fulfilled` or `instruction.failed`.

Loop prevention:

- repeated identical idempotency key within active deadline is coalesced;
- actuator does not retry after fulfilled observation;
- if desktop capture is not ready, status is caveated instead of blindly
  repeating commands.

## Boundaries

- Do not implement arbitrary command execution.
- Do not treat actuation attempt as observation fulfillment.
- Do not bypass gateway/API authorization or proposal judgment for
  model-generated actions.
- Do not make active loops mandatory for ordinary capture.
- Do not execute remote or destructive capabilities in the first slice.
