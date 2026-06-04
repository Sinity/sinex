# Source-Unit Drain Protocol

This record defines the shutdown and recovery contract for `sinexd::sources`
source units that emit material-provenance events from live or staged source
material. It is the shared protocol for acquisition jobs, parser lanes,
child-process sidecars, private-mode transitions, and continuity diagnostics.

The source-unit runtime lives under `crate/sinexd/src/sources/` and
`crate/sinexd/src/node_sdk/`. The database material lifecycle is carried by
`raw.source_material_registry.status` and the source-material repository.

## Goals

- stop source input without losing admitted work;
- preserve stable source-material provenance for events already emitted;
- make incomplete or unrecoverable material visible as operator evidence;
- let continuity reports distinguish planned gaps from crashes, private mode,
  load shedding, and ephemeral loss;
- keep NATS as transport, not as the only source of shutdown truth.

This protocol does not require every source shape to be recoverable. Some
sources are inherently ephemeral; their correct behavior is auditable gap
evidence, not invented reconstruction.

## Drain Phases

The drain controller owns a per-source-unit phase machine. It wraps the
SDK `RuntimeDrainController` signal and adds active-work accounting and
operator-visible phases.

| Phase | Meaning | Required action |
|-------|---------|-----------------|
| `Idle` | Unit is accepting source input. | Normal sensing or finite acquisition. |
| `StoppingAccept` | Drain requested. | Stop accepting new source records, bytes, child-process output, or scan ranges. |
| `FinishingActive` | Existing admitted work is draining. | Let active parser/acquisition guards finish or hit the bounded timeout. |
| `FlushingIntents` | Event intents admitted before drain are being flushed. | Publish or settle admitted event batches according to their transport class. |
| `WaitingConfirmations` | The unit is waiting for persistence acknowledgements. | Wait for confirmations within policy; record missing confirmations rather than blocking forever. |
| `FinalizingMaterials` | Open material handles are being closed. | Finalize, cancel, fail, or mark recovered partial material. |
| `SavingCheckpoint` | Durable source-unit state is being saved. | Persist checkpoint/state after material finalization decisions are visible. |
| `Drained` | Unit can exit. | Exit or hand control back to the supervisor. |

The first `request_drain(unit_id)` call raises the SDK drain signal and enters
`StoppingAccept`. Later calls are idempotent. Source loops should wrap admitted
units of work in `work_guard()` so `FinishingActive` has concrete in-flight
evidence.

## Source-Material States

`raw.source_material_registry.status` is the canonical material state. The
schema allows `sensing`, `completed`, `cancelled`, `recovered_partial`, and
`failed`.

| State | Meaning | Replay implication |
|-------|---------|--------------------|
| `sensing` | In-flight material exists and may still receive bytes or records. | Events may cite the material, but `total_bytes` and anchor completeness are not stable yet. |
| `completed` | Material finalized with stable content or a stable terminal extent. | Replay can re-read the material subset represented by the registry row. |
| `cancelled` | Operator or policy stopped the material before it should produce a completed record. | No silent loss claim; continuity should treat the cancellation as deliberate evidence. |
| `recovered_partial` | Startup found material that crashed mid-flight and preserved only a bounded subset. | Replay covers the recovered subset only; continuity classifies adjacent seams as recovered partial. |
| `failed` | Parser, acquisition, assembler, or storage failed the material. | Replayability is weak and the failure reason must be visible in metadata. |

`abandoned` is not a separate schema status today. It is a lifecycle outcome
represented by terminal `cancelled` or `failed` plus metadata that records the
orphan TTL, last progress, and reason. If later implementation needs a distinct
operator-facing label, it should be a projection over those persisted facts
unless query behavior requires a real status value.

## Gap Evidence

`GapEvidence` records restart and interruption facts:

- `unit_id`
- `crashed_at`
- `restarted_at`
- `drain_phase_at_crash`
- `in_flight_count`

Every restart should produce either clean-start evidence or gap evidence. The
evidence can be logged initially, but durable runtime implementations should
promote it into an event or source-material metadata before claiming continuity
coverage.

Continuity surfaces classify these cases from material status, privacy class,
coverage contract, and gap evidence:

| Gap kind | Evidence source |
|----------|-----------------|
| Planned restart | Drain reached `Drained` and checkpoint/material finalization completed. |
| Startup gap fill | Restart observed prior state and replay/import filled the expected range. |
| Recovered partial | A neighboring material is `recovered_partial`. |
| Ephemeral stream gap | Coverage contract says the stream cannot be reconstructed. |
| Private-mode gap | Source material or seam has private/redacted privacy classification. |
| Load-shed gap | QoS policy dropped or suppressed low-value work under pressure. |

## Acquisition Jobs

Finite acquisition jobs use the same protocol as continuous units:

1. register in-flight material before emitting material-provenance events;
2. hold an active-work guard while reading or parsing;
3. on normal completion, finalize material as `completed`;
4. on cancellation, enter drain and mark admitted material `cancelled` or
   `recovered_partial` according to how much material was persisted;
5. save acquisition progress after material status is visible.

This prevents each adapter shape from inventing a bespoke shutdown model. The
source-unit drain phase is the lifecycle clock; source-specific code only
decides how much material can be finalized.

## Child Processes And Sidecars

Child processes and retained sidecars are isolation mechanisms, not separate
source identities. The parent source unit owns:

- the source-unit identity;
- the in-flight material handle;
- the drain phase;
- heartbeat timeout policy;
- final material state and gap evidence.

On drain, the parent stops accepting child output, asks the child to stop, then
waits through `FinishingActive` within a bounded timeout. If the child exits
cleanly, the parent finalizes normally. If the child crashes or misses its
heartbeat deadline, the parent records degraded per-unit status and marks the
material `recovered_partial`, `failed`, or `cancelled` according to available
bytes and operator intent.

## Private Mode And Load Shedding

Private-mode and configuration toggles are explicit lifecycle transitions:

- `pause`: stop accepting new input and leave no gap only when no source
  coverage was promised for the paused interval;
- `drain`: run the full phase protocol and finalize admitted material;
- `suppress`: avoid creating sensitive events and record a private-mode gap;
- `tombstone`: remove or hide already persisted material through the data
  lifecycle path, not by mutating events in place.

Load shedding follows the runtime QoS policy. Critical provenance-bearing work
must drain or settle; lower-value observations may produce a load-shed gap when
pressure policy says dropping is preferable to stalling the host.

## NATS Interaction

NATS carries event intents and confirmations, but source-material state remains
the durable authority.

- Publishing uses the transport class policy for backpressure and ack waits.
- During drain, admitted event intents must either publish, settle to failure,
  or become explicit missing-confirmation evidence.
- Confirmation waits are bounded; timeout is evidence, not permission to claim
  clean completion.
- DB commits and source-material status updates are the authority for replay and
  continuity. NATS-only observations are insufficient for closure.

## Verification Requirements

Unit tests:

- phase transitions from `Idle` through `Drained`;
- `work_guard()` increments/decrements active-work count;
- active-work timeout behavior;
- idempotent double drain;
- clean-start and crash gap evidence.

Integration tests:

- full seven-phase drain through a source-unit obligation;
- restart after mid-drain crash produces gap evidence;
- per-unit drain isolation when multiple source units share a host;
- recovered-partial material affects continuity classification.

VM or deployment-shaped tests:

- `systemctl restart` reaches `Drained` before exit for a live unit;
- `kill -9` during sensing produces recovered or failed material evidence;
- repeated crash does not erase the previous gap;
- child-process crash degrades only the owning source unit;
- private-mode and load-shed gaps appear in continuity reports.

Current narrow proof exists in source-unit drain obligations and the
controller unit tests. The remaining deployment-shaped tests should cite this
record rather than redefining per-source shutdown semantics.
