# Shared work-control contract

`runtime::work_control` is the common admission, cancellation, accounting, and
resumption primitive for maintenance and rebuild work. A caller supplies a
`WorkIdentity`, a `WorkBudget`, a cancellation token, and (when resuming) the
last durable `WorkProgress` cursor. The controller reports `Completed`,
`Partial`, `Cancelled`, or `Failed`; a partial result is never presented as a
complete scan.

Budgets are opt-in. The generic default is unlimited because the controller
cannot infer a safe deadline or throughput for a large CAS, replay, or import.
Callers that need bounded service impact choose explicit item, byte, runtime,
or sustained-rate limits and persist the returned cursor. `WorkAdmission`
provides FIFO concurrency control, and cancellation is cooperative and wakes
waiters promptly.

## Current integration map

| Caller | Status | Boundary and evidence |
| --- | --- | --- |
| CAS fsck/orphan reconciliation | Migrated | `cas_fsck::check_cas_with_options_and_control` and `check_cas_bounded_with_control` use the controller for cancellation, runtime/byte/entry limits, resumable directory cursors, streamed status reporting, and fail-closed destructive boundaries. Focused CAS fsck tests cover cancellation, budget stops, cursor resumption, bounded reporting, and apply refusal. |
| Historical replay/import | Not migrated | Normal historical source scans have an adapter cursor, parser checkpoint, `CommitFrontier`, durable-emission receipt, and separate `ScanPacer`; CAS material replay at `runtime/parser/adapter_source.rs:707-928` returns `Checkpoint::None`. The exact bridge is tracked by `sinex-0r01.1.1.1`, with `sinex-w4i` and `sinex-vxu` retained as durability prerequisites. |
| Projection rebuilds | Intentionally excluded for now | Replay invalidation marks `projection_registry` stale, but the live invalidation consumer and scope recompute state still need the coupled `sinex-lk67` fix. WorkProgress cannot replace projection scope identity or readiness state. |
| Schema apply/backfill | Intentionally excluded for now | Declarative schema convergence remains atomic and fail-fast. The backfill route at `sinex-schema/src/backfill.rs:136-255` owns its advisory lock, frozen event horizon, and keyset cursor; failure state, quiescence, preview, and scale work remain in `sinex-tjy`, `sinex-audit-backfill-no-dryrun`, `sinex-p61n`, and `sinex-5ai`. |
| Material/CAS GC | Partially migrated | CAS orphan inspection uses the controller. Material registry and blob deletion still require durable leases, quarantine, and deletion receipts from `sinex-r6d.14` and `sinex-cgcs` before sharing the destructive control path. |
| Other maintenance loops | Not migrated | Each caller must be classified before being wired: either adopt the contract with a durable cursor and authority recheck, be explicitly unlimited by design, or receive a linked Beads follow-up. Historical pacing observations remain separate in `sinex-audit-2n9-unlimited-negctrl-cap`, `sinex-b8xr`, and `sinex-racu`. |

## Audit disposition: sinex-0r01.1.1

No new shared pause/resume implementation is safe in this audit slice. `WorkController` is bounded and testable on CAS fsck, but `ScanPacer` already owns a distinct token bucket, backlog gate, and transient progress projection. The source cancellation path is an operation-local `watch<bool>` that drops the worker scan future, while replay persistence currently records an event count rather than a source cursor or occurrence position. Combining those surfaces without an operation-persisted cursor and outcome would create false resumability and could replay an archived scope blindly.

The execution-grade bridge is `sinex-0r01.1.1.1`. Its first code boundaries are `runtime/work_control.rs`, `runtime/pacing.rs`, `runtime/stream/runner/{command_listener,dispatch}.rs`, `runtime/parser/adapter_source.rs:707-928` and `:1448-1995`, and `api/replay_control/execution/{replay_writer,mod}.rs`. It must preserve the existing `CommitFrontier` plus durable-emission receipt as the source cursor authority, keep `WorkBudget::default()` unlimited, and fail closed before destructive mutation when a bounded route cannot persist a resumable cursor.

The controller is a coordination mechanism, not a policy that silently caps all
work. Destructive callers must re-check authority immediately before mutation;
elapsed time or an mtime grace period is never proof that deletion is safe.
