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
| Historical replay/import | Not migrated | Replay and source-drain loops have their own durable cursor and pacing contracts. They must adopt this controller only when their operation record can persist the same identity, cursor, and partial outcome. The open `sinex-w4i` and `sinex-r6d` durability work remains the prerequisite rather than being hidden behind a generic wrapper. |
| Projection rebuilds | Not migrated | Projection invalidation/rebuild has operation and stale-registry state but still needs a caller-specific cursor and mutation-boundary contract. Keep this tracked with the open projection/replay beads (`sinex-68c.4` descendants and `sinex-lk67`). |
| Schema apply/backfill | Intentionally excluded for now | Declarative schema convergence must remain atomic and fail-fast; backfills need a separate database-side batch/checkpoint design. Do not impose an arbitrary wall-clock refusal. Track scale work in the open reimport/schema beads (`sinex-p61n`, `sinex-5ai`). |
| Material/CAS GC | Partially migrated | CAS orphan inspection uses the controller. Material assembler maintenance has separate retry and terminal-settlement state and must not be treated as covered until its lifecycle work (`sinex-r6d.14`, `sinex-cgcs`) supplies durable leases and deletion receipts. |
| Other maintenance loops | Not migrated | Each caller must be classified before being wired: either adopt the contract with a durable cursor and authority recheck, be explicitly unlimited by design, or receive a linked Beads follow-up. |

The controller is a coordination mechanism, not a policy that silently caps all
work. Destructive callers must re-check authority immediately before mutation;
elapsed time or an mtime grace period is never proof that deletion is safe.
