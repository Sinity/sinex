# Work-control contract

`runtime::work_control` is the common execution contract for expensive
maintenance and rebuild work. It is deliberately policy-free by default:
`WorkBudget::default()` does not impose a wall-clock, item, or throughput cap.
Callers may add limits when they have a measured host-admission reason.

The contract separates four concerns:

- `WorkCancellation` is sticky and wakes admission/rate/pressure waiters.
- `WorkAdmission` is FIFO and limits concurrent operations sharing a resource.
- `WorkBudget` accounts items and bytes and optionally paces sustained rates.
- `WorkProgress` is a single resumable cursor. The caller owns persistence; the
  controller intentionally retains no batch history.

Pressure is a pause condition, not a proof of failure or a correctness bound.
`WorkController::wait_for_pressure` can use PSI, database-pool availability,
JetStream backlog, or a caller-defined signal. A caller resumes from the last
durably persisted cursor after the pressure clears.

An operation must distinguish these outcomes:

- `Completed`: all work in its declared scope was processed;
- `Partial(reason)`: an explicit budget stopped the pass before scope completion;
- `Cancelled`: an operator or supervisor cancellation stopped it;
- `Failed`: the caller observed an operational error and marked it as such.

Destructive callers must perform `destructive_boundary_check()` immediately
before mutation and re-read their authority there. A runtime limit, an mtime
grace period, or an old snapshot is never sufficient authorization to delete.

## Integration census

| Operation | Current relationship to the controller | Required next step |
| --- | --- | --- |
| CAS fsck / orphan reconciliation | Migrated. The normal route is unlimited by default; explicit bounded passes report incomplete and `--apply` refuses partial deletion. | Stream the filesystem enumeration/status output and persist a cursor before making CAS cleanup resumable at very large scale. |
| CAS GC | Reuses the CAS fsck route for the local backend; legacy git-annex remains a separate external command. | Give external-command execution the same cancellation, admission, and outcome envelope. |
| Source replay / historical import | Not migrated. Source and automaton checkpoints have their own domain-specific durability contracts. | Adapt the controller around existing source/automaton cursors only after the durable-emission receipt and replay-scope contracts are closed; do not replace those authorities with a generic counter. |
| Projection rebuild / invalidation | Not migrated. Projection registry and invalidation state are the authority. | Add controller progress as an execution layer over projection scope, preserving registry freshness and invalidation semantics. |
| Schema apply / backfill | Not migrated. `xtask` and schema backfill state own these operations. | Add an adapter at the `xtask` operation boundary, with database statement timeouts and resumable relation/keyset cursors. |
| Snapshot / restore | Not migrated. Snapshot manifests and component verification own correctness. | Use controller progress for scheduling and cancellation, while manifest/component hashes remain the authority. |
| Tombstone / purge / other destructive lifecycle operations | Not migrated. Operation state, tombstone authority, and final rechecks own safety. | Share admission/cancellation/progress only after each mutation path retains its transaction and final authority recheck. |

The census is intentionally explicit: “uses a progress-like field” is not
treated as “uses the common controller,” and a generic budget must not weaken a
domain-specific durability or deletion invariant.
