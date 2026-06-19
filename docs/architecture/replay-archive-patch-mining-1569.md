# Replay Archive Patch Mining (#1569)

The #1569 re-audit report is accepted for the replay/archive orphan-window
slice, not as a mandate to wire or delete deployment-inactive invalidation code.

## Accepted

- Derived event inserts must reject `source_event_ids` that do not refer to live
  `core.events` rows. Array lineage is not protected by a database FK, so the
  repository must validate it before admitting derived rows.
- Replay cascade expansion, scope metadata collection, and archive deletion must
  happen inside one database transaction. The replay saga must not hold that
  transaction open across NATS invalidation publish or source scan request/reply.
- Replay writer comments should state the real boundary: DB archive membership
  is atomic; post-commit invalidation delivery remains operation/debt work.

## Rejected

- Do not wire the deployment-inactive `adapter/invalidate.rs` path in this PR.
  The runtime still dispatches through the event bridge, so wiring/deleting that
  path is a separate product/runtime decision.
- Do not add a second derivation or projection-debt model here. #1901 owns the
  unified `DebtListView`; #1903 owns `DerivationSpec`.

## Remaining

- TTL archive has the same split plan/archive shape and should reuse the
  transaction-scoped archive helper in a follow-up.
- Post-archive invalidation delivery failures should surface through
  `OperationView` or unified projection debt rows once the replay correctness
  boundary is stable.
