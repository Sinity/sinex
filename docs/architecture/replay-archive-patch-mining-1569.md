# Replay Archive Patch Mining (#1569)

The #1569 re-audit report was accepted for the replay/archive orphan-window
slice, not as a mandate to create a second projection-debt model or bypass the
operation/debt recovery path. This note is patch-mining history; current
runtime truth lives in the replay writer, operation state machine, and
verification gates.

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

- Do not add a second derivation or projection-debt model here. #1901 owns the
  unified `DebtListView`; #1903 owns `DerivationSpec`.

## Current State

- TTL archive has the same split plan/archive shape and should reuse the
  transaction-scoped archive helper if it grows beyond the existing archive
  path.
- Replay/archive invalidation recovery is no longer tracked by this historical
  patch-mining note. The live contract is durable operation metadata plus the
  existing operation/debt recovery surface; stale source comments should point
  there rather than to a deployment-inactive scan-loop decision.
