# Event Validator

`validator.rs` centralises payload and schema checks before events hit storage.

- Resolves schema metadata through `sinex-schema` and caches lookups.
- Applies per-event validation and accumulates `ValidationStats`.
- Surfaces actionable failure messages for satellites while preserving security
  boundaries.

Whenever schema contracts change, update this documentation alongside the
validation flows so operators understand the new guardrails.
