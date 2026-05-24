# Prompt Router And Budget Ledger

**Status:** dissolved into issue tracking. The substantive contract
that lived here — `ModelTaskRequest`/`RoutingDecision` Rust contracts,
the `llm.prompt_templates`/`routing_policies`/`routing_decisions`/
`budget_ledger` schema, the "callers ask the router; never hardcode
prompt bodies / provider / model" mandatory invariant, deterministic
bucket hashing for A/B + canary, privacy-result routing table,
relation-to-other-systems table, first-slice steps, and the boundaries
list — now lives in [issue #1116 (feat(llm): add prompt registry
router and budget ledger)](https://github.com/Sinity/sinex/issues/1116)
as a design comment.

`#1116` is the live tracking issue. Replay/model-effect cache is
`#1063`. Shadow comparison across model/prompt versions is `#1109` +
`docs/architecture/semantic-epochs-shadow-lanes.md`.

The contract sits **above** recorded model effects and **below** the
proposal/judgment/finalizer authority (canonical promotion).

**Related:** `docs/architecture/proposal-judgment-finalizer.md`,
`docs/architecture/semantic-epochs-shadow-lanes.md`,
`docs/architecture/inference-decision-metadata.md`,
`docs/architecture/embedding-runtime.md`.
