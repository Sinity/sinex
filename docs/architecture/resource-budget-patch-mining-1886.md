# Resource Budget Patch Mining (#1886)

The turbo resource-budget patch was useful as behavior guidance, but not as a
literal public model. Current Sinex already has package-level
`ResourceBudgetSpec` under `source_contracts`, and #1886 is about making runtime
pressure visible and bounded without creating a parallel scheduler vocabulary.

## Accepted

- Confirmation-buffer retained payload bytes are bounded separately from event
  count in `feature/runtime/confirmation-payload-budget-1886`.
- Raw-ingest DLQ list responses carry a structured `DlqPressureSignal` so CLI,
  TUI, and RPC consumers can see pending messages, pending bytes, retry batch
  size, runtime action, and operator action without parsing prose.

## Rejected

- Do not add the stale standalone `resource_budget.rs` module from
  `sinex-resource-budget-turbo.patch`; it duplicates the current
  `source_contracts::ResourceBudgetSpec` contract.
- Do not make resource pressure authorize hidden disclosure changes, retention
  deletion, or admission bypass. Budget pressure may throttle, inspect, defer,
  or surface debt; policy remains owned by admission/privacy/operation paths.

## Remaining

The still-open #1886 work is memory-owner evidence: attribute live resident
memory and queued bytes to concrete runtime owners, then expose those snapshots
through the operator debt/coverage/operation surfaces without starting the
runtime by default.
