# Resource Budget Patch Mining (#1886)

The turbo resource-budget patch was useful as behavior guidance, but not as a
literal public model. Current Sinex has package-level `ResourceBudgetSpec`
under `source_contracts`; the accepted work is about making runtime pressure
visible and bounded without creating a parallel scheduler vocabulary.

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

## Reconciled

The #1886 patch-mining trail is reconciled by the controlled confirmation-buffer
and journald-feedback regression harness. That harness is the accepted local
closure surface for the original incident while production Sinex remains stopped
for host-RAM reasons; it does not require restarting the live deployment.

Future live-service load testing is a new performance investigation only if the
operator intentionally restarts the production stack and observes a fresh memory
growth pattern. Resource-pressure cleanup should start from fresh runtime
evidence and attach it to the owning runtime/package-completeness surface
instead of reopening the stale patch vocabulary here.
