# Instruction And Actuator Loops

**Status:** dissolved into issue tracking. The substantive contract
that lived here — instructions-as-typed-events decision (rejecting
arbitrary-command-payloads / observation-with-intent-flag / gateway-
RPC-only / DB-command-table-only), the
Instruction/ActuationAttempt/ExpectationStatus payload shapes, four
event families (`instruction.requested` / `actuation.attempted` /
`instruction.fulfilled` / `instruction.failed`), five authority
classes with the mandatory "model_suggested is not executable without
proposal/judgment" rule, the 9-step loop-prevention checklist, the
"reconciler not actuator decides fulfillment" load-bearing invariant,
QoS + privacy table, and the Hyprland workspace-switch proof slice —
now lives in [issue #1104 (design(runtime): define instruction events
and actuator loops)](https://github.com/Sinity/sinex/issues/1104) as
a design comment.

`#1104` is the live tracking issue. The proposal/judgment/finalizer
substrate that gates model-suggested actions is owned by
`docs/architecture/proposal-judgment-finalizer.md`. The runtime
private-mode that can block actuation is owned by
`docs/architecture/runtime-private-mode.md`. Hyprland workspace
instruction proof has its own implementation issue at `#1349`.

**Related:** `docs/architecture/proposal-judgment-finalizer.md`,
`docs/architecture/runtime-private-mode.md`,
`docs/architecture/operator-surfaces.md`,
`docs/architecture/runtime-qos.md`.
