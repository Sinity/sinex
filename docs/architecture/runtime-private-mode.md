# Runtime Private Mode

Status: architecture contract for #1071. Implementation first slice is tracked
in #1353.

Private mode is an operator-controlled runtime suppression state. It prevents
live capture from creating candidate material or events where possible. It is
not a redaction engine, not a raw-material retention policy, and not evidence
that a specific private activity happened.

## State Shape

The runtime state should be represented as:

```json
{
  "enabled": true,
  "reason_class": "operator_private",
  "actor": "sinity",
  "started_at": "2026-05-17T05:00:00Z",
  "expires_at": null,
  "affected_source_classes": ["desktop", "clipboard", "terminal"],
  "updated_by_operation_id": "018f..."
}
```

Field rules:

- `enabled`: authoritative boolean.
- `reason_class`: coarse class only. Avoid detailed reasons that defeat
  deniability.
- `actor`: user, operator, deterministic policy, or test fixture.
- `started_at` / `expires_at`: interval used by live services and continuity
  caveats.
- `affected_source_classes`: empty means all live capture classes.
- `updated_by_operation_id`: links to an operation/audit record when available.

## Authority And Persistence

The state must survive service restart. Initial persistence can be a state file
under the runtime state directory plus an operation/audit record for toggles.
Services must load current state before resuming live capture.

Gateway/CLI may toggle and query the state. Source nodes consume the state; they
do not decide policy by themselves.

## Capture Behavior

Live producers check private mode:

1. before source acquisition;
2. before durable source-material transport;
3. before event-intent publication.

When private mode covers a source class:

- high-sensitivity sources fail closed if state cannot be read;
- ordinary sources may fail closed or degrade according to their declared
  privacy tier and runtime policy;
- already-staged finite parser jobs continue unless private mode explicitly
  scopes replay/process operations;
- dropped/suppressed capture should create only coarse continuity evidence when
  policy permits it.

## Deniability And Caveats

Private mode explains absence, but should not expose detailed absence. A
continuity caveat may say "capture intentionally suppressed" for a broad window
without recording what would have been captured or why.

Do not create per-source detailed omission events by default. If an operator
needs more auditability for a source, that should be explicit policy and not the
default private-mode behavior.

## Composition With Other Privacy Layers

Private mode composes with, but does not replace:

- admission privacy policy (#1042);
- source-material/raw retention policy (#1065);
- admitted event envelope privacy/admission checks (#1064);
- parser field-level `#[suppress_if]` behavior;
- source readiness and continuity reporting.

Private mode is the earliest runtime gate. If capture still emits material or
events because the source was out of scope, normal privacy/admission policy must
still run.

## Verification

First implementation must prove:

- enable/disable/query through gateway or `sinexctl --json`;
- restart loads state before source acquisition;
- one high-sensitivity producer suppresses capture when enabled;
- one ordinary producer follows its declared policy;
- continuity/readiness surfaces classify the interval as private-mode caveated
  without leaking detailed suppressed content;
- state-read failure on high-sensitivity paths fails closed.

## Non-Goals

- Do not implement per-field redaction or encryption here.
- Do not define raw-material retention classes here.
- Do not make private mode a proof that activity occurred.
- Do not force already-staged finite parser jobs to stop unless the operator
  explicitly scopes the state to processing/replay.
