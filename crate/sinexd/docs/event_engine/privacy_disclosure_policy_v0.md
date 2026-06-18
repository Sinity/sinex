# Privacy disclosure policy v0 (#1693)

This slice separates two privacy moments that were previously easy to blur:

1. **Admission policy** may mutate an admitted event payload before persistence.
2. **Disclosure policy** decides what a specific operator surface may display from already-stored data.

The runtime path is intentionally operator-owned. It uses the DB-backed privacy policy rules and field bindings managed through `privacy.policy.*`; it does not add hard-coded source censorship and it does not treat `source` as privacy authority. Source and event type remain selectors for operator-authored rules only.

## Contexts

`PolicyEngine::disclose_*` accepts a finite `DisclosureContext`:

- `view`: event cards, timelines, TUI panels, MCP/read views.
- `export`: files or artifacts that may leave the live runtime boundary.
- `log`: process diagnostics and debug output.
- `completion`: shell completions and prompt adornments.
- `dlq`: dead-letter queue previews where source/type may be unavailable.

Only `view` and `dlq` are wired in this patch. `sinexctl timeline`, `sinexctl events recent`, `sinexctl events errors`, and the TUI recent-event panel consume the server-side `events.cards` view path, so they inherit the same disclosure decisions and caveats instead of rebuilding cards from raw query DTOs. The other contexts exist so follow-on surfaces cannot invent parallel privacy vocabularies. v0 applies the same operator-authored DB rules in every disclosure context; destination-specific rule scopes should be added as a policy-schema extension rather than hard-coded at call sites.

## Runtime behavior

For structured events, `PolicyEngine::disclose_event_payload(event, DisclosureContext::View)` clones the event, applies matching DB rules against its payload, and returns a `DisclosureDecision`. `PolicyEngine::disclose_event_text(event, text, DisclosureContext::View)` applies every operator rule whose source/type selector matches the event to derived snippet text, including field-bound rules, because snippets no longer carry JSON pointers. `events.cards` builds `EventCardView` objects from the disclosed event clone, so payload previews and snippet-derived summaries are sanitized before rendering.

For DLQ previews, `PolicyEngine::disclose_json_value(value, DisclosureContext::Dlq)` applies only global DB rules. Scoped rules are not silently promoted to untyped DLQ bytes; if an operator needs DLQ-wide handling, they bind a global rule.

Every content change returns:

- `PrivacyStateView { state: redacted|suppressed, reason: ... }`.
- A `policy.disclosure_applied` caveat.
- A `Policy` object reference with command/RPC hints for `privacy.policy.list`.

This means suppression or redaction is visible in the same finite view envelope/card vocabulary that other operator caveats use. Raw stored data is not silently removed by the view path.

## Operator-control model

Operators control disclosure by creating rules and scopes:

- `privacy.policy.rule.add` defines the matcher and action.
- `privacy.policy.scope.bind` or `privacy.policy.field.bind` binds source/type/field selectors.
- `privacy.policy.rule.set_enabled` can disable policy without code changes.
- `privacy.policy.list` explains the active rule set for audit and UI caveat drill-down.

Actions keep their existing meaning: redact, mask, hash, encrypt, or suppress. The disclosure evaluator reports that a policy was applied; the rule/action remains owned by the DB policy repository.

## Debt and caveat integration points

This patch wires caveats into `EventCardView.caveats` and `DlqMessagePeek.privacy_caveats`. That is the first debt hook: any future `CaptureDebt` or `AdmissionDebt` projection can count `policy.disclosure_applied` by surface, rule, source family, and context.

Immediate residuals:

- `events.query` still returns raw `EventQueryResult` because that DTO has no caveat/privacy-state channel. `sinexctl timeline`, `sinexctl events recent`, `sinexctl events errors`, and the TUI recent-event panel now prefer `events.cards`; other operator-facing clients should do the same until the raw query result grows an explicit disclosure envelope or is split into a raw/admin method.
- `export`, `log`, and `completion` contexts are vocabulary only in this patch. They must call `PolicyEngine::disclose_*` before rendering sensitive values.
- Destination-specific rule bindings should extend the policy schema so operators can choose different actions for view/export/log/completion/DLQ without runtime hard-coding.
- Field metadata and source package disclosure defaults should compile into DB policy seed/proposal records rather than becoming hard-coded runtime behavior.
