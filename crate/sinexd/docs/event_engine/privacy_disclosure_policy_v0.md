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
- `telemetry`: metrics, traces, and OpenTelemetry-compatible projections.

`sinexctl timeline`, `sinexctl events recent`, `sinexctl events errors`, SSE payload subscriptions, and the TUI recent-event panel consume the server-side `events.cards`/SSE view paths, so they inherit the same disclosure decisions and caveats instead of rebuilding cards from raw query DTOs. DLQ previews call `DisclosureContext::Dlq` over untyped JSON and only apply global operator rules. Completion and telemetry surfaces do not sample raw payload values: completions expose command grammar, source IDs, event types, and schema keys; OpenTelemetry-compatible projections expose refs, counts, timings, and bounded aggregate attributes with an explicit disclosure boundary. v0 applies the same operator-authored DB rules wherever a surface renders event/DLQ content; destination-specific rule scopes should be added as a policy-schema extension rather than hard-coded at call sites.

## Destination matrix

| Destination | Disclosure path | Evidence |
| --- | --- | --- |
| View/API/TUI/MCP | `crate/sinexd/src/api/handlers/query.rs::event_card_list_with_policy` applies `DisclosureContext::View` to payloads and snippets before building `EventCardView`; `crate/sinexd/src/api/sse_handler.rs::disclose_and_match_sse_event` applies the same context before SSE filtering/emission. | `disclosure_contexts_cover_operator_destinations`; `payload_subscription_matches_disclosed_event_not_raw_secret`; event-card caveats point to `privacy.policy.list`. |
| Export/artifact | `crate/sinexctl/src/commands/privacy.rs::build_privacy_export_report` emits a metadata-only export receipt with `disclosure_context = "export"` and explicit `payload_redacted` / `snippet_redacted` flags instead of serializing raw payload or snippet fields. | `privacy_export_renderers_omit_payload_and_snippet_material`; `privacy_export_requires_explicit_scope`. |
| Log/error | Log-bound malformed confirmation diagnostics in `crate/sinexd/src/api/sse_bus.rs::parse_confirmation` and `crate/sinexd/src/event_engine/jetstream_consumer.rs::process_confirmation_retry_batch` report length plus BLAKE3 fingerprints for invalid operator-supplied IDs, not raw field values. Structured logs may still include persisted event UUID refs, source IDs, event types, counts, sizes, and timings. | `parse_confirmation_reports_invalid_event_id`; `payload_fingerprint_does_not_disclose_raw_log_bytes`; `disclosure_safe_fingerprint_omits_raw_confirmation_retry_id`. |
| Completion/prompt | `crate/sinexctl/src/commands/completion_endpoint.rs::payload_schema_keys` derives payload completions from static schema keys after source/type selection; candidates do not query stored event payload values. | `payload_key_completion_exposes_schema_keys_not_values`. |
| DLQ/quarantine | `crate/sinexd/src/api/handlers/dlq.rs::payload_preview` calls `PolicyEngine::disclose_json_value(..., DisclosureContext::Dlq)` over untyped payload previews, carries `payload_redacted`, and attaches `policy.disclosure_applied` caveats. Scoped source/type rules are not promoted to unknown DLQ bytes. | `payload_preview_redacts_raw_dlq_secret_bytes_by_db_policy`; `disclosure_policy_leak_fixture_covers_event_cards_and_dlq`; `payload_preview_truncates_without_breaking_unicode`. |
| Telemetry/OTel | `crate/sinex-primitives/src/otel_projection.rs::gateway_stats_to_otel_metrics_projection` projects gateway telemetry into counts, timings, stable refs, and bounded aggregate attributes only; the DTO carries `OtelDisclosureBoundary` listing omitted raw/material/text attribute families. | `gateway_stats_projection_uses_refs_counts_and_timings_not_raw_payloads`; #1967 owns broader OTel surface expansion. |

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
- Log/error helpers must use structured metadata or explicit disclosure before rendering user/material/event text.
- Destination-specific rule bindings should extend the policy schema so operators can choose different actions for view/export/log/completion/DLQ without runtime hard-coding.
- Field metadata and source package disclosure defaults should compile into DB policy seed/proposal records rather than becoming hard-coded runtime behavior.
