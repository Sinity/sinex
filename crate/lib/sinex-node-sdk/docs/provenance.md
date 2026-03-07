# Ingestion & Provenance Patterns
> Last Verified: 2025-12-02 (manual review)
> **Purpose:** Working guide for provenance rules, node/ingestor boundaries, and Stage-as-You-Go patterns.

This note distils the actionable architecture guidance from longer-form
Sinex architecture essays into a concise reference for engineers working on nodes,
ingestors, and automata. For operational notes and deployment expectations,
cross-reference `docs/current/architecture/Core_Architecture.md` and `nixos/README.md`.

## 1. Sensor / Ingestor Separation

| Layer  | Responsibility | Examples |
|--------|----------------|----------|
| **Sensor** | Generic byte acquisition (sockets, files, subprocess pipes, HTTP polling). Handles reconnection, buffering, rate limiting. | `sinex-sensor-socket`, `sinex-sensor-file`, `sinex-sensor-api`, `sinex-sensor-subprocess` |
| **Ingestor** | Source-specific parsing that transforms raw slices into structured `core.events`. Owns schema validation, timestamp derivation, and provenance links. | `sinex-hyprland-ingestor`, terminal command ingestor, clipboard ingestor |

Key rules:
- Sensors are reusable libraries or daemons; never bake socket/file handling into the ingestor itself.
- Ingestors must remain deterministic and idempotent: given the same source
  material slice, they emit the same event payload and provenance metadata.

## 2. Stage-as-you-go Pattern

Live streams (sockets, tailing files) must keep provenance intact while minimising
latency. Follow this template:

1. **Start-up:** create an "in-flight" entry in `raw.source_material_registry`
   with `status = 'sensing'` and `checksum = NULL`.
2. **Emit events immediately:** as bytes arrive, ingestors emit events that point
   to the in-flight source material (`source_material_id`). Use the
   `offset_start`, `offset_end`, and `anchor_byte` columns to point to the exact
   byte range.
3. **Periodic commit:** on interval or shutdown, flush the buffered bytes into
   git-annex, compute the checksum, update the registry record, and create a new
   in-flight entry for the next segment.
4. **Re-scan at boot:** continuous ingestors perform a "scan-on-startup"
   against their watch directories since the last checkpoint before returning to
   live mode.

## 3. Timestamp Taxonomy

Ingestors must document how they derive `ts_orig` (the "happened at" time):

- `intrinsic` – the source material carries reliable timestamps (e.g. Atuin
  database dumps). Use those directly.
- `external_wrapper` – Sinex added framing metadata when staging (e.g. prepended
  timestamp in a streamed chunk); strip the wrapper to recover `ts_orig`.
- `inferred` / `none` – inputs with no trustworthy timestamp;
  fall back to ordered heuristics: operator override → file mtime → staging time.

## 4. Instructional Events & Actuators

Whenever Sinex emits `command.*` events, the same provenance guarantees apply:

- Actuator nodes subscribe to the command namespace and publish their own
  observational events (e.g. `desktop.workspace.switched`) to record outcomes.
- Commands are treated as first-class events with typed payloads; the absence
  of a separate "intent" field keeps observation and instruction symmetric.
- Always log success/failure via follow-up events so the operations log
  (`core.operations_log`) explains what happened.

## 5. Checklist for New nodes

- [ ] Sensor boundary defined; reusable code extracted into `sinex-sensor-*` crates.
- [ ] Stage-as-you-go implementation documented and tested.
- [ ] Checkpoint + replay story proven (integration test with the `#[sinex_test]`
      macro).
- [ ] Event payload schema registered with validation coverage.
- [ ] Provenance columns populated and invariants enforced (`UNIQUE(source_material_id, anchor_byte)`).
- [ ] Security review performed using the guardrails in `docs/current/security.md`
      (auth, TLS, secrets, data hygiene).

Keeping these rules crisp prevents the architecture from collapsing back into a
ball of bespoke ingest scripts.
