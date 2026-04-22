## Deployment Readiness

Canonical deployment and host-activation follow-up is tracked in GitHub under:

- `#308` — Core hardening follow-up: SDK, test harness, runtime proof boundaries.

Scratch notes are temporary investigation material only; do not treat `.agent/scratch/` as a
durable deployment backlog.

**Current state as of 2026-04-22:** `sinex.enable = true; provisionDatabase = true` is deployed on
`sinnix-prime`. The host has active systemd units for the gateway, ingest daemon, filesystem,
system, terminal, desktop, browser, canonicalizer, analytics, health automaton, and session
detector. Target-user access preparatory units are active/exited for desktop, terminal, browser,
and document scan surfaces.

The trustworthy gap is no longer "can the services start?" or "do target-user bridges exist?".
Runtime-target descriptors, proof-carrying scenarios, evidence bundles, and resource-shape
benchmarks now exist as the repeatable proof spine. Remaining deployment work is narrower:
operator-visible derived-node telemetry, VM coverage representative of the deployed hardening
model, and the next transport/failure-policy decisions before broader source growth.

### Current Live Surface

| Component | Status | Notes |
|-----------|--------|-------|
| Gateway | active | Unit is running; status/readiness belongs to the explicit runtime-target/status snapshot surface from `#310`/`#322`. |
| ingestd | active | Source-material frame stream ordering and hot-path batching are deployed. |
| Filesystem node | active | Metadata-only and zero-byte observations now use SDK buffered append streams rather than one material per event. |
| System node | active | Startup historical import is bounded off the continuous path; journal IDs are deterministic UUIDv7 with valid variant bits. |
| Terminal node | active | Continuous watchers bootstrap from live tail and source records use SDK append-stream anchors. |
| Desktop node | active | Target-user bridge has been proven under systemd hardening. |
| Browser node | active | Startup replay was removed from snapshot mode; real dataset and host wiring hardening landed in `#320`. |
| Automata | active | Canonicalizer, analytics, health, and session-detector units are running; output quality/lag/budget proof landed in `#321`, operator-visible telemetry remains `#334`. |
| Schema apply | active/exited | Declarative schema apply unit is present and has run under systemd. |

### Recently Closed Deployment Risks

| Risk | Closure |
|------|---------|
| Browser snapshot blocked systemd readiness with large static replay | Snapshot startup no longer performs historical replay. |
| Material lifecycle frames arrived out of order across separate streams | SDK now publishes ordered source-material frames through one stream family. |
| Tiny source records caused material-frame and git-annex pressure | SDK append streams batch logical records and route small materials through local CAS. |
| Unbounded journal import and coredump pressure froze the host | Continuous startup no longer imports all journal history; system node uses bounded historical scans. |
| Invalid producer UUIDs poisoned ingestd COPY batches | ingestd rejects malformed UUIDv7 variants; system producer emits deterministic RFC4122 UUIDv7 IDs. |
| Duplicate BLAKE3 blob inserts caused persistence retry loops | Blob repository deduplicates by BLAKE3. |
| Checkout-local status confused dev/prod health | Runtime targets and consolidated status snapshots landed through `#310`/`#311`/`#322`. |
| Historical/browser proof relied on host forensics | `#319`/`#320` proved those paths through the normal node/runtime plane. |
| Runtime incidents lacked reusable evidence | `#485`/`#316`/`#315`/`#317` added proof catalog, evidence bundles, source scenarios, and resource-shape benchmarks. |

### Remaining Proof Work

| Gap | Tracking |
|-----|----------|
| Operator-visible derived-node health and telemetry | `#334` |
| Session detector deployment/readiness follow-through | `#329` |
| Deployment-hardening and target-user bridge VM coverage | `#318`, `#234` |
| Publish intent, DLQ/failure routing, and drain semantics | `#326`, `#327`, `#338` |
| Schema-source bundle and late-arriving temporal decisions | `#233`, `#325` |

### Service User Permission Model

The sinex service user (uid=991) runs all services. The target user (sinity, uid=1000) owns the data.

| Resource | sinex access? | Why |
|----------|---------------|-----|
| `/realm/project/*` | YES | World-readable project roots. |
| systemd journal | YES | System node consumes journal data under service hardening. |
| Hyprland socket (`/run/user/1000/hypr/`) | YES | Desktop node emits source-material traffic under the target-runtime bridge. |
| Atuin DB (`~/.local/share/atuin/history.db`) | YES | Terminal node reads target-home history paths after ACL-mask fixes. |
| Browser history roots | YES | Browser target-access unit is active/exited; dataset correctness remains issue-tracked. |
| `/home/sinity` broadly | NO | ProtectHome-style restrictions remain intentional; access is via explicit bridges. |
