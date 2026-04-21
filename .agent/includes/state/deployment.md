## Deployment Readiness

Canonical deployment and host-activation follow-up is tracked in GitHub under:

- `#308` — Core hardening follow-up: SDK, test harness, runtime proof boundaries.

Scratch notes are temporary investigation material only; do not treat `.agent/scratch/` as a
durable deployment backlog.

**Current state as of 2026-04-21:** `sinex.enable = true; provisionDatabase = true` is deployed on
`sinnix-prime`. The host has active systemd units for the gateway, ingest daemon, filesystem,
system, terminal, desktop, browser, canonicalizer, analytics, health automaton, and session
detector. Target-user access preparatory units are active/exited for desktop, terminal, browser,
and document scan surfaces.

The trustworthy gap is no longer "can the services start?" or "do target-user bridges exist?".
The remaining deployment work is to make proof boundaries explicit and repeatable: which runtime
target is being checked, which stack produced the evidence, which source paths were exercised, and
whether derived outputs are current and useful.

### Current Live Surface

| Component | Status | Notes |
|-----------|--------|-------|
| Gateway | active | Unit is running; status/readiness should be reported through the explicit runtime target work in `#310`/`#322`. |
| ingestd | active | Source-material frame stream ordering and hot-path batching are deployed. |
| Filesystem node | active | Metadata-only and zero-byte observations now use SDK buffered append streams rather than one material per event. |
| System node | active | Startup historical import is bounded off the continuous path; journal IDs are deterministic UUIDv7 with valid variant bits. |
| Terminal node | active | Continuous watchers bootstrap from live tail and source records use SDK append-stream anchors. |
| Desktop node | active | Target-user bridge has been proven under systemd hardening. |
| Browser node | active | Startup replay was removed from snapshot mode; real dataset hardening remains tracked in `#320`. |
| Automata | active | Canonicalizer, analytics, health, and session-detector units are running; output quality/lag/budget proof remains tracked in `#321`. |
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

### Remaining Proof Work

| Gap | Tracking |
|-----|----------|
| Explicit dev vs deployed runtime target descriptors | `#310`, `#311`, `#322` |
| Historical backfill through the normal node/runtime plane | `#319` |
| Browser-history real dataset and host wiring hardening | `#320` |
| Automata derived-output quality, lag, and runtime budgets | `#321`, `#263`, `#325` |
| Runtime/system pressure scenarios in tests and benchmarks | `#315`, `#316`, `#317`, `#318`, `#324` |

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
