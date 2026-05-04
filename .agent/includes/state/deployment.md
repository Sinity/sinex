## Deployment

**Current state:** `sinex.enable = true; provisionDatabase = true` deployed on `sinnix-prime`.
All ingestors, automata (via `sinex-process` per #944), ingestd, and gateway are live under systemd.

### Current Live Surface

| Component | Status | Notes |
|-----------|--------|-------|
| Gateway | active | JSON-RPC + SSE. Auth via token-suffix RBAC (no revocation — token rotation only). |
| ingestd | active | Batch writes, COPY protocol for ≥50-event batches, validation. |
| Filesystem node | active | Metadata-only observations use SDK buffered append streams. |
| System node | active | Bounded historical import; deterministic UUIDv7 journal IDs. |
| Terminal node | active | Continuous watchers bootstrap from live tail. |
| Desktop node | active | Target-user bridge proven under systemd hardening. No watcher recovery loop (#992). |
| Browser node | active | Startup replay removed from snapshot mode. |
| sinex-process | active | 6 automata via per-automaton systemd services. Telemetry wired via DerivedNodeAdapter. |
| Schema apply | active/exited | Declarative convergence via `sinex-schema apply`. |

### Service User Permission Model

The sinex service user (uid=991) runs all services. The target user (sinity, uid=1000) owns the data.

| Resource | sinex access? | Why |
|----------|---------------|-----|
| `/realm/project/*` | YES | World-readable project roots. |
| systemd journal | YES | System node consumes journal data. |
| Hyprland socket (`/run/user/1000/hypr/`) | YES | Desktop node target-runtime bridge. |
| Atuin DB (`~/.local/share/atuin/history.db`) | YES | Terminal node after ACL-mask fixes. |
| Browser history roots | YES | Browser target-access units. |
| `/home/sinity` broadly | NO | ProtectHome; access via explicit bridges. |

### Known Deployment Gaps

| Gap | Tracking |
|-----|----------|
| Operator-visible derived-node telemetry not yet in `sinexctl`/status surfaces | #334 |
| Publish intent/DLQ/failure-routing/drain semantics not finalized | #326, #327 |
| VM coverage lags runtime-target model | #318 |
| No backup tooling (no WAL archiving, no pg_basebackup) | #945 |
| NixOS service hardening: `RestrictSUIDSGID` + `PrivateIPC` added to all service configs; git-annex gated behind `legacyAnnexData` | #990 |
| Local CAS has zero GC — disk fill guaranteed over time | #987, #848 |
