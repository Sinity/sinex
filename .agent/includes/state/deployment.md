## Deployment

**Current state:** `sinex.enable = true; provisionDatabase = true` deployed on `sinnix-prime`.
All ingestors, automata (via `sinex-process` per #944), ingestd, and gateway are live under systemd.
Source-worker host (#1054/#1081/#1223) provides the unified per-source-unit dispatch path; legacy
per-ingestor crates were removed in Wave-B fold.

### Current Live Surface

| Component | Status | Notes |
|-----------|--------|-------|
| Gateway | active | JSON-RPC + SSE. Auth via token-suffix RBAC (no revocation — token rotation only). |
| ingestd | active | Batch writes, COPY protocol for ≥50-event batches, validation. |
| Source-worker host | active | Per-source-unit dispatch; 14 workspace members post-fold. |
| sinex-process | active | 6 automata via per-automaton systemd services. Telemetry wired via DerivedNodeAdapter. |
| Schema apply | active/exited | Declarative convergence via `sinex-schema apply`. |
| Backup (#945) | wired | WAL archiving + pg_basebackup hooks landed; operator supplies `walArchiveCommand`. |
| CAS GC (#987) | wired | Delete-on-tombstone landed; size limits enforced. |
| NixOS hardening (#990) | wired | `RestrictSUIDSGID`, `PrivateIPC`, pool-size, restart rate-limiting. |
| Ingestor health (#1009) | wired | Periodic health emission in `IngestorNodeAdapter::run_continuous`. |
| Settlement (#1010) | wired | `FailurePolicy::settle()` in production error path. |
| Derived-node telemetry | active | Prongs 1+2 landed (#1243, #1250). #1241 awaits live DLQ verification. |

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
| Wayland/Hyprland bridge for desktop source units | #1234 |
| Real DbusBackend (currently returns Err) | #1235 |
| Native-adapter fs fold pending SDK extension | #1224 |
| Live deploy-side verification of derived-node telemetry | #1241 |
| Production-shaped VM proof suite | #1135, #1132 |
