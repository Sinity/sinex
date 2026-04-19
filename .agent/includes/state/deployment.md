## Deployment Readiness

Canonical deferred deployment/host-activation backlog lives in:

- `.agent/scratch/041-advanced-horizon-plan.md`

**Current state:** `sinex.enable = true; provisionDatabase = true` on `sinnix-prime`, and the
host has been switched successfully under the checked-in NixOS module graph. The trustworthy gap
is no longer "can the services start?" or "can the target-user bridges be established?" but
"has the live prod stack been proven from a clean persisted-smoke/query loop?".

A clean local development proof now exists: `xtask run core` plus real terminal/filesystem/gateway
traffic were observed through `NATS -> ingestd -> Postgres -> sinexctl query` on the dev stack.
On-host, gateway readiness is healthy, `/run/agenix/sinex-gateway-admin-token` materializes, and
the desktop/terminal services now emit real source-material traffic under systemd hardening.

### What Works Now

| Component | Status | Notes |
|-----------|--------|-------|
| Compilation | 0 errors, 0 warnings | Clean workspace |
| FS ingestor | FIXED — needs rebuild | OOM-killing at 256M; NixOS module raised to 1G |
| System ingestor | ACTIVE in prod | 386K dbus signals in 17h on sinnix-prime |
| ingestd | ACTIVE in prod | 225K+ events processed; sd_notify support present |
| gateway | FIXED — needs rebuild | Was dead (empty agenix token); re-encrypted in sinnix repo |
| Schema apply | READY | `sinex-schema-apply.service` exists in both NixOS paths |
| sinexctl | READY | query, trace, telemetry, context, report, import subcommands |
| Local end-to-end proof | VERIFIED | gateway + terminal + filesystem traffic persisted on the dev stack and was queried back through `sinexctl` |
| Desktop ingestor on host | ACTIVE in prod | 163 window.focused + 100K+ unhandled dbus signals; activewindowv2 race fixed |
| Terminal ingestor on host | ACTIVE in prod | 73K commands from Atuin in 17h on sinnix-prime |
| Automata event bridge | FIXED | `DerivedNodeAdapter` now uses confirmation stream bridge (was stuck in invalidation-only loop) |
| Production pipeline | PROVEN | 562K real events in sinex_prod from 3 ingestors over 17h |

### What's Still Blocking Trusted Production

| Component | Blocker | Fix |
|-----------|---------|-----|
| Gateway smoke | Token was empty; token fixed in sinnix. Gateway dead until rebuild. | `nixos-rebuild switch` then `sinexctl gateway ingest → sinexctl query` |
| FS ingestor stability | OOM-killing; MemoryMax fix in NixOS module needs rebuild. | `nixos-rebuild switch` |
| Production historical-path proof | Terminal/desktop historical scans not yet query-verified on prod. | Re-run historical scans post-rebuild and query resulting rows. |
| Operator telemetry rollout | The deployed host still has the stale operator telemetry schema. Root cause was invalid continuous aggregates over the `id`-partitioned hypertable, not a routing gap. | Deploy the updated schema apply that switches the six `_1h` operator surfaces to hourly `ts_coided` views. |

### Activation Sequence (Critical Path)

```
Phase 0: switch host with checked-in `sinex.enable = true` graph
         Verify: managed units active, `/ready` healthy, admin token materialized
Phase 1: prove clean local pipeline end to end
         Verify: gateway + terminal + filesystem traffic queryable on dev stack
Phase 2: prove host access bridges under real hardening
         Verify: desktop/terminal create source material on `sinnix-prime`
Phase 3: clean prod persisted smoke
         Verify: real gateway ingest is queryable back from `sinex_prod`
Phase 4: historical backfill on the node/runtime plane
         Verify: desktop/terminal historical rows land through normal pipeline
Phase 5: stabilize (monitor DLQ, batch latency, node health)
```

### Service User Permission Model

The sinex service user (uid=991) runs all services. The target user (sinity, uid=1000) owns the data.

| Resource | sinex access? | Why |
|----------|---------------|-----|
| `/realm/project/*` | YES | World-readable (755) |
| systemd journal | YES | journald API access |
| Hyprland socket (`/run/user/1000/hypr/`) | **YES (live host proof)** | Desktop ingestor emits real source-material traffic under the target-runtime bridge |
| Atuin DB (`~/.local/share/atuin/history.db`) | **YES (live host proof)** | Terminal ingestor reads the target-home history paths after the ACL-mask fix |
| `/home/sinity` | **NO** | ProtectHome=true on most services |

### Evolution Phases

```
A (done)   → Activation: FS + system ingestors give file changes + journal events
B (done)   → Full capture bridges: terminal + desktop host access proved
C (done)   → 562K real events in prod; automata bridged to event stream
D (next)   → Gateway smoke + FS OOM fix via rebuild; historical backfill proof
E (days)   → Intelligence: session detector deploy; monitor automata derived output
F (weeks)  → Semantic: embedding pipeline, hybrid search
```
