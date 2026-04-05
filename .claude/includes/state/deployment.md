## Deployment Readiness

Canonical deferred deployment/host-activation backlog lives in:

- `.claude/scratch/041-advanced-horizon-plan.md`

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
| FS ingestor | READY | `/realm` is btrfs mount (755), `ProtectHome=read-only` override exists |
| System ingestor | READY | journald API access works for service user |
| ingestd | READY | sd_notify support added |
| gateway | READY | sd_notify added, `Type=simple` override in NixOS |
| Schema apply | READY | `sinex-schema-apply.service` exists in both NixOS paths |
| sinexctl | READY | query, trace, telemetry, context, report, import subcommands |
| Local end-to-end proof | VERIFIED | gateway + terminal + filesystem traffic persisted on the dev stack and was queried back through `sinexctl` |
| Desktop ingestor on host | VERIFIED | Hyprland/window-manager traffic creates and finalizes source material on `sinnix-prime` |
| Terminal ingestor on host | VERIFIED | target-home ACL bridge now permits `.zsh_history` and Atuin reads; fresh source material was created after host switch |
| Gateway admin token on host | VERIFIED | `/run/agenix/sinex-gateway-admin-token` is readable after switch |

### What's Still Blocking Trusted Production

| Component | Blocker | Fix |
|-----------|---------|-----|
| Production persisted smoke | The live prod host still needs a clean `events.ingest -> NATS -> ingestd -> Postgres -> query` proof after clearing earlier poisoned/raw backlog state | Reset the prod proof surface, ingest a smoke event through the real gateway, and query it back |
| Production historical-path proof | Terminal/desktop access and live source-material emission are now proved, but the host still lacks a clean proof for historical backfill behavior on the prod stack | Re-run terminal/desktop historical scans on the cleaned prod environment and query the resulting rows |

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
C (next)   → Clean prod persisted smoke + historical proofs
D (days)   → Intelligence: entity extractor, session detector (SDK complete, logic vacant)
E (weeks)  → Semantic: embedding pipeline, hybrid search
```
