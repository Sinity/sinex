## Deployment Readiness

**Current state:** `sinex.enable = false; provisionDatabase = false` on sinnix-prime. Zero production events.
2.83M ActivityWatch events + 65K Atuin commands sit in parallel capture infrastructure. Only `sinexctl import atuin` exists (pipeline-bypassing). No ActivityWatch import path exists at all. SDK SQLite adapter needed for proper ingestor-driven import.

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

### What's Blocked (Host Proof, Not Core Rust)

| Component | Blocker | Fix |
|-----------|---------|-----|
| Desktop ingestor | `sinnix` now wires target-runtime bind mounts, but the dark host has not yet proven live Hyprland socket access end to end | First enabled-host proof on `sinnix-prime` |
| Terminal ingestor | `sinnix` now wires target-home bind mounts for Atuin/history access, but the dark host has not yet proven the service can read them | First enabled-host proof on `sinnix-prime` |
| Gateway admin token | agenix fallback path is wired, but the dark host has not yet proven `/run/agenix/sinex-gateway-admin-token` materialization | First enabled-host proof on `sinnix-prime` |

### Activation Sequence (Critical Path)

```
Phase 0: nixos-rebuild switch (pick up code changes)
Phase 1: provisionDatabase = true → schema applied, DB ready
         Verify: psql sinex_prod -c "SELECT count(*) FROM core.events" → 0
Phase 2: enable = true with ONLY ingestd + gateway + fs-ingestor + system-ingestor
         Verify: sinexctl status, create test event, query it back
Phase 3: Historical import (limited — SDK adapter gap)
         sinexctl import atuin → 65K events (bypasses pipeline, only existing import)
         ActivityWatch: NO import path exists yet (needs SDK SQLite adapter or new CLI command)
         After: refresh all CAs manually
Phase 4: Enable remaining nodes + automata (config changes only)
Phase 5: Stabilize (monitor DLQ, batch latency, node health)
```

### Service User Permission Model

The sinex service user (uid=991) runs all services. The target user (sinity, uid=1000) owns the data.

| Resource | sinex access? | Why |
|----------|---------------|-----|
| `/realm/project/*` | YES | World-readable (755) |
| systemd journal | YES | journald API access |
| Hyprland socket (`/run/user/1000/hypr/`) | **CONFIGURED, UNPROVEN** | `sinnix` bridge now binds target runtime paths; host proof still pending |
| Atuin DB (`~/.local/share/atuin/history.db`) | **CONFIGURED, UNPROVEN** | `sinnix` bridge now binds target-home history paths; host proof still pending |
| `/home/sinity` | **NO** | ProtectHome=true on most services |

### Evolution Phases

```
A (now)    → Activation: FS + system ingestors give file changes + journal events
B (hours)  → Import: 3M historical events, refresh CAs
C (config) → Full capture: terminal + desktop with permission fixes
D (days)   → Intelligence: entity extractor, session detector (SDK complete, logic vacant)
E (weeks)  → Semantic: embedding pipeline, hybrid search
```
