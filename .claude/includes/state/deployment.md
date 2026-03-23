## Deployment Readiness

**Current state:** `sinex.enable = false; provisionDatabase = false` on sinnix-prime. Zero production events.
2.83M ActivityWatch events + 65K Atuin commands sit in parallel capture infrastructure, ready for import.

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

### What's Blocked (NixOS Config, Not Rust)

| Component | Blocker | Fix |
|-----------|---------|-----|
| Desktop ingestor | Hyprland socket at `/run/user/1000/hypr/` — 700 sinity:users, sinex (uid=991) blocked | `BindReadOnlyPaths` or ACL via tmpfiles.d |
| Terminal ingestor | No `.bash_history`/`.zsh_history` (user uses Atuin). Atuin DB at 600 sinity:users | `BindReadOnlyPaths` for Atuin DB |
| Gateway admin token | Needs agenix secret verification | Check sinnix secrets config |

### Activation Sequence (Critical Path)

```
Phase 0: nixos-rebuild switch (pick up code changes)
Phase 1: provisionDatabase = true → schema applied, DB ready
         Verify: psql sinex_prod -c "SELECT count(*) FROM core.events" → 0
Phase 2: enable = true with ONLY ingestd + gateway + fs-ingestor + system-ingestor
         Verify: sinexctl status, create test event, query it back
Phase 3: Historical import (hours of runtime)
         sinexctl import atuin → 65K events
         sinexctl import activitywatch → 2.83M events
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
| Hyprland socket (`/run/user/1000/hypr/`) | **NO** | 700 sinity:users |
| Atuin DB (`~/.local/share/atuin/history.db`) | **NO** | 600 sinity:users |
| `/home/sinity` | **NO** | ProtectHome=true on most services |

### Evolution Phases

```
A (now)    → Activation: FS + system ingestors give file changes + journal events
B (hours)  → Import: 3M historical events, refresh CAs
C (config) → Full capture: terminal + desktop with permission fixes
D (days)   → Intelligence: entity extractor, session detector (SDK complete, logic vacant)
E (weeks)  → Semantic: embedding pipeline, hybrid search
```
