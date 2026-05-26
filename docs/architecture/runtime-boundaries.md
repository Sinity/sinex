# Runtime Topology & Boundaries

Status: design record for #1083, #1084. Part of Wave 2 (#1126).

## Process Topology

```
┌─────────────────────────────────────────────────────────┐
│                      sinex system                       │
│                                                         │
│  ┌──────────┐  ┌──────────┐  ┌───────────────────────┐ │
│  │ ingestd  │  │ gateway  │  │  source-worker host    │ │
│  │ (1 inst) │  │ (1 inst) │  │  (N inst, per unit)    │ │
│  └────┬─────┘  └────┬─────┘  │                         │ │
│       │             │        │  ┌───────────────────┐  │ │
│       │    ┌────────┘        │  │source-unit runner │  │ │
│       │    │                 │  │ • adapter         │  │ │
│       ▼    ▼                 │  │ • parser          │  │ │
│  ┌─────────────┐             │  │ • checkpoint      │  │ │
│  │  PostgreSQL │             │  │ • health          │  │ │
│  └─────────────┘             │  └───────────────────┘  │ │
│                              └───────────────────────┘ │
│                                                         │
│  Legacy ingestors (per domain, transitional):           │
│  ┌──────┐ ┌──────────┐ ┌─────────┐ ┌────────┐         │
│  │  fs  │ │ terminal │ │ desktop │ │ system │         │
│  └──────┘ └──────────┘ └─────────┘ └────────┘         │
└─────────────────────────────────────────────────────────┘
```

## Trust Boundaries

| Boundary | Mechanism | Notes |
|----------|-----------|-------|
| NATS → ingestd | Envelope validation (#1064) | EventIntent required for durable transport |
| ingestd → DB | XOR provenance CHECK, material FK | Defense-in-depth |
| DB → gateway | Role-based access (readonly/write/admin) | Token-suffix RBAC, no revocation |
| Gateway → CLI | TLS + bearer token | Stateless auth |
| External producer → NATS | EventIntent envelope | JSON over NATS, no Rust SDK required |
| Source-worker → NATS | Same envelope path | Runs in same trust domain |

## Service User Model

| User | UID | Access |
|------|-----|--------|
| `sinex` | 991 | All services. `/realm/project/*` (world-readable). journald. Hyprland socket bridge. |
| `sinity` | 1000 | Data owner. `/home/sinity` (700). `/realm/data/`. |
| `root` | 0 | ACL bridge oneshots only. |

## Source-Worker Migration

Current: 6 domain-specific ingestor binaries → systemd services.
Target: 1 `sinex-source-worker` binary → N per-source-unit systemd services (like sinex-process automata).

Legacy ingestors continue operating during migration. New source units are added as source-worker instances. Existing ingestors are retired when their source units are fully migrated.

## NATS Role

NATS remains the durable transport for:
- Live external producers (Polylogue bridge, browser extensions)
- Confirmations and DLQ
- Replay progress and invalidation
- Derived automata subscriptions

Staged local material parsing may bypass NATS when source-worker runs in-process with ingestd (#1054 decision pending).

Refs: #1054, #1061, #1081, #1125, #1126.
