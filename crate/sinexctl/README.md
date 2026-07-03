# sinexctl

`sinexctl` is the Rust CLI for operating Sinex through the `sinexd` API.

## Quick Start

```bash
# Open the offline-friendly command center
sinexctl

# Check API reachability and runtime health
sinexctl runtime gateway ping --token "$SINEX_API_TOKEN"
sinexctl runtime health --token "$SINEX_API_TOKEN"

# Query recent events
sinexctl events query -s 1h --token "$SINEX_API_TOKEN"

# Recall activity context for session resumption
sinexctl recall --window 2h --token "$SINEX_API_TOKEN"

# List runtime modules and replay operations
sinexctl runtime list --token "$SINEX_API_TOKEN"
sinexctl ops replay list --token "$SINEX_API_TOKEN"

# Inspect automata runtime health and checkpoint position
sinexctl runtime automata --token "$SINEX_API_TOKEN"

# Run runtime evidence checks, including passive derived-signal checks, managed
# document-scan smoke, collector-surface evidence, and historical-backfill evidence
sinexctl ops verify --document-smoke --source-evidence --historical-evidence --token "$SINEX_API_TOKEN"

# Inspect DLQ state
sinexctl ops dlq list --token "$SINEX_API_TOKEN"
```

## Command Groups

The public root command tree is deliberately small. Older shortcut roots such as
`gateway`, `core`, `verify`, `demo`, `dlq`, `replay`, `lifecycle`, `blob`, and
`state` are nested under the canonical groups below.

- `events`: event search, filtering, relations, tracing, streaming, and annotation
- `query`: shared query-unit selection via a query expression (e.g. `query 'events where source = "terminal" limit 50'`)
- `recall`: compact activity context around a point in time, using the shared context/query substrate
- `show`: resolve and inspect a public Sinex object ref (`<kind>:<id>`)
- `sources`: source material inventory, staging, readiness, continuity, drift, and coverage
- `runtime`: gateway reachability, runtime health, module list/status/drain/resume/horizon, and automata health
- `ops`: operations, operation jobs, DLQ, replay, lifecycle, audit, blob, state, instructions, bounded verification, and demo seeding
- `privacy`: private-mode and policy posture
- `tasks`: task projection and lifecycle
- `record`: manual canonical records
- `docs`: document search, retrieval, and chunk browsing
- `semantic`: semantic epochs, shadow lanes, curation, and LLM policy inspection
- `metrics`: telemetry, throughput, and activity reports
- `tui`: interactive operator workbench
- `config`: local CLI preferences and runtime target inspection

## Connection and Auth

Global flags (available on most commands):

- `--rpc-url` (default `https://127.0.0.1:9999`)
- `--token` or `--token-file`
- `--ca-cert`
- `--client-cert` + `--client-key` (mTLS)
- `--insecure` (dev only)
- `--timeout`
- `--format`
- `--runtime-target` (loads gateway/auth/TLS settings from a runtime target descriptor)

Environment variables (directly supported by CLI flags/token loader):

- `SINEX_API_URL`
- `SINEX_API_TOKEN`
- `SINEX_RUNTIME_TARGET_CONFIG`

When `--runtime-target` or `SINEX_RUNTIME_TARGET_CONFIG` is set, descriptor
values populate the API URL, token file, and TLS material before explicit
CLI flags are applied. The bare `sinexctl` command center and
`sinexctl runtime health` show the loaded target so live runtime health is tied
to the descriptor that supplied the connection settings.

## Structured Completion

```bash
sinexctl _complete --line "sinexctl events source:" --cursor 24 --format json
```

## Local Preferences File

```bash
sinexctl config init
sinexctl config show -f yaml
sinexctl config path
```

Default location:

- Linux/macOS: `~/.config/sinexctl/config.toml`
- Windows: `%APPDATA%/sinex/sinexctl/config.toml`

The file stores local preferences only:

- `default_format`
- `editor`
- command aliases
- table theme

Runtime connection/auth/TLS settings are intentionally not persisted there;
use environment variables, CLI flags, or a runtime target descriptor.
