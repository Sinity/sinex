# sinexctl

`sinexctl` is the Rust CLI for operating Sinex through `sinex-gateway`.

## Quick Start

```bash
# Check gateway reachability
sinexctl gateway ping --token "$SINEX_RPC_TOKEN"

# Query recent events
sinexctl query -s 1h --token "$SINEX_RPC_TOKEN"

# List nodes and replay operations
sinexctl node list --token "$SINEX_RPC_TOKEN"
sinexctl replay list --token "$SINEX_RPC_TOKEN"

# Inspect derived-node/automata runtime health and checkpoint position
sinexctl automata --token "$SINEX_RPC_TOKEN"

# Run trust verification, including passive derived-signal checks, managed
# document-scan smoke, collector-surface evidence, and historical-backfill proof
sinexctl verify --document-smoke --source-proof --historical-proof --token "$SINEX_RPC_TOKEN"

# Inspect DLQ state
sinexctl dlq list --token "$SINEX_RPC_TOKEN"
```

## Command Groups

- `gateway`: connectivity/version checks
- `core`: system health
- `query`: event search and filtering
- `verify`: trust/proof checks for pipeline, gateway reachability, automata deployment smoke, managed document-scan smoke, enabled long-running collector surfaces (with recent-vs-historical evidence), locally declared deployment surfaces, and historical backfill
- `node`: list/status/drain/resume/horizon
- `automata`: derived-node runtime health, checkpoints, replay/output recency, and SDK telemetry
- `replay`: plan/submit/watch/list
- `dlq`: list/peek/requeue/purge
- `ops`, `audit`: operation lifecycle and audit trail
- `lifecycle`: archive/restore/tombstone workflows
- `gitops`: schema source management
- `status`, `recent`, `errors`, `watch`, `tui`: operator shortcuts
- `demo`: deterministic dev data seeding
- `config`, `completions`: local CLI management

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

- `SINEX_RPC_URL`
- `SINEX_RPC_TOKEN`
- `SINEX_RUNTIME_TARGET_CONFIG`

When `--runtime-target` or `SINEX_RUNTIME_TARGET_CONFIG` is set, descriptor
values populate the gateway URL, token file, and TLS material before explicit
CLI flags are applied. `sinexctl status` prints the loaded target so live
runtime health is tied to the descriptor that supplied the connection settings.

## Completions

```bash
sinexctl completions bash > ~/.local/share/bash-completion/completions/sinexctl
sinexctl completions zsh > ~/.zfunc/_sinexctl
sinexctl completions fish > ~/.config/fish/completions/sinexctl.fish
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
- `aliases`
- `theme`

Gateway URL, auth token, TLS paths, and timeouts come from CLI flags or env vars.
