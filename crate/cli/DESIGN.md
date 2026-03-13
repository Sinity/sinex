# sinexctl Design Notes

This document captures the current design of `sinexctl`.

## Scope

`sinexctl` is an operator CLI for Sinex with two execution paths:

- Gateway RPC path (default for most commands)
- Direct database path (`sinexctl db ...`) for diagnostics

## Command Shape

```text
sinexctl [GLOBAL_OPTIONS] <COMMAND>
```

Global options are layered with runtime env and local preferences:

- CLI flags
- Runtime environment variables (`SINEX_RPC_URL`, `SINEX_RPC_TOKEN`, TLS/token path vars)
- Local preference file (`~/.config/sinexctl/config.toml`) for format/theme/editor/aliases
- Built-in defaults

## Command Families

- Gateway/system: `gateway`, `core`
- Query/inspection: `query`, `status`, `recent`, `errors`, `watch`, `tui`
- Operations: `ops`, `audit`, `node`, `replay`, `dlq`, `lifecycle`, `gitops`
- Local tooling: `config`, `completions`, `db`

## Transport and Auth

- Transport: HTTPS JSON-RPC
- Auth header: `Authorization: Bearer <token>`
- Token resolution order:
  1. `--token`
  2. `SINEX_RPC_TOKEN`
  3. `--token-file`
  4. `~/.config/sinex/token`
- TLS options:
  - `--ca-cert`
  - `--client-cert` + `--client-key` for mTLS
  - `--insecure` for development only

## Output and UX

- Output formats: `table`, `json`, `yaml`
- Command modules own output formatting and examples
- Shell completions are generated from Clap metadata (`sinexctl completions <shell>`)

## Design Constraints

- Keep command names aligned with gateway method namespaces where practical (`replay.*`, `dlq.*`, `node.*`).
- Keep direct-DB behavior isolated to `db` commands; avoid hidden fallback modes.
- Prefer explicit operator intent for destructive flows (`dlq purge --confirm`, lifecycle tombstone approvals).

## Pointers

- Entrypoint: `crate/cli/src/main.rs`
- Commands: `crate/cli/src/commands/`
- Gateway client: `crate/cli/src/client/gateway.rs`
- Config layering: `crate/cli/src/config.rs`
