# sinexctl Design Notes

This document captures the current design of `sinexctl`.

## Scope

`sinexctl` is an operator CLI for Sinex. Its control path is the gateway RPC surface.

## Command Shape

```text
sinexctl [GLOBAL_OPTIONS] <COMMAND>
```

Global options are layered with runtime env and local preferences:

- CLI flags
- Runtime target descriptor (`--runtime-target` / `SINEX_RUNTIME_TARGET_CONFIG`)
- Runtime environment variables (`SINEX_RPC_URL`, `SINEX_RPC_TOKEN`, TLS/token path vars)
- Local preference file (`~/.config/sinexctl/config.toml`) for format/theme/editor/aliases
- Built-in defaults

Runtime target descriptors bridge deployed-host configuration into the live
operator CLI without making `xtask` the production control surface. They supply
gateway URL, auth token file, TLS trust material, and target identity. Explicit
CLI flags still win so one-off overrides remain possible.

## Command Families

- Gateway/system: `gateway`, `core`
- Query/inspection: `query`, `verify` (passive trust checks plus optional active gateway/automata/document deployment smoke and descriptor-aware collector-surface evidence, distinguishing recent emission from merely historical persisted rows, with local deployment-descriptor awareness for managed oneshot surfaces), `automata`, `status`, `recent`, `errors`, `watch`, `tui`
- Operations: `ops`, `audit`, `node`, `replay`, `dlq`, `lifecycle`, `gitops`
- Local tooling: `config`, `completions`

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
- Keep operator behavior on the gateway surface; avoid hidden direct-DB fallback modes.
- Prefer explicit operator intent for destructive flows (`dlq purge --confirm`, lifecycle tombstone approvals).

## Pointers

- Entrypoint: `crate/cli/src/main.rs`
- Commands: `crate/cli/src/commands/`
- Gateway client: `crate/cli/src/client/gateway.rs`
- Config layering: `crate/cli/src/config.rs`
