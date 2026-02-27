# CLI Framework Architecture

**Analysis Date**: 2026-01-24

## Architecture

The runtime wraps `sinex-node-sdk` primitives in a `clap`-based CLI.

```
NodeCli (clap)
  ├── NatsArgs (TLS/Auth)
  └── NodeCommand
       ├── Service (Long-running daemon)
       ├── Scan (One-shot processing)
       └── Explore (Diagnostic)
```

## Critical Findings

-   **TLS Default**: TLS is disabled by default (`require_tls = false`). Production must explicitly enable it.
-   **Validation**: Some inputs (`database_url`, `service_name`) lack validation.
-   **Panic Risk**: Explore mode has a known panic if `node` ownership is mishandled (see Fix below).

## Configuration

Precedence:
1.  CLI Arguments
2.  Environment Variables
3.  `--node-config` JSON
4.  Built-in Defaults

## Subcommands

### `service`
Runs the node as a long-lived daemon.
-   Connects to NATS and Postgres.
-   Participates in coordination/leader election.
-   Supports `dry-run` (no DB writes).

### `scan`
Runs a one-shot scan of specific targets.
-   Useful for backfilling or ad-hoc ingestion.
-   Supports checkpoints and time horizons.

### `explore`
Diagnostics for node state.
-   `--source-state`: Inspect internal tracking state.
-   `--ingestion-history`: View recent history.
