# Sinex devshell runtime loop

This note owns the local development/runtime path for a checkout. It is about
working and debugging from a repository clone, not about deploying the system
through Sinnix/NixOS.

## Rules

- Build, check, test, help, status, and pre-push flows must not start `sinexd`.
- Read-only probes must not start checkout-local Postgres or NATS.
- `xtask infra start` is the explicit point where local Postgres/NATS may start.
- `xtask run ...` is the explicit point where local `sinexd` may start.
- `xtask infra stop` stops only the current checkout by default.
- `xtask infra stop --all-checkouts` may stop only processes proven from
  `/proc` to belong to Sinex dev infra for this user; it must not touch
  system-managed Postgres, NATS, or `sinexd`.

## Normal local runtime workflow

From a checkout:

```bash
xtask infra status
xtask infra status --all-checkouts
xtask infra smoke --dry-run

xtask infra start
xtask run core --dry-run
xtask run core --logs
xtask infra status
xtask infra stop
```

Use `xtask run core --dry-run` before a real run when you only need to inspect
the checkout-local runtime coordinates. It prints the checkout root, dev-state
directory, log directory, database URL, NATS URL, API URL when configured, and
job directory without starting `sinexd`.

Use `xtask infra smoke --reset-first` when changing devshell/runtime plumbing.
The smoke verifies this sequence:

1. The current checkout starts from a stopped local infra state.
2. Read-only probes complete without starting Postgres, NATS, or `sinexd`.
3. Local Postgres/NATS start only during the explicit start phase.
4. `xtask run core --dry-run` reports local runtime coordinates without
   starting `sinexd`.
5. Current-checkout Postgres/NATS stop before the command exits.
6. All-checkout inventory remains available for stale state and RAM inspection.

`xtask infra smoke --dry-run` is a no-service plan and inventory check. It is
safe to run before deciding whether to start local infra.

Use `xtask infra smoke --reset-first --run-core` when the change needs the full
dev-local runtime proof. That opt-in path starts `xtask run core` as a managed
background job, waits until `xtask infra status` observes checkout-local
`sinexd`, cancels the job through `xtask jobs`, verifies `sinexd` disappears
from infra status, and then stops Postgres/NATS. The default smoke intentionally
does not run this phase so routine wrapper/check changes do not compile or start
`sinexd`.

## Why isolated checkout-local services remain the default

The current default is still per-checkout isolated Postgres/NATS under
`/var/cache/sinex/$USER/<checkout-hash>/dev-state`.

That default is not aesthetic. It protects branch-sensitive state:

- SQLx compile-time validation uses the checkout schema.
- Schema apply, strict-diff, and drift checks are tied to the checked-out code.
- Integration tests can create and destroy databases without crossing branches.
- NATS/JetStream subjects, streams, consumers, DLQ state, and replay state stay
  scoped to the checkout.
- Multiple agents can work in separate worktrees without sharing mutable runtime
  state by accident.

A shared user/system Postgres or NATS can become a future optimization only if
it proves branch-safe namespacing and cleanup for schema drift, SQLx validation,
test databases, destructive tests, JetStream streams, durable consumers, DLQ
state, and concurrent worktrees. Until that proof exists, the optimization is to
make isolated services lighter, explicit, visible, and easy to stop.

## RAM and cache expectations

`xtask infra status` reports current-checkout RSS for Postgres, NATS, and
dev-local `sinexd`.

`xtask infra status --all-checkouts` reports aggregate RSS and state size across
all checkout-local dev-state roots. Use it after checks, interrupted runs, and
agent worktree cleanup.

Expected current behavior:

- No local infra: no checkout-local Postgres/NATS/`sinexd` processes.
- One explicit checkout-local stack: Postgres/NATS RSS appears in `infra status`.
- Checkout-local Postgres is tuned for development rather than production: the
  managed config uses 128 connections, 32MB shared buffers, 6 worker processes,
  and 2 TimescaleDB background workers. These values are deliberately enough for
  SQLx validation and local tests, not for production traffic.
- Checkout-local NATS JetStream is bounded at 64MB memory and 256MB file storage
  per checkout. A larger local runtime/debug session should opt in explicitly
  instead of making every compile/test checkout pay the resident cost.
- Multiple checkout-local stacks: aggregate RSS appears in
  `infra status --all-checkouts`.
- Local `sinexd`: visible in current and all-checkout status when its current
  directory proves checkout ownership.

Cleanup commands:

```bash
xtask infra stop
xtask infra stop --all-checkouts --stale-only
xtask infra stop --all-checkouts --dry-run
xtask infra stop --all-checkouts
```

If swap remains full after dev infra has stopped, treat that as host runtime
state, not as Nix policy. Inspect current memory owners and pressure before
changing deployment configuration.

## Smoke before claiming devshell/runtime changes

For changes under `flake.nix`, `xtask/src/commands/infra.rs`,
`xtask/src/infra/**`, `xtask/src/commands/run.rs`, pre-push hooks, or
devshell wrappers, use:

```bash
xtask infra stop
pgrep -a 'sinexd|postgres|postmaster|nats-server' || true
xtask infra smoke --reset-first
xtask infra smoke --reset-first --run-core
xtask infra status --all-checkouts
```

When the wrapper itself is the target, run the smoke through the devshell entry:

```bash
nix develop --command xtask infra smoke --reset-first
nix develop --command xtask infra smoke --reset-first --run-core
```

Do not replace this with a test that only asserts a config literal or flag name.
The behavior that matters is whether processes start, whether ownership is
visible, whether RAM cost is reported, and whether explicit cleanup works.
