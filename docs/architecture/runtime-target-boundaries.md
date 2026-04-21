# Runtime Target Boundaries

Sinex has three distinct operational planes. They must stay explicit because
mixing their health signals is how a healthy deployed host can look broken when
the local checkout stack is down.

## Command Ownership

| Surface | Owns | Does not own |
| --- | --- | --- |
| `xtask` | Repository development loops, checkout-local infra, generated docs, CI-style verification, local background jobs, developer ergonomics | Production operation, host proof commands, source ingestion semantics |
| `sinexctl` | Live Sinex runtime operation through the gateway, event/query/replay/lifecycle/DLQ/node/status commands | Repository build/test loops, devshell state, local background job bookkeeping |
| Rust tests | Correctness of crates, SDK behavior, ingestion semantics, replay/provenance invariants, gateway contracts | Operator dashboards, host activation proof |
| Benchmarks/load tests | Measured throughput, latency, resource ceilings, regression trends | Arbitrary pass/fail "resource contracts" detached from measured baselines |
| NixOS VM tests | Deployment wiring, service activation, hardening, host-like integration behavior | Routine local unit/integration correctness |
| NixOS modules | Declarative deployed-host configuration and exported runtime descriptors | Checkout-local dev defaults |

This means:

- `xtask prove host` is the wrong shape. Host proof belongs to NixOS VM tests,
  NixOS activation checks, and `sinexctl` live-runtime probes.
- `xtask exercise source-material` is the wrong shape. Source-material ingestion
  correctness belongs to SDK/node tests and VM integration tests.
- `xtask status` may display runtime signals, but only as an attributed view of
  the target it is explicitly probing. It must never silently merge checkout
  state with deployed-host state.

## Runtime Descriptors

Sinex uses two descriptor classes:

- `deployment-readiness`: a NixOS-authored proof artifact for "what this host is
  expected to run". It includes enabled surfaces, service units, target user
  bridges, and deployment expectations.
- `runtime-target`: a connection/status descriptor for "which runtime should a
  tool probe". It includes gateway, database, NATS, state directories, service
  names, descriptor source, and target kind.

The deployment descriptor can be converted into a runtime target, but they are
not the same object. The conversion is intentionally lossy: readiness metadata
about all capture surfaces is useful for deployment checks, while status tools
need a narrow, composable connection target.

## Default Target Semantics

`xtask status` defaults to `checkout-local`.

It derives that target from the current checkout's `.sinex` stack config and
developer config. If the deployed host has a runtime descriptor under
`/etc/sinex`, `xtask status` still does not use it unless an explicit future
selector says so. This keeps the MOTD honest: `pg:offline`, `gateway:down`, or
`ingestd:unknown` mean the checkout-local target is down, not that production is
down.

`sinexctl` defaults to explicit gateway configuration.

It can consume a runtime-target descriptor to populate gateway URL, token file,
TLS material, and target labeling. Once a descriptor is loaded, `sinexctl status`
reports the target before reporting live gateway health.

## Status Snapshot Rules

Any status snapshot must preserve source attribution:

- infrastructure probes identify whether they came from checkout-local stack
  config, runtime target config, systemd, gateway RPC, NATS, or database
  telemetry;
- stale telemetry is not equivalent to down services;
- missing telemetry is not equivalent to healthy services;
- gateway readiness, DB/NATS reachability, service unit state, node heartbeat,
  consumer lag, batch latency, and history/job state remain separate signals.

The status renderer may summarize these signals, but JSON output must keep the
target and source fields so scripts and agents do not infer the wrong plane.

## Verification Expectations

Runtime-target work is complete only when all of these are covered:

- descriptor parse/load tests;
- conversion tests from deployment-readiness to runtime-target;
- `xtask status --summary --json` includes a target and defaults to
  checkout-local;
- `xtask status` tests prove deployed descriptors are not implicitly loaded into
  checkout-local status;
- `sinexctl --runtime-target <path> status` applies gateway/auth/TLS values from
  the descriptor and prints the target in human status output;
- NixOS exports `/etc/sinex/runtime-target.json` beside
  `/etc/sinex/deployment-readiness.json`.
