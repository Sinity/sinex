# Runtime Target Boundaries

Sinex has three distinct operational planes. They must stay explicit because
mixing their health signals is how a healthy deployed host can look broken when
the local checkout stack is down.

## Command Ownership

| Surface | Owns | Does not own |
| --- | --- | --- |
| `xtask` | Repository development loops, generated docs, CI-style verification, developer ergonomics | Development-service lifetime, production operation, host proof commands, source ingestion semantics |
| AgentCTL | Declared worktree development-service leases, bounded loopback ports, service cgroups, cancellation, and lease status | Arbitrary commands, product readiness policy, or repository verification |
| `sinexctl` | Live Sinex runtime operation through `sinexd::api`, event/query/replay/lifecycle/DLQ/runtime/status commands | Repository build/test loops, devshell state, local background job bookkeeping |
| Rust tests | Correctness of crates, SDK behavior, ingestion semantics, replay/provenance invariants, API contracts | Operator dashboards, host activation proof |
| Benchmarks/load tests | Measured throughput, latency, resource ceilings, regression trends | Arbitrary pass/fail "resource contracts" detached from measured baselines |
| NixOS VM tests | Deployment wiring, service activation, hardening, host-like integration behavior | Routine local unit/integration correctness |
| NixOS modules | Declarative deployed-host configuration and exported runtime descriptors | Checkout-local dev defaults |

This means:

- `xtask prove host` is the wrong shape. Host proof belongs to NixOS VM tests,
  NixOS activation checks, and `sinexctl` live-runtime probes.
- `xtask exercise source-material` is the wrong shape. Source-material ingestion
  correctness belongs to runtime tests and VM integration tests.
- AgentCTL lease status identifies only declared checkout-local development
  services. It must never silently merge that state with deployed-host state.

## Runtime Descriptors

Sinex uses two descriptor classes:

- `deployment-readiness`: a NixOS-authored proof artifact for "what this host is
  expected to run". It includes enabled surfaces, service units, target user
  bridges, and deployment expectations.
- `runtime-target`: a connection/status descriptor for "which runtime should a
  tool probe". It includes API endpoint, database, NATS, state directories, service
  names, descriptor source, and target kind.

The deployment descriptor can be converted into a runtime target, but they are
not the same object. The conversion is intentionally lossy: readiness metadata
about all capture surfaces is useful for deployment checks, while status tools
need a narrow, composable connection target.

## Default Target Semantics

`agentctl job get <job-id>` is checkout-local lease inspection.

It reports the descriptor-declared service and its bounded loopback lease. It
does not consume deployed runtime descriptors under `/etc/sinex`, and it does
not make product-health claims.

`sinexctl` defaults to explicit API configuration.

It can consume a runtime-target descriptor to populate API URL, token file,
TLS material, and target labeling. Once a descriptor is loaded, `sinexctl`
reports the target before reporting live API health.

## Status Snapshot Rules

Any status snapshot must preserve source attribution:

- infrastructure probes identify whether they came from checkout-local stack
  config, runtime target config, systemd, API RPC, NATS, or database
  telemetry;
- stale telemetry is not equivalent to down services;
- missing telemetry is not equivalent to healthy services;
- API readiness, DB/NATS reachability, service unit state, runtime heartbeat,
  consumer lag, batch latency, and history/job state remain separate signals.

The status renderer may summarize these signals, but JSON output must keep the
target and source fields so scripts and agents do not infer the wrong plane.

## Verification Expectations

Runtime-target work is complete only when all of these are covered:

- descriptor parse/load tests;
- conversion tests from deployment-readiness to runtime-target;
- AgentCTL descriptor tests prove lease metadata remains bounded and does not
  consume deployed runtime descriptors;
- `sinexctl --runtime-target <path> status` applies API/auth/TLS values from
  the descriptor and prints the target in human status output;
- NixOS exports `/etc/sinex/runtime-target.json` beside
  `/etc/sinex/deployment-readiness.json`.
