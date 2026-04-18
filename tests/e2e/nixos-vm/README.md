# Sinex VM Test Suite

This directory contains the NixOS VM scenarios used to exercise Sinex deployment
paths and host/runtime behavior.

## Quick Start

```bash
# List exported VM checks
xtask test vm --list

# Run the exported smoke slice
xtask test vm --category smoke

# Run the exported integration slice
xtask test vm --category integration

# Build one exported check and keep the failed VM derivation around
xtask test vm --keep-failed basic
```

## Host Requirements

- Linux host with KVM enabled (`/dev/kvm` accessible for the current user).
- Minimum 4 CPU cores (8+ recommended for integration/performance suites).
- Minimum 8 GB RAM (16+ GB recommended for performance/parallel runs).
- At least 20 GB free disk space.

## CI Coverage

- The default GitHub Actions gate in `.github/workflows/ci.yml` does **not** execute
  this directory directly. It runs only the Postgres-backed `xtask ci workspace`
  gate.
- The public VM runner surface is the exported flake-check suite from
  `tests/e2e/nixos-vm/default.nix`.
- `xtask test vm` runs those exported checks; updating the scenario registry there
  changes the public VM surface.

## Runner Model

`xtask test vm` builds exported flake checks under `.#checks.<system>.sinex-vm-*`.
It follows the scenario registry in `tests/e2e/nixos-vm/default.nix`.

- `--list` shows the currently exported checks.
- `--category smoke|integration|performance|chaos|all` selects from the built-in
  catalogue.
- `--validate` syntax-checks the scenario files without running them.
- Positional test names run individual exported checks, for example `xtask test vm basic`.

## Test Structure

### Common Modules

- **test-base.nix**: Minimal base configuration for all tests
- **test-helpers.nix**: Python and bash helper functions
- **vm-configs.nix**: Predefined VM profiles (minimal, standard, performance, large)

### Test Categories

1. **Smoke**
   - `basic`
   - `replay-smoke`
2. **Integration**
   - `preflight`
   - `maintenance`
   - `node-matrix`
   - `multi-source`
   - `failure-recovery`
   - `kitty-eventsource`
   - `mtls-enforcement`
   - `sinexctl-e2e`
   - `hostile-host`
   - `migration-stress`
3. **Performance**
   - `performance`
   - `production-scale`
4. **Chaos**
   - `chaos-network-partition`
   - `chaos-process-restart`
   - `chaos-clock-skew`
   - `xtask-concurrency`

## Coverage Notes

- `xtask test vm --validate` is the cheap way to keep the scenario tree honest
  when you touch scenario files or shared VM helper modules.
- `basic` is the fast proof that gateway ingress, automata, and the managed
  document scan surface all function on a booted VM.
- `node-matrix` is the deployment-honesty proof: every long-running
  node/automaton unit must start, the managed document surface must actually
  ingest, and `sinexctl verify --gateway-smoke --automata-smoke --document-smoke --source-proof --historical-proof`
  must pass on the booted VM.
- `multi-source` is the broad runtime exercise scenario; it now drives the
  managed document scan surface alongside filesystem, terminal, desktop,
  system, and automata traffic, then proves those enabled collector surfaces
  and implemented historical backfill surfaces through `sinexctl verify --source-proof --historical-proof`.
  The collector proof distinguishes recent emission from merely historical
  persisted evidence so stale rows cannot masquerade as a live surface.
- `xtask test vm` gives `basic`, `node-matrix`, and `multi-source` the extended
  60-minute timeout budget because they build and boot the widened full-runtime
  closure rather than a minimal smoke VM.
- Default CI still does not exercise the VM suite automatically; treat these
  scenarios as explicit deployment-path coverage that must be invoked on purpose.

## Writing New Tests

### Basic Test Template

```nix
{ pkgs
, sinex-ingestd
, sinex-gateway
, pg_jsonschema
, sinex ? null
, sinexCli ? null
, ... }:

pkgs.testers.nixosTest {
  name = "sinex-my-test";
  
  nodes.machine = { config, pkgs, lib, ... }: {
    imports = [
      (import ../common/test-base.nix {
        inherit config pkgs lib sinex-ingestd sinex-gateway pg_jsonschema sinex sinexCli;
      })
    ];

    # Provide sinexCli when the scenario needs maintenance timers or
    # sinex-cli tooling. Leaving it null skips those timers gracefully.
    
    # Override VM profile if needed
    virtualisation.vmProfile = "performance";
    
    # Test-specific configuration
    services.sinex = {
      # Your config here
    };
  };
  
  testScript = ''
    import sys
    sys.path.append('/etc/nixos-test')
    from test_helpers import TestHelpers
    
    start_all()
    helpers = TestHelpers(machine)
    
    # Wait for system
    helpers.wait_for_sinex_ready()
    
    # Your test logic here
    with subtest("My test case"):
        initial_count = helpers.get_event_count()
        helpers.generate_events(100, "mytest")
        assert helpers.wait_for_event_processing(initial_count + 100)
  '';
}
```

### Available Helper Functions

```python
# Test helpers
helpers.wait_for_sinex_ready(timeout=60)
helpers.get_event_count() -> int
helpers.generate_events(count, prefix="test", path="/var/lib/sinex/watched") -> int
helpers.check_service_health(service_name) -> bool
helpers.wait_for_event_processing(expected_count, timeout=30) -> bool
helpers.cleanup_test_data(path="/var/lib/sinex/watched")
helpers.check_wayland_available() -> bool
helpers.measure_operation_time(operation) -> float
```

### Health Check Commands

```bash
# Check system health
sinex-health-check

# Generate test events
sinex-test-event [type] [count]

# Monitor system live
sinex-monitor [interval_seconds]
```

## Troubleshooting

### Test Failures

1. **Timeout errors**: Increase timeout with `-t` flag
2. **Resource exhaustion**: Use larger VM profile
3. **Flaky tests**: Check for brittle ordering, missing waits, or resource pressure
4. **Service failures**: Use health checks to validate readiness

### Debugging Failed Tests

```bash
# Keep failed derivation outputs for inspection
xtask test vm --keep-failed basic

# Or build the check directly with full Nix logs
nix build -L .#checks.$(nix eval --impure --raw --expr builtins.currentSystem).sinex-vm-basic --keep-failed

# Inspect the kept-failed directory Nix prints on failure.
```
