# Sinex VM Test Suite

This directory contains NixOS VM-based integration and performance tests for Sinex.

## Quick Start

```bash
# Run smoke tests (quick validation)
NIX_CONFIG=$'experimental-features = nix-command flakes\naccept-flake-config = true' \
  ./tests/e2e/nixos-vm/run-vm-tests.sh -c smoke

# Run all tests
NIX_CONFIG=$'experimental-features = nix-command flakes\naccept-flake-config = true' \
  ./tests/e2e/nixos-vm/run-vm-tests.sh -c all

# Debug a specific test
NIX_CONFIG=$'experimental-features = nix-command flakes\naccept-flake-config = true' \
  ./tests/e2e/nixos-vm/run-vm-tests.sh -d basic

# Run tests in parallel (experimental)
NIX_CONFIG=$'experimental-features = nix-command flakes\naccept-flake-config = true' \
  ./tests/e2e/nixos-vm/run-vm-tests.sh -p 2 -c smoke
```

## CI Coverage

- Flake checks (and CI) currently build/run only the `basic` smoke and `preflight` coordination suites for fast gating.
- All other scenarios (`maintenance`, `satellite-matrix`, `multi-source`, `failure-recovery`, `performance`) are manual/optional; run them locally with `./tests/e2e/nixos-vm/run-vm-tests.sh -c integration` or `-c all` before landing changes that touch their areas.

## Test Runner

The enhanced test runner (`run-vm-tests.sh`) provides:

- **Test categorization**: smoke, integration, performance, chaos
- **Debugging support**: Keep VMs running after failure with `-d`
- **Parallel execution**: Run tests concurrently with `-p`
- **Detailed reporting**: Test results saved to `./test-results/`
- **Configurable timeouts**: Default 15 minutes per test (override with `-t`)

### Examples

```bash
# List available tests
./tests/e2e/nixos-vm/run-vm-tests.sh -l

# Run specific category
./tests/e2e/nixos-vm/run-vm-tests.sh -c performance

# Debug mode (keeps VM on failure)
./tests/e2e/nixos-vm/run-vm-tests.sh -d basic

# Custom timeout and output directory
./tests/e2e/nixos-vm/run-vm-tests.sh -t 3600 -o /tmp/test-results -c all
```

## Test Structure

### Common Modules

- **test-base.nix**: Minimal base configuration for all tests
- **test-helpers.nix**: Python and bash helper functions
- **vm-configs.nix**: Predefined VM profiles (minimal, standard, performance, large)
- **health-checks.nix**: System health monitoring utilities

### Test Categories

1. **Smoke Tests** (`test-scenarios/basic-flow.nix`)
   - Quick validation of core functionality
   - ~2-5 minutes runtime
   - Minimal resource requirements

2. **Integration Tests** 
   - Comprehensive feature validation
   - Multiple event source testing (`test-scenarios/satellite-matrix.nix`)
   - Maintenance timers and git-annex flow (`test-scenarios/maintenance.nix`)
   - Multi-source stress path (`test-scenarios/multi-source.nix`)
   - Failure recovery drills (`test-scenarios/failure-recovery.nix`)
   - Pre-flight and coordinated updates (`preflight_deployment_test.nix`)
   - *(Coming soon: production scale)*

3. **Performance Tests**
   - Satellite performance harness (`test-scenarios/performance.nix`)
   - Load-generator coverage across filesystem/system sources
   - Metrics inspection via helper scripts

4. **Chaos Tests** *(legacy – pending satellite migration)*
   - Failure injection across satellites and core services (legacy architecture)
   - Cascading and resource-storm scenarios with recovery asserts
   - Continuous monitoring via the chaos control plane

### Modernization Roadmap

- [x] Baseline smoke test (`basic`)
- [x] Pre-flight / coordination coverage (`preflight`)
- [x] Maintenance timers & blob storage (`maintenance`)
- [x] Satellite constellation matrix (`satellite-matrix`)
- [x] Re-enable multi-source stress testing on satellites
- [x] Port failure-recovery suite to new services
- [x] Restore performance suite on satellites
- [ ] Rebuild production-scale soak on satellites
- [ ] Port chaos scenarios with monitoring assertions

## Key Improvements

### 1. Faster Test Execution
- **tmpfs for test data**: File operations use memory instead of disk
- **Optimized VM profiles**: Right-sized resources for each test type
- **Service optimization**: Disabled unnecessary services
- **Batch operations**: Events generated in batches instead of one-by-one

### 2. Better Stability
- **Retry logic**: `wait_until_succeeds` for flaky operations
- **Optional Wayland**: Tests gracefully handle missing window manager
- **Health checks**: Proper service readiness validation
- **Resource monitoring**: Prevent test failures from resource exhaustion

### 3. Enhanced Debugging
- **Keep failed VMs**: Debug mode keeps VMs running after failure
- **Test helpers**: Common operations wrapped in reusable functions
- **Health monitoring**: Built-in health check and monitoring tools
- **Detailed logging**: All test output saved to result files

### 4. Developer Experience
- **Categorized tests**: Run subsets based on testing needs
- **Progress reporting**: Real-time test status and duration
- **Summary reports**: Aggregate results with pass/fail rates
- **Interactive monitoring**: `sinex-monitor` for live system status

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
3. **Flaky tests**: Check for missing retry logic
4. **Service failures**: Use health checks to validate readiness

### Debugging Failed Tests

```bash
# Run test in debug mode
NIX_CONFIG=$'experimental-features = nix-command flakes\naccept-flake-config = true' \
  ./tests/e2e/nixos-vm/run-vm-tests.sh -d failing-test

# When test fails, VM keeps running
# Find VM build directory in output
cd /tmp/nix-build-*.drv-0/

# Connect to VM
./bin/nixos-test-driver

# In Python REPL:
>>> machine.shell_interact()
# Now you're in the VM shell for debugging
```

### Performance Issues

1. **Slow VM startup**: Use minimal profile for simple tests
2. **High memory usage**: Check for memory leaks in test
3. **Disk I/O bottleneck**: Ensure tmpfs is used for test data
4. **CPU saturation**: Limit parallel event generation

## Future Improvements

- [ ] VM snapshot/restore for faster test initialization
- [ ] Test result caching based on code changes
- [ ] Distributed test execution across multiple machines
- [ ] Integration with CI/CD pipelines
- [ ] Visual test result dashboard
