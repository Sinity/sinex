# NixOS VM Tests for Sinex

This directory contains NixOS VM-based end-to-end tests for Sinex. These tests spin up virtual machines to test the complete system in a real environment.

## Structure

```
nixos-vm/
├── default.nix         # Test entry point
├── vm-config.nix       # Shared VM configuration
├── test-scenarios/     # Individual test scenarios
│   ├── basic-flow.nix  # Basic E2E test ✅
│   ├── multi-source.nix # Multi-source stress test ✅
│   ├── failure-recovery.nix # Failure recovery test ✅
│   └── performance.nix # Performance validation ✅
└── lib/                # Shared test utilities
    ├── assertions.nix  # Test assertion helpers (TODO)
    └── helpers.nix     # Common test functions (TODO)
```

## Running Tests

### Basic Test
```bash
# Run the basic flow test
just test-vm

# Run with interactive debugging (keeps VM on failure)
just test-vm-interactive

# Run directly with Nix
nix build .#checks.x86_64-linux.sinex-vm-basic -L
```

### What the Basic Test Does

1. **System Setup**:
   - Boots a NixOS VM with PostgreSQL + TimescaleDB
   - Creates test user and watched directories
   - Starts a simulated Sinex collector service

2. **Test Scenarios**:
   - File creation event capture
   - Multiple event handling
   - Service restart resilience
   - Database connectivity verification

3. **Verification**:
   - Events are captured and logged
   - Query interface returns events
   - Stats show correct event counts
   - Service recovers from restarts

## Current Status

- ✅ **Basic infrastructure**: VM config, test runner, simple test
- ✅ **PostgreSQL + TimescaleDB**: Database setup in VM
- ✅ **Real Sinex integration**: Full collector and worker binaries in VM
- ✅ **Advanced scenarios**: Multi-source stress, failure recovery, performance validation
- ✅ **Comprehensive testing**: All major failure modes and performance characteristics covered

## Test Scenarios

### Basic Flow Test (`basic-flow.nix`)
- System setup and initialization
- Individual event source validation (filesystem, shell history, Atuin, asciinema, clipboard, D-Bus, Hyprland)
- Service restart resilience
- Database connectivity and schema validation

### Multi-Source Stress Test (`multi-source.nix`)
- Concurrent operation of all event sources under load
- Configurable stress intensity (low/medium/high)
- System stability validation under high-frequency event generation
- Resource usage monitoring and memory/CPU validation
- Event distribution verification across sources

### Failure Recovery Test (`failure-recovery.nix`)
- Database disconnection and reconnection handling
- Collector and worker crash recovery
- Memory pressure and disk space resilience
- Network partition simulation
- Multiple simultaneous failure scenarios
- Graceful degradation validation

### Performance Validation Test (`performance.nix`)
- High-frequency event generation (burst, sustained, ramp-up, spike patterns)
- Multi-source concurrent load testing
- Query performance under load
- Database performance analysis
- System resource utilization monitoring
- Performance regression validation

## Running All Tests

```bash
# Run individual test scenarios
just test-vm-basic
just test-vm-multi-source  
just test-vm-failure-recovery
just test-vm-performance

# Run all VM tests
just test-vm-all
```

## Debugging

If a test fails, you can:

1. Use `test-vm-interactive` to keep the VM running
2. Connect to the VM console to debug
3. Check `/tmp/sinex-events.log` in the VM for captured events
4. Review PostgreSQL logs at `/var/log/postgresql/`

## Writing New Tests

1. Copy `basic-flow.nix` as a template
2. Modify the test scenario in `testScript`
3. Add to `default.nix` exports
4. Update flake.nix checks
5. Add justfile command if needed