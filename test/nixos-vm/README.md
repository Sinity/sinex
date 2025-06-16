# NixOS VM Tests for Sinex

This directory contains NixOS VM-based end-to-end tests for Sinex. These tests spin up virtual machines to test the complete system in a real environment.

## Structure

```
nixos-vm/
├── default.nix         # Test entry point
├── vm-config.nix       # Shared VM configuration
├── test-scenarios/     # Individual test scenarios
│   ├── basic-flow.nix  # Basic E2E test (implemented)
│   ├── multi-source.nix # Multi-source stress test (TODO)
│   ├── failure-recovery.nix # Failure recovery test (TODO)
│   └── performance.nix # Performance validation (TODO)
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
- ✅ **Simulated Sinex**: Basic collector and query commands
- 🚧 **Real Sinex integration**: Need to build actual binaries in VM
- 🚧 **Advanced scenarios**: Multi-source, failure recovery, performance

## Next Steps

1. Integrate real Sinex binaries into the VM
2. Add actual event sources (filesystem watcher, terminal monitor)
3. Implement more complex test scenarios
4. Add visual regression tests with screenshots
5. Create performance benchmarks

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