# Sinex End-to-End Tests

This directory contains comprehensive end-to-end tests that validate the entire Sinex pipeline by generating real system events and verifying they are captured correctly.

## Test Categories

### 1. Full System Test (`full_system_test.rs`)
Complete E2E test that requires all systems to be available:
- Filesystem monitoring (always available)
- Hyprland window manager (requires running Hyprland)
- Kitty terminal (requires Kitty with remote control enabled)

Run with:
```bash
cargo test --test integration e2e::test_full_system_with_real_events -- --ignored
```

### 2. Adaptive E2E Test (`adaptive_e2e_test.rs`)
Intelligent test that adapts to available systems:
- Always tests filesystem events
- Conditionally tests Kitty if available
- Conditionally tests Hyprland if available

Run with:
```bash
cargo test --test integration e2e::test_adaptive_full_system
```

### 3. Minimal E2E Test
Basic test that only requires filesystem (always available):
```bash
cargo test --test integration e2e::test_minimal_e2e
```

## Running E2E Tests

### Using the Test Runner Script
```bash
# Run adaptive and minimal tests (default)
./test/e2e/run_e2e_tests.sh

# Run all tests including full system test
./test/e2e/run_e2e_tests.sh --all

# Run tests in dry-run mode
./test/e2e/run_e2e_tests.sh --dry-run

# Run only the full system test
./test/e2e/run_e2e_tests.sh --full
```

### Manual Test Execution
```bash
# Make sure you're in nix develop shell
nix develop

# Run specific test
cargo test --test integration e2e::test_adaptive_full_system -- --nocapture

# Run all E2E tests
cargo test --test integration e2e:: -- --nocapture
```

## What Gets Tested

### Filesystem Events
- File creation, modification, deletion
- Directory creation
- Nested file operations
- Multiple rapid changes

### Hyprland Events (if available)
- Workspace switching via `hyprctl`
- Window creation/destruction
- Periodic state snapshots
- Monitor configuration changes

### Kitty Events (if available)
- Command execution via remote control
- Terminal output capture
- Command history

### System Integration
- Heartbeat mechanisms
- Event routing and processing
- Database persistence
- Concurrent ingestor operation

## Prerequisites

### Minimal Setup (always works)
- Just the Sinex database setup

### Full Setup (for complete testing)
1. **Hyprland**: Must be running with `$HYPRLAND_INSTANCE_SIGNATURE` set
2. **Kitty**: Must be running with remote control enabled:
   ```bash
   kitty -o allow_remote_control=yes
   ```
3. **Database**: PostgreSQL with migrations applied (automatic in nix shell)

## Test Architecture

The E2E tests:
1. Start actual ingestor processes
2. Generate real system events
3. Wait for event capture and processing
4. Query the database to verify events were stored correctly
5. Clean up processes

### Key Features
- **Adaptive**: Tests work with whatever systems are available
- **Real Events**: Actually interacts with the system (creates files, runs commands)
- **Process Management**: Properly starts and stops ingestor processes
- **Timeout Protection**: Tests have reasonable timeouts to prevent hanging
- **Detailed Logging**: Use `--nocapture` to see what's happening

## Troubleshooting

### Tests Failing
1. Check if you're in the nix develop shell
2. Verify database is accessible: `echo $DATABASE_URL`
3. For Hyprland tests: ensure `hyprctl version` works
4. For Kitty tests: ensure `kitty @ ls` works
5. Check ingestor logs with `--nocapture` flag

### No Events Captured
- Increase wait times in tests (some systems are slower)
- Check if ingestors are crashing (see output with `--nocapture`)
- Verify config files are being created correctly
- Check database permissions

### Performance
- E2E tests are slower than unit tests (they do real work)
- Full system test may take 10-30 seconds
- Adaptive tests are faster (5-15 seconds)
- Minimal test is quickest (2-5 seconds)