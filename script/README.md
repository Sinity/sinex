# Scripts Directory - Migration to Flake Apps

Most functionality has been migrated to Nix flake apps for better integration and reliability.

## ✅ **Replaced by Flake Apps:**

| Old Script | New Flake App | Description |
|------------|---------------|-------------|
| `setup_database.sh` | `nix run .#db-setup` | Database setup and management |
| `run_tests.sh` | `nix run .#test` | Test runner |
| `db_reset.sh` | `nix run .#db-setup reset` | Database reset |
| `setup_test_db.sh` | `nix run .#db-setup test` | Test database setup |

## 🆕 **New Flake Apps:**

| App | Command | Description |
|-----|---------|-------------|
| **dev** | `nix run .#dev` | Full development environment with mprocs |
| **monitor** | `nix run .#monitor` | Real-time TUI monitoring dashboard |
| **build** | `nix run .#build` | Build all workspace members |
| **check** | `nix run .#check` | Check and lint code |

## 📊 **Monitor App Features:**

- **Interactive Dashboard**: Real-time event counts, source breakdown
- **Live Event Stream**: Tail events as they arrive
- **Process Status**: Check running ingestors and database
- **System Stats**: Disk, memory, database size

```bash
# Launch interactive dashboard
nix run .#monitor

# Direct modes
nix run .#monitor events  # Show recent events
nix run .#monitor live    # Live event tail
```

## 🚀 **Dev App Features:**

- **Process Management**: Uses mprocs to manage all services
- **Hot Keys**: Start/stop individual ingestors
- **Environment Detection**: Auto-starts appropriate services
- **Multiple Modes**: Interactive, background, or db-only

```bash
# Interactive development environment  
nix run .#dev

# Just database
nix run .#dev db-only

# Background services
nix run .#dev background
```

## ⚠️ **Remaining Scripts:**

The following scripts may still be useful but could be candidates for future migration:

- `chaos_test.sh` - Stress testing
- `diagnose_assumptions.sh` - Diagnostics
- `live_monitor.sh` - Basic monitoring (superseded by monitor app)
- `real_world_test.sh` - Real-world testing
- Various development helpers

## 🎯 **Benefits of Flake Apps:**

1. **Self-contained**: No shell script dependencies
2. **Nix Integration**: Proper dependency management
3. **Cross-platform**: Work consistently across systems
4. **Discoverable**: `nix flake show` lists all apps
5. **Composable**: Can be combined and extended
6. **Type-safe**: Better error handling and validation

## 📋 **Quick Reference:**

```bash
# Essential workflow
nix run .#db-setup dev     # Setup database
nix run .#dev              # Start development environment
nix run .#monitor          # Monitor in another terminal
nix run .#test             # Run tests
nix run .#build            # Build everything
```