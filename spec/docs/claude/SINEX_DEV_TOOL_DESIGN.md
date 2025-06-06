# Sinex Development Tool Design Document

## Overview

The Sinex development tool (`sinex`) is a unified CLI/TUI application that eliminates environment management friction during development and operation of the Sinex system. It provides zero-ambiguity context awareness, intelligent defaults, and progressive disclosure of functionality.

## Core Problem Statement

Environment management is cognitive poison. Every time a developer must think about:
- Which database am I using?
- Is PostgreSQL running?
- What port is it on?
- Are there duplicate ingestors?
- Which environment variables do I need?

They lose flow state and productivity. This tool makes these questions **impossible to ask** by handling all environment concerns automatically.

## Design Principles

### 1. Zero Ambiguity
Every command provides clear feedback about what environment it's operating in:
```
sinex status
> 🟢 Dev Mode | DB: sinex_dev@localhost:5432 | Ingestors: 2/3 running
```

### 2. Context-Aware Intelligence
The tool automatically detects its operating context and adapts behavior:
- In project directory → Development mode
- System-wide → Production mode
- Anywhere else → Helpful guidance

### 3. Fail-Safe Defaults
When in doubt, the tool chooses the safest option and explains its decision.

### 4. Progressive Disclosure
Simple tasks are simple, complex tasks are possible:
```
sinex              # Dashboard (most users stop here)
sinex start        # Start everything
sinex db connect   # Direct database access (advanced)
```

## Architecture

### Context Detection

```rust
pub enum SinexContext {
    Development {
        project_root: PathBuf,
        cargo_target: PathBuf,
        database: DatabaseConfig,
    },
    System {
        database: DatabaseConfig,
        services: Vec<SystemdService>,
    },
    Hybrid {
        database: DatabaseConfig,      // System PostgreSQL
        project_root: PathBuf,         // Local ingestors
    }
}

impl SinexContext {
    pub fn detect() -> Result<Self> {
        // 1. Check if in Sinex project (Cargo.toml with workspace.package.name = "sinex")
        // 2. Check for .sinex.lock file indicating active dev session
        // 3. Check systemd services
        // 4. Determine appropriate mode
    }
}
```

### Database Strategy

Use system PostgreSQL with multiple databases (conventional approach):

```
postgresql://localhost:5432/
├── sinex_prod         # Production data (NixOS module)
├── sinex_dev          # Development data (local development)
├── sinex_test         # Test data (stable test database)
└── sinex_ephemeral_*  # Temporary test databases
```

### Process Management

```rust
pub struct ProcessManager {
    lock_file: PathBuf,  // ~/.local/share/sinex/locks/
    processes: HashMap<String, ProcessInfo>,
}

pub struct ProcessInfo {
    pid: u32,
    name: String,
    mode: ProcessMode,
    started_at: SystemTime,
    health: HealthStatus,
}

impl ProcessManager {
    pub fn ensure_singleton(&self, name: &str) -> Result<()> {
        // Prevent duplicate ingestors across dev/system
    }
}
```

## Command Structure

### Core Commands

#### `sinex` (no args)
Opens interactive TUI dashboard showing:
- Current mode (Dev/System/Hybrid)
- Database status and connection info
- Running ingestors with event rates
- Recent events stream
- System health metrics

#### `sinex start [component]`
Intelligent startup with conflict detection:
```bash
$ sinex start
> 🚀 Starting development environment...
> ✓ Database: sinex_dev@localhost:5432 (already running)
> ✓ Starting filesystem-ingestor (dev build)
> ✓ Starting kitty-ingestor (dev build)
> ⚠️  Skipping hyprland-ingestor (not in Hyprland session)

$ sinex start filesystem  # Start specific component
> ⚠️  filesystem-ingestor already running (PID: 12345)
> [r] Restart  [a] Attach logs  [k] Kill  [c] Cancel
```

#### `sinex stop [component]`
Graceful shutdown with confirmation for data safety.

#### `sinex status`
Quick one-line status suitable for prompts/scripts:
```bash
$ sinex status
Dev | DB: ✓ | Ingestors: 2/3 | Events/min: 847
```

#### `sinex logs [component] [--follow]`
Unified log viewing across all components with filtering.

### Database Commands

#### `sinex db setup`
Ensures database exists with correct permissions and extensions:
```sql
CREATE DATABASE IF NOT EXISTS sinex_dev;
CREATE EXTENSION IF NOT EXISTS ulid;
CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS vector;
```

#### `sinex db reset`
Safe reset with confirmation and backup option.

#### `sinex db connect`
Opens `psql` with correct connection parameters.

#### `sinex db migrate [status|run|create]`
Wraps `sqlx migrate` with proper DATABASE_URL.

### Advanced Commands

#### `sinex fix`
Nuclear option for recovering from broken states:
```
$ sinex fix
> 🔧 Detecting issues...
> ✗ Stale lock file found (PID 9999 dead)
> ✗ Database sinex_dev missing
> ✗ Migration checksum mismatch
> 
> Fix all issues? [Y/n] y
> ✓ Removed stale locks
> ✓ Created database with extensions
> ✓ Reset migration state
> ✅ Environment restored
```

#### `sinex config`
View/edit configuration with validation:
```
$ sinex config set log.level debug
$ sinex config get database.url
postgresql://localhost:5432/sinex_dev
```

## User Experience Flows

### First-Time Setup
```bash
$ git clone https://github.com/sinex/sinex
$ cd sinex
$ sinex
> 🎉 Welcome to Sinex! First-time setup detected.
> 
> I'll help you set up your development environment:
> 1. ✓ PostgreSQL detected at localhost:5432
> 2. Create development database? [Y/n] y
>    ✓ Created sinex_dev database
> 3. Run migrations? [Y/n] y
>    ✓ Applied 12 migrations
> 
> Ready! Run 'sinex start' to begin.
```

### Daily Development
```bash
$ cd ~/projects/sinex
$ sinex start
# Work happens, no environment thoughts needed
$ sinex stop
```

### Debugging Issues
```bash
$ sinex doctor
> 🏥 Sinex System Diagnostics
> 
> Database:
>   ✓ PostgreSQL 16.0 running
>   ✓ sinex_dev exists
>   ✓ All extensions loaded
>   ✓ 23,847 events in database
> 
> Ingestors:
>   ✓ filesystem-ingestor: healthy (847 events/min)
>   ⚠️ kitty-ingestor: degraded (0 events/min for 5m)
>   ✗ hyprland-ingestor: not running
> 
> Suggestions:
>   • kitty-ingestor may need restart: 'sinex restart kitty'
```

## Implementation Phases

### Phase 1: MVP (Week 1)
- Context detection
- Basic start/stop/status
- Database setup/connect
- Simple process management

### Phase 2: Intelligence (Week 2)
- Lock file management
- Duplicate detection
- Health monitoring
- TUI dashboard

### Phase 3: Polish (Week 3)
- Advanced diagnostics
- Configuration management
- Log aggregation
- Performance metrics

## Technical Decisions

### Language: Rust
- Single binary, fast startup
- Good process management libraries
- Integrates with existing codebase

### TUI Framework: Ratatui
- Modern, well-maintained
- Good async support
- Excellent keybinding handling

### Configuration: TOML + Environment
- TOML for persistent settings
- Environment for overrides
- XDG Base Directory compliance

### IPC: Unix Domain Sockets
- For health checks
- Event rate monitoring
- Graceful shutdown coordination

## Error Handling Philosophy

Every error must:
1. Explain what went wrong in user terms
2. Suggest a concrete fix
3. Provide an escape hatch

Example:
```
Error: Cannot start filesystem-ingestor

The database at localhost:5432 is not accessible.

Possible fixes:
  1. Start PostgreSQL: 'sudo systemctl start postgresql'
  2. Check connection: 'psql -h localhost -p 5432'
  3. Use different database: 'sinex config set database.url <url>'

For more help: 'sinex doctor'
```

## Security Considerations

- Never store credentials in lock files
- Use user's PostgreSQL permissions
- Validate all subprocess arguments
- Secure IPC channels with user-only permissions

## Future Considerations

### Plugin Architecture
Allow custom commands via `sinex-*` binaries in PATH.

### Remote Development
Support for connecting to remote Sinex instances.

### Integration with NixOS Module
Detect when running on NixOS with Sinex module enabled.

## Success Metrics

The tool succeeds when:
1. Developers never think about environment setup
2. "It just works" becomes the normal experience
3. New contributors can start developing in <5 minutes
4. Environment-related issues drop to near zero

## Conclusion

This tool is not just a convenience—it's a fundamental fix to the cognitive overhead that kills developer productivity. By making the right thing the easy thing, we ensure developers spend time on what matters: building Sinex, not fighting their environment.