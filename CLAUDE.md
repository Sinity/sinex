# CLAUDE.md

This file provides comprehensive guidance for all agents working with the Sinex project, including workflows, role assignments, and operational procedures.

## 🎯 Project Purpose & Architecture

Sinex is an event-driven data capture system that records everything happening on a computer for later analysis.

**Core Flow**: EventSources → UnifiedCollector → Event Substrate → Workers → Query Interface

- **EventSources**: Individual event capturing components (filesystem, terminals, window managers)
- **UnifiedCollector**: Central coordinator that manages all event sources
- **Event Substrate**: PostgreSQL + TimescaleDB with ULID keys, stores immutable events
- **Workers**: Process events concurrently using `SELECT FOR UPDATE SKIP LOCKED`
- **Query Interface**: Python CLI for exploring captured events

## 🏗️ Key Patterns & Conventions

### EventSource Pattern

All event sources implement this trait for the unified collector:

```rust
#[async_trait]
impl EventSource for MyEventSource {
    type Config = MyConfig;
    const SOURCE_NAME: &'static str = "my_source";
    
    async fn initialize(ctx: EventSourceContext) -> Result<Self> {
        // Initialize with config from context
    }
    
    async fn stream_events(&mut self, tx: mpsc::Sender<RawEvent>) -> Result<()> {
        // Stream events continuously until shutdown
    }
}
```

### Database Patterns

- All primary keys use ULID (time-ordered, distributed-safe)
- Events are immutable once written to `raw.events`
- Schema validation via pg_jsonschema
- Concurrent work distribution via `FOR UPDATE SKIP LOCKED`

### Code Organization

- Consolidate related code - avoid excessive file atomization
- Tests go in categorized subdirectories under `test/`
- Put my working docs in `spec/docs/claude/`
- Clean up obsolete code/files proactively
- Avoid proliferating around arbitrary ad-hoc scripts, documentation and other such files. There's designated space for such documentation needs you might have - spec/docs/claude

## 🚨 Critical Agent Responsibilities

### Task Completion Protocol
1. **Code changes are not complete until committed to git**
2. **Always examine actual source code**, not just documentation which may be outdated
3. **Clean up as you go** - remove obsolete files, update related documentation
4. **Update SQLX cache** when adding new database queries (`just sqlx-prepare`)
5. **Run tests** before committing (`just test`)
6. **Update this CLAUDE.md** when adding new patterns or workflows

### Quality Gates
- [ ] Code compiles: `cargo check --workspace`
- [ ] Tests pass: `just test`
- [ ] SQLX cache updated: `just sqlx-check`
- [ ] Nix build works: `nix build`
- [ ] Changes committed: `git status` shows clean
- [ ] Documentation updated if patterns changed

## 📁 Project Map

```
sinex/
├── crate/                    # Core Rust libraries
│   ├── sinex-core/           # EventSource trait, registry, common types
│   ├── sinex-db/             # Database models and pooling
│   ├── sinex-ulid/           # ULID ↔ UUID conversion
│   ├── sinex-collector/      # UnifiedCollector binary
│   ├── sinex-events/         # All event source implementations
│   ├── sinex-worker/         # Worker implementations
│   ├── sinex-promo-worker/   # Promotion queue worker
│   └── sinex-annex/          # Git Annex integration
├── config/                   # Example configurations
│   ├── unified-collector/    # Collector config examples
│   └── clipboard-with-annex.toml
├── test/                    # Hierarchically organized test suites
│   ├── unit/                # Unit tests (component isolation)
│   │   ├── core/            # Core library tests
│   │   └── db/              # Database model tests
│   ├── integration/         # Integration tests (component interaction)
│   │   ├── database/        # Database integration tests
│   │   ├── collector/       # Collector integration tests
│   │   ├── worker/          # Worker integration tests
│   │   └── event_sources/   # Event source integration tests
│   ├── system/              # System-level tests (full system validation)
│   │   ├── end_to_end/      # Complete pipeline tests
│   │   ├── external/        # External service integration
│   │   ├── performance/     # Performance and benchmarking
│   │   └── regression/      # Regression tests for specific bugs
│   ├── nixos-vm/            # NixOS VM integration tests
│   ├── cli/                 # Python CLI tests
│   ├── agent/               # Agent lifecycle tests
│   ├── common/              # Shared test utilities and helpers
│   ├── model/               # Data model tests
│   ├── ulid/                # ULID-specific tests
│   ├── ingestor/            # Event ingestor tests  
│   ├── validation/          # Event validation tests
│   └── adversarial/         # Stress and security tests
├── migrations/              # SQL schema migrations (sqlx)
├── script/                  # Utility scripts
│   └── init_git_annex.sh    # Git annex repository setup
├── spec/                    # Documentation
│   ├── SADI.md             # Start here - doc index
│   ├── STAD.md             # Architecture document
│   ├── VISION.md           # Project vision
│   ├── combo/              # Combined docs for easy reading
│   ├── diagram/            # Architecture diagrams
│   │   └── render.sh       # Diagram rendering script
│   └── docs/               # Detailed documentation
│       ├── adr/            # Architecture decision records
│       ├── arch_modules/   # Architecture module docs
│       ├── claude/         # My working area
│       ├── security/       # Security documentation
│       └── tims/           # Implementation specs
└── cli/                     # Python query tools
    └── exo.py              # Main CLI interface
```

## 🛠️ Common Tasks

### Development Setup

```bash
nix develop                      # Always run first - enters dev shell, database setup is automatic
cargo check --workspace         # Verify build
just                            # See available commands
```

### Database Management

The database (`sinex_dev`) is automatically created and migrations applied when entering the nix shell. No manual setup needed!

```bash
just psql                       # Direct database connection
just migrate                    # Apply migrations manually if needed
just migrate-create feature_name # Create new migration

# If you need to reset the database:
dropdb sinex_dev && createdb sinex_dev && just migrate
```

### Git Annex Setup (for blob storage)

```bash
./script/init_git_annex.sh      # Initialize git-annex repository
# Follow the script output to set SINEX_ANNEX_PATH
```

### PostgreSQL Extension Setup

The project requires `pg_jsonschema` extension for JSON Schema validation. Since we use the global PostgreSQL system, install it via:

**Option 1: NixOS System Configuration**

```nix
services.postgresql = {
  enable = true;
  package = pkgs.postgresql_16;
  extraPlugins = with pkgs.postgresql16Packages; [
    # ... other extensions
    # Add pg_jsonschema when available in nixpkgs
  ];
};
```

**Option 2: Manual Installation**

```bash
# Download and install from releases
# https://github.com/supabase/pg_jsonschema/releases
# Follow installation instructions for your PostgreSQL version
```

### Running the Collector

```bash
# Run the unified collector (config logged at startup)
cargo run --bin sinex-collector                    # Run with default config
cargo run --bin sinex-collector -- --dry-run       # Test mode without database
cargo run --bin sinex-collector -- --event-log events.json  # Log to file
cargo run --bin sinex-collector -- --config my-config.toml  # Custom config
cargo run --bin sinex-collector -- --no-db         # Skip database entirely

# Just commands for convenience
just unified                   # Run unified collector (via nix)
just worker                    # Run promotion worker (via nix)
just ingestors-start           # Start both in background
just ingestors-stop            # Stop all running
```

Config loading priority:

1. `--config` command line argument
2. `SINEX_CONFIG` environment variable
3. `unified-collector.toml` in current directory
4. `~/.config/sinex/collector.toml`
5. Built-in defaults (uses DATABASE_URL automatically)

Example configs available in `config/`:

- `unified-collector/minimal.toml` - Basic filesystem monitoring
- `unified-collector/development.toml` - Common dev sources
- `unified-collector/with-annex.toml` - With git-annex blob storage
- `clipboard-with-annex.toml` - Clipboard capture example

### Database Work

```bash
just migrate                    # Apply migrations
just migrate-create feature_name # New migration
just psql                      # Direct connection

# SQLX cache management
just sqlx-prepare              # Update SQLX cache
just sqlx-check               # Check if cache is up to date
```

### Testing

```bash
just test                       # All tests
just test-unit                  # Unit tests (component isolation)
just test-integration           # Integration tests (component interaction)
just test-system                # System tests (full pipeline validation)
just test-database              # Database-specific tests
just test-collector             # Collector tests
just test-worker                # Worker tests
just test-event-sources         # Event source tests
just test-all                   # Comprehensive test suite
just watch                      # Continuous testing

# Coverage reporting
just coverage                   # Run tests with coverage
just coverage-html              # Generate HTML coverage report
just coverage-lcov              # Generate LCOV format for CI
just coverage-report            # Open coverage report in browser

# Test specific areas
cargo test --test integration   # All integration tests
cargo test --test unit          # All unit tests
cargo test --test system        # All system tests
```

### Query Interface (exo.py)

```bash
# Basic queries
just query                      # View recent 10 events
just query 50                  # View recent 50 events
./cli/exo.py query --source filesystem --after "1 hour ago"

# Schema management
./cli/exo.py schema list        # List registered schemas
./cli/exo.py schema get <id>    # View specific schema

# Agent monitoring
./cli/exo.py agent list         # List all agents
./cli/exo.py agent status <name> # Check agent status

# Event sources
./cli/exo.py sources            # List available event sources

# Blob management (requires git-annex)
./cli/exo.py blob list          # List stored blobs
./cli/exo.py blob get <key>     # Retrieve blob content
```

### Debugging

```bash
cargo test -- --nocapture      # See test output
RUST_LOG=debug cargo run       # Debug logging
```

## 🗄️ Database Schema

**Core Tables**:

- `raw.events` - Immutable event storage (hypertable)
- `sinex_schemas.event_payload_schemas` - JSON schemas
- `sinex_schemas.agent_manifests` - Registered ingestors
- `sinex_schemas.promotion_queue` - Event processing queue

**Key Types**:

- `RawEvent` - Universal event structure
- `EventSource` - Trait for event capturing components
- `UnifiedCollector` - Central coordinator managing all sources
- `EventRegistry` - Registry of all known event types and their sources

## ⚡ Quick References

### Path Dependencies

```toml
sinex-db = { path = "../../crate/sinex-db" }    # Not src/!
```

### Local PostgreSQL

```
postgresql:///sinex_dev?host=/run/postgresql
```

### Event Types

- `sources::FILESYSTEM`, `sources::TERMINAL_KITTY`, `sources::HYPRLAND`
- Event types defined in `crate/sinex-events/`

### Key Crates

- `sinex-core` - Common types, EventSource trait, registry
- `sinex-db` - Database layer and models
- `sinex-collector` - UnifiedCollector binary and coordination
- `sinex-events` - All specific event source implementations
- `sinex-worker` - Event processing workers
- `sinex-promo-worker` - Promotion queue worker
- `sinex-annex` - Git Annex integration for large files

## 📚 Where to Look

- **Architecture Overview**: `spec/STAD.md`
- **Getting Started**: `spec/SADI.md`
- **Project Vision**: `spec/VISION.md`
- **Implementation Details**: `spec/docs/tims/`
- **Design Decisions**: `spec/docs/adr/`
- **My Working Notes**: `spec/docs/claude/`
- **Diagrams**: `spec/diagram/` (run `./render.sh` to regenerate)

## 🚦 Environment Checks

- Always in nix shell? (`nix develop`)
- Database running? (automatic in nix shell)
- Migrations applied? (automatic in nix shell)
- SQLX cache current? (`just sqlx-check`)

## 💡 Principles

- Events are immutable facts
- Ingestors just capture, workers process
- Use existing patterns before creating new ones
- Clean up as you go - don't let cruft accumulate
- Check the TIMs before implementing features

## 🔧 Technical Learnings

### SQLX Offline Mode

- SQLX requires `.sqlx/` cache directory for offline builds
- Update cache with: `cargo sqlx prepare --workspace -- --all-targets --all-features`
- Some crates may need individual `cargo sqlx prepare` + merge to workspace
- Cache must be updated when adding new `sqlx::query!` macros
- Missing cache shows as: "SQLX_OFFLINE=true but there is no cached data"

### Nix Build Requirements

- **Critical**: Nix only sees git-tracked files - commit `.sqlx/` and hidden directories
- Untracked/unstaged files are invisible to Nix builds
- "Git tree is dirty" warnings indicate uncommitted changes Nix won't see
- Build failures in Nix that work locally = check git status first

### Debugging Patterns

- Use `just` commands - they have correct flags/environment
- `cargo sqlx prepare` needs `--all-targets --all-features` flags
- Check workspace members individually if commands miss packages
- Recent commits (`git log`) reveal when cache updates are needed

## 🚀 AUTOMATION MASTERY & REUSABLE TOOLS

### Comprehensive Automation Toolkit Location
**`/realm/project/sinex/test/automation/`** - Complete automation infrastructure

### Core Automation Tools & Success Metrics

#### 1. **AST-Grep Pattern Engine** (`test/automation/ast-grep/`)
**Proven patterns for systematic code transformation:**

```yaml
# TempDir Migration (100% success rate)
rules:
  - pattern: TempDir::new().unwrap()
    fix: resources::temp_dir()?

# EventSourceContext Consolidation (91% success rate)  
rules:
  - pattern: EventSourceContext::new($CONFIG)
    fix: event_sources::test_context($CONFIG)

# RawEvent Construction (66% success rate)
rules:
  - pattern: RawEvent { $$$fields }
    fix: events::generic_adversarial_event($source, $event_type, $payload, $version)
```

#### 2. **Python Automation Scripts** (`test/automation/python-scripts/`)
**Intelligent code modification with AST-awareness:**

- **`ok-return-fixer.py`**: Adds missing Ok(()) returns using brace counting
- **`bulk-import-consolidator.py`**: Converts imports to prelude (72-87% reduction)
- **`rawevent_aggressive_automation.py`**: Ultra-aggressive RawEvent pattern replacement (66% success)

#### 3. **Test Infrastructure Revolution** (`test/common/`)
**Comprehensive helper ecosystem:**

```rust
// Single import covers 90% of test needs
use crate::common::prelude::*;

// Database test macros eliminate 5-10 lines each
test_with_pool!(my_test, pool, { /* test logic */ });
integration_test!(my_test, pool, { /* with migrations */ });

// Rich event builder ecosystem (12+ specialized builders)
let events = events::test_event_batch("source", "type", 100);
let chaos_event = events::agent_heartbeat_chaos_event("agent", Some("v1.0"));
```

### Automation Success Metrics Achieved

| **Category** | **Before** | **After** | **Reduction** | **Tools Used** |
|--------------|------------|-----------|---------------|----------------|
| **Compilation Errors** | 60+ | 0 | 100% | ast-grep + Python |
| **TempDir::new().unwrap()** | 26+ | 0 | 100% | ast-grep automation |
| **EventSourceContext::new()** | 45 | 4* | 91% | ast-grep + imports |
| **Manual RawEvent constructions** | 72 | 24 | **66.7%** | Python + builders |
| **Import statements (complex files)** | 14-18 | 2-5 | 72-87% | Prelude consolidation |

*_4 remaining are legitimate unit tests for context functionality_

### Reusable Automation Patterns

#### **Pattern 1: AST-Grep Systematic Replacement**
```bash
# Template for any pattern-based transformation
ast-grep run -c automation/ast-grep/pattern-config.yml test/ -U

# Example usage:
cd test/automation/
./run-all-transformations.sh  # Runs complete automation pipeline
```

#### **Pattern 2: Python Intelligent Analysis**
```python
# Template for complex structural changes
def analyze_pattern(content):
    # Pattern detection logic
    return transformation_strategy

def apply_transformation(filepath):
    # Intelligent modification with verification
    with open(filepath, 'r') as f:
        content = f.read()
    
    # Apply changes with context awareness
    modified_content = transform_content(content)
    
    # Verify and write back
    if verify_syntax(modified_content):
        with open(filepath, 'w') as f:
            f.write(modified_content)
```

### Future Automation Opportunities

#### **Immediate Targets (High ROI)**
1. **Remaining RawEvent instances** (24 → <10): Target specialized patterns
2. **Assertion consolidation** (25+ files): `assert_event_count_range()` helpers
3. **SQL query helpers** (20+ files): Common database operation patterns

### Reusable Automation Commands

```bash
# Complete automation pipeline
cd test/automation && ./run-all-transformations.sh

# Individual tools
python3 python-scripts/ok-return-fixer.py test/
python3 python-scripts/bulk-import-consolidator.py test/
python3 python-scripts/rawevent_aggressive_automation.py

# AST-grep individual patterns
ast-grep run -c ast-grep/tempdir-migration.yml test/ -U
ast-grep run -c ast-grep/eventsource-context.yml test/ -U
```

### Knowledge Management for Future Claude Agents

**Location**: All automation tools and documentation are preserved in:
- **`/realm/project/sinex/test/automation/`** - Complete toolkit
- **`/realm/project/sinex/CLAUDE.md`** - This knowledge base
- **Individual tool documentation** - Each script has embedded usage examples

**Usage**: Future agents can leverage this infrastructure by:
1. Reading the automation toolkit documentation
2. Applying proven patterns to new scenarios
3. Extending existing tools for new patterns
4. Contributing new automation discoveries back to the toolkit

This represents a **reusable automation mastery** that can be applied to any similar codebase transformation project.

## 🎭 Agent Role Assignments

### Primary Development Agent
**Role**: Lead implementation of new features and architectural changes
**Responsibilities**:
- Implement core EventSource patterns and major features
- Create new database migrations and schema changes
- Update NixOS modules and deployment configuration
- Write comprehensive tests for new functionality
- Update architecture documentation (STAD.md, TIMs)

**Branch Pattern**: `claude/YYYY-MM-DD-feature-name`
**Commit Pattern**: `feat: implement X` or `refactor: improve Y`

### Bug Fix Agent
**Role**: Address bugs, regressions, and system reliability issues
**Responsibilities**:
- Fix failing tests and compilation errors
- Address performance issues and memory leaks
- Handle deployment and operational issues
- Create regression tests for fixed bugs
- Update troubleshooting documentation

**Branch Pattern**: `claude/YYYY-MM-DD-fix-issue-name`
**Commit Pattern**: `fix: resolve X` or `perf: optimize Y`

### Documentation Agent
**Role**: Maintain project documentation and knowledge management
**Responsibilities**:
- Update spec/ documentation when code changes
- Maintain CLAUDE.md with current workflows
- Create architectural diagrams and decision records
- Update example configurations and usage guides
- Keep project map current with actual structure

**Branch Pattern**: `claude/YYYY-MM-DD-docs-topic`
**Commit Pattern**: `docs: update X documentation`

### Testing Agent
**Role**: Expand test coverage and validation scenarios
**Responsibilities**:
- Write unit, integration, and system tests
- Create adversarial and edge case tests
- Implement test automation and CI improvements
- Add performance benchmarks and stress tests
- Validate NixOS VM test scenarios

**Branch Pattern**: `claude/YYYY-MM-DD-test-area`
**Commit Pattern**: `test: add X validation` or `ci: improve Y automation`

### Operations Agent
**Role**: Deployment, monitoring, and system reliability
**Responsibilities**:
- Improve NixOS deployment modules
- Implement health monitoring and alerting
- Create backup and recovery procedures
- Optimize database performance and maintenance
- Handle version management and rollback procedures

**Branch Pattern**: `claude/YYYY-MM-DD-ops-improvement`
**Commit Pattern**: `ops: improve X deployment` or `monitoring: add Y metrics`

## 📋 Standardized Workflows

### Git Workflow Protocol

#### For All Agents:
1. **Start with clean state**: `git checkout master && git pull`
2. **Create descriptive branch**: `git checkout -b claude/2024-06-17-role-description`
3. **Make focused commits**: Each commit addresses one logical change
4. **Test before committing**: Run quality gates (see Critical Responsibilities)
5. **Push and create PR**: `git push origin branch-name`
6. **Squash merge to master**: Keep history clean
7. **Delete branch**: `git branch -d branch-name`

#### Commit Message Standards:
```
type(scope): description

Longer explanation if needed, including:
- Why this change was necessary
- What alternative approaches were considered
- Any breaking changes or migration steps

Fixes #123
Closes #456

🤖 Generated with Claude Code
Co-Authored-By: Claude <noreply@anthropic.com>
```

**Types**: `feat`, `fix`, `docs`, `test`, `refactor`, `ops`, `perf`, `ci`
**Scopes**: `core`, `db`, `collector`, `worker`, `events`, `nixos`, `cli`

### Database Change Workflow

#### Adding New Tables/Columns:
1. **Create migration**: `just migrate-create descriptive_name`
2. **Write idempotent SQL**: Use `IF NOT EXISTS`, `IF EXISTS` appropriately
3. **Update Rust models**: Add/modify structs in `sinex-db`
4. **Add queries**: Create new `sqlx::query!` macros as needed
5. **Update SQLX cache**: `just sqlx-prepare`
6. **Test migration**: `dropdb sinex_dev && createdb sinex_dev && just migrate`
7. **Write tests**: Validate new schema and queries work
8. **Commit everything**: Including `.sqlx/` cache files

#### Migration Safety Rules:
- Never delete columns or tables in production migrations
- Use separate migrations for schema and data changes
- Test migrations on realistic data volumes
- Always have rollback procedure documented

### Testing Workflow

#### Test Categories & When to Use:
- **Unit Tests** (`test/unit/`): Test individual functions and components
- **Integration Tests** (`test/integration/`): Test component interactions
- **System Tests** (`test/system/`): Test complete workflows end-to-end
- **Adversarial Tests** (`test/adversarial/`): Edge cases, attacks, stress scenarios

#### Testing Protocol:
1. **Write tests first** for new features (TDD when practical)
2. **Run affected tests**: `cargo test <module>` for quick feedback
3. **Run full test suite**: `just test-all` before committing
4. **Add regression tests**: For every bug fixed
5. **Update test documentation**: When adding new test patterns

#### Test Quality Standards:
- Tests must be deterministic (no flaky tests)
- Tests must be isolated (no shared state)
- Tests must be fast (unit tests <1s, integration <10s)
- Tests must have clear failure messages
- Tests must clean up after themselves

### Code Review Workflow

#### Self-Review Checklist:
- [ ] Code follows established patterns in codebase
- [ ] Error handling is comprehensive and appropriate
- [ ] Logging provides useful debugging information
- [ ] Performance impact considered and measured
- [ ] Security implications reviewed
- [ ] Documentation updated for public interfaces
- [ ] Migration path provided for breaking changes

#### Architecture Change Protocol:
1. **Create ADR**: Document decision in `spec/docs/adr/`
2. **Update STAD.md**: Reflect architectural changes
3. **Create implementation plan**: Break into phases
4. **Get consensus**: Discuss with other agents via issues
5. **Implement incrementally**: Small, reviewable changes
6. **Update diagrams**: Keep `spec/diagram/` current

### Deployment Workflow

#### NixOS Module Development:
1. **Test in development**: Use NixOS VM tests first
2. **Validate configuration**: `nix flake check`
3. **Test actual deployment**: On development system
4. **Update documentation**: Module options and examples
5. **Create migration guide**: For existing deployments

#### Health Monitoring Protocol:
1. **Every service** must emit heartbeat events every 30-60 seconds
2. **Include rich context**: Memory usage, event counts, errors
3. **Use structured logging**: JSON format for parsing
4. **Implement graceful shutdown**: Handle SIGTERM properly
5. **Provide health endpoints**: HTTP endpoint for external monitoring

### Emergency Response Workflow

#### Production Issues:
1. **Assess impact**: Is data being lost? Are events being missed?
2. **Immediate mitigation**: Stop ingestion if corruption risk
3. **Create hotfix branch**: `claude/YYYY-MM-DD-hotfix-issue`
4. **Implement minimal fix**: Address symptoms first
5. **Test thoroughly**: Can't make production worse
6. **Deploy carefully**: Have rollback plan ready
7. **Root cause analysis**: Create issue for proper fix

#### System Recovery:
1. **Database corruption**: Restore from backup, replay events
2. **Service failures**: Check logs, restart with health monitoring
3. **Configuration errors**: Rollback to last known good
4. **Performance issues**: Check resource usage, scale appropriately

## 🔧 Technical Standards

### Code Style Guidelines
- **Rust**: Follow `rustfmt` and `clippy` recommendations
- **SQL**: Use lowercase with snake_case, descriptive names
- **Nix**: Follow nixpkgs conventions, use lib functions
- **Python**: Follow PEP 8, use type hints
- **Bash**: Use `set -euo pipefail`, quote variables

### Performance Requirements
- **Event ingestion**: >1000 events/second single-threaded
- **Database queries**: <100ms for typical operations
- **Memory usage**: <512MB per service in steady state
- **Startup time**: <30 seconds for all services
- **Health checks**: <5 seconds response time

### Security Requirements
- **No secrets in logs**: Redact sensitive information
- **Database connections**: Use connection pooling with limits
- **File permissions**: Restrict access to service user only
- **Input validation**: Validate all external inputs
- **Error handling**: Don't leak internal information

### Monitoring Standards
- **Structured logging**: Use JSON format with consistent fields
- **Metrics emission**: Prometheus-compatible metrics
- **Error tracking**: Include stack traces and context
- **Performance monitoring**: Track latency and throughput
- **Health indicators**: CPU, memory, disk, network usage

## 📊 Quality Metrics

### Development Velocity
- **Time to first commit**: <30 minutes from task assignment
- **Test coverage**: >80% for core functionality
- **Build time**: <5 minutes for full workspace
- **Deployment time**: <10 minutes including health checks

### System Reliability
- **Uptime target**: 99.9% for data collection
- **Data loss tolerance**: Zero tolerance for event loss
- **Recovery time**: <15 minutes for service restart
- **Rollback time**: <5 minutes for configuration issues

### Agent Coordination
- **Conflict resolution**: <24 hours for merge conflicts
- **Communication**: Use GitHub issues for coordination
- **Knowledge sharing**: Update CLAUDE.md with new patterns
- **Documentation lag**: <7 days between code and doc updates

## 🎯 Success Criteria

For any agent completing work on Sinex:

### Functional Success
- [ ] All intended functionality works as specified
- [ ] No regressions in existing functionality
- [ ] Performance meets or exceeds requirements
- [ ] Error handling is comprehensive and appropriate

### Technical Success
- [ ] Code follows established patterns and conventions
- [ ] Tests are comprehensive and pass consistently
- [ ] Documentation is accurate and up-to-date
- [ ] SQLX cache is current and builds work offline

### Operational Success
- [ ] Deployment is idempotent and reliable
- [ ] Health monitoring provides useful visibility
- [ ] Rollback procedures work as expected
- [ ] System performance is maintained or improved

### Knowledge Success
- [ ] CLAUDE.md updated with new patterns or changes
- [ ] Other agents can build upon the work without confusion
- [ ] Architectural decisions are documented with rationale
- [ ] Future maintenance is clearly supported

## 🔄 Continuous Improvement

### Regular Reviews
- **Weekly**: Review agent effectiveness and coordination
- **Monthly**: Update workflows based on lessons learned
- **Quarterly**: Assess architecture and performance trends
- **Annually**: Major technology and pattern updates

### Feedback Integration
- **Error patterns**: Turn repeated issues into automated checks
- **Performance bottlenecks**: Proactively address before they impact users
- **Documentation gaps**: Fill based on common questions
- **Tool improvements**: Automate manual processes where possible

### Knowledge Management
- **Capture tribal knowledge**: Document non-obvious patterns
- **Share lessons learned**: Update this file with new insights
- **Cross-train capabilities**: Agents should understand multiple roles
- **Maintain expertise**: Keep up with Rust, Nix, PostgreSQL best practices

---

## 🧪 Testing Principles

1. **Don't test the language/library** - Focus on our business logic
2. **Don't test assignment** - Test behavior, not simple data movement
3. **Test behavior, not implementation** - Validate business rules and system behavior
4. **Focus on edge cases** - Boundary conditions and error scenarios provide most value
5. **Test integration points** - Validate how components work together
6. **Make tests maintainable** - Clear, focused tests that evolve with the code
