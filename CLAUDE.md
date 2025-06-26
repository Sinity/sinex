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

### New Abstractions & Patterns

The Sinex codebase has been enhanced with five core abstractions for better error handling, validation, and testing:

#### 1. ValidationChain
Fluent, composable validation with error accumulation:
```rust
let result = ValidationChain::validate(value, "field_name")
    .not_empty()
    .min_length(5)
    .custom(|v| v.contains("test"), "must contain test")
    .into_result();
```

#### 2. ErrorContext
Rich error context with chaining for better debugging:
```rust
let error = CoreError::database("Connection failed")
    .with_context("host", "localhost")
    .with_event_id(event_id)
    .with_operation("database_connect")
    .build();
```

#### 3. ChannelSenderExt/ReceiverExt
Enhanced async channel operations with timeouts and monitoring:
```rust
sender.send_or_log(item, "context").await?;
let batch = receiver.recv_batch(10, Duration::from_millis(100)).await;
```

#### 4. ConfigExtractor
Type-safe configuration access with validation:
```rust
let url = config.require_str("database.url")?;
let pool_size = config.u64_or("database.pool_size", 10);
```

#### 5. Enhanced Test Infrastructure
Rich test assertions and utilities:
```rust
assert_event_inserted_with_context(pool, &event, "test_context").await?;
assert_validation_passes(validation_chain)?;
let mut batch = TestAssertionBatch::new("test_batch");
```

**Usage Guide**: See `spec/docs/claude/abstraction_usage_guide.md` for comprehensive examples.

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

#### Test Infrastructure

The test suite uses a unified test infrastructure with the `#[sinex_test]` macro, `TestContext`, and enhanced abstractions:

```rust
#[sinex_test]
async fn test_example(ctx: TestContext) -> TestResult {
    // Create test data using enhanced builders
    let event = EventBuilder::filesystem().path("/test/file.txt").created().build();
    
    // Insert with enhanced error context
    let event_id = assert_event_inserted_with_context(
        ctx.pool(), 
        &event, 
        "example_test_context"
    ).await?;
    
    // Use ValidationChain for comprehensive validation
    let validation = assert_with_validation(event.source.clone(), "event_source")
        .not_empty()
        .min_length(3);
    assert_validation_passes(validation)?;
    
    // Use TestAssertionBatch for multiple assertions
    let mut batch = TestAssertionBatch::new("example_assertions");
    batch.assert_that(|| {
        assert_eq_with_context(&event.event_type, "file.created", "event type check")
    }, "event type validation");
    batch.execute()?;
    
    Ok(())
}
```

**Key Benefits**:
- Shared database pool (50 connections) instead of per-test pools
- Automatic test isolation via transactions
- Enhanced assertions with rich error context
- ValidationChain integration for fluent validation
- Channel testing utilities for async patterns
- Configuration testing with type-safe extraction
- Faster test startup and execution

#### Running Tests

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

#### Writing Tests

- Use `#[sinex_test]` instead of `#[tokio::test]` for database tests
- Accept `ctx: TestContext` parameter instead of `pool: PgPool`
- Access database via `ctx.pool()` method
- Use test utilities from `crate::common::prelude::*`
- Tests are automatically isolated in transactions

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

### Common Test Patterns

```rust
// Creating test events
let event = RawEventBuilder::new("source", "type", json!({"data": "test"})).build();
let event_id = insert_event(ctx.pool(), &event).await?;

// Using test assertions
assertions::assert_event_inserted(ctx.pool(), &event).await?;
assertions::assert_event_insertion_fails(ctx.pool(), &invalid_event).await?;

// Waiting for async operations
ctx.wait_for_work_queue(0).await?;

// Creating test data batches
let events = generators::test_events(10);
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

## 🚀 SYSTEMATIC CODE TRANSFORMATION METHODOLOGY

### Automation Best Practices

**Critical Principle**: Automation can be powerful but also dangerous. Always prefer incremental, verifiable changes over bulk transformations.

#### When to Automate vs Manual

**Consider Automation When**:
- Pattern occurs 20+ times with minimal variation
- Changes are mechanical and predictable
- Verification can be automated (e.g., compilation checks)
- Risk of human error in repetitive changes is high

**Prefer Manual Approach When**:
- Context varies significantly between instances
- Changes require semantic understanding
- Files contain complex interdependencies
- Pattern occurs <10 times

#### Safe Automation Process

1. **Backup First**: Always commit current state before automation
2. **Test on Subset**: Apply automation to 1-2 files first
3. **Verify Immediately**: Check compilation after each batch
4. **Use Version Control**: Commit working states frequently
5. **Review Changes**: Use `git diff` to verify transformations

### Automation Assessment Framework

When facing repetitive code patterns, use this systematic approach:

#### **Step 1: Pattern Analysis & Impact Assessment**
```bash
# Count instances to measure potential impact
rg -c "pattern" codebase/ --type rust
fd "*.rs" | xargs rg "pattern" | wc -l

# Categorize by concentration
# High-impact: 20+ instances, or 5+ instances in single file
# Medium-impact: 5-20 instances across multiple files  
# Low-impact: <5 scattered instances
```

**Key Insight**: Don't settle for <10% improvements. If automation yields poor results, investigate why - often the pattern analysis was too conservative.

#### **Step 2: Tool Selection Decision Matrix**

| **Pattern Type** | **Recommended Tool** | **When to Use** | **When NOT to Use** |
|------------------|---------------------|-----------------|-------------------|
| **Structural/Syntactic** | ast-grep | Clear AST patterns, systematic replacements | Complex context analysis needed |
| **Logic-based Analysis** | Python scripts | Multi-step analysis, context awareness | Simple string replacements |
| **String/Regex** | sed/awk/rg | Simple substitutions, file renames | Syntax-aware changes |
| **Helper Infrastructure** | Manual design | Before mass conversion | As afterthought |

#### **Step 3: Success Factor Checklist**

**✅ Do This:**
- Build helper infrastructure BEFORE mass conversion
- Start conservative, then iterate more aggressively
- Include verification loops (compilation, tests) in automation
- Account for legitimate manual patterns (database mapping, unit tests)
- Measure and track improvements to maintain momentum

**❌ Avoid This:**
- Automating without counting instances first
- Being too conservative with pattern matching
- Missing import dependencies in transformations
- Automating patterns that should remain manual
- Settling for poor results without investigating root cause

### Common Failure Modes & Solutions

#### **"Minimal Impact" (4% improvement)**
**Root Cause**: Pattern too conservative, missed variations
**Solution**: Analyze why patterns didn't match, create more aggressive matching

#### **"Compilation Breaks"**  
**Root Cause**: Missing imports, context not preserved
**Solution**: Add systematic import management, test incremental changes

#### **"Legitimate Code Broken"**
**Root Cause**: Automated patterns that should stay manual
**Solution**: Add pattern analysis to detect and skip legitimate cases

### ROI Calculation for Automation

**High ROI Indicators:**
- 20+ instances of nearly identical pattern
- Pattern causes frequent developer friction
- Pattern is error-prone (unwrap(), manual construction)
- Clear helper function can eliminate repetition

**Low ROI Indicators:**
- <5 scattered instances
- Pattern requires significant context to understand
- Manual version is clearer than automated version
- One-time conversion with no ongoing benefit

### Common Automation Pitfalls

1. **Over-aggressive Pattern Matching**: Replacing too broadly can corrupt unrelated code
2. **Context Loss**: Automated tools may not preserve necessary context (imports, types)
3. **Cascading Errors**: One bad transformation can break many files
4. **Incomplete Transformations**: Partial migrations leave code in inconsistent state
5. **Tool Limitations**: Regex/sed can't handle nested structures or syntax-aware changes

### Recommended Automation Tools

- **ast-grep**: Syntax-aware transformations for complex patterns
- **sed/awk**: Simple string replacements (use with caution)
- **Custom scripts**: Python/Rust for complex, context-aware transformations
- **IDE refactoring**: Often safer for smaller-scale changes

**Remember**: The goal is working code, not perfect automation. When in doubt, do it manually.

### Automation Iteration Strategy

1. **Conservative Start**: Automate obvious, safe patterns first
2. **Measure Impact**: Count reductions and compilation success
3. **Analyze Gaps**: Why didn't more patterns match?
4. **Aggressive Iteration**: Expand patterns based on gap analysis
5. **Verification**: Ensure no legitimate patterns broken
6. **Helper Infrastructure**: Build ecosystem to support conversions

### Key Process Insights

**Psychological Factors:**
- Poor initial results (4%) indicate methodology problems, not pattern limits
- Question assumptions when automation underperforms
- Breakthrough results often come from aggressive iteration after conservative start

**Technical Factors:**
- Helper infrastructure quality determines conversion success
- Import management is critical for large-scale transformations  
- Verification loops prevent compounding errors
- Pattern analysis beats brute-force automation

**Strategic Factors:**
- High-concentration files yield better ROI than scattered instances
- Building ecosystem first enables mass conversion later
- Systematic approach scales better than ad-hoc fixes

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
